# In UI/visualizer_ui.py
import tkinter as tk
from tkinter import ttk, messagebox, TclError, scrolledtext
from tkcalendar import DateEntry
import os
import importlib
import sys
import pandas as pd
import numpy as np
import threading
import traceback
import pkgutil
import Strategies

from typing import List, Optional

# --- THIS IS THE FIX ---
def get_project_root():
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

project_root = get_project_root()
# --- END OF FIX ---

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Core.data_handler import load_unified_data
from Core.backtester import run_r_backtest
from Core.visualizer import plot_day_summary, open_file
from Core.strategy_base import BaseStrategy
import inspect
from typing import Optional, List

ASSET_CONFIG = {
    'EUR_USD': {'base_tf': '30s'},
    'GBP_USD': {'base_tf': '30s'},
    'XAU_USD': {'base_tf': '30s'},
    'USD_JPY': {'base_tf': '30s'},
    'USD_CAD': {'base_tf': '30s'},
}

def get_available_assets():
    data_path = os.path.join(project_root, 'Data')
    available_assets = []
    if os.path.exists(data_path):
        for folder_name in os.listdir(data_path):
            if folder_name.endswith('_resampled') and os.path.isdir(os.path.join(data_path, folder_name)):
                asset_name = folder_name.replace('_resampled', '')
                if asset_name in ASSET_CONFIG:
                    available_assets.append(asset_name)
    return sorted(available_assets)

class VisualizerUI(tk.Toplevel):
    """
    The UI window for the Strategy Visualizer. This tool runs a single-day
    backtest for a selected strategy and generates an interactive Plotly chart
    to visualize the price action and trades.
    """
    def __init__(self, master: Optional[tk.Tk] = None):
        """Initializes the VisualizerUI window and its components."""
        super().__init__(master)
        self.master_app = master
        self.title("Strategy Visualizer")
        self.geometry("500x550")
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.backtest_thread = None

        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)

        ttk.Label(main_frame, text="Asset to Visualize:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, padx=10, pady=5, sticky='w')
        self.asset_var = tk.StringVar()
        self.asset_dropdown = ttk.Combobox(main_frame, textvariable=self.asset_var, state="readonly", postcommand=self.populate_assets)
        self.asset_dropdown.grid(row=0, column=1, padx=10, pady=5, sticky='ew')
        
        ttk.Label(main_frame, text="Strategy to Use:", font=('Helvetica', 10, 'bold')).grid(row=1, column=0, padx=10, pady=5, sticky='w')
        self.strategy_var = tk.StringVar()
        self.strategy_dropdown = ttk.Combobox(main_frame, textvariable=self.strategy_var, state="readonly", postcommand=self.populate_strategies)
        self.strategy_dropdown.grid(row=1, column=1, padx=10, pady=5, sticky='ew')

        ttk.Label(main_frame, text="Date to Visualize (D-M-Y):", font=('Helvetica', 10, 'bold')).grid(row=2, column=0, padx=10, pady=5, sticky='w')
        self.date_entry = DateEntry(main_frame, date_pattern='dd-mm-yyyy')
        self.date_entry.grid(row=2, column=1, padx=10, pady=5, sticky='w')

        ttk.Label(main_frame, text="RR Target:", font=('Helvetica', 10, 'bold')).grid(row=3, column=0, padx=10, pady=5, sticky='w')
        self.rr_entry = ttk.Entry(main_frame, width=10)
        self.rr_entry.insert(0, "3.0")
        self.rr_entry.grid(row=3, column=1, padx=10, pady=5, sticky='w')

        self.be_var = tk.BooleanVar(value=True)
        self.be_check = ttk.Checkbutton(main_frame, text="Use Break-Even", variable=self.be_var)
        self.be_check.grid(row=4, column=1, padx=10, pady=5, sticky='w')
        
        self.verbose_var = tk.BooleanVar(value=False)
        self.verbose_check = ttk.Checkbutton(main_frame, text="Enable Strategy Debug Log (If Supported)", variable=self.verbose_var)
        self.verbose_check.grid(row=5, column=0, columnspan=2, padx=10, pady=5, sticky='w')
        
        self.run_button = ttk.Button(main_frame, text="Generate Chart", command=self.start_backtest_thread)
        self.run_button.grid(row=6, column=0, columnspan=2, pady=10)
        
        log_frame = ttk.LabelFrame(main_frame, text="Status Log")
        log_frame.grid(row=7, column=0, columnspan=2, sticky="ew", padx=10)
        self.log_widget = scrolledtext.ScrolledText(log_frame, state='disabled', height=8, wrap=tk.WORD)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.back_button = ttk.Button(main_frame, text="< Back to Master", command=self.on_closing)
        self.back_button.grid(row=8, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_columnconfigure(1, weight=1)
        self.populate_assets()
        self.populate_strategies()

    def update_log(self, message):
        def task():
            try:
                self.log_widget.config(state='normal')
                self.log_widget.insert(tk.END, message.strip() + "\n")
                self.log_widget.config(state='disabled')
                self.log_widget.see(tk.END)
            except TclError: pass
        self.after(0, task)

    def on_closing(self):
        if self.backtest_thread and self.backtest_thread.is_alive():
            if not messagebox.askokcancel("Quit?", "A chart is being generated. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def populate_assets(self):
        assets = get_available_assets()
        self.asset_dropdown['values'] = assets
        if assets: self.asset_dropdown.current(0)

    def populate_strategies(self):
        strats = []
        for importer, modname, ispkg in pkgutil.walk_packages(path=Strategies.__path__, prefix=Strategies.__name__ + '.', onerror=lambda x: None):
            if modname.split('.')[-1].startswith('strategy_'):
                strats.append(modname)
        self.strategy_dropdown['values'] = sorted(strats)
        if strats and not self.strategy_var.get(): self.strategy_dropdown.current(0)

    def get_strategy_instance(self, module_path: str) -> Optional[BaseStrategy]:
        """Dynamically loads a strategy module and returns an instance of the strategy class."""
        if not module_path:
            return None
        try:
            module = importlib.import_module(module_path)
            importlib.reload(module)
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
                    return obj()
        except (ImportError, AttributeError) as e:
            print(f"Could not get strategy instance from {module_path}: {e}")
        return None

    def run_backtest_logic(self, params: tuple):
        """
        The core logic for the visualizer, run in a separate thread.
        It loads data, runs a one-day backtest, and generates a plot.
        """
        log_callback = lambda msg: self.update_log(msg)
        try:
            asset_name, strategy_path, date_obj, rr, use_be, verbose_mode = params
            target_date = date_obj
            log_callback(f"\n--- Visualizing {strategy_path} for {asset_name} on {target_date} ---")

            # Load data and strategy
            full_unified_df = load_unified_data(asset_name)
            if full_unified_df.empty: raise ValueError("Unified data file not found or is empty.")
            
            strategy_instance = self.get_strategy_instance(strategy_path)
            if not strategy_instance: raise ValueError(f"Could not load strategy from {strategy_path}")

            # Filter data for the target day
            full_unified_df['ny_time'] = full_unified_df.index.tz_convert('America/New_York')
            day_data_df = full_unified_df[full_unified_df['ny_time'].dt.date == target_date].copy()
            if day_data_df.empty: raise ValueError(f"No data found for {target_date} in the unified file.")
            
            # Generate signals
            conditions_df = strategy_instance.generate_conditions(day_data_df.copy(), strategy_params={})
            signal_tf = strategy_instance.STRATEGY_TIMEFRAME
            
            # Combine signals
            final_mask = conditions_df['base_pattern_cond']
            if 'session_cond' in conditions_df.columns:
                final_mask &= conditions_df['session_cond']
            open_col = f'open_{signal_tf}'
            is_new_candle_start = day_data_df[open_col].ne(day_data_df[open_col].shift(1))
            final_mask &= is_new_candle_start
            
            long_signal_mask = final_mask & conditions_df['is_bullish']
            short_signal_mask = final_mask & conditions_df['is_bearish']

            signals_df = pd.DataFrame(index=day_data_df.index)
            signals_df['signal'] = np.where(long_signal_mask, 1, np.where(short_signal_mask, -1, 0))
            signals_df['entry_price'] = np.where(long_signal_mask, conditions_df['entry_price'], np.where(short_signal_mask, conditions_df['entry_price'], np.nan))
            signals_df['sl_price'] = np.where(long_signal_mask, conditions_df['sl_price_long'], np.where(short_signal_mask, conditions_df['sl_price_short'], np.nan))

            day_data_df['signal'] = signals_df['signal']
            day_data_df['entry_price'] = signals_df['entry_price']
            day_data_df['sl_price'] = signals_df['sl_price']
            
            # Run a one-day backtest
            execution_timeframe = ASSET_CONFIG[asset_name]['base_tf']
            trades_df = run_r_backtest(day_data_df, rr, use_be, rr / 2.0, execution_timeframe, allow_multiple_trades=True, status_callback=log_callback)
            
            # Prepare data for plotting
            log_callback("Preparing data for visualization...")
            plot_cols = [f'open_{signal_tf}', f'high_{signal_tf}', f'low_{signal_tf}', f'close_{signal_tf}', f'volume_{signal_tf}']
            if not all(col in day_data_df.columns for col in plot_cols):
                raise KeyError(f"One or more required columns for timeframe '{signal_tf}' not found in the data.")

            temp_df = day_data_df[plot_cols + ['ny_time']].copy()
            is_new_signal_candle = temp_df[f'open_{signal_tf}'].ne(temp_df[f'open_{signal_tf}'].shift())
            plot_df = temp_df[is_new_signal_candle].copy()
            plot_df.rename(columns={f'open_{signal_tf}': 'open', f'high_{signal_tf}': 'high', f'low_{signal_tf}': 'low', f'close_{signal_tf}': 'close', f'volume_{signal_tf}': 'volume'}, inplace=True)
            log_callback(f"Prepared {len(plot_df)} unique candles for {signal_tf} chart visualization.")
            
            strategy_name_for_plot = strategy_path.split('.')[-1]
            saved_image_path = plot_day_summary(plot_df, trades_df, asset_name, strategy_name_for_plot, date_obj.strftime('%Y-%m-%d'))
            
            self.after(0, self.on_backtest_complete, saved_image_path)

        except Exception as e:
            tb = traceback.format_exc(); error_message = f"An error occurred in the visualizer logic:\n\n{e}\n\n{tb}"
            self.after(0, self.on_backtest_error, error_message)
            
    def start_backtest_thread(self):
        try:
            asset = self.asset_var.get(); strategy_path = self.strategy_var.get()
            if not asset or not strategy_path: raise ValueError("Asset and Strategy must be selected.")
            params = (asset, strategy_path, self.date_entry.get_date(), float(self.rr_entry.get()), self.be_var.get(), self.verbose_var.get())
        except (ValueError, KeyError, TclError) as e:
            messagebox.showerror("Input Error", f"Please ensure all fields are filled out correctly.\n\nDetails: {e}"); return
            
        self.run_button.config(text="Running...", state="disabled")
        self.log_widget.config(state='normal'); self.log_widget.delete('1.0', tk.END); self.log_widget.config(state='disabled')
        
        self.backtest_thread = threading.Thread(target=self.run_backtest_logic, args=(params,), daemon=True)
        self.backtest_thread.start()
        
    def on_backtest_complete(self, saved_image_path):
        self.run_button.config(text="Generate Chart", state="normal")
        self.backtest_thread = None
        if saved_image_path:
            if messagebox.askyesno("Success", f"Chart saved to:\n{os.path.abspath(saved_image_path)}\n\nDo you want to open it now?"):
                open_file(saved_image_path)
        else:
            messagebox.showinfo("Chart Not Generated", "No trades or data to plot for this day.")
            
    def on_backtest_error(self, error):
        self.run_button.config(text="Generate Chart", state="normal")
        self.backtest_thread = None
        messagebox.showerror("Failed to Generate Chart", error)