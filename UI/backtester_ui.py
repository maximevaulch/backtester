# In UI/backtester_ui.py
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, TclError
from tkcalendar import DateEntry
import os
import importlib
import pkgutil
from datetime import time
import sys
import threading
import traceback
import inspect
import Strategies


from Core.strategy_base import BaseStrategy
from typing import List, Optional

def get_project_root():
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

project_root = get_project_root()

from Core.main import run_full_backtest
from Core.visualizer import open_file

def get_available_assets() -> List[str]:
    """Scans the Data directory to find available assets for testing."""
    data_path = os.path.join(project_root, 'Data')
    assets = []
    if os.path.exists(data_path):
        for folder_name in os.listdir(data_path):
            if folder_name.endswith('_resampled') and os.path.isdir(os.path.join(data_path, folder_name)):
                assets.append(folder_name.replace('_resampled', ''))
    return sorted(assets)

class BacktesterUI(tk.Toplevel):
    """The main UI window for configuring and running backtests."""
    def __init__(self, master: Optional[tk.Tk] = None):
        """Initializes the BacktesterUI window and its components."""
        super().__init__(master)
        self.master_app = master
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.title("Backtester")
        self.geometry("500x800")
        self.backtest_thread = None

        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)
        
        ttk.Label(main_frame, text="Asset to Test:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, padx=10, pady=5, sticky='w')
        self.asset_var = tk.StringVar()
        self.asset_dropdown = ttk.Combobox(main_frame, textvariable=self.asset_var, state="readonly", postcommand=self.populate_assets)
        self.asset_dropdown.grid(row=0, column=1, padx=10, pady=5, sticky='ew')
        
        ttk.Label(main_frame, text="Strategy to Use:", font=('Helvetica', 10, 'bold')).grid(row=1, column=0, padx=10, pady=5, sticky='w')
        self.strategy_var = tk.StringVar()
        self.strategy_dropdown = ttk.Combobox(main_frame, textvariable=self.strategy_var, postcommand=self.populate_strategies, state="readonly")
        self.strategy_dropdown.grid(row=1, column=1, padx=10, pady=5, sticky='ew')
        self.strategy_dropdown.bind('<<ComboboxSelected>>', self.on_strategy_select)

        ttk.Label(main_frame, text="Start Date (D-M-Y):", font=('Helvetica', 10, 'bold')).grid(row=2, column=0, padx=10, pady=5, sticky='w')
        self.start_date_entry = DateEntry(main_frame, date_pattern='dd-mm-yyyy')
        self.start_date_entry.grid(row=2, column=1, padx=10, pady=5, sticky='w')
        
        ttk.Label(main_frame, text="End Date (D-M-Y):", font=('Helvetica', 10, 'bold')).grid(row=3, column=0, padx=10, pady=5, sticky='w')
        self.end_date_entry = DateEntry(main_frame, date_pattern='dd-mm-yyyy')
        self.end_date_entry.grid(row=3, column=1, padx=10, pady=5, sticky='w')
        
        self.conditions_frame = ttk.LabelFrame(main_frame, text="Strategy Conditions to Test")
        self.conditions_frame.grid(row=4, column=0, columnspan=2, padx=10, pady=10, sticky='ew')
        self.filter_vars = {}

        self.session_options_frame = ttk.LabelFrame(main_frame, text="Session Filter (NY Time)")
        self.session_options_frame.grid(row=5, column=0, columnspan=2, padx=10, pady=10, sticky='ew')
        time_values = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
        self.session_start_label = ttk.Label(self.session_options_frame, text="Session Start:")
        self.session_start_dropdown = ttk.Combobox(self.session_options_frame, values=time_values, width=8, state="readonly")
        self.session_start_dropdown.set("22:00")
        self.session_end_label = ttk.Label(self.session_options_frame, text="Session End:")
        self.session_end_dropdown = ttk.Combobox(self.session_options_frame, values=time_values, width=8, state="readonly")
        self.session_end_dropdown.set("07:00")

        scenarios_frame = ttk.LabelFrame(main_frame, text="Exit Scenarios (RR)")
        scenarios_frame.grid(row=6, column=0, columnspan=2, padx=10, pady=10, sticky='ew')
        self.scenario_entries = []
        ttk.Label(scenarios_frame, text="RR").grid(row=0, column=0, padx=5); ttk.Label(scenarios_frame, text="Use BE?").grid(row=0, column=1, padx=5)
        for i in range(5):
            rr_entry = ttk.Entry(scenarios_frame, width=10); rr_entry.grid(row=i+1, column=0, padx=5, pady=2)
            be_var = tk.BooleanVar(); be_check = ttk.Checkbutton(scenarios_frame, variable=be_var); be_check.grid(row=i+1, column=1, padx=5)
            self.scenario_entries.append({'rr': rr_entry, 'be': be_var})

        self.multi_trade_var = tk.BooleanVar(value=True)
        self.multi_trade_check = ttk.Checkbutton(main_frame, text="Allow Multiple Overlapping Trades", variable=self.multi_trade_var)
        self.multi_trade_check.grid(row=7, column=0, columnspan=2, padx=10, pady=10, sticky='w')
        
        self.run_button = ttk.Button(main_frame, text="Run Backtest", command=self.start_backtest_thread)
        self.run_button.grid(row=8, column=0, columnspan=2, pady=10)

        log_frame = ttk.LabelFrame(main_frame, text="Status Log")
        log_frame.grid(row=9, column=0, columnspan=2, sticky="ew", padx=10)
        self.log_widget = scrolledtext.ScrolledText(log_frame, state='disabled', height=10, wrap=tk.WORD)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.back_button = ttk.Button(main_frame, text="< Back to Master", command=self.on_closing)
        self.back_button.grid(row=10, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_columnconfigure(1, weight=1)
        self.populate_assets()
        self.populate_strategies()
        self.on_strategy_select()

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
            if not messagebox.askokcancel("Quit?", "A backtest is running. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def get_strategy_instance(self, module_path: str) -> Optional[BaseStrategy]:
        """
        Dynamically loads a strategy module and returns an instance of the strategy class.

        Args:
            module_path: The full module path of the strategy (e.g., 'Strategies.strategy_PR').

        Returns:
            An instance of the BaseStrategy subclass found in the module, or None if not found.
        """
        if not module_path:
            return None
        try:
            module = importlib.import_module(module_path)
            importlib.reload(module)  # Reload to get the latest changes during development
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
                    return obj()  # Instantiate the class
        except (ImportError, AttributeError) as e:
            print(f"Could not get strategy instance from {module_path}: {e}")
        return None

    def on_strategy_select(self, event: Optional[tk.Event] = None):
        """
        Handles the event when a new strategy is selected from the dropdown.
        It dynamically updates the UI to show the relevant filters and session options.
        """
        for widget in self.session_options_frame.winfo_children(): widget.grid_forget()
        for widget in self.conditions_frame.winfo_children(): widget.grid_forget()
        self.filter_vars.clear()

        strategy_instance = self.get_strategy_instance(self.strategy_var.get())
        if not strategy_instance:
            return

        if strategy_instance.SESSION_TYPE == 'optional':
            self.session_start_label.grid(row=0, column=0, padx=5, pady=2, sticky='w')
            self.session_start_dropdown.grid(row=0, column=1, padx=5, pady=2, sticky='w')
            self.session_end_label.grid(row=1, column=0, padx=5, pady=2, sticky='w')
            self.session_end_dropdown.grid(row=1, column=1, padx=5, pady=2, sticky='w')

        if strategy_instance.AVAILABLE_FILTERS:
            for i, filter_name in enumerate(strategy_instance.AVAILABLE_FILTERS):
                var = tk.BooleanVar()
                chk = ttk.Checkbutton(self.conditions_frame, text=f"Use {filter_name} Condition", variable=var)
                chk.grid(row=i, column=0, padx=5, pady=2, sticky='w')
                self.filter_vars[filter_name] = var

    def populate_assets(self):
        """Populates the asset dropdown with available resampled asset data."""
        assets = get_available_assets()
        self.asset_dropdown['values'] = assets
        if assets: self.asset_dropdown.current(0)

    def populate_strategies(self):
        """Discovers and populates the strategy dropdown with all valid strategy modules."""
        strats = []
        for importer, modname, ispkg in pkgutil.walk_packages(path=Strategies.__path__, prefix=Strategies.__name__ + '.', onerror=lambda x: None):
            if modname.split('.')[-1].startswith('strategy_'):
                strats.append(modname)
        self.strategy_dropdown['values'] = sorted(strats)
        if strats and not self.strategy_var.get(): self.strategy_dropdown.current(0)

    def backtest_logic(self, *args):
        asset_name, start_date, end_date, scenarios, strategy_instance, strategy_params, selected_filters, allow_multiple_trades, log_callback = args
        try:
            report_path = run_full_backtest(
                asset_name, start_date, end_date, scenarios,
                strategy_instance=strategy_instance,
                strategy_params=strategy_params,
                selected_filters=selected_filters,
                allow_multiple_trades=allow_multiple_trades,
                status_callback=log_callback
            )
            if report_path and os.path.exists(report_path):
                self.after(0, lambda p=report_path: self.on_backtest_success(p))
            else:
                self.after(0, lambda: messagebox.showinfo("Success", f"Backtest for {asset_name} complete! No trades were taken or report was not generated."))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda: messagebox.showerror("Backtest Failed", f"An error occurred during the backtest:\n\n{e}\n\n{tb}"))
        finally:
            self.after(0, self.on_task_complete)
    
    def on_backtest_success(self, report_path):
        if messagebox.askyesno("Success", f"Backtest complete! Report saved to:\n{report_path}\n\nDo you want to open it now?"):
            open_file(report_path)

    def start_backtest_thread(self):
        """
        Validates user inputs, gathers all parameters, and starts the backtest
        process in a separate thread to keep the UI responsive.
        """
        try:
            asset_name = self.asset_var.get()
            strategy_path = self.strategy_var.get()
            if not asset_name or not strategy_path: raise ValueError("Asset and Strategy must be selected.")
            
            start_date = self.start_date_entry.get_date().strftime('%Y-%m-%d')
            end_date = self.end_date_entry.get_date().strftime('%Y-%m-%d')

            strategy_instance = self.get_strategy_instance(strategy_path)
            if not strategy_instance:
                raise ValueError(f"Could not load strategy from {strategy_path}")

            selected_filters = [name for name, var in self.filter_vars.items() if var.get()]
            strategy_params = {}
            if strategy_instance.SESSION_TYPE == 'optional':
                start_str, end_str = self.session_start_dropdown.get(), self.session_end_dropdown.get()
                if start_str and end_str:
                    time.fromisoformat(start_str); time.fromisoformat(end_str)
                    strategy_params['session_start_str'] = start_str
                    strategy_params['session_end_str'] = end_str
                elif start_str or end_str: raise ValueError("Both session start and end times must be provided.")

            scenarios = []
            for entry in self.scenario_entries:
                rr_str = entry['rr'].get()
                if rr_str:
                    rr = float(rr_str); use_be = entry['be'].get()
                    scenarios.append({'rr': rr, 'use_be': use_be, 'be_trigger_r': rr / 2.0})
            if not scenarios: raise ValueError("Please define at least one RR scenario.")
        except Exception as e:
            messagebox.showerror("Input Error", f"Please check your inputs: {e}"); return
        
        self.run_button.config(text="Running...", state="disabled")
        self.log_widget.config(state='normal'); self.log_widget.delete('1.0', tk.END); self.log_widget.config(state='disabled')
        
        log_callback = lambda msg: self.update_log(msg)
        allow_multiple_trades = self.multi_trade_var.get()
        args = (asset_name, start_date, end_date, scenarios, strategy_instance, strategy_params, selected_filters, allow_multiple_trades, log_callback)
        
        self.backtest_thread = threading.Thread(target=self.backtest_logic, args=args, daemon=True)
        self.backtest_thread.start()

    def on_task_complete(self):
        try:
            self.run_button.config(text="Run Backtest", state="normal")
            self.backtest_thread = None
        except TclError: pass