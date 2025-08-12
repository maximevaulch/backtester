# In UI/final_data_check.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, TclError
import pandas as pd
import os
import sys
import threading
import subprocess
import math

def get_project_root():
    # ... (function is unchanged) ...
    if getattr(sys, 'frozen', False): return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = get_project_root()

def open_file(filepath):
    # ... (function is unchanged) ...
    try:
        if sys.platform == "win32": os.startfile(os.path.normpath(filepath))
        else: subprocess.call(["open" if sys.platform == "darwin" else "xdg-open", filepath])
    except Exception as e: messagebox.showerror("Error", f"Could not open file:\n{e}")

def get_theoretical_candles(date, timeframe_str):
    # ... (function is unchanged) ...
    weekday = date.weekday()
    if weekday == 4: trading_hours = 17
    elif weekday == 5: trading_hours = 0
    elif weekday == 6: trading_hours = 7
    else: trading_hours = 24
    total_seconds_in_day = trading_hours * 3600
    try:
        seconds_per_candle = pd.to_timedelta(timeframe_str).total_seconds()
        if seconds_per_candle == 0: return 0
        return math.floor(total_seconds_in_day / seconds_per_candle)
    except (ValueError, TypeError): return 0

# --- THIS FUNCTION IS NOW MODIFIED ---
def run_analysis(folder_path, status_callback=None):
    def log(message):
        if status_callback: status_callback(message)
        else: print(message)
    
    log(f"\n--- Analyzing folder: {folder_path} ---")
    all_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
    if not all_files: return False, "No .parquet files found in the selected folder."
    
    parsed_files = []
    # ... (rest of the function is the same, just replacing print with log) ...
    for f in all_files:
        try:
            base_name = f.replace('.parquet', ''); tf_label = base_name.split('_')[-1]
            pd_tf = tf_label.upper() if len(tf_label) == 1 else tf_label
            if 'H' in pd_tf: pd_tf = pd_tf.replace('h','H')
            delta = pd.to_timedelta(pd_tf)
            parsed_files.append({'path': os.path.join(folder_path, f), 'tf_label': tf_label, 'pd_tf': pd_tf, 'delta': delta})
        except (ValueError, IndexError) as e:
            log(f"Warning: Could not parse timeframe from '{f}'. Error: {e}. Skipping.")
    if not parsed_files: return False, "Could not parse any valid timeframe files in the folder."
    
    parsed_files.sort(key=lambda x: x['delta'])
    master_df = None
    for file_info in parsed_files:
        tf_label = file_info['tf_label']; log(f"  -> Processing {tf_label}...")
        df = pd.read_parquet(file_info['path'])
        df.index = df.index.tz_localize('UTC') if df.index.tz is None else df.index.tz_convert('UTC')
        df.index = df.index.tz_convert('America/New_York')
        daily_counts = df.resample('D').size(); daily_counts.name = f"{tf_label}_Available"
        master_df = pd.DataFrame(daily_counts) if master_df is None else master_df.join(daily_counts, how='outer')
        
    for file_info in parsed_files:
        tf_label, pd_tf = file_info['tf_label'], file_info['pd_tf']; log(f"  -> Calculating theoretical counts for {tf_label}...")
        master_df[f'{tf_label}_Theoretical'] = master_df.index.to_series().apply(lambda d: get_theoretical_candles(d, pd_tf))
        
    master_df.fillna(0, inplace=True); master_df = master_df.astype(int); master_df.index.name = 'Date (NY)'
    final_columns = [col for tf_info in parsed_files for col in (f"{tf_info['tf_label']}_Available", f"{tf_info['tf_label']}_Theoretical")]
    master_df = master_df[final_columns]; master_df.index = master_df.index.tz_localize(None)
    output_filename = os.path.join(folder_path, "daily_candle_audit.xlsx")
    
    log(f"\n--- Saving report to: {output_filename} ---")
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        master_df.to_excel(writer, sheet_name='Daily Candle Count')
        worksheet = writer.sheets['Daily Candle Count']
        for column_cells in worksheet.columns:
            max_length = 0; column = column_cells[0].column_letter
            for cell in column_cells:
                try:
                    if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                except: pass
            adjusted_width = (max_length + 2); worksheet.column_dimensions[column].width = adjusted_width
            
    return True, output_filename

def get_available_resampled_assets():
    # ... (function is unchanged) ...
    data_path = os.path.join(project_root, 'Data')
    available_assets = []
    if not os.path.exists(data_path): return []
    for item in os.listdir(data_path):
        if os.path.isdir(os.path.join(data_path, item)) and item.endswith('_resampled'):
            available_assets.append(item.replace('_resampled', ''))
    return sorted(available_assets)

class AuditUI(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.master_app = master
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.title("Final Data Audit Tool")
        self.geometry("500x400")
        self.analysis_thread = None
        
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)

        controls_frame = ttk.Frame(main_frame)
        controls_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(controls_frame, text="Asset to Audit:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, sticky="w", pady=5)
        self.asset_var = tk.StringVar()
        self.asset_dropdown = ttk.Combobox(controls_frame, textvariable=self.asset_var, state="readonly", postcommand=self.populate_assets)
        self.asset_dropdown.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.run_button = ttk.Button(controls_frame, text="Generate Audit Report", command=self.start_analysis)
        self.run_button.grid(row=2, column=0, columnspan=2, pady=10)
        controls_frame.grid_columnconfigure(0, weight=1)

        log_frame = ttk.LabelFrame(main_frame, text="Status Log")
        log_frame.pack(fill="both", expand=True)
        self.log_widget = scrolledtext.ScrolledText(log_frame, state='disabled', height=10, wrap=tk.WORD)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.back_button = ttk.Button(main_frame, text="< Back to Master", command=self.on_closing)
        self.back_button.pack(pady=(10, 0))

        self.populate_assets()

    def update_log(self, message):
        def task():
            try:
                self.log_widget.config(state='normal')
                self.log_widget.insert(tk.END, message + "\n")
                self.log_widget.config(state='disabled'); self.log_widget.see(tk.END)
            except TclError: pass
        self.after(0, task)

    def on_closing(self):
        if self.analysis_thread and self.analysis_thread.is_alive():
            if not messagebox.askokcancel("Quit?", "An analysis is running. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def analysis_logic(self, folder_path):
        log_callback = lambda msg: self.update_log(msg)
        try:
            success, result_path = run_analysis(folder_path, status_callback=log_callback)
            if success:
                self.after(0, lambda p=result_path: self.on_success(p))
            else:
                self.after(0, lambda: messagebox.showerror("Analysis Failed", result_path))
        except Exception as e:
            log_callback(f"\nFATAL ERROR: {e}")
            self.after(0, lambda err=e: messagebox.showerror("Critical Error", f"An unexpected error occurred:\n{err}"))
        finally:
            self.after(0, self.on_task_complete)
            
    def on_success(self, report_path):
        if messagebox.askyesno("Success", f"Audit report saved to:\n{report_path}\n\nDo you want to open it now?"):
            open_file(report_path)

    def start_analysis(self):
        asset_name = self.asset_var.get()
        if not asset_name:
            messagebox.showerror("Error", "Please select an asset to audit."); return

        folder_name = f"{asset_name}_resampled"
        folder_path = os.path.join(project_root, 'Data', folder_name)
        if not os.path.isdir(folder_path):
            messagebox.showerror("Error", f"Could not find resampled folder:\n{folder_path}"); return
        
        self.run_button.config(text="Analyzing...", state="disabled")
        self.log_widget.config(state='normal'); self.log_widget.delete('1.0', tk.END); self.log_widget.config(state='disabled')
        
        self.analysis_thread = threading.Thread(target=self.analysis_logic, args=(folder_path,), daemon=True)
        self.analysis_thread.start()

    def on_task_complete(self):
        try:
            self.run_button.config(text="Generate Audit Report", state="normal")
            self.analysis_thread = None
        except TclError: pass

    def populate_assets(self):
        assets = get_available_resampled_assets()
        self.asset_dropdown['values'] = assets
        if assets and not self.asset_var.get(): self.asset_dropdown.current(0)