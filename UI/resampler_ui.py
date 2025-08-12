# In UI/resampler_ui.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, TclError
import os
import sys
import threading

# --- THIS IS THE FIX ---
def get_project_root():
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

project_root = get_project_root()
# --- END OF FIX ---

if project_root not in sys.path: sys.path.insert(0, project_root)

from Core.resampler import run_resampling

# ... (The rest of this file is now correct and doesn't need changes) ...
def get_available_healed_assets():
    data_path = os.path.join(project_root, 'Data')
    available_assets = set()
    if not os.path.exists(data_path): return []
    for filename in os.listdir(data_path):
        if filename.endswith('_healed.parquet') and os.path.isfile(os.path.join(data_path, filename)):
            try:
                parts = filename.split('_'); asset_name = f"{parts[0]}_{parts[1]}"
                available_assets.add(asset_name)
            except IndexError: continue
    return sorted(list(available_assets))

class ResamplerUI(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.master_app = master
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.title("Step 3: Timeframe Resampler")
        self.geometry("500x400")
        self.resampling_thread = None
        
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)

        controls_frame = ttk.Frame(main_frame)
        controls_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(controls_frame, text="Asset to Resample:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, sticky="w", pady=5)
        self.asset_var = tk.StringVar()
        self.asset_dropdown = ttk.Combobox(controls_frame, textvariable=self.asset_var, state="readonly", postcommand=self.populate_assets)
        self.asset_dropdown.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.run_button = ttk.Button(controls_frame, text="Run Resampler", command=self.start_resampling)
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
        if self.resampling_thread and self.resampling_thread.is_alive():
            if not messagebox.askokcancel("Quit?", "A resampling task is running. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def resampling_logic(self, file_path):
        log_callback = lambda msg: self.update_log(msg)
        try:
            success, report_dict = run_resampling(file_path, status_callback=log_callback)
            if success:
                summary = "Resampling Complete!\n\n" + "\n".join([f"- {fname}: {count}" for fname, count in report_dict.items()])
                self.after(0, lambda: messagebox.showinfo("Success", summary))
            else:
                error_message = report_dict.get("Error", "An unknown error occurred.")
                self.after(0, lambda: messagebox.showwarning("Resampling Failed", f"Reason: {error_message}"))
        except Exception as e:
            log_callback(f"\nFATAL ERROR: {e}")
            self.after(0, lambda err=e: messagebox.showerror("Resampling Error", f"A critical error occurred:\n{err}"))
        finally:
            self.after(0, self.on_task_complete)

    def start_resampling(self):
        asset_name = self.asset_var.get()
        if not asset_name:
            messagebox.showerror("Error", "Please select an asset to resample."); return

        data_path = os.path.join(project_root, 'Data')
        healed_filename = next((f for f in os.listdir(data_path) if f.startswith(asset_name) and f.endswith('_healed.parquet')), None)
        if not healed_filename:
            messagebox.showerror("Error", f"Could not find a healed file for '{asset_name}'."); return

        file_path = os.path.join(data_path, healed_filename)
        self.run_button.config(text="Resampling...", state="disabled")
        self.log_widget.config(state='normal'); self.log_widget.delete('1.0', tk.END); self.log_widget.config(state='disabled')
        
        self.resampling_thread = threading.Thread(target=self.resampling_logic, args=(file_path,), daemon=True)
        self.resampling_thread.start()

    def on_task_complete(self):
        try:
            self.run_button.config(text="Run Resampler", state="normal")
            self.resampling_thread = None
        except TclError: pass

    def populate_assets(self):
        assets = get_available_healed_assets()
        self.asset_dropdown['values'] = assets
        if assets and not self.asset_var.get(): self.asset_dropdown.current(0)
        elif not assets: self.asset_dropdown['values'] = []; self.asset_var.set('')