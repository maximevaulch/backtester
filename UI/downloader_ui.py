# In UI/downloader_ui.py
import tkinter as tk
from tkinter import ttk, messagebox, TclError, scrolledtext
from tkcalendar import DateEntry
import os
import sys
import threading
import shutil

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

from Core.oanda_downloader import run_download, analyze_raw_data
from Core.updater import run_full_update

class DownloaderUI(tk.Toplevel):
    # ... (The rest of this file is now correct and doesn't need changes) ...
    def __init__(self, master=None):
        super().__init__(master)
        self.master_app = master
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.title("Data Manager")
        self.geometry("500x500")

        self.pipeline_thread = None

        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)

        ttk.Label(main_frame, text="Mode:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, padx=10, pady=10, sticky='w')
        self.mode_var = tk.StringVar(value="New Download")
        self.mode_dropdown = ttk.Combobox(main_frame, textvariable=self.mode_var, values=["New Download", "Update All"], state="readonly")
        self.mode_dropdown.grid(row=0, column=1, padx=10, pady=10, sticky='ew')
        self.mode_dropdown.bind('<<ComboboxSelected>>', self.on_mode_select)

        self.new_download_frame = ttk.Frame(main_frame)
        self.new_download_frame.grid(row=1, column=0, columnspan=2, sticky='ewns', padx=5)
        
        controls_frame = ttk.Frame(self.new_download_frame)
        controls_frame.pack(fill="x", pady=(0, 10))

        self.instruments = ["EUR_USD", "GBP_USD", "XAU_USD", "USD_JPY", "AUD_USD"]
        ttk.Label(controls_frame, text="Instrument:").grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.instrument_var = tk.StringVar(value=self.instruments[0])
        self.instrument_dropdown = ttk.Combobox(controls_frame, textvariable=self.instrument_var, values=self.instruments, state="readonly")
        self.instrument_dropdown.grid(row=0, column=1, padx=5, pady=5, sticky='ew')

        ttk.Label(controls_frame, text="Granularity:").grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.granularity_var = tk.StringVar(value="S30") 
        granularities = ["S30"] 
        self.granularity_dropdown = ttk.Combobox(controls_frame, textvariable=self.granularity_var, values=granularities, state="readonly")
        self.granularity_dropdown.grid(row=1, column=1, padx=5, pady=5, sticky='ew')

        ttk.Label(controls_frame, text="Start Date (D-M-Y):").grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.start_date_entry = DateEntry(controls_frame, date_pattern='dd-mm-yyyy')
        self.start_date_entry.grid(row=2, column=1, padx=5, pady=5, sticky='w')

        self.new_download_button = ttk.Button(controls_frame, text="Start New Download", command=self.start_new_download)
        self.new_download_button.grid(row=3, column=0, columnspan=2, pady=20)
        
        new_log_frame = ttk.LabelFrame(self.new_download_frame, text="Status Log")
        new_log_frame.pack(fill="both", expand=True)
        self.new_download_log = scrolledtext.ScrolledText(new_log_frame, state='disabled', height=10, wrap=tk.WORD)
        self.new_download_log.pack(fill="both", expand=True, padx=5, pady=5)

        self.update_all_frame = ttk.Frame(main_frame)
        ttk.Label(self.update_all_frame, text="This will update, heal, and resample ALL existing raw data sets.", wraplength=400).pack(pady=10)
        self.update_all_button = ttk.Button(self.update_all_frame, text="Update All Datasets", command=self.start_update_pipeline)
        self.update_all_button.pack(pady=10)
        
        update_log_frame = ttk.LabelFrame(self.update_all_frame, text="Status Log")
        update_log_frame.pack(fill="both", expand=True, pady=10)
        self.update_all_log = scrolledtext.ScrolledText(update_log_frame, state='disabled', height=10, wrap=tk.WORD)
        self.update_all_log.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.back_button = ttk.Button(main_frame, text="< Back to Master", command=self.on_closing)
        self.back_button.grid(row=2, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(1, weight=1)
        self.on_mode_select()

    def update_log(self, log_widget, message):
        def task():
            try:
                log_widget.config(state='normal')
                log_widget.insert(tk.END, message + "\n")
                log_widget.config(state='disabled')
                log_widget.see(tk.END)
            except TclError: pass 
        self.after(0, task)

    def on_closing(self):
        if self.pipeline_thread and self.pipeline_thread.is_alive():
            if not messagebox.askokcancel("Quit?", "A data pipeline is currently running. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def on_mode_select(self, event=None):
        mode = self.mode_var.get()
        if mode == "New Download":
            self.update_all_frame.grid_forget()
            self.new_download_frame.grid(row=1, column=0, columnspan=2, sticky='ewns', padx=5)
            self.title("Data Manager - New Download")
        else:
            self.new_download_frame.grid_forget()
            self.update_all_frame.grid(row=1, column=0, columnspan=2, sticky='ewns', padx=5)
            self.title("Data Manager - Update All")

    def start_new_download(self):
        self.new_download_button.config(state="disabled")
        self.new_download_log.config(state='normal')
        self.new_download_log.delete('1.0', tk.END)
        self.new_download_log.config(state='disabled')
        
        instrument = self.instrument_var.get()
        granularity = self.granularity_var.get()
        data_folder = os.path.join(project_root, 'Data')
        raw_folder_path = os.path.join(data_folder, f"{instrument}_{granularity}")

        if os.path.exists(raw_folder_path):
            if not messagebox.askyesno("Confirm Deletion", f"Folder '{os.path.basename(raw_folder_path)}' already exists and will be DELETED.\n\nProceed?", icon='warning'):
                self.new_download_button.config(state="normal")
                return
        
        self.pipeline_thread = threading.Thread(target=self.new_download_logic, args=(raw_folder_path, instrument, granularity), daemon=True)
        self.pipeline_thread.start()
        self.after(100, lambda: self.check_thread_status(self.new_download_button))

    def new_download_logic(self, folder_path, instrument, granularity):
        log_callback = lambda msg: self.update_log(self.new_download_log, msg)
        try:
            if os.path.exists(folder_path):
                log_callback(f"Deleting existing folder: {os.path.basename(folder_path)}")
                shutil.rmtree(folder_path)
            
            start_date = self.start_date_entry.get_date().strftime('%Y-%m-%d')
            run_download(instrument=instrument, granularity=granularity, start_date_str=start_date, status_callback=log_callback)
            
            log_callback("\n--- Analyzing downloaded data ---")
            report_string = analyze_raw_data(folder_path)
            log_callback(report_string)
            self.after(0, lambda: messagebox.showinfo("Complete", "New download process finished successfully. Please launch the Healer."))
        except Exception as e:
            log_callback(f"\nFATAL ERROR: {e}")
            self.after(0, lambda: messagebox.showerror("Download Failed", f"An error occurred: {e}"))

    def start_update_pipeline(self):
        self.update_all_button.config(state="disabled")
        self.update_all_log.config(state='normal')
        self.update_all_log.delete('1.0', tk.END)
        self.update_all_log.config(state='disabled')
        
        log_callback = lambda msg: self.update_log(self.update_all_log, msg)
        self.pipeline_thread = threading.Thread(target=run_full_update, args=(log_callback,), daemon=True)
        self.pipeline_thread.start()
        self.after(100, lambda: self.check_thread_status(self.update_all_button))

    def check_thread_status(self, button_to_enable):
        if self.pipeline_thread and self.pipeline_thread.is_alive():
            self.after(100, lambda: self.check_thread_status(button_to_enable))
        else:
            try: button_to_enable.config(state="normal")
            except TclError: pass