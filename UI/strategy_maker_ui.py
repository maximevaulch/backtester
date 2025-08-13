# In UI/strategy_maker_ui.py
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, TclError
import os
import sys
import threading

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Core.strategy_maker import create_strategy_file

class StrategyMakerUI(tk.Toplevel):
    """
    The UI window for the Strategy Maker tool. This tool allows users to
    specify basic parameters (name, timeframe, etc.) and generates a
    starter Python script for a new strategy, which can then be edited
    manually.
    """
    def __init__(self, master=None):
        super().__init__(master)
        self.master_app = master
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.title("Baseline Strategy Maker")
        self.geometry("500x450")
        self.generation_thread = None
        
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill="both", expand=True)

        ttk.Label(main_frame, text="Strategy Name:", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, padx=10, pady=5, sticky='w')
        self.name_var = tk.StringVar()
        self.name_entry = ttk.Entry(main_frame, textvariable=self.name_var)
        self.name_entry.grid(row=0, column=1, padx=10, pady=5, sticky='ew')
        
        ttk.Label(main_frame, text="Analysis Timeframe:", font=('Helvetica', 10, 'bold')).grid(row=1, column=0, padx=10, pady=5, sticky='w')
        self.tf_var = tk.StringVar()
        self.tf_dropdown = ttk.Combobox(main_frame, textvariable=self.tf_var, state="readonly", values=['1min', '2min', '3min', '5min', '15min', '30min', '45min', '1H', '4H', 'D'])
        self.tf_dropdown.grid(row=1, column=1, padx=10, pady=5, sticky='ew')
        self.tf_dropdown.set('15min')

        ttk.Label(main_frame, text="Session Type:", font=('Helvetica', 10, 'bold')).grid(row=2, column=0, padx=10, pady=5, sticky='w')
        self.session_var = tk.StringVar()
        self.session_dropdown = ttk.Combobox(main_frame, textvariable=self.session_var, state="readonly", values=['optional', 'fixed'])
        self.session_dropdown.grid(row=2, column=1, padx=10, pady=5, sticky='ew')
        self.session_dropdown.set('optional')

        ttk.Label(main_frame, text="Optional Filters:", font=('Helvetica', 10, 'bold')).grid(row=3, column=0, padx=10, pady=5, sticky='w')
        self.filters_var = tk.StringVar()
        self.filters_entry = ttk.Entry(main_frame, textvariable=self.filters_var)
        self.filters_entry.grid(row=3, column=1, padx=10, pady=5, sticky='ew')
        ttk.Label(main_frame, text="(e.g., Volume, RSI, TimeOfDay)", font=('Helvetica', 8)).grid(row=4, column=1, padx=10, pady=(0,5), sticky='w')

        self.run_button = ttk.Button(main_frame, text="Generate Script", command=self.start_generation)
        self.run_button.grid(row=5, column=0, columnspan=2, pady=10)

        log_frame = ttk.LabelFrame(main_frame, text="Status Log")
        log_frame.grid(row=6, column=0, columnspan=2, sticky="ew", padx=10)
        self.log_widget = scrolledtext.ScrolledText(log_frame, state='disabled', height=5, wrap=tk.WORD)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.back_button = ttk.Button(main_frame, text="< Back to Master", command=self.on_closing)
        self.back_button.grid(row=7, column=0, columnspan=2, pady=(10, 0))

        main_frame.grid_columnconfigure(1, weight=1)

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
        if self.generation_thread and self.generation_thread.is_alive():
            if not messagebox.askokcancel("Quit?", "A script is being generated. Are you sure you want to quit?"): return
        self.destroy()
        if hasattr(self, 'master_app') and self.master_app: self.master_app.deiconify()

    def generation_logic(self, params):
        log_callback = lambda msg: self.update_log(msg)
        try:
            success, message = create_strategy_file(*params, status_callback=log_callback)
            if success:
                self.after(0, lambda p=message: self.on_success(p))
            else:
                self.after(0, lambda m=message: messagebox.showerror("Generation Failed", m))
        except Exception as e:
            log_callback(f"\nFATAL ERROR: {e}")
            self.after(0, lambda err=e: messagebox.showerror("Critical Error", f"An unexpected error occurred:\n{err}"))
        finally:
            self.after(0, self.on_task_complete)
            
    def on_success(self, file_path):
        messagebox.showinfo("Success", f"Strategy template created successfully!\n\nLocation:\n{file_path}")

    def start_generation(self):
        params = (self.name_var.get(), self.tf_var.get(), self.session_var.get(), self.filters_var.get())
        if not params[0]:
            messagebox.showerror("Input Error", "Strategy Name cannot be empty."); return

        self.run_button.config(text="Generating...", state="disabled")
        self.log_widget.config(state='normal'); self.log_widget.delete('1.0', tk.END); self.log_widget.config(state='disabled')
        
        self.generation_thread = threading.Thread(target=self.generation_logic, args=(params,), daemon=True)
        self.generation_thread.start()

    def on_task_complete(self):
        try:
            self.run_button.config(text="Generate Script", state="normal")
            self.generation_thread = None
        except TclError: pass