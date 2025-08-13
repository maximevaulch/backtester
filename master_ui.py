# In master_ui.py
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import font as tkfont
import importlib
import sys
import os
import traceback
from dotenv import load_dotenv, find_dotenv

# --- Helper function to find the project root ---
def get_project_root() -> str:
    """
    Gets the project root directory, handling both standard script execution
    and execution as a bundled executable (e.g., PyInstaller).
    """
    if getattr(sys, 'frozen', False):
        # Running in a bundle
        return os.path.dirname(sys.executable)
    # Running in a normal Python environment
    return os.path.dirname(os.path.abspath(__file__))

# --- Add project root to sys.path for external modules ---
sys.path.insert(0, get_project_root())

# --- Editor Window for .env file ---
class EnvEditorUI(tk.Toplevel):
    """A Toplevel window for editing OANDA API credentials in the .env file."""
    def __init__(self, master: tk.Tk):
        """
        Initializes the EnvEditorUI window.

        Args:
            master: The parent tkinter window.
        """
        super().__init__(master)
        self.title("Edit API Credentials")
        self.geometry("400x180")
        self.transient(master)
        self.grab_set()
        
        main_frame = ttk.Frame(self, padding=15)
        main_frame.pack(fill="both", expand=True)

        entry_frame = ttk.Frame(main_frame)
        entry_frame.pack(fill="x", expand=True)
        
        ttk.Label(entry_frame, text="Access Token:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.token_var = tk.StringVar()
        ttk.Entry(entry_frame, textvariable=self.token_var, width=40).grid(row=0, column=1, sticky="ew")
        
        ttk.Label(entry_frame, text="Account ID:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.account_id_var = tk.StringVar()
        ttk.Entry(entry_frame, textvariable=self.account_id_var, width=40).grid(row=1, column=1, sticky="ew")

        ttk.Label(entry_frame, text="Environment:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.env_var = tk.StringVar()
        env_dropdown = ttk.Combobox(entry_frame, textvariable=self.env_var, values=["Practice", "Live"], state="readonly")
        env_dropdown.grid(row=2, column=1, sticky="w")

        entry_frame.grid_columnconfigure(1, weight=1)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)
        ttk.Button(button_frame, text="Save", command=self.save_env).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side="left", padx=5)

        self.load_existing_env()
        self.lift()
        self.focus_force()

    def load_existing_env(self):
        """Loads credentials from an existing .env file into the entry fields."""
        load_dotenv(find_dotenv())
        self.token_var.set(os.getenv("OANDA_ACCESS_TOKEN", ""))
        self.account_id_var.set(os.getenv("OANDA_ACCOUNT_ID", ""))
        env = os.getenv("OANDA_ENVIRONMENT", "practice")
        self.env_var.set("Live" if env.lower() == 'live' else "Practice")

    def save_env(self):
        """Saves the entered credentials to the .env file in the project root."""
        token = self.token_var.get().strip()
        account_id = self.account_id_var.get().strip()
        environment = 'live' if self.env_var.get() == "Live" else 'practice'
        if not token or not account_id:
            messagebox.showerror("Input Error", "Token and Account ID cannot be empty.", parent=self)
            return
        env_content = (
            f'OANDA_ACCESS_TOKEN="{token}"\n'
            f'OANDA_ACCOUNT_ID="{account_id}"\n'
            f'OANDA_ENVIRONMENT="{environment}"\n'
        )
        try:
            env_path = os.path.join(get_project_root(), '.env')
            with open(env_path, 'w') as f:
                f.write(env_content)
            messagebox.showinfo("Success", ".env file has been updated.", parent=self)
            self.destroy()
        except Exception as e:
            messagebox.showerror("File Error", f"Could not write to .env file:\n{e}", parent=self)

# --- Main Application Window ---
class MasterUI(tk.Tk):
    """The main application window that serves as the central control panel."""
    def __init__(self):
        """Initializes the MasterUI application window and its widgets."""
        super().__init__()
        self.title("Master Control Panel")
        self.geometry("550x340")
        
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill="both", expand=True)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_columnconfigure(1, weight=1)

        data_frame = ttk.LabelFrame(main_frame, text="Data Management", padding=15)
        data_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        data_frame.grid_columnconfigure(0, weight=1)
        
        ttk.Button(data_frame, text="Data Manager", command=self.launch_downloader).grid(row=0, column=0, pady=5, sticky="ew")
        ttk.Button(data_frame, text="Healer (Manual)", command=self.launch_healer).grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Button(data_frame, text="Resampler (Manual)", command=self.launch_resampler).grid(row=2, column=0, pady=5, sticky="ew")
        ttk.Button(data_frame, text="Final Check", command=self.launch_checker).grid(row=3, column=0, pady=5, sticky="ew")

        backtest_frame = ttk.LabelFrame(main_frame, text="Analysis & Backtesting", padding=15)
        backtest_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        backtest_frame.grid_columnconfigure(0, weight=1)

        ttk.Button(backtest_frame, text="Baseline Strategy Maker", command=self.launch_strategy_maker).grid(row=0, column=0, pady=5, sticky="ew")
        ttk.Button(backtest_frame, text="Backtester", command=self.launch_backtester).grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Button(backtest_frame, text="Visualizer", command=self.launch_visualizer).grid(row=2, column=0, pady=5, sticky="ew")

        # --- THIS IS THE RESTORED CLICKABLE LABEL ---
        hyperlink_font = tkfont.Font(self, family="Helvetica", size=10, underline=True)
        edit_env_label = ttk.Label(main_frame, text="Edit OANDA API Credentials", 
                                   font=hyperlink_font, cursor="hand2", foreground="blue")
        edit_env_label.grid(row=1, column=0, columnspan=2, pady=(10, 0))
        edit_env_label.bind("<Button-1>", self.open_env_editor)

    def open_env_editor(self, event=None):
        """Opens the .env file editor window."""
        editor = EnvEditorUI(master=self)
        editor.wait_window()

    def launch_app(self, module_name: str, class_name: str):
        """
        A generic function to launch a UI component from a module.

        It hides the master window, dynamically imports the specified module from the UI
        package, instantiates the class, and shows an error if it fails.

        Args:
            module_name: The name of the module file (e.g., 'downloader_ui').
            class_name: The name of the class to instantiate (e.g., 'DownloaderUI').
        """
        self.withdraw()
        try:
            full_module_name = f"UI.{module_name}"
            if full_module_name in sys.modules:
                importlib.reload(sys.modules[full_module_name])
            module = importlib.import_module(full_module_name)
            ui_class = getattr(module, class_name)
            app_instance = ui_class(master=self)
        except Exception as e:
            self.deiconify()
            error_details = traceback.format_exc()
            messagebox.showerror("Launch Error", f"Failed to open {class_name}.\n\nError: {e}\n\n{error_details}")

    def launch_downloader(self): self.launch_app('downloader_ui', 'DownloaderUI')
    def launch_healer(self): self.launch_app('healer_ui', 'HealerUI')
    def launch_resampler(self): self.launch_app('resampler_ui', 'ResamplerUI')
    def launch_checker(self): self.launch_app('final_data_check', 'AuditUI')
    def launch_backtester(self): self.launch_app('backtester_ui', 'BacktesterUI')
    def launch_visualizer(self): self.launch_app('visualizer_ui', 'VisualizerUI')
    def launch_strategy_maker(self): self.launch_app('strategy_maker_ui', 'StrategyMakerUI')

if __name__ == "__main__":
    app = MasterUI()
    app.mainloop()