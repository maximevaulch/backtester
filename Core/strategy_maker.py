# In Core/strategy_maker.py
import os
import re
import sys
from typing import List, Optional, Tuple

def get_project_root() -> str:
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _generate_filter_definitions(filters_list: List[str]) -> str:
    """Generates placeholder code for optional filter conditions."""
    if not filters_list:
        return "        # No optional filters defined for this strategy."
    lines = [f"# TODO: Implement logic for the '{f}' filter.\n        filter_{f}_cond = pd.Series(True, index=df_tf.index)" for f in filters_list]
    return "\n\n        ".join(lines)

def _generate_filter_assignments(filters_list: List[str]) -> str:
    """Generates the DataFrame assignment lines for the filters."""
    if not filters_list: return ""
    return "\n        ".join([f"conditions_df['filter_{f}'] = filter_{f}_cond" for f in filters_list])

def _generate_session_logic(session_type: str) -> str:
    """Generates session handling logic based on the session type."""
    if session_type == 'optional':
        return """
        # This strategy uses an optional session filter provided by the backtester.
        # The 'session_cond' column will be automatically added by the backtester if a session is selected in the UI.
        # No specific logic is needed here.
        pass
        """
    return """
        # This strategy uses a fixed session. Define the start and end times here.
        SESSION_START = time(9, 30)
        SESSION_END = time(16, 0)

        # Ensure the ny_time column exists
        if 'ny_time' not in df_tf.columns:
            df_tf['ny_time'] = df_tf.index.tz_convert('America/New_York')

        df_tf_time = df_tf['ny_time'].dt.time
        conditions_df['session_cond'] = (df_tf_time >= SESSION_START) & (df_tf_time <= SESSION_END)
    """

def create_strategy_file(strategy_name: str, analysis_tf: str, session_type: str, filters_str: str, status_callback: Optional[callable] = None) -> Tuple[bool, str]:
    """
    Generates a new strategy file from a template based on user inputs.

    Args:
        strategy_name: The name for the new strategy (e.g., 'MyCoolStrategy').
        analysis_tf: The primary analysis timeframe (e.g., '15min', '1H').
        session_type: The session handling type ('optional' or 'fixed').
        filters_str: A comma-separated string of optional filter names.
        status_callback: An optional function for logging progress.

    Returns:
        A tuple containing:
        - bool: True on success, False on failure.
        - str: The full path to the new file or an error message.
    """
    def log(message: str):
        if status_callback: status_callback(message)
        else: print(message)

    project_root = get_project_root()
    log(f"-> Project root identified as: {project_root}")

    if not strategy_name or not re.match(r'^[a-zA-Z0-9_]+$', strategy_name):
        return False, "Invalid Strategy Name. Use only letters, numbers, and underscores."
    if not analysis_tf: return False, "Analysis Timeframe cannot be empty."
    if not session_type: return False, "Session Type cannot be empty."

    # Sanitize class name to be valid Python CamelCase
    class_name = f"Strategy{strategy_name.replace('_', ' ').title().replace(' ', '')}"
    filename = f"strategy_{strategy_name.lower()}.py"

    output_dir = os.path.join(project_root, 'Strategies')
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    log(f"-> Preparing to create file at: {full_path}")

    if os.path.exists(full_path):
        return False, f"File already exists: {full_path}"

    filters_list = [f.strip() for f in filters_str.split(',') if f.strip()]
    
    template = f"""# In {full_path}
import pandas as pd
import numpy as np
from datetime import time
from typing import List, Dict, Any
from Core.strategy_base import BaseStrategy

class {class_name}(BaseStrategy):
    \"\"\"
    A template for the '{strategy_name}' strategy.
    \"\"\"

    @property
    def STRATEGY_TIMEFRAME(self) -> str:
        return '{analysis_tf}'

    @property
    def SESSION_TYPE(self) -> str:
        return '{session_type}'

    @property
    def AVAILABLE_FILTERS(self) -> List[str]:
        return {str(filters_list)}

    def generate_conditions(self, df: pd.DataFrame, strategy_params: Dict[str, Any] = {{}}) -> pd.DataFrame:
        \"\"\"
        Calculates all potential signal conditions for the {strategy_name} strategy.
        \"\"\"
        print(f"-> Calculating all conditions for {strategy_name} Strategy...")

        tf = self.STRATEGY_TIMEFRAME
        open_col, high_col, low_col, close_col = f'open_{{tf}}', f'high_{{tf}}', f'low_{{tf}}', f'close_{{tf}}'

        # Work on a resampled DataFrame at the strategy's timeframe
        is_new_candle_start = df[open_col].ne(df[open_col].shift(1))
        df_tf = df[is_new_candle_start].copy()

        # --- 1. Base Pattern Condition ---
        # This is the core entry condition for your strategy.
        # TODO: Replace with your actual entry logic.
        base_pattern_cond = pd.Series(False, index=df_tf.index)

        # --- 2. Optional Filters ---
        {_generate_filter_definitions(filters_list)}

        # --- 3. Directional Information ---
        # Define which direction the base_pattern_cond applies to.
        # TODO: Refine directional logic if necessary.
        is_bullish = base_pattern_cond
        is_bearish = pd.Series(False, index=df_tf.index) # Example: Only long signals

        # --- 4. Entry and Stop Loss Prices ---
        # Define how entry and stop loss prices are calculated.
        # TODO: Adjust entry and SL price calculations as needed.
        entry_price = df_tf[open_col]
        sl_price_long = df_tf[low_col].shift(1)
        sl_price_short = df_tf[high_col].shift(1)

        # --- 5. Assemble the final DataFrame ---
        conditions_df = pd.DataFrame(index=df_tf.index)
        conditions_df['base_pattern_cond'] = base_pattern_cond
        {_generate_filter_assignments(filters_list)}

        # Session condition handling
        {_generate_session_logic(session_type)}

        conditions_df['is_bullish'] = is_bullish
        conditions_df['is_bearish'] = is_bearish
        conditions_df['entry_price'] = entry_price
        conditions_df['sl_price_long'] = sl_price_long
        conditions_df['sl_price_short'] = sl_price_short

        # Forward-fill the conditions to the original DataFrame's index
        return conditions_df.reindex(df.index, method='ffill').fillna(False)
"""
    try:
        with open(full_path, 'w') as f:
            f.write(template)
        log(f"-> Successfully created strategy template: {full_path}")
        return True, full_path
    except Exception as e:
        log(f"Error creating strategy file: {e}")
        return False, str(e)