# In Core/strategy_maker.py
import os
import re
import sys

def get_project_root():
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_filter_definitions(filters_list):
    if not filters_list:
        return "# No optional filters defined for this strategy."
    lines = [f"# Example for filter '{f}'\n    filter_{f}_cond = pd.Series(True, index=df_tf.index) # TODO: Implement logic for {f} filter" for f in filters_list]
    return "\n    ".join(lines)

def generate_filter_assignments(filters_list):
    if not filters_list: return ""
    return "\n    ".join([f"conditions_df['filter_{f}'] = filter_{f}_cond" for f in filters_list])

def generate_session_block(session_type):
    if session_type == 'fixed':
        return """
    # This strategy uses a fixed session. Define the start and end times here.
    SESSION_START = time(9, 30)
    SESSION_END = time(16, 0)
    
    df_tf['ny_time'] = df_tf.index.tz_convert('America/New_York').time
    session_cond = (df_tf['ny_time'] >= SESSION_START) & (df_tf['ny_time'] <= SESSION_END)
"""
    return """
    # This strategy uses an optional session filter. No `session_cond` is needed.
"""

def generate_session_assignment(session_type):
    if session_type == 'fixed':
        return "conditions_df['session_cond'] = session_cond"
    return "# No fixed session, so no 'session_cond' column is assigned."

def create_strategy_file(strategy_name: str, analysis_tf: str, session_type: str, filters_str: str, status_callback=None):
    def log(message):
        if status_callback: status_callback(message)
        else: print(message)

    project_root = get_project_root()
    log(f"-> Project root identified as: {project_root}")

    if not strategy_name or not re.match(r'^[a-zA-Z0-9_]+$', strategy_name):
        return False, "Invalid Strategy Name. Use only letters, numbers, and underscores."
    if not analysis_tf: return False, "Analysis Timeframe cannot be empty."
    if not session_type: return False, "Session Type cannot be empty."

    output_dir = os.path.join(project_root, 'Strategies', 'workinprogress')
    os.makedirs(output_dir, exist_ok=True)
    filename = f"strategy_{strategy_name}.py"
    full_path = os.path.join(output_dir, filename)
    log(f"-> Preparing to create file at: {full_path}")

    if os.path.exists(full_path):
        return False, f"File already exists: {full_path}"

    filters_list = [f.strip() for f in filters_str.split(',') if f.strip()]
    
    template = f"""# In Strategies/workinprogress/{filename}
import pandas as pd
import numpy as np
from datetime import time

# --- Strategy Metadata ---
STRATEGY_TIMEFRAME = '{analysis_tf}'
SESSION_TYPE = '{session_type}'
AVAILABLE_FILTERS = {str(filters_list)}

def generate_conditions(df: pd.DataFrame, strategy_params: dict = {{}}) -> pd.DataFrame:
    \"\"\"
    Calculates all potential signal conditions for the {strategy_name} strategy.
    \"\"\"
    print(f"-> Calculating all conditions for {strategy_name} Strategy...")
    
    tf = STRATEGY_TIMEFRAME
    open_col, high_col, low_col, close_col = f'open_{{tf}}', f'high_{{tf}}', f'low_{{tf}}', f'close_{{tf}}'
    
    is_new_candle_start = df[open_col].ne(df[open_col].shift(1))
    df_tf = df[is_new_candle_start].copy()
    
    # --- 1. Base Pattern Condition ---
    # TODO: Define the core entry condition.
    base_pattern_cond = pd.Series(False, index=df_tf.index)

    # --- 2. Optional Filters ---
    {generate_filter_definitions(filters_list)}
    
    # --- 3. Session Condition (if applicable) ---
    {generate_session_block(session_type)}

    # --- 4. Directional Information & Entry/SL Prices ---
    is_bullish = base_pattern_cond
    is_bearish = pd.Series(False, index=df_tf.index)
    
    entry_price = df_tf[open_col]
    sl_price_long = df_tf[low_col].shift(1)
    sl_price_short = df_tf[high_col].shift(1)
    
    # --- 5. Assemble the final DataFrame ---
    conditions_df = pd.DataFrame(index=df_tf.index)
    conditions_df['base_pattern_cond'] = base_pattern_cond
    {generate_filter_assignments(filters_list)}
    {generate_session_assignment(session_type)}
    
    conditions_df['is_bullish'] = is_bullish
    conditions_df['is_bearish'] = is_bearish
    conditions_df['entry_price'] = entry_price
    conditions_df['sl_price_long'] = sl_price_long
    conditions_df['sl_price_short'] = sl_price_short
    
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