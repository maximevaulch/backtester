# In Strategies/strategy_PR.py
import pandas as pd
import numpy as np
from datetime import time
from typing import Optional

# --- STRATEGY METADATA ---
STRATEGY_TIMEFRAME = '15min'
SESSION_TYPE = 'optional'
AVAILABLE_FILTERS = ['Volume', 'Body']

def generate_conditions(df: pd.DataFrame, strategy_params: dict = {}) -> pd.DataFrame:
    """
    Calculates all potential signal conditions for the PR strategy.
    It does NOT generate a session_cond column, deferring session filtering to the backtester.
    """
    print(f"-> Calculating all conditions for PR Strategy (24/7 logic)...")
    
    tf = STRATEGY_TIMEFRAME
    open_col, high_col, low_col, close_col, volume_col = f'open_{tf}', f'high_{tf}', f'low_{tf}', f'close_{tf}', f'volume_{tf}'
    
    is_new_candle_start = df[open_col].ne(df[open_col].shift(1))
    df_15min = df[is_new_candle_start].copy()

    # Base Pattern Condition
    pattern_cond = (df_15min[high_col].shift(1) >= df_15min[high_col].shift(2)) & \
                   (df_15min[low_col].shift(1) <= df_15min[low_col].shift(2))
    
    # Filter: Volume Condition
    volume_cond = df_15min[volume_col].shift(1) > df_15min[volume_col].shift(2)
    
    # Filter: Body Condition
    signal_close = df_15min[close_col].shift(1)
    prev_open = df_15min[open_col].shift(2)
    prev_close = df_15min[close_col].shift(2)
    prev_body_top = pd.concat([prev_open, prev_close], axis=1).max(axis=1)
    prev_body_bottom = pd.concat([prev_open, prev_close], axis=1).min(axis=1)
    body_cond = ~((signal_close < prev_body_top) & (signal_close > prev_body_bottom))
        
    # Directional Information
    signal_open = df_15min[open_col].shift(1)
    is_bullish = signal_close > signal_open
    is_bearish = signal_close < signal_open
    
    # --- ASSEMBLE THE RESULTS DATAFRAME ---
    conditions_df = pd.DataFrame(index=df_15min.index)
    
    conditions_df['base_pattern_cond'] = pattern_cond
    conditions_df['filter_Volume'] = volume_cond
    conditions_df['filter_Body'] = body_cond
    # No 'session_cond' column is generated here
    
    conditions_df['is_bullish'] = is_bullish
    conditions_df['is_bearish'] = is_bearish
    conditions_df['entry_price'] = df_15min[open_col]
    conditions_df['sl_price_long'] = df_15min[low_col].shift(1)
    conditions_df['sl_price_short'] = df_15min[high_col].shift(1)

    # --- Session Condition ---
    session_start_str = strategy_params.get('session_start_str')
    session_end_str = strategy_params.get('session_end_str')

    if 'ny_time' not in df_15min.columns:
        df_15min['ny_time'] = df_15min.index.tz_convert('America/New_York')

    if session_start_str and session_end_str:
        start_time = time.fromisoformat(session_start_str)
        end_time = time.fromisoformat(session_end_str)

        df_15min_ny_time = df_15min['ny_time'].dt.time

        if start_time > end_time: # Overnight session
            session_mask = (df_15min_ny_time >= start_time) | (df_15min_ny_time <= end_time)
        else:
            session_mask = (df_15min_ny_time >= start_time) & (df_15min_ny_time <= end_time)

        conditions_df['session_cond'] = session_mask
        print(f"Applied session filter: {start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}")
    else:
        conditions_df['session_cond'] = True
        print("No session filter applied, running on all data.")

    final_df = conditions_df.reindex(df.index, method='ffill')
    
    # Ensure boolean columns are filled with False, not True, after ffill
    bool_cols = ['base_pattern_cond', 'filter_Volume', 'filter_Body', 'is_bullish', 'is_bearish', 'session_cond']
    for col in bool_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna(False)

    return final_df