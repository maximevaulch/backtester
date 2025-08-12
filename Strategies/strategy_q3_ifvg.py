# In Strategies/strategy_q3_ifvg.py
import pandas as pd
import numpy as np
from datetime import time

# --- Strategy Metadata ---
STRATEGY_TIMEFRAME = '1min'
SESSION_TYPE = 'fixed' # Sessions are at fixed times, not optional
AVAILABLE_FILTERS = []

def generate_conditions(df: pd.DataFrame, strategy_params: dict = {}) -> pd.DataFrame:
    """
    Calculates signal conditions for the "Killzone Sweep + iFVG" strategy.
    This implementation uses a stateful loop to process candles sequentially,
    as the strategy's logic is highly dependent on the order of events within a day.
    """
    print(f"--- Calculating conditions for Q3_iFVG Strategy ---")

    if 'ny_time' not in df.columns:
        raise ValueError("'ny_time' column required for strategy execution.")

    tf = STRATEGY_TIMEFRAME
    open_col, high_col, low_col, close_col = f'open_{tf}', f'high_{tf}', f'low_{tf}', f'close_{tf}'
    required_cols = [open_col, high_col, low_col, close_col]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Required columns for '{tf}' timeframe not found in DataFrame.")

    # --- Time Definitions (America/New_York) from Blueprint ---
    KZ1_START, KZ1_END = time(1, 30), time(2, 14, 59)
    TZ1_START_CHECK, TZ1_START, TZ1_END = time(2, 16), time(2, 16), time(2, 38, 59)
    KZ2_START, KZ2_END = time(3, 0), time(3, 44, 59)
    TZ2_START_CHECK, TZ2_START, TZ2_END = time(3, 46), time(3, 46), time(4, 8, 59)

    results = []
    is_new_candle = df[open_col].ne(df[open_col].shift())
    candles_to_process = df[is_new_candle]

    current_day = None
    kz1_high, kz1_low, kz2_high, kz2_low = np.nan, np.nan, np.nan, np.nan
    kz1_long_setup_valid, kz1_short_setup_valid = True, True
    kz1_low_sweep, kz1_high_sweep = False, False
    kz2_long_setup_valid, kz2_short_setup_valid = True, True
    kz2_low_sweep, kz2_high_sweep = False, False
    last_bullish_fvg, last_bearish_fvg = None, None

    print("Processing candles sequentially...")
    for i, row in enumerate(candles_to_process.itertuples()):
        ny_date = row.ny_time.date()
        current_time = row.ny_time.time()

        if ny_date != current_day:
            current_day = ny_date
            kz1_high, kz1_low, kz2_high, kz2_low = np.nan, np.nan, np.nan, np.nan
            kz1_long_setup_valid, kz1_short_setup_valid = True, True
            kz1_low_sweep, kz1_high_sweep = False, False
            kz2_long_setup_valid, kz2_short_setup_valid = True, True
            kz2_low_sweep, kz2_high_sweep = False, False
            last_bullish_fvg, last_bearish_fvg = None, None

        row_high, row_low, row_open, row_close = getattr(row, high_col), getattr(row, low_col), getattr(row, open_col), getattr(row, close_col)

        if i >= 2:
            prev_row2 = candles_to_process.iloc[i-2]
            if row_low > prev_row2[high_col]: last_bullish_fvg = {'bottom': prev_row2[high_col], 'start_idx': prev_row2.name}
            if row_high < prev_row2[low_col]: last_bearish_fvg = {'top': prev_row2[low_col], 'start_idx': prev_row2.name}

        if KZ1_START <= current_time <= KZ1_END:
            kz1_high = max(kz1_high, row_high) if not np.isnan(kz1_high) else row_high
            kz1_low = min(kz1_low, row_low) if not np.isnan(kz1_low) else row_low

        if current_time == TZ1_START_CHECK and not np.isnan(kz1_high):
            if row_open > kz1_high: kz1_short_setup_valid = False
            if row_open < kz1_low:  kz1_long_setup_valid = False

        if TZ1_START <= current_time <= TZ1_END and not np.isnan(kz1_high):
            if not kz1_high_sweep and row_high > kz1_high: kz1_high_sweep = True
            if not kz1_low_sweep and row_low < kz1_low:   kz1_low_sweep = True

            if (kz1_long_setup_valid and kz1_low_sweep and last_bearish_fvg and row_close > last_bearish_fvg['top']):
                sl_range_df = df.loc[last_bearish_fvg['start_idx']:row.Index]
                sl_price = sl_range_df[low_col].min()
                results.append({'timestamp': row.Index, 'is_bullish': True, 'is_bearish': False, 'entry_price': row_close, 'sl_price_long': sl_price})
                kz1_low_sweep = False
            
            if (kz1_short_setup_valid and kz1_high_sweep and last_bullish_fvg and row_close < last_bullish_fvg['bottom']):
                sl_range_df = df.loc[last_bullish_fvg['start_idx']:row.Index]
                sl_price = sl_range_df[high_col].max()
                results.append({'timestamp': row.Index, 'is_bullish': False, 'is_bearish': True, 'entry_price': row_close, 'sl_price_short': sl_price})
                kz1_high_sweep = False

        if KZ2_START <= current_time <= KZ2_END:
            kz2_high = max(kz2_high, row_high) if not np.isnan(kz2_high) else row_high
            kz2_low = min(kz2_low, row_low) if not np.isnan(kz2_low) else row_low

        if current_time == TZ2_START_CHECK and not np.isnan(kz2_high):
            if row_open > kz2_high: kz2_short_setup_valid = False
            if row_open < kz2_low:  kz2_long_setup_valid = False

        if TZ2_START <= current_time <= TZ2_END and not np.isnan(kz2_high):
            if not kz2_high_sweep and row_high > kz2_high: kz2_high_sweep = True
            if not kz2_low_sweep and row_low < kz2_low:   kz2_low_sweep = True

            if (kz2_long_setup_valid and kz2_low_sweep and last_bearish_fvg and row_close > last_bearish_fvg['top']):
                sl_range_df = df.loc[last_bearish_fvg['start_idx']:row.Index]
                sl_price = sl_range_df[low_col].min()
                results.append({'timestamp': row.Index, 'is_bullish': True, 'is_bearish': False, 'entry_price': row_close, 'sl_price_long': sl_price})
                kz2_low_sweep = False
            
            if (kz2_short_setup_valid and kz2_high_sweep and last_bullish_fvg and row_close < last_bullish_fvg['bottom']):
                sl_range_df = df.loc[last_bullish_fvg['start_idx']:row.Index]
                sl_price = sl_range_df[high_col].max()
                results.append({'timestamp': row.Index, 'is_bullish': False, 'is_bearish': True, 'entry_price': row_close, 'sl_price_short': sl_price})
                kz2_high_sweep = False

    # --- THIS IS THE FIX ---
    # Handle the case where no signals were found for the given day.
    if not results:
        print("--- No Q3_iFVG signals found for this period. ---")
        conditions_df = pd.DataFrame(index=df.index)
        # Create a properly structured but empty DataFrame
        for col in ['base_pattern_cond', 'is_bullish', 'is_bearish', 'session_cond']:
            conditions_df[col] = False
        for col in ['entry_price', 'sl_price_long', 'sl_price_short']:
            conditions_df[col] = np.nan
        return conditions_df
    # --- END OF FIX ---

    signals_df = pd.DataFrame(results).set_index('timestamp')
    conditions_df = pd.DataFrame(index=df.index)
    
    # Fill missing columns with default values before reindexing
    if 'sl_price_long' not in signals_df.columns: signals_df['sl_price_long'] = np.nan
    if 'sl_price_short' not in signals_df.columns: signals_df['sl_price_short'] = np.nan

    conditions_df['is_bullish'] = signals_df['is_bullish'].reindex(df.index, fill_value=False)
    conditions_df['is_bearish'] = signals_df['is_bearish'].reindex(df.index, fill_value=False)
    conditions_df['base_pattern_cond'] = conditions_df['is_bullish'] | conditions_df['is_bearish']
    
    conditions_df['entry_price'] = signals_df['entry_price'].reindex(df.index)
    conditions_df['sl_price_long'] = signals_df['sl_price_long'].reindex(df.index)
    conditions_df['sl_price_short'] = signals_df['sl_price_short'].reindex(df.index)
    
    df_ny_time = df['ny_time'].dt.time
    is_in_tz1 = (df_ny_time >= TZ1_START) & (df_ny_time <= TZ1_END)
    is_in_tz2 = (df_ny_time >= TZ2_START) & (df_ny_time <= TZ2_END)
    conditions_df['session_cond'] = is_in_tz1 | is_in_tz2
    
    print(f"--- Found {len(signals_df)} potential Q3_iFVG signals. ---")
    return conditions_df