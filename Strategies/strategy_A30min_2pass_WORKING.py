# In Strategies/strategy_A30min.py
import pandas as pd
import numpy as np
import os

# --- STRATEGY METADATA ---
STRATEGY_TIMEFRAME = '30min' # The timeframe for analysis and zone identification
SESSION_TYPE = 'optional'
AVAILABLE_FILTERS = []

# --- HELPER FUNCTION FOR FVG DETECTION ---
def find_fvg_near_index(df, origin_idx, fvg_type, tf_cols):
    high_col, low_col = tf_cols['high'], tf_cols['low']
    patterns_to_check = {
        "Origin is C2": (origin_idx - 1, origin_idx, origin_idx + 1),
        "Origin is C1": (origin_idx, origin_idx + 1, origin_idx + 2),
        "After Origin is C1": (origin_idx + 1, origin_idx + 2, origin_idx + 3)
    }
    for check_name, (c1_idx, c2_idx, c3_idx) in patterns_to_check.items():
        try:
            if c1_idx < 0 or c3_idx >= len(df): continue
            c1, c3 = df.iloc[c1_idx], df.iloc[c3_idx]
            if (fvg_type == 'BEARISH' and c1[low_col] > c3[high_col]) or \
               (fvg_type == 'BULLISH' and c1[high_col] < c3[low_col]):
                return check_name, {'c1_idx': c1_idx, 'c2_idx': c2_idx, 'c3_idx': c3_idx}
        except IndexError: continue
    return None, None

def generate_conditions(df: pd.DataFrame, strategy_params: dict = {}) -> pd.DataFrame:
    print(f"--- Running Strategy: 30m Order Block with FVG Refinement ---")

    # --- THIS IS THE FIX (Part 1): Make the entire DataFrame timezone-aware from the start ---
    if 'ny_time' not in df.columns:
        df['ny_time'] = df.index.tz_convert('America/New_York')

    # ===============================================================================================
    # PHASE 1: IDENTIFY TRADING ZONES USING THE 30-MINUTE TIMEFRAME
    # ===============================================================================================
    tf = STRATEGY_TIMEFRAME
    open_col, high_col, low_col, close_col = f'open_{tf}', f'high_{tf}', f'low_{tf}', f'close_{tf}'
    tf_cols = {'open': open_col, 'high': high_col, 'low': low_col, 'close': close_col}
    
    is_new_candle_start = df[open_col].ne(df[open_col].shift(1))
    df_30min = df[is_new_candle_start].copy()

    active_trading_zones = [] 
    
    bullish_legs_to_monitor, bearish_legs_to_monitor = [], []
    last_confirmed_vh, last_confirmed_vl = None, None
    g_lastStructureType = None
    anchorVL_trigger_high, activeLeg_low, activeLeg_low_idx = np.nan, np.nan, -1
    anchorVH_trigger_low, activeLeg_high, activeLeg_high_idx = np.nan, np.nan, -1

    for i, row in enumerate(df_30min.itertuples(name='Candle')):
        row_close = getattr(row, close_col)
        if not np.isnan(anchorVL_trigger_high):
            if np.isnan(activeLeg_low) or getattr(row, low_col) < activeLeg_low: activeLeg_low, activeLeg_low_idx = getattr(row, low_col), i
        if not np.isnan(anchorVH_trigger_low):
            if np.isnan(activeLeg_high) or getattr(row, high_col) > activeLeg_high: activeLeg_high, activeLeg_high_idx = getattr(row, high_col), i
        is_bearish, is_bullish = row_close < getattr(row, open_col), row_close > getattr(row, open_col)
        if is_bearish and g_lastStructureType != 'VL':
            if np.isnan(anchorVL_trigger_high): anchorVL_trigger_high, activeLeg_low, activeLeg_low_idx = getattr(row, high_col), getattr(row, low_col), i
            elif row_close < anchorVL_trigger_high: anchorVL_trigger_high = getattr(row, high_col)
        if is_bullish and g_lastStructureType != 'VH':
            if np.isnan(anchorVH_trigger_low): anchorVH_trigger_low, activeLeg_high, activeLeg_high_idx = getattr(row, low_col), getattr(row, high_col), i
            elif row_close > anchorVH_trigger_low: anchorVH_trigger_low = getattr(row, low_col)
        new_vl_confirmed = not np.isnan(anchorVL_trigger_high) and row_close > anchorVL_trigger_high
        new_vh_confirmed = not np.isnan(anchorVH_trigger_low) and row_close < anchorVH_trigger_low

        if new_vl_confirmed:
            new_point = {'price': activeLeg_low, 'idx': activeLeg_low_idx, 'time': row.Index}
            if last_confirmed_vh: bearish_legs_to_monitor.append({'leg_high': last_confirmed_vh['price'], 'leg_low': new_point['price'], 'origin_candle_idx': new_point['idx'], 'is_violated': False})
            last_confirmed_vl = new_point; g_lastStructureType = 'VL'; anchorVL_trigger_high, activeLeg_low = np.nan, np.nan; anchorVH_trigger_low, activeLeg_high, activeLeg_high_idx = getattr(row, low_col), getattr(row, high_col), i
        if new_vh_confirmed:
            new_point = {'price': activeLeg_high, 'idx': activeLeg_high_idx, 'time': row.Index}
            if last_confirmed_vl: bullish_legs_to_monitor.append({'leg_low': last_confirmed_vl['price'], 'leg_high': new_point['price'], 'origin_candle_idx': new_point['idx'], 'is_violated': False})
            last_confirmed_vh = new_point; g_lastStructureType = 'VH'; anchorVH_trigger_low, activeLeg_high = np.nan, np.nan; anchorVL_trigger_high, activeLeg_low, activeLeg_low_idx = getattr(row, high_col), getattr(row, low_col), i

        for leg in bullish_legs_to_monitor:
            if not leg['is_violated'] and row_close < leg['leg_low']:
                leg['is_violated'] = True
                check_name, fvg_indices = find_fvg_near_index(df_30min, leg['origin_candle_idx'], 'BEARISH', tf_cols)
                if fvg_indices:
                    origin_candle = df_30min.iloc[leg['origin_candle_idx']]; fvg_c1 = df_30min.iloc[fvg_indices['c1_idx']]
                    if check_name == "Origin is C2": zone_low = fvg_c1[low_col]; zone_high = max(fvg_c1[high_col], origin_candle[high_col])
                    elif check_name == "Origin is C1": zone_low = origin_candle[low_col]; candle_before = df_30min.iloc[leg['origin_candle_idx']-1] if leg['origin_candle_idx'] > 0 else None; zone_high = max(origin_candle[high_col], candle_before[high_col]) if candle_before is not None else origin_candle[high_col]
                    else: zone_low = fvg_c1[low_col]; candle_before = df_30min.iloc[leg['origin_candle_idx']-1] if leg['origin_candle_idx'] > 0 else None; zone_high = max(fvg_c1[high_col], origin_candle[high_col], candle_before[high_col]) if candle_before is not None else max(fvg_c1[high_col], origin_candle[high_col])
                    mitigation_df = df_30min.iloc[fvg_indices['c2_idx'] + 1 : i]
                    if mitigation_df.empty or not (mitigation_df[high_col] >= zone_low).any():
                        active_trading_zones.append({'type': 'BEARISH', 'zone_low': zone_low, 'zone_high': zone_high, 'created_at': row.Index})

        for leg in bearish_legs_to_monitor:
            if not leg['is_violated'] and row_close > leg['leg_high']:
                leg['is_violated'] = True
                check_name, fvg_indices = find_fvg_near_index(df_30min, leg['origin_candle_idx'], 'BULLISH', tf_cols)
                if fvg_indices:
                    origin_candle = df_30min.iloc[leg['origin_candle_idx']]; fvg_c1 = df_30min.iloc[fvg_indices['c1_idx']]
                    if check_name == "Origin is C2": zone_high = fvg_c1[high_col]; zone_low = min(fvg_c1[low_col], origin_candle[low_col])
                    elif check_name == "Origin is C1": zone_high = origin_candle[high_col]; candle_before = df_30min.iloc[leg['origin_candle_idx'] - 1] if leg['origin_candle_idx'] > 0 else None; zone_low = min(origin_candle[low_col], candle_before[low_col]) if candle_before is not None else origin_candle[low_col]
                    else: zone_high = fvg_c1[high_col]; candle_before = df_30min.iloc[leg['origin_candle_idx']-1] if leg['origin_candle_idx'] > 0 else None; zone_low = min(fvg_c1[low_col], origin_candle[low_col], candle_before[low_col]) if candle_before is not None else min(fvg_c1[low_col], origin_candle[low_col])
                    mitigation_df = df_30min.iloc[fvg_indices['c2_idx'] + 1 : i]
                    if mitigation_df.empty or not (mitigation_df[low_col] <= zone_high).any():
                        active_trading_zones.append({'type': 'BULLISH', 'zone_low': zone_low, 'zone_high': zone_high, 'created_at': row.Index})

    print(f"Phase 1 complete. Identified {len(active_trading_zones)} potential trading zones.")

    # ===============================================================================================
    # PHASE 2: GENERATE TRADING SIGNALS
    # ===============================================================================================
    conditions_df = pd.DataFrame(index=df.index)
    conditions_df['base_pattern_cond'] = False
    conditions_df['is_bullish'] = False
    conditions_df['is_bearish'] = False
    conditions_df['entry_price'] = np.nan
    conditions_df['sl_price_long'] = np.nan
    conditions_df['sl_price_short'] = np.nan
    
    exec_high_col, exec_low_col = f'high_30s', f'low_30s'

    for row in df.itertuples():
        if not active_trading_zones: continue 
        for zone in list(active_trading_zones):
            # --- THIS IS THE FIX (Part 2): Ensure we don't trade a zone on the same 30min bar it was created ---
            if row.Index <= zone['created_at']:
                continue

            if zone['type'] == 'BEARISH' and getattr(row, exec_high_col) >= zone['zone_low']:
                conditions_df.at[row.Index, 'base_pattern_cond'] = True
                conditions_df.at[row.Index, 'is_bearish'] = True
                conditions_df.at[row.Index, 'entry_price'] = zone['zone_low']
                conditions_df.at[row.Index, 'sl_price_short'] = zone['zone_high']
                active_trading_zones.remove(zone) 
                break 
            
            elif zone['type'] == 'BULLISH' and getattr(row, exec_low_col) <= zone['zone_high']:
                conditions_df.at[row.Index, 'base_pattern_cond'] = True
                conditions_df.at[row.Index, 'is_bullish'] = True
                conditions_df.at[row.Index, 'entry_price'] = zone['zone_high']
                conditions_df.at[row.Index, 'sl_price_long'] = zone['zone_low']
                active_trading_zones.remove(zone)
                break
                
    print(f"Phase 2 complete. Generated {conditions_df['base_pattern_cond'].sum()} trade signals.")
    return conditions_df