# In Strategies/strategy_A30min.py
import pandas as pd
import numpy as np
import os
from datetime import time

# --- STRATEGY METADATA ---
STRATEGY_TIMEFRAME = '30min'
SESSION_TYPE = 'optional'
AVAILABLE_FILTERS = []

# --- DEBUGGING INFRASTRUCTURE ---
event_log = []

def log_event(timestamp, event_type, details, leg=None, zone=None):
    entry = {'timestamp': timestamp, 'event_type': event_type, 'details': details}
    if leg: entry.update({'leg_type': leg.get('type'), 'leg_low': leg.get('low'), 'leg_high': leg.get('high')})
    if zone: entry.update({'zone_low': zone.get('zone_low'), 'zone_high': zone.get('zone_high')})
    event_log.append(entry)

# --- HELPER FUNCTIONS ---
def find_fvg_near_index(df, origin_idx, fvg_type, tf_cols):
    high_col, low_col = tf_cols['high'], tf_cols['low']
    patterns_to_check = {"Origin is C2": (origin_idx - 1, origin_idx, origin_idx + 1),"Origin is C1": (origin_idx, origin_idx + 1, origin_idx + 2),"After Origin is C1": (origin_idx + 1, origin_idx + 2, origin_idx + 3)}
    for check_name, (c1_idx, c2_idx, c3_idx) in patterns_to_check.items():
        try:
            if c1_idx < 0 or c3_idx >= len(df): continue
            c1, c3 = df.iloc[c1_idx], df.iloc[c3_idx]
            if (fvg_type == 'BEARISH' and c1[low_col] > c3[high_col]) or (fvg_type == 'BULLISH' and c1[high_col] < c3[low_col]):
                return check_name, {'c1_idx': c1_idx, 'c2_idx': c2_idx, 'c3_idx': c3_idx}
        except IndexError: continue
    return None, None

def perform_zone_creation_logic(leg, violation_idx, df_30min, tf_cols, fvg_type, timestamp):
    origin_candle_idx = leg['origin_candle_idx']
    check_name, fvg_indices = find_fvg_near_index(df_30min, origin_candle_idx, fvg_type, tf_cols)
    if not fvg_indices:
        log_event(timestamp, 'FVG_CHECK_FAILED', f"No {fvg_type} FVG found near origin idx {origin_candle_idx}.", leg=leg)
        return None
    log_event(timestamp, 'FVG_CHECK_PASSED', f"Found {fvg_type} FVG via rule: '{check_name}'.", leg=leg)
    origin_candle = df_30min.iloc[origin_candle_idx]; fvg_c1 = df_30min.iloc[fvg_indices['c1_idx']]
    zone_low, zone_high = np.nan, np.nan; high_col, low_col = tf_cols['high'], tf_cols['low']
    if fvg_type == 'BEARISH':
        if check_name == "Origin is C2": zone_low = fvg_c1[low_col]; zone_high = max(fvg_c1[high_col], origin_candle[high_col])
        elif check_name == "Origin is C1": zone_low = origin_candle[low_col]; candle_before = df_30min.iloc[origin_candle_idx-1] if origin_candle_idx > 0 else None; zone_high = max(origin_candle[high_col], candle_before[high_col]) if candle_before is not None else origin_candle[high_col]
        else: zone_low = fvg_c1[low_col]; candle_before = df_30min.iloc[origin_candle_idx-1] if origin_candle_idx > 0 else None; zone_high = max(fvg_c1[high_col], origin_candle[high_col], candle_before[high_col]) if candle_before is not None else max(fvg_c1[high_col], origin_candle[high_col])
    else:
        if check_name == "Origin is C2": zone_high = fvg_c1[high_col]; zone_low = min(fvg_c1[low_col], origin_candle[low_col])
        elif check_name == "Origin is C1": zone_high = origin_candle[high_col]; candle_before = df_30min.iloc[origin_candle_idx-1] if origin_candle_idx > 0 else None; zone_low = min(origin_candle[low_col], candle_before[low_col]) if candle_before is not None else origin_candle[low_col]
        else: zone_high = fvg_c1[high_col]; candle_before = df_30min.iloc[origin_candle_idx-1] if origin_candle_idx > 0 else None; zone_low = min(fvg_c1[low_col], origin_candle[low_col], candle_before[low_col]) if candle_before is not None else min(fvg_c1[low_col], origin_candle[low_col])
    mitigation_check_df = df_30min.iloc[fvg_indices['c2_idx'] + 1 : violation_idx]
    is_mitigated = False
    if not mitigation_check_df.empty:
        if fvg_type == 'BEARISH': is_mitigated = (mitigation_check_df[high_col] >= zone_low).any()
        else: is_mitigated = (mitigation_check_df[low_col] <= zone_high).any()
    if is_mitigated:
        log_event(timestamp, 'ZONE_DISCARDED', "Reason: Zone was already mitigated.", leg=leg, zone={'zone_low': zone_low, 'zone_high': zone_high})
        return None
    log_event(timestamp, 'TRADING_ZONE_DEFINED', "Zone is fresh and confirmed.", leg=leg, zone={'zone_low': zone_low, 'zone_high': zone_high})
    return {'type': fvg_type, 'zone_low': zone_low, 'zone_high': zone_high}

def generate_conditions(df: pd.DataFrame, strategy_params: dict = {}) -> pd.DataFrame:
    global event_log
    event_log = []
    print(f"--- Running Strategy: 30m OB DEBUG VERSION (Single-Pass) ---")
    if 'ny_time' not in df.columns: df['ny_time'] = df.index.tz_convert('America/New_York')

    tf = STRATEGY_TIMEFRAME
    open_col, high_col, low_col, close_col = f'open_{tf}', f'high_{tf}', f'low_{tf}', f'close_{tf}'
    tf_cols = {'open': open_col, 'high': high_col, 'low': low_col, 'close': close_col}
    is_new_candle_start = df[open_col].ne(df[open_col].shift(1))
    df_30min = df[is_new_candle_start].copy()

    conditions_df = pd.DataFrame(index=df.index); conditions_df['base_pattern_cond'] = False; conditions_df['is_bullish'] = False; conditions_df['is_bearish'] = False
    conditions_df['entry_price'], conditions_df['sl_price_long'], conditions_df['sl_price_short'] = np.nan, np.nan, np.nan
    active_trading_zones, all_legs = [], []
    last_confirmed_vh, last_confirmed_vl, g_lastStructureType = None, None, None
    anchorVL_trigger_high, activeLeg_low, activeLeg_low_idx = np.nan, np.nan, -1
    anchorVH_trigger_low, activeLeg_high, activeLeg_high_idx = np.nan, np.nan, -1
    
    for i, row in enumerate(df_30min.itertuples(name='Candle')):
        row_close = getattr(row, close_col)
        # Find Swing Points & Form Legs
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
            if last_confirmed_vh:
                new_leg = {'type': 'BEARISH', 'high': last_confirmed_vh['price'], 'low': new_point['price'], 'origin_candle_idx': new_point['idx'], 'status': 'monitoring'}
                all_legs.append(new_leg); log_event(row.Index, 'LEG_FORMED', f"Bearish leg from {new_leg['high']:.5f} to {new_leg['low']:.5f}", leg=new_leg)
            last_confirmed_vl = new_point; g_lastStructureType = 'VL'; anchorVL_trigger_high, activeLeg_low = np.nan, np.nan; anchorVH_trigger_low, activeLeg_high, activeLeg_high_idx = getattr(row, low_col), getattr(row, high_col), i
        if new_vh_confirmed:
            new_point = {'price': activeLeg_high, 'idx': activeLeg_high_idx, 'time': row.Index}
            if last_confirmed_vl:
                new_leg = {'type': 'BULLISH', 'low': last_confirmed_vl['price'], 'high': new_point['price'], 'origin_candle_idx': new_point['idx'], 'status': 'monitoring'}
                all_legs.append(new_leg); log_event(row.Index, 'LEG_FORMED', f"Bullish leg from {new_leg['low']:.5f} to {new_leg['high']:.5f}", leg=new_leg)
            last_confirmed_vh = new_point; g_lastStructureType = 'VH'; anchorVH_trigger_low, activeLeg_high = np.nan, np.nan; anchorVL_trigger_high, activeLeg_low, activeLeg_low_idx = getattr(row, high_col), getattr(row, low_col), i

        # Process Leg Statuses
        for leg in all_legs:
            if leg['status'] == 'pending_fvg_check' and i >= leg['check_at_idx']:
                log_event(row.Index, 'PENDING_CHECK_EXECUTED', "Executing delayed FVG check.", leg=leg)
                zone_info = perform_zone_creation_logic(leg, leg['violation_idx'], df_30min, tf_cols, 'BEARISH' if leg['type'] == 'BULLISH' else 'BULLISH', row.Index)
                if zone_info: active_trading_zones.append(zone_info)
                leg['status'] = 'processed'
            elif leg['status'] == 'monitoring':
                is_violated = (leg['type'] == 'BULLISH' and row_close < leg['low']) or (leg['type'] == 'BEARISH' and row_close > leg['high'])
                if is_violated:
                    log_event(row.Index, 'LEG_VIOLATED', f"Leg violated by close price {row_close:.5f}.", leg=leg)
                    violation_idx, origin_idx = i, leg['origin_candle_idx']; idx_diff = violation_idx - origin_idx
                    if idx_diff >= 3:
                        log_event(row.Index, 'FVG_CHECK_TRIGGERED', f"Violation distance is {idx_diff} (>=3), checking immediately.", leg=leg)
                        zone_info = perform_zone_creation_logic(leg, violation_idx, df_30min, tf_cols, 'BEARISH' if leg['type'] == 'BULLISH' else 'BULLISH', row.Index)
                        if zone_info: active_trading_zones.append(zone_info)
                        leg['status'] = 'processed'
                    else:
                        leg['status'] = 'pending_fvg_check'; leg['violation_idx'] = violation_idx
                        leg['check_at_idx'] = origin_idx + 2 if idx_diff == 1 else origin_idx + 3
                        log_event(row.Index, 'FVG_CHECK_DELAYED', f"Delaying check until index {leg['check_at_idx']}.", leg=leg)
        
        # Check for Entries within the current 30min candle's scope
        start_time = row.Index
        end_time = df_30min.index[i+1] if i + 1 < len(df_30min) else df.index[-1]
        exec_df = df[(df.index >= start_time) & (df.index < end_time)]
        if not exec_df.empty and active_trading_zones:
            exec_high_col, exec_low_col = f'high_30s', f'low_30s'
            for exec_row in exec_df.itertuples():
                for zone in list(active_trading_zones):
                    entry_triggered = False
                    if zone['type'] == 'BEARISH' and getattr(exec_row, exec_high_col) >= zone['zone_low']:
                        conditions_df.at[exec_row.Index, 'base_pattern_cond'] = True; conditions_df.at[exec_row.Index, 'is_bearish'] = True
                        conditions_df.at[exec_row.Index, 'entry_price'] = zone['zone_low']; conditions_df.at[exec_row.Index, 'sl_price_short'] = zone['zone_high']
                        entry_triggered = True
                    elif zone['type'] == 'BULLISH' and getattr(exec_row, exec_low_col) <= zone['zone_high']:
                        conditions_df.at[exec_row.Index, 'base_pattern_cond'] = True; conditions_df.at[exec_row.Index, 'is_bullish'] = True
                        conditions_df.at[exec_row.Index, 'entry_price'] = zone['zone_high']; conditions_df.at[exec_row.Index, 'sl_price_long'] = zone['zone_low']
                        entry_triggered = True
                    if entry_triggered:
                        log_event(exec_row.Index, 'ENTRY_SIGNAL_GENERATED', f"Entry triggered on {zone['type']} zone.", zone=zone)
                        active_trading_zones.remove(zone); break
    
    if event_log:
        log_columns = ['timestamp', 'event_type', 'details', 'leg_type', 'leg_low', 'leg_high', 'zone_low', 'zone_high']
        debug_df = pd.DataFrame(event_log, columns=log_columns).sort_values(by='timestamp').reset_index(drop=True)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        debug_file_path = os.path.join(project_root, 'debug_30mOB_full_trace.csv')
        debug_df.to_csv(debug_file_path, index=False)
        print(f"--- STRATEGY DEBUG LOG SAVED TO: {debug_file_path} ---")

    # --- Session Condition ---
    session_start_str = strategy_params.get('session_start_str')
    session_end_str = strategy_params.get('session_end_str')

    if session_start_str and session_end_str:
        start_time = time.fromisoformat(session_start_str)
        end_time = time.fromisoformat(session_end_str)

        df_ny_time = df['ny_time'].dt.time

        if start_time > end_time: # Overnight session
            conditions_df['session_cond'] = (df_ny_time >= start_time) | (df_ny_time <= end_time)
        else:
            conditions_df['session_cond'] = (df_ny_time >= start_time) & (df_ny_time <= end_time)

        print(f"Applied session filter: {start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}")
    else:
        conditions_df['session_cond'] = True
        print("No session filter applied, running on all data.")

    print(f"Strategy analysis complete. Generated {conditions_df['base_pattern_cond'].sum()} trade signals.")
    return conditions_df