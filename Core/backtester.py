# In Core/backtester.py
import pandas as pd
import numpy as np

def run_r_backtest(df: pd.DataFrame, risk_reward_ratio: float, use_breakeven: bool, 
                   breakeven_trigger_r: float, execution_timeframe: str,
                   allow_multiple_trades: bool = False, status_callback=None):
    
    def log(message):
        if status_callback:
            status_callback(message)
        # No console fallback to avoid spamming from this low-level function

    exec_high_col, exec_low_col = f'high_{execution_timeframe}', f'low_{execution_timeframe}'
    if exec_high_col not in df.columns or exec_low_col not in df.columns:
        raise ValueError(f"Execution timeframe columns '{exec_high_col}' or '{exec_low_col}' not found in DataFrame.")

    trades_log = []
    
    if allow_multiple_trades:
        active_trades = []
        for row in df.itertuples():
            current_exec_high = getattr(row, exec_high_col)
            current_exec_low = getattr(row, exec_low_col)
            for trade in list(active_trades):
                if use_breakeven and not trade['be_triggered']:
                    if (trade['direction'] == 'LONG' and current_exec_high >= trade['be_price']) or \
                       (trade['direction'] == 'SHORT' and current_exec_low <= trade['be_price']):
                        trade['sl'] = trade['entry']; trade['be_triggered'] = True
                
                exit_reason, r_multiple, exit_price = None, 0.0, None
                if (trade['direction'] == 'LONG' and current_exec_low <= trade['sl']):
                    exit_reason, exit_price = 'Stop Loss', trade['sl']; r_multiple = 0.0 if trade['be_triggered'] else -1.0
                elif (trade['direction'] == 'SHORT' and current_exec_high >= trade['sl']):
                    exit_reason, exit_price = 'Stop Loss', trade['sl']; r_multiple = 0.0 if trade['be_triggered'] else -1.0
                elif (trade['direction'] == 'LONG' and current_exec_high >= trade['tp']):
                    exit_reason, exit_price = 'Take Profit', trade['tp']; r_multiple = risk_reward_ratio
                elif (trade['direction'] == 'SHORT' and current_exec_low <= trade['tp']):
                    exit_reason, exit_price = 'Take Profit', trade['tp']; r_multiple = risk_reward_ratio
                
                if exit_reason:
                    trades_log.append({'Entry Time': trade['entry_time'], 'Entry Price': trade['entry'], 'Direction': trade['direction'], 'Exit Time': row.Index, 'Exit Price': exit_price, 'Exit Reason': exit_reason, 'R-Multiple': r_multiple})
                    active_trades.remove(trade)

            if row.signal != 0:
                entry_price, sl_price = row.entry_price, row.sl_price
                if pd.isna(entry_price) or pd.isna(sl_price) or abs(entry_price - sl_price) == 0: continue
                risk = abs(entry_price - sl_price)
                direction = 'LONG' if row.signal == 1 else 'SHORT'
                tp_price = entry_price + (risk * risk_reward_ratio) if direction == 'LONG' else entry_price - (risk * risk_reward_ratio)
                be_price = entry_price + (risk * breakeven_trigger_r) if direction == 'LONG' else entry_price - (risk * breakeven_trigger_r)
                active_trades.append({'entry': entry_price, 'sl': sl_price, 'tp': tp_price, 'be_price': be_price, 'direction': direction, 'entry_time': row.Index, 'be_triggered': False})

    else:
        in_position = False
        position_details = {}
        for row in df.itertuples():
            current_exec_high = getattr(row, exec_high_col)
            current_exec_low = getattr(row, exec_low_col)
            if in_position:
                if use_breakeven and not position_details['be_triggered']:
                    if (position_details['direction'] == 'LONG' and current_exec_high >= position_details['be_price']) or \
                       (position_details['direction'] == 'SHORT' and current_exec_low <= position_details['be_price']):
                        position_details['sl'] = position_details['entry']; position_details['be_triggered'] = True
                
                exit_reason, r_multiple, exit_price = None, 0.0, None
                if (position_details['direction'] == 'LONG' and current_exec_low <= position_details['sl']):
                    exit_reason, exit_price = 'Stop Loss', position_details['sl']; r_multiple = 0.0 if position_details['be_triggered'] else -1.0
                elif (position_details['direction'] == 'SHORT' and current_exec_high >= position_details['sl']):
                    exit_reason, exit_price = 'Stop Loss', position_details['sl']; r_multiple = 0.0 if position_details['be_triggered'] else -1.0
                elif (position_details['direction'] == 'LONG' and current_exec_high >= position_details['tp']):
                    exit_reason, exit_price = 'Take Profit', position_details['tp']; r_multiple = risk_reward_ratio
                elif (position_details['direction'] == 'SHORT' and current_exec_low <= position_details['tp']):
                    exit_reason, exit_price = 'Take Profit', position_details['tp']; r_multiple = risk_reward_ratio

                if exit_reason:
                    trades_log.append({'Entry Time': position_details['entry_time'], 'Entry Price': position_details['entry'], 'Direction': position_details['direction'], 'Exit Time': row.Index, 'Exit Price': exit_price, 'Exit Reason': exit_reason, 'R-Multiple': r_multiple})
                    in_position = False

            if not in_position and row.signal != 0:
                entry_price, sl_price = row.entry_price, row.sl_price
                if pd.isna(entry_price) or pd.isna(sl_price) or abs(entry_price - sl_price) == 0: continue
                risk = abs(entry_price - sl_price)
                direction = 'LONG' if row.signal == 1 else 'SHORT'
                tp_price = entry_price + (risk * risk_reward_ratio) if direction == 'LONG' else entry_price - (risk * risk_reward_ratio)
                be_price = entry_price + (risk * breakeven_trigger_r) if direction == 'LONG' else entry_price - (risk * breakeven_trigger_r)
                in_position = True
                position_details = {'entry': entry_price, 'sl': sl_price, 'tp': tp_price, 'be_price': be_price, 'direction': direction, 'entry_time': row.Index, 'be_triggered': False}

    log(f"    -> Backtest Complete. Found {len(trades_log)} trades.")
    return pd.DataFrame(trades_log)