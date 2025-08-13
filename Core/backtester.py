# In Core/backtester.py
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Trade:
    """A simple dataclass to hold the state of an active trade."""
    entry_price: float
    stop_loss: float
    take_profit: float
    breakeven_price: float
    direction: str
    entry_time: pd.Timestamp
    be_triggered: bool = False
    risk_per_share: float = field(init=False)

    def __post_init__(self):
        self.risk_per_share = abs(self.entry_price - self.stop_loss)

def run_r_backtest(df: pd.DataFrame, risk_reward_ratio: float, use_breakeven: bool,
                   breakeven_trigger_r: float, execution_timeframe: str,
                   allow_multiple_trades: bool = False, status_callback: Optional[callable] = None) -> pd.DataFrame:
    """
    Runs a risk-to-reward (R-multiple) based backtest on a given DataFrame of signals.

    This function iterates through the data candle by candle, manages active trades,
    and logs completed trades based on stop-loss, take-profit, or break-even rules.

    Args:
        df: DataFrame with price data and a 'signal' column (1 for long, -1 for short, 0 for no signal).
            Must also contain 'entry_price' and 'sl_price' for signals.
        risk_reward_ratio: The target R-multiple for take-profit (e.g., 2.0 for a 2:1 RR).
        use_breakeven: If True, moves the stop-loss to the entry price when the breakeven trigger is hit.
        breakeven_trigger_r: The R-multiple at which the break-even rule is activated.
        execution_timeframe: The timeframe of the OHLC data to use for checking trade exits (e.g., '30s', '1min').
        allow_multiple_trades: If True, allows multiple trades to be open simultaneously. If False, only one trade can be open at a time.
        status_callback: An optional function to call for logging status updates.

    Returns:
        A pandas DataFrame containing a log of all completed trades.
    """
    def log(message: str):
        if status_callback:
            status_callback(message)
        # No console fallback to avoid spamming from this low-level function

    exec_high_col, exec_low_col = f'high_{execution_timeframe}', f'low_{execution_timeframe}'
    if exec_high_col not in df.columns or exec_low_col not in df.columns:
        raise ValueError(f"Execution timeframe columns '{exec_high_col}' or '{exec_low_col}' not found in DataFrame.")

    trades_log: List[dict] = []
    active_trades: List[Trade] = []

    for row in df.itertuples():
        current_exec_high = getattr(row, exec_high_col)
        current_exec_low = getattr(row, exec_low_col)

        # --- Manage existing trades ---
        for trade in list(active_trades):
            # Check for break-even adjustment
            if use_breakeven and not trade.be_triggered:
                if (trade.direction == 'LONG' and current_exec_high >= trade.breakeven_price) or \
                   (trade.direction == 'SHORT' and current_exec_low <= trade.breakeven_price):
                    trade.stop_loss = trade.entry_price
                    trade.be_triggered = True

            # Check for exit conditions
            exit_reason, r_multiple, exit_price = None, 0.0, None
            if trade.direction == 'LONG':
                if current_exec_low <= trade.stop_loss:
                    exit_reason, exit_price = 'Stop Loss', trade.stop_loss
                    r_multiple = 0.0 if trade.be_triggered else -1.0
                elif current_exec_high >= trade.take_profit:
                    exit_reason, exit_price = 'Take Profit', trade.take_profit
                    r_multiple = risk_reward_ratio
            elif trade.direction == 'SHORT':
                if current_exec_high >= trade.stop_loss:
                    exit_reason, exit_price = 'Stop Loss', trade.stop_loss
                    r_multiple = 0.0 if trade.be_triggered else -1.0
                elif current_exec_low <= trade.take_profit:
                    exit_reason, exit_price = 'Take Profit', trade.take_profit
                    r_multiple = risk_reward_ratio

            if exit_reason:
                trades_log.append({
                    'Entry Time': trade.entry_time, 'Entry Price': trade.entry_price, 'Direction': trade.direction,
                    'Exit Time': row.Index, 'Exit Price': exit_price, 'Exit Reason': exit_reason, 'R-Multiple': r_multiple
                })
                active_trades.remove(trade)

        # --- Check for new trade entries ---
        can_open_new_trade = allow_multiple_trades or not active_trades
        if can_open_new_trade and row.signal != 0:
            entry_price, sl_price = row.entry_price, row.sl_price
            if pd.isna(entry_price) or pd.isna(sl_price) or abs(entry_price - sl_price) == 0:
                continue

            risk = abs(entry_price - sl_price)
            direction = 'LONG' if row.signal == 1 else 'SHORT'

            tp_price = entry_price + (risk * risk_reward_ratio) if direction == 'LONG' else entry_price - (risk * risk_reward_ratio)
            be_price = entry_price + (risk * breakeven_trigger_r) if direction == 'LONG' else entry_price - (risk * breakeven_trigger_r)

            new_trade = Trade(
                entry_price=entry_price,
                stop_loss=sl_price,
                take_profit=tp_price,
                breakeven_price=be_price,
                direction=direction,
                entry_time=row.Index
            )
            active_trades.append(new_trade)

    log(f"    -> Backtest Complete. Found {len(trades_log)} trades.")
    return pd.DataFrame(trades_log)