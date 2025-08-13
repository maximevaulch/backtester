# In Core/main.py
import pandas as pd
import numpy as np
import os
import sys
from itertools import chain, combinations
from datetime import time
import json

def get_project_root():
    """Gets the project root, handling both script and frozen exe."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

project_root = get_project_root()

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Core.data_handler import load_unified_data
from Core.backtester import run_r_backtest
from Core.analysis import get_performance_stats, export_scenario_to_excel
from Core.strategy_base import BaseStrategy

def load_asset_config():
    """Loads asset configuration from the JSON file."""
    config_path = os.path.join(project_root, 'config', 'asset_config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Asset configuration file not found at {config_path}")
        return {}
    except json.JSONDecodeError:
        print(f"ERROR: Could not decode asset configuration file at {config_path}")
        return {}

ASSET_CONFIG = load_asset_config()

def get_unique_filename(path):
    if not os.path.exists(path): return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base}_{i}{ext}"): i += 1
    return f"{base}_{i}{ext}"

def _prepare_data(asset_name: str, ny_start_date: str, ny_end_date: str, warmup_days: int, log: callable) -> pd.DataFrame:
    """
    Loads and prepares the historical data for backtesting.
    Includes a warm-up period before the official start date.
    """
    log(f"-> Loading and preparing data for {asset_name}...")
    full_unified_df = load_unified_data(asset_name=asset_name)
    if full_unified_df.empty:
        log("!!! ERROR: Unified data is empty. Aborting. !!!")
        return pd.DataFrame()

    full_unified_df['ny_time'] = full_unified_df.index.tz_convert('America/New_York')

    user_start_dt = pd.to_datetime(ny_start_date).date()
    user_end_dt = pd.to_datetime(ny_end_date).date()
    analysis_start_dt = user_start_dt - pd.Timedelta(days=warmup_days)

    log(f"-> Using warm-up period of {warmup_days} days. Analysis starts from: {analysis_start_dt.strftime('%Y-%m-%d')}")
    
    analysis_df = full_unified_df[full_unified_df['ny_time'].dt.date.between(analysis_start_dt, user_end_dt)].copy()
    
    if analysis_df.empty:
        log("!!! ERROR: No data available for the specified date range including warm-up. !!!")
        return pd.DataFrame()
        
    return analysis_df

def _generate_signals(strategy_instance: BaseStrategy, analysis_df: pd.DataFrame, strategy_params: dict, log: callable) -> pd.DataFrame:
    """
    Generates trading signals using the provided strategy instance.
    """
    log(f"-> Using strategy: {strategy_instance.__class__.__name__}")
    conditions_df = strategy_instance.generate_conditions(analysis_df.copy(), strategy_params=strategy_params)
    return conditions_df

def _run_backtest_scenarios(analysis_df: pd.DataFrame, conditions_df: pd.DataFrame, rr_scenarios: list,
                            selected_filters: list, strategy_instance: BaseStrategy, asset_name: str,
                            ny_start_date: str, ny_end_date: str,
                            allow_multiple_trades: bool, log: callable, status_callback: callable) -> dict:
    """
    Runs backtests for all combinations of filters and R:R scenarios.
    """
    all_scenario_results = {}
    filter_combinations = list(chain.from_iterable(combinations(selected_filters, r) for r in range(len(selected_filters) + 1)))
    user_start_dt = pd.to_datetime(ny_start_date).date()
    user_end_dt = pd.to_datetime(ny_end_date).date()

    for filt_combo in filter_combinations:
        # 1. Combine base conditions with selected filters
        final_mask = conditions_df['base_pattern_cond']
        if 'session_cond' in conditions_df.columns:
            final_mask &= conditions_df['session_cond']

        combo_name = "Base" if not filt_combo else "+".join(filt_combo)
        for filt in filt_combo:
            final_mask &= conditions_df[f'filter_{filt}']

        # 2. Filter signals to the start of each new strategy candle
        signal_tf = strategy_instance.STRATEGY_TIMEFRAME
        open_col = f'open_{signal_tf}'
        if open_col in analysis_df.columns:
            is_new_signal_candle = analysis_df[open_col].ne(analysis_df[open_col].shift(1))
            final_mask &= is_new_signal_candle

        # 3. Create signal DataFrame
        long_signal_mask = final_mask & conditions_df['is_bullish']
        short_signal_mask = final_mask & conditions_df['is_bearish']

        signals_df = pd.DataFrame(index=analysis_df.index)
        signals_df['signal'] = 0
        signals_df.loc[long_signal_mask, 'signal'] = 1
        signals_df.loc[short_signal_mask, 'signal'] = -1
        signals_df['entry_price'] = np.where(long_signal_mask, conditions_df['entry_price'], np.where(short_signal_mask, conditions_df['entry_price'], np.nan))
        signals_df['sl_price'] = np.where(long_signal_mask, conditions_df['sl_price_long'], np.where(short_signal_mask, conditions_df['sl_price_short'], np.nan))

        # 4. Merge signals into the main analysis DataFrame
        analysis_df['signal'] = signals_df['signal']
        analysis_df['entry_price'] = signals_df['entry_price']
        analysis_df['sl_price'] = signals_df['sl_price']
        
        # 5. Run backtest for each R:R scenario
        backtest_df = analysis_df[analysis_df['ny_time'].dt.date.between(user_start_dt, user_end_dt)].copy()
        execution_timeframe = ASSET_CONFIG[asset_name]['base_tf']

        for rr_scenario in rr_scenarios:
            scenario_name = f"{combo_name}_{rr_scenario['rr']:.1f}R" + ("_BE" if rr_scenario['use_be'] else "")
            log(f"  - Simulating: {scenario_name}")
            
            trades_df = run_r_backtest(
                df=backtest_df,
                risk_reward_ratio=rr_scenario['rr'],
                use_breakeven=rr_scenario['use_be'],
                breakeven_trigger_r=rr_scenario['be_trigger_r'],
                execution_timeframe=execution_timeframe,
                allow_multiple_trades=allow_multiple_trades,
                status_callback=status_callback
            )
            
            overall, monthly, daily = get_performance_stats(trades_df)
            all_scenario_results[scenario_name] = {'trades': trades_df, 'overall': overall, 'monthly': monthly, 'daily': daily}

    return all_scenario_results

def _generate_excel_report(all_scenario_results: dict, strategy_name: str, asset_name: str,
                           ny_start_date: str, ny_end_date: str, log: callable) -> str:
    """
    Generates a summary and detailed Excel report from backtest results.
    """
    log("\n-> Generating Excel report...")
    summary_data = []
    for name, results in all_scenario_results.items():
        overall_df = results['overall']
        if overall_df is not None and not overall_df.empty:
            summary_data.append({
                'Scenario': name,
                'Wins': overall_df.loc[overall_df['Metric'] == 'Winners', 'Value'].iloc[0],
                'Losses': overall_df.loc[overall_df['Metric'] == 'Losers', 'Value'].iloc[0],
                'Break-Evens': overall_df.loc[overall_df['Metric'] == 'Break-Evens', 'Value'].iloc[0],
                'Win Rate %': overall_df.loc[overall_df['Metric'] == 'Win Rate (W/(W+L)) %', 'Value'].iloc[0],
                'Total R Gain': overall_df.loc[overall_df['Metric'] == 'Total R Gain', 'Value'].iloc[0]
            })
        else:
            summary_data.append({'Scenario': name, 'Wins': 0, 'Losses': 0, 'Break-Evens': 0, 'Win Rate %': 'N/A', 'Total R Gain': '0.00R'})
    
    summary_df = pd.DataFrame(summary_data)
    results_folder = os.path.join(project_root, "Results")
    os.makedirs(results_folder, exist_ok=True)

    base_filename = os.path.join(results_folder, f"{strategy_name}_{asset_name}_{ny_start_date}_to_{ny_end_date}.xlsx")
    excel_filename = get_unique_filename(base_filename)

    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Scenarios Summary', index=False)
        for name, results in all_scenario_results.items():
            export_scenario_to_excel(writer, name, results['trades'], results['overall'], results['monthly'], results['daily'])

    log(f"-> Report saved: {excel_filename}")
    return excel_filename

def run_full_backtest(asset_name: str, ny_start_date: str, ny_end_date: str, rr_scenarios: list,
                      strategy_instance: BaseStrategy, strategy_params: dict = {}, selected_filters: list = [],
                      allow_multiple_trades: bool = False, status_callback: callable = None):
    """
    Orchestrates the entire backtesting process for a given asset and strategy.
    """
    def log(message):
        if status_callback:
            status_callback(message)
        else:
            print(message)

    log(f"\n=======================================================")
    log(f" PROCESSING ASSET: {asset_name} | NY Dates: {ny_start_date} to {ny_end_date}")
    log(f"=======================================================")

    # 1. Load and prepare data
    analysis_df = _prepare_data(asset_name, ny_start_date, ny_end_date, warmup_days=90, log=log)
    if analysis_df.empty:
        return None

    # 2. Generate signals from strategy
    conditions_df = _generate_signals(strategy_instance, analysis_df, strategy_params, log)

    # 3. Run backtests for all scenarios
    all_scenario_results = _run_backtest_scenarios(
        analysis_df=analysis_df,
        conditions_df=conditions_df,
        rr_scenarios=rr_scenarios,
        selected_filters=selected_filters,
        strategy_instance=strategy_instance,
        asset_name=asset_name,
        ny_start_date=ny_start_date,
        ny_end_date=ny_end_date,
        allow_multiple_trades=allow_multiple_trades,
        log=log,
        status_callback=status_callback
    )

    # 4. Generate and save the final Excel report
    strategy_name = strategy_instance.__class__.__name__
    excel_filename = _generate_excel_report(
        all_scenario_results=all_scenario_results,
        strategy_name=strategy_name,
        asset_name=asset_name,
        ny_start_date=ny_start_date,
        ny_end_date=ny_end_date,
        log=log
    )

    log(f"\nFinished processing {asset_name}.")
    return excel_filename