# In Core/main.py
import pandas as pd
import numpy as np
import os
import sys
from itertools import chain, combinations
from datetime import time

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

ASSET_CONFIG = {
    'EUR_USD': {'base_tf': '30s'},
    'GBP_USD': {'base_tf': '30s'},
    'XAU_USD': {'base_tf': '30s'},
    'USD_JPY': {'base_tf': '30s'},
    'USD_CAD': {'base_tf': '30s'},
}

def get_unique_filename(path):
    if not os.path.exists(path): return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base}_{i}{ext}"): i += 1
    return f"{base}_{i}{ext}"

def run_full_backtest(asset_name, ny_start_date, ny_end_date, rr_scenarios, 
                      strategy_module, strategy_params={}, selected_filters=[],
                      allow_multiple_trades=False, status_callback=None):
    
    def log(message):
        if status_callback:
            status_callback(message)
        else:
            print(message)

    log(f"\n=======================================================")
    log(f" PROCESSING ASSET: {asset_name} | NY Dates: {ny_start_date} to {ny_end_date}")
    log(f"=======================================================")

    full_unified_df = load_unified_data(asset_name=asset_name)
    if full_unified_df.empty:
        log("!!! ERROR: Unified data is empty. Aborting. !!!")
        return

    full_unified_df['ny_time'] = full_unified_df.index.tz_convert('America/New_York')

    warmup_days = 90
    user_start_dt = pd.to_datetime(ny_start_date).date()
    user_end_dt = pd.to_datetime(ny_end_date).date()
    analysis_start_dt = user_start_dt - pd.Timedelta(days=warmup_days)

    log(f"-> Using warm-up period of {warmup_days} days. Analysis starts from: {analysis_start_dt.strftime('%Y-%m-%d')}")
    
    analysis_df = full_unified_df[full_unified_df['ny_time'].dt.date.between(analysis_start_dt, user_end_dt)].copy()
    
    if analysis_df.empty:
        log("!!! ERROR: No data available for the specified date range including warm-up. !!!")
        return
        
    condition_calculator = getattr(strategy_module, 'generate_conditions')
    log(f"-> Using strategy: {strategy_module.__name__}")
    
    conditions_df = condition_calculator(analysis_df.copy(), strategy_params=strategy_params)
    
    filter_combinations = list(chain.from_iterable(combinations(selected_filters, r) for r in range(len(selected_filters) + 1)))
    
    all_scenario_results = {}

    for filt_combo in filter_combinations:
        final_mask = conditions_df['base_pattern_cond']
        if 'session_cond' in conditions_df.columns:
            final_mask &= conditions_df['session_cond']
            log("   - Applying strategy's internal (Fixed) session rules.")
        if not filt_combo:
            combo_name = "Base"
        else:
            combo_name = "+".join(filt_combo)
            for filt in filt_combo:
                final_mask &= conditions_df[f'filter_{filt}']

        signal_tf = getattr(strategy_module, 'STRATEGY_TIMEFRAME')
        open_col = f'open_{signal_tf}'
        if open_col in analysis_df.columns:
            is_new_signal_candle = analysis_df[open_col].ne(analysis_df[open_col].shift(1))
            final_mask &= is_new_signal_candle
            log(f"   - Filtering signals to the start of each '{signal_tf}' candle.")

        long_signal_mask = final_mask & conditions_df['is_bullish']
        short_signal_mask = final_mask & conditions_df['is_bearish']

        signals_df = pd.DataFrame(index=analysis_df.index)
        signals_df['signal'] = 0
        signals_df.loc[long_signal_mask, 'signal'] = 1
        signals_df.loc[short_signal_mask, 'signal'] = -1
        signals_df['entry_price'] = np.where(long_signal_mask, conditions_df['entry_price'], np.where(short_signal_mask, conditions_df['entry_price'], np.nan))
        signals_df['sl_price'] = np.where(long_signal_mask, conditions_df['sl_price_long'], np.where(short_signal_mask, conditions_df['sl_price_short'], np.nan))

        analysis_df['signal'] = signals_df['signal']
        analysis_df['entry_price'] = signals_df['entry_price']
        analysis_df['sl_price'] = signals_df['sl_price']
        
        backtest_df = analysis_df[analysis_df['ny_time'].dt.date.between(user_start_dt, user_end_dt)].copy()
        execution_timeframe = ASSET_CONFIG[asset_name]['base_tf']

        for rr_scenario in rr_scenarios:
            scenario_name = f"{combo_name}_{rr_scenario['rr']:.1f}R" + ("_BE" if rr_scenario['use_be'] else "")
            log(f"  - Simulating: {scenario_name}")
            
            all_trades_df = run_r_backtest(backtest_df, 
                                       risk_reward_ratio=rr_scenario['rr'],
                                       use_breakeven=rr_scenario['use_be'],
                                       breakeven_trigger_r=rr_scenario['be_trigger_r'],
                                       execution_timeframe=execution_timeframe,
                                       allow_multiple_trades=allow_multiple_trades,
                                       status_callback=status_callback)
            
            session_start_str = strategy_params.get('session_start_str')
            session_end_str = strategy_params.get('session_end_str')
            
            if session_start_str and session_end_str and not all_trades_df.empty:
                start_time = time.fromisoformat(session_start_str)
                end_time = time.fromisoformat(session_end_str)

                def is_in_session(timestamp):
                    ny_time = timestamp.tz_convert('America/New_York').time()
                    if start_time > end_time:
                        return ny_time >= start_time or ny_time <= end_time
                    else:
                        return start_time <= ny_time <= end_time
                
                session_mask = all_trades_df['Entry Time'].apply(is_in_session)
                trades_df = all_trades_df[session_mask].copy()

                log(f"    -> Filtered for session: Found {len(trades_df)} trades out of {len(all_trades_df)} total potential trades.")
            else:
                trades_df = all_trades_df
            
            overall, monthly, daily = get_performance_stats(trades_df)
            all_scenario_results[scenario_name] = {'trades': trades_df, 'overall': overall, 'monthly': monthly, 'daily': daily}

    summary_data = []
    for name, results in all_scenario_results.items():
        overall_df = results['overall']
        if overall_df is not None and not overall_df.empty:
             summary_data.append({'Scenario': name, 'Wins': overall_df.loc[overall_df['Metric'] == 'Winners', 'Value'].iloc[0], 'Losses': overall_df.loc[overall_df['Metric'] == 'Losers', 'Value'].iloc[0], 'Break-Evens': overall_df.loc[overall_df['Metric'] == 'Break-Evens', 'Value'].iloc[0], 'Win Rate %': overall_df.loc[overall_df['Metric'] == 'Win Rate (W/(W+L)) %', 'Value'].iloc[0], 'Total R Gain': overall_df.loc[overall_df['Metric'] == 'Total R Gain', 'Value'].iloc[0]})
        else:
            summary_data.append({'Scenario': name, 'Wins': 0, 'Losses': 0, 'Break-Evens': 0, 'Win Rate %': 'N/A', 'Total R Gain': '0.00R'})
    
    summary_df = pd.DataFrame(summary_data)
    results_folder = os.path.join(project_root, "Results"); os.makedirs(results_folder, exist_ok=True)
    strategy_name = strategy_module.__name__.split('.')[-1]
    base_filename = os.path.join(results_folder, f"{strategy_name}_{asset_name}_{ny_start_date}_to_{ny_end_date}.xlsx")
    excel_filename = get_unique_filename(base_filename)
    log(f"\nGenerating Excel report: {excel_filename}")
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Scenarios Summary', index=False)
        for name, results in all_scenario_results.items():
            export_scenario_to_excel(writer, name, results['trades'], results['overall'], results['monthly'], results['daily'])
    log(f"\nFinished processing {asset_name}. Report saved.")
    return excel_filename