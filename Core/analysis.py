# In analysis.py
import pandas as pd
from typing import Optional, Tuple

def _calculate_overall_stats(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates overall performance statistics."""
    total_trades = len(trades_df)
    winners = trades_df[trades_df['R-Multiple'] > 0]
    losers = trades_df[trades_df['R-Multiple'] < 0]
    breakevens = trades_df[trades_df['R-Multiple'] == 0]

    num_winners, num_losers, num_breakevens = len(winners), len(losers), len(breakevens)
    deciding_trades = num_winners + num_losers
    win_rate = (num_winners / deciding_trades * 100) if deciding_trades > 0 else 0
    total_r_gain = trades_df['R-Multiple'].sum()

    # Calculate streaks
    trades_df['Win'] = (trades_df['R-Multiple'] > 0).astype(int)
    trades_df['Loss'] = (trades_df['R-Multiple'] < 0).astype(int)
    win_streaks = trades_df['Win'].groupby((trades_df['Win'] != trades_df['Win'].shift()).cumsum()).cumsum()
    loss_streaks = trades_df['Loss'].groupby((trades_df['Loss'] != trades_df['Loss'].shift()).cumsum()).cumsum()
    max_win_streak = win_streaks.max()
    max_loss_streak = loss_streaks.max()

    return pd.DataFrame({
        'Metric': ['Total Trades', 'Winners', 'Losers', 'Break-Evens', 'Win Rate (W/(W+L)) %', 'Total R Gain', 'Max Consecutive Wins', 'Max Consecutive Losses'],
        'Value': [total_trades, num_winners, num_losers, num_breakevens, f"{win_rate:.2f}", f"{total_r_gain:.2f}R", max_win_streak, max_loss_streak]
    })

def _calculate_monthly_stats(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates month-by-month performance statistics."""
    if 'Entry Time' not in trades_df.columns or trades_df['Entry Time'].empty:
        return pd.DataFrame()

    monthly_stats_list = []
    monthly_df = trades_df.set_index('Entry Time')
    monthly_groups = monthly_df.resample('ME')
    
    for period, group in monthly_groups:
        if group.empty: continue
        month_str = period.strftime('%Y-%m')
        m_total_trades = len(group)
        m_num_winners = len(group[group['R-Multiple'] > 0])
        m_num_losers = len(group[group['R-Multiple'] < 0])
        m_num_breakevens = len(group[group['R-Multiple'] == 0])
        m_deciding_trades = m_num_winners + m_num_losers
        m_win_rate = (m_num_winners / m_deciding_trades * 100) if m_deciding_trades > 0 else 0
        m_total_r_gain = group['R-Multiple'].sum()
        monthly_stats_list.append({
            'Month': month_str, 'Trades': m_total_trades, 'W': m_num_winners, 'L': m_num_losers,
            'BE': m_num_breakevens, 'Win Rate %': f"{m_win_rate:.2f}", 'Monthly R Gain': f"{m_total_r_gain:.2f}R"
        })
    return pd.DataFrame(monthly_stats_list)

def _calculate_daily_stats(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates day-of-the-week performance statistics."""
    if 'Entry Time' not in trades_df.columns or trades_df['Entry Time'].empty:
        return pd.DataFrame()
        
    daily_stats_list = []
    trades_df['DayOfWeek'] = trades_df['Entry Time'].dt.day_name()
    day_groups = trades_df.groupby('DayOfWeek')
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    if day_groups.groups.keys():
        for day in days_order:
            if day in day_groups.groups:
                group = day_groups.get_group(day)
                d_total_trades = len(group)
                d_num_winners = len(group[group['R-Multiple'] > 0])
                d_num_losers = len(group[group['R-Multiple'] < 0])
                d_num_breakevens = len(group[group['R-Multiple'] == 0])
                d_deciding_trades = d_num_winners + d_num_losers
                d_win_rate = (d_num_winners / d_deciding_trades * 100) if d_deciding_trades > 0 else 0
                d_total_r_gain = group['R-Multiple'].sum()
                daily_stats_list.append({
                    'Day': day, 'Trades': d_total_trades, 'W': d_num_winners, 'L': d_num_losers, 'BE': d_num_breakevens,
                    'Win Rate %': f"{d_win_rate:.2f}", 'Total R Gain': f"{d_total_r_gain:.2f}R"
                })
    return pd.DataFrame(daily_stats_list)

def get_performance_stats(trades_df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Calculates overall, monthly, and daily performance statistics from a DataFrame of trades.

    Args:
        trades_df: A DataFrame containing the log of trades. Must include 'R-Multiple' and 'Entry Time' columns.

    Returns:
        A tuple containing three DataFrames: (overall_stats, monthly_stats, daily_stats).
        Returns (None, None, None) if the input DataFrame is empty.
    """
    if trades_df.empty:
        return None, None, None

    overall_stats = _calculate_overall_stats(trades_df)
    monthly_stats = _calculate_monthly_stats(trades_df)
    daily_stats = _calculate_daily_stats(trades_df)

    return overall_stats, monthly_stats, daily_stats

def print_performance_stats(overall_stats: Optional[pd.DataFrame], monthly_stats: Optional[pd.DataFrame], daily_stats: Optional[pd.DataFrame]):
    """
    Prints performance statistics to the console in a readable format.

    Args:
        overall_stats: DataFrame with overall performance metrics.
        monthly_stats: DataFrame with month-by-month performance.
        daily_stats: DataFrame with day-of-the-week performance.
    """
    if overall_stats is None:
        print("No trades to analyze.")
        return

    print("\n--- Overall Performance ---")
    print(overall_stats.to_string(index=False))

    if monthly_stats is not None and not monthly_stats.empty:
        print("\n--- Monthly Performance ---")
        print(monthly_stats.to_string(index=False))

    if daily_stats is not None and not daily_stats.empty:
        print("\n--- Performance by Day of Week ---")
        print(daily_stats.to_string(index=False))

def export_scenario_to_excel(writer: pd.ExcelWriter, sheet_name: str, trades_df: pd.DataFrame,
                             overall_stats: Optional[pd.DataFrame], monthly_stats: Optional[pd.DataFrame], daily_stats: Optional[pd.DataFrame]):
    """
    Exports all performance statistics and the full trade log for a single scenario to a sheet in an Excel workbook.

    Args:
        writer: The pandas ExcelWriter object.
        sheet_name: The name for the new sheet.
        trades_df: DataFrame containing the trade log for the scenario.
        overall_stats: DataFrame with the scenario's overall performance.
        monthly_stats: DataFrame with the scenario's monthly performance.
        daily_stats: DataFrame with the scenario's daily performance.
    """
    if overall_stats is None:
        pd.DataFrame([{'Status': 'No trades were taken in this period.'}]).to_excel(writer, sheet_name=sheet_name, index=False)
        return

    worksheet = writer.sheets[sheet_name]

    # Write stats tables
    worksheet.cell(row=1, column=1, value="Overall Performance")
    overall_stats.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)

    start_row_monthly = len(overall_stats) + 4
    if monthly_stats is not None and not monthly_stats.empty:
        worksheet.cell(row=start_row_monthly, column=1, value="Monthly Performance")
        monthly_stats.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_monthly)

    start_row_daily = start_row_monthly + (len(monthly_stats) + 3 if monthly_stats is not None and not monthly_stats.empty else 0)
    if daily_stats is not None and not daily_stats.empty:
        worksheet.cell(row=start_row_daily, column=1, value="Performance by Day of Week")
        daily_stats.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_daily)

    # Write full trade log
    start_row_trades = start_row_daily + (len(daily_stats) + 3 if daily_stats is not None and not daily_stats.empty else 0)
    if not trades_df.empty:
        worksheet.cell(row=start_row_trades, column=1, value="Full Trade Log (in NY Time)")
        export_trades = trades_df.copy()
        # Convert timezone and remove tz info for cleaner Excel output
        export_trades['Entry Time'] = export_trades['Entry Time'].dt.tz_convert('America/New_York').dt.tz_localize(None)
        export_trades['Exit Time'] = export_trades['Exit Time'].dt.tz_convert('America/New_York').dt.tz_localize(None)
        export_trades.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_trades + 1)

    # Auto-adjust column widths for readability
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        worksheet.column_dimensions[column_letter].width = adjusted_width