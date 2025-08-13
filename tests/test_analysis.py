import pandas as pd
import pytest
from Core.analysis import get_performance_stats

@pytest.fixture
def sample_trades_df() -> pd.DataFrame:
    """Creates a sample DataFrame of trades for testing."""
    data = {
        'Entry Time': [
            pd.to_datetime('2023-01-02 10:00', utc=True), # Mon, Jan
            pd.to_datetime('2023-01-02 11:00', utc=True), # Mon, Jan
            pd.to_datetime('2023-01-03 12:00', utc=True), # Tue, Jan
            pd.to_datetime('2023-02-05 14:00', utc=True), # Sun, Feb
            pd.to_datetime('2023-02-05 15:00', utc=True), # Sun, Feb
        ],
        'R-Multiple': [2.0, -1.0, 0.0, -1.0, 3.0] # 2 Wins, 2 Losses, 1 BE
    }
    return pd.DataFrame(data)

@pytest.fixture
def empty_trades_df() -> pd.DataFrame:
    """Creates an empty DataFrame."""
    return pd.DataFrame(columns=['Entry Time', 'R-Multiple'])

def test_get_performance_stats_with_data(sample_trades_df):
    """Tests that all stats are calculated correctly from a sample DataFrame."""
    overall, monthly, daily = get_performance_stats(sample_trades_df)

    # --- Test Overall Stats ---
    assert overall is not None
    assert overall.loc[overall['Metric'] == 'Total Trades', 'Value'].iloc[0] == 5
    assert overall.loc[overall['Metric'] == 'Winners', 'Value'].iloc[0] == 2
    assert overall.loc[overall['Metric'] == 'Losers', 'Value'].iloc[0] == 2
    assert overall.loc[overall['Metric'] == 'Break-Evens', 'Value'].iloc[0] == 1
    assert overall.loc[overall['Metric'] == 'Win Rate (W/(W+L)) %', 'Value'].iloc[0] == "50.00"
    assert overall.loc[overall['Metric'] == 'Total R Gain', 'Value'].iloc[0] == "3.00R"

    # --- Test Monthly Stats ---
    assert monthly is not None
    assert len(monthly) == 2
    # January
    jan_stats = monthly[monthly['Month'] == '2023-01']
    assert jan_stats['Trades'].iloc[0] == 3
    assert jan_stats['W'].iloc[0] == 1
    assert jan_stats['L'].iloc[0] == 1
    assert jan_stats['BE'].iloc[0] == 1
    assert jan_stats['Monthly R Gain'].iloc[0] == "1.00R"
    # February
    feb_stats = monthly[monthly['Month'] == '2023-02']
    assert feb_stats['Trades'].iloc[0] == 2
    assert feb_stats['W'].iloc[0] == 1
    assert feb_stats['L'].iloc[0] == 1
    assert feb_stats['BE'].iloc[0] == 0
    assert feb_stats['Monthly R Gain'].iloc[0] == "2.00R"

    # --- Test Daily Stats ---
    assert daily is not None
    assert len(daily) == 3 # Mon, Tue, Sun
    # Monday
    mon_stats = daily[daily['Day'] == 'Monday']
    assert mon_stats['Trades'].iloc[0] == 2
    assert mon_stats['W'].iloc[0] == 1
    assert mon_stats['L'].iloc[0] == 1
    assert mon_stats['Total R Gain'].iloc[0] == "1.00R"
    # Tuesday
    tue_stats = daily[daily['Day'] == 'Tuesday']
    assert tue_stats['Trades'].iloc[0] == 1
    assert tue_stats['W'].iloc[0] == 0
    assert tue_stats['L'].iloc[0] == 0
    assert tue_stats['BE'].iloc[0] == 1
    assert tue_stats['Total R Gain'].iloc[0] == "0.00R"
    # Sunday
    sun_stats = daily[daily['Day'] == 'Sunday']
    assert sun_stats['Trades'].iloc[0] == 2
    assert sun_stats['W'].iloc[0] == 1
    assert sun_stats['L'].iloc[0] == 1
    assert sun_stats['Total R Gain'].iloc[0] == "2.00R"

def test_get_performance_stats_empty_df(empty_trades_df):
    """Tests that the function handles an empty DataFrame gracefully."""
    overall, monthly, daily = get_performance_stats(empty_trades_df)
    assert overall is None
    assert monthly is None
    assert daily is None
