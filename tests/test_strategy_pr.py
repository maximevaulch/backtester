import pandas as pd
import pytest
from Strategies.strategy_PR import StrategyPR

@pytest.fixture
def strategy_pr_instance() -> StrategyPR:
    """Returns an instance of the StrategyPR class."""
    return StrategyPR()

@pytest.fixture
def sample_market_data() -> pd.DataFrame:
    """
    Creates a sample DataFrame for testing the PR strategy.
    It includes candles that should and should not trigger signals.
    """
    # Base timeframe for the PR strategy is 15min
    tf = '15min'

    data = {
        f'open_{tf}':  [100, 102, 105, 103, 110, 108],
        f'high_{tf}':  [103, 104, 110, 106, 112, 110],
        f'low_{tf}':   [99,  101, 100, 102, 109, 107],
        f'close_{tf}': [102, 103, 106, 105, 111, 109],
        f'volume_{tf}': [1000, 1500, 1200, 2000, 900, 3000]
    }
    # Timestamps are arbitrary but must be sequential
    index = pd.to_datetime([
        '2023-01-01 09:00', '2023-01-01 09:15', '2023-01-01 09:30',
        '2023-01-01 09:45', '2023-01-01 10:00', '2023-01-01 10:15'
    ], utc=True)

    # The main test will be on the candle at index 2 (09:30)
    # Let's analyze the setup for the signal at 09:30, which checks the two previous candles (09:15 and 09:00)
    # Signal Candle (the one being evaluated): df.iloc[1] (09:15)
    # Previous Candle: df.iloc[0] (09:00)
    #
    # 1. Pattern Condition: high[1] (104) >= high[0] (103) AND low[1] (101) <= low[0] (99) -> FALSE
    #    This means the candle at 09:30 will NOT be a signal.
    #
    # Let's adjust for a signal at 09:45 (index 3). This checks candles at index 2 and 1.
    # Signal Candle (evaluated): df.iloc[2] (09:30) -> high=110, low=100
    # Previous Candle: df.iloc[1] (09:15) -> high=104, low=101
    # Pattern: high[2](110) >= high[1](104) AND low[2](100) <= low[1](101) -> TRUE
    # Volume: volume[2](1200) > volume[1](1500) -> FALSE. This will fail the volume filter.
    #
    # Let's adjust for a signal at 10:15 (index 5). This checks candles at index 4 and 3.
    # Signal Candle (evaluated): df.iloc[4] (10:00) -> high=112, low=109, close=111, open=110
    # Previous Candle: df.iloc[3] (09:45) -> high=106, low=102, close=105, open=103
    # Pattern: high[4](112) >= high[3](106) AND low[4](109) <= low[3](102) -> FALSE
    #
    # Let's recraft the data for a clear signal at index 2.
    data = {
        f'open_{tf}':  [100, 105, 102, 108],
        f'high_{tf}':  [103, 110, 104, 110],
        f'low_{tf}':   [99,  100, 101, 107],
        f'close_{tf}': [102, 106, 103, 109],
        f'volume_{tf}': [1000, 1200, 1500, 800]
    }
    # Signal should be at index 2, checking candles at index 1 and 0.
    # Signal Candle (evaluated): df.iloc[1] -> high=110, low=100
    # Previous Candle: df.iloc[0] -> high=103, low=99
    # Pattern: high[1](110) >= high[0](103) AND low[1](100) <= low[0](99) -> FALSE. Still wrong.
    #
    # Let's try again. Signal at index 2, based on candle 1 ("signal candle") and candle 0 ("previous candle")
    # Pattern: high of signal_candle >= high of prev_candle AND low of signal_candle <= low of prev_candle
    data = {
        #                0      1 (Signal Candle)   2 (Signal Generated Here)  3
        f'open_{tf}':  [100,   105,                102,                       108],
        f'high_{tf}':  [103,   110,                104,                       110],
        f'low_{tf}':   [101,   100,                101,                       107],
        f'close_{tf}': [102,   108,                103,                       109], # Candle 1 is bullish
        f'volume_{tf}': [1000, 1500,                1100,                       800]
    }
    # For signal at index 2:
    # Pattern: high[1](110) >= high[0](103) AND low[1](100) <= low[0](101) -> TRUE
    # Volume: volume[1](1500) > volume[0](1000) -> TRUE
    # Body: close[1](108) is NOT between open[0](100) and close[0](102) -> TRUE
    # Direction: close[1](108) > open[1](105) -> Bullish
    # This should be a valid bullish signal at index 2.

    df = pd.DataFrame(data, index=pd.to_datetime(['2023-01-01 09:00', '2023-01-01 09:15', '2023-01-01 09:30', '2023-01-01 09:45'], utc=True))
    # Add dummy columns for other timeframes to simulate unified data
    df['open_30s'] = df[f'open_{tf}']
    df['high_30s'] = df[f'high_{tf}']
    df['low_30s'] = df[f'low_{tf}']
    df['close_30s'] = df[f'close_{tf}']
    return df


def test_strategy_pr_signal_generation(strategy_pr_instance, sample_market_data):
    """
    Tests that the StrategyPR correctly identifies a valid signal,
    calculates entry/sl, and sets the correct flags.
    """
    # Act: Generate the conditions
    conditions_df = strategy_pr_instance.generate_conditions(sample_market_data)

    # Assert: Check the results at the timestamp where a signal is expected
    signal_timestamp = pd.to_datetime('2023-01-01 09:30', utc=True)
    signal_row = conditions_df.loc[signal_timestamp]

    assert signal_row['base_pattern_cond'] == True
    assert signal_row['filter_Volume'] == True
    assert signal_row['filter_Body'] == True
    assert signal_row['is_bullish'] == True
    assert signal_row['is_bearish'] == False

    # Entry is the open of the candle *after* the signal candle
    assert signal_row['entry_price'] == sample_market_data.loc[signal_timestamp, 'open_15min'] # 102
    # SL is the low of the signal candle itself (candle at index 1)
    assert signal_row['sl_price_long'] == sample_market_data.iloc[1]['low_15min'] # 100

    # Assert: Check that no other signals were generated
    other_rows = conditions_df.drop(signal_timestamp)
    assert not other_rows['base_pattern_cond'].any()
