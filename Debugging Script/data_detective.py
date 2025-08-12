# In data_detective.py
import pandas as pd
import math
from data_handler import load_asset_data

# --- The "Ground Truth" Evidence You Provided ---
ANCHOR_CANDLES = [
    {
        "time": "2025-07-23 01:30:00",
        "open": 1.17348, "high": 1.17348, "low": 1.17333, "close": 1.17338
    },
    {
        "time": "2025-07-23 02:14:00",
        "open": 1.17330, "high": 1.17332, "low": 1.17321, "close": 1.17322
    }
]

# --- Main Detective Work ---
if __name__ == "__main__":
    print("--- Starting Data Detective Investigation ---")
    
    # To find a NY Day, we need to load its UTC day and the next UTC day
    start_date_utc = '2025-07-22'
    end_date_utc = '2025-07-24'
    asset = 'EUR_USD'
    
    print(f"Loading raw data for UTC dates: {start_date_utc} to {end_date_utc}")
    
    # 1. Load the raw data using our trusted handler
    raw_data = load_asset_data(asset_name=asset, 
                               start_date_str=start_date_utc, 
                               end_date_str=end_date_utc)
    
    if raw_data.empty:
        print("!!! INVESTIGATION FAILED: No raw data loaded. Check file paths and dates.")
    else:
        print(f"Successfully loaded {len(raw_data):,} raw tick data.")

        # 2. Resample to 1-minute candles
        aggregation_rules = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
        candle_data = raw_data.resample('1min').agg(aggregation_rules)
        candle_data.dropna(inplace=True)
        print(f"Resampled to {len(candle_data):,} clean 1-minute candles.")

        # 3. Convert to NY Time for searching
        candle_data['ny_time'] = candle_data.index.tz_convert('America/New_York')
        candle_data.set_index('ny_time', inplace=True)
        print("Converted index to NY Time for searching.")

        # 4. Search for each anchor candle
        for anchor in ANCHOR_CANDLES:
            timestamp_str = anchor["time"]
            print(f"\nSearching for candle at NY Time: {timestamp_str}...")
            
            try:
                # Use .loc to find the exact candle by its timestamp
                found_candle = candle_data.loc[timestamp_str]
                
                print(">>> CANDLE FOUND! <<<")
                print(f"    Data from file: O={found_candle.open}, H={found_candle.high}, L={found_candle.low}, C={found_candle.close}")
                print(f"    Your evidence:  O={anchor['open']}, H={anchor['high']}, L={anchor['low']}, C={anchor['close']}")

                # 5. Compare the data
                is_match = (math.isclose(found_candle.open, anchor['open']) and
                            math.isclose(found_candle.high, anchor['high']) and
                            math.isclose(found_candle.low, anchor['low']) and
                            math.isclose(found_candle.close, anchor['close']))

                if is_match:
                    print("    VERDICT: PERFECT MATCH! The data exists and is correct.")
                else:
                    print("    VERDICT: MISMATCH! The candle exists, but OHLC values are different.")

            except KeyError:
                print(f"    VERDICT: CANDLE NOT FOUND! The candle at {timestamp_str} does not exist in the loaded data.")