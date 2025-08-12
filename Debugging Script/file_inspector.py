# In file_inspector.py
import os
import pandas as pd
import math

# --- CONFIGURATION ---
# List the exact filenames you want to inspect one by one.
FILES_TO_INSPECT = [
    '2025-07-20.parquet'
]
DATA_FOLDER = r'D:\Coding\Backtesting_project\Data\EUR_USD_S5'

# The "Ground Truth" Evidence You Provided
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

# --- Main Inspection ---
if __name__ == "__main__":
    for file_name in FILES_TO_INSPECT:
        full_path = os.path.join(DATA_FOLDER, file_name)
        
        print(f"\n=======================================================")
        print(f" Inspecting File: {file_name} ")
        print(f"=======================================================")

        if not os.path.exists(full_path):
            print("!!! FILE NOT FOUND at path:", full_path)
            continue

        # 1. Load the single, raw file directly
        raw_data = pd.read_parquet(full_path)
        print(f"Loaded {len(raw_data)} raw ticks from this file.")

        # 2. Inspect the raw timestamp range
        min_utc_time = raw_data.index.min()
        max_utc_time = raw_data.index.max()
        
        print("\n--- Raw Timestamp Analysis ---")
        print(f"Min Timestamp (UTC): {min_utc_time}")
        print(f"Max Timestamp (UTC): {max_utc_time}")
        
        # Convert to NY time to see if it spills over the day
        min_ny_time = min_utc_time.tz_convert('America/New_York')
        max_ny_time = max_utc_time.tz_convert('America/New_York')
        print(f"Min Timestamp (NY):  {min_ny_time}")
        print(f"Max Timestamp (NY):  {max_ny_time}")

        # 3. Resample and search for anchor candles
        aggregation_rules = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
        candle_data = raw_data.resample('1min').agg(aggregation_rules)
        candle_data.dropna(inplace=True)
        candle_data['ny_time'] = candle_data.index.tz_convert('America/New_York')
        candle_data.set_index('ny_time', inplace=True)
        
        print("\n--- Anchor Candle Search ---")
        for anchor in ANCHOR_CANDLES:
            timestamp_str = anchor["time"]
            print(f"Searching for candle at NY Time: {timestamp_str}...")
            
            try:
                found_candle = candle_data.loc[timestamp_str]
                print(">>> CANDLE FOUND! <<<")
                # Compare the data
                is_match = (math.isclose(found_candle.open, anchor['open']) and
                            math.isclose(found_candle.high, anchor['high']) and
                            math.isclose(found_candle.low, anchor['low']) and
                            math.isclose(found_candle.close, anchor['close']))
                if is_match:
                    print("    VERDICT: PERFECT MATCH! The candle is in this file.")
                else:
                    print("    VERDICT: MISMATCH! A candle exists at this time, but OHLC is different.")

            except KeyError:
                print(f"    VERDICT: CANDLE NOT FOUND in this file.")