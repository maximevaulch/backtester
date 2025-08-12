# In Core/data_healer.py
import pandas as pd
import os
import numpy as np
from .data_handler import load_all_asset_data

def run_healing(raw_folder_path, status_callback=None):
    """
    Loads raw data, fills gaps, and saves a single 'healed' file,
    reporting progress via a callback.
    """
    def log(message):
        if status_callback: status_callback(message)
        else: print(message)

    log("\n=======================================================")
    log(f" STEP 2: HEALING FOLDER: {os.path.basename(raw_folder_path)}")
    log("=======================================================")

    dataset_name = os.path.basename(raw_folder_path)
    output_dir = os.path.dirname(raw_folder_path)
    output_filename = os.path.join(output_dir, f"{dataset_name}_healed.parquet")

    log(f"-> Loading all raw data from '{dataset_name}' folder...")
    raw_df = load_all_asset_data(dataset_name)
    if raw_df.empty:
        log("!!! No data found. Aborting healing. !!!")
        return False, None, 0, 0

    initial_count = len(raw_df)
    log(f"-> Initial raw candle count: {initial_count:,}")

    if len(raw_df) < 2:
        log("!!! Not enough data to determine frequency. Aborting. !!!")
        return False, None, initial_count, 0
    
    raw_df['is_original'] = True
    inferred_freq = raw_df.index.to_series().diff().mode()[0]
    log(f"-> Inferred base frequency: {inferred_freq}")
    
    log(f"-> Creating a complete timeline from {raw_df.index.min()} to {raw_df.index.max()}...")
    master_index = pd.date_range(start=raw_df.index.min(), end=raw_df.index.max(), freq=inferred_freq, tz='UTC')
    
    healed_df = raw_df.reindex(master_index)
    del raw_df, master_index

    healed_df['is_original'] = healed_df['is_original'].fillna(False).astype(bool)
    
    log("-> Healing small data gaps (forward-fill)...")
    price_cols = ['open', 'high', 'low', 'close']
    healed_df[price_cols] = healed_df[price_cols].ffill()
    healed_df['volume'] = healed_df['volume'].fillna(0)

    log("-> Removing ARTIFICIAL data from weekend/market-close gaps...")
    is_artificial = ~healed_df['is_original']
    is_saturday = healed_df.index.dayofweek == 5
    is_sunday_before_open = (healed_df.index.dayofweek == 6) & (healed_df.index.hour < 21)
    is_friday_after_close = (healed_df.index.dayofweek == 4) & (healed_df.index.hour >= 21)
    rows_to_remove = is_artificial & (is_saturday | is_sunday_before_open | is_friday_after_close)
    healed_df = healed_df[~rows_to_remove]
    
    healed_df.drop(columns=['is_original'], inplace=True)
    healed_df.dropna(inplace=True)

    healed_df = healed_df.astype({'open': np.float32, 'high': np.float32, 'low': np.float32, 'close': np.float32, 'volume': np.int32})
    
    final_count = len(healed_df)
    log(f"-> Final healed candle count: {final_count:,}")
    
    log(f"-> Saving healed data file to: {output_filename}")
    healed_df.to_parquet(output_filename, engine='pyarrow', compression='snappy')
    log(f"--- Successfully healed and saved {os.path.basename(output_filename)}. Now, run the resampler ---")

    return True, output_filename, initial_count, final_count