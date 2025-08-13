# In Core/data_healer.py
import pandas as pd
import os
import numpy as np
from typing import Optional, Tuple
from .data_handler import load_all_asset_data

def _create_master_index(df: pd.DataFrame, log: callable) -> Optional[pd.DatetimeIndex]:
    """Infers frequency and creates a complete DatetimeIndex."""
    if len(df) < 2:
        log("!!! Not enough data to determine frequency. Aborting. !!!")
        return None
    inferred_freq = df.index.to_series().diff().mode()[0]
    log(f"-> Inferred base frequency: {inferred_freq}")
    log(f"-> Creating a complete timeline from {df.index.min()} to {df.index.max()}...")
    return pd.date_range(start=df.index.min(), end=df.index.max(), freq=inferred_freq, tz='UTC')

def _fill_data_gaps(df: pd.DataFrame, log: callable) -> pd.DataFrame:
    """Fills missing price data with forward-fill and volume with 0."""
    log("-> Healing small data gaps (forward-fill)...")
    price_cols = ['open', 'high', 'low', 'close']
    df[price_cols] = df[price_cols].ffill()
    df['volume'] = df['volume'].fillna(0)
    return df

def _remove_weekend_data(df: pd.DataFrame, log: callable) -> pd.DataFrame:
    """Removes artificially generated data that falls on weekends or market close times."""
    log("-> Removing ARTIFICIAL data from weekend/market-close gaps...")
    is_artificial = ~df['is_original']
    is_saturday = df.index.dayofweek == 5
    is_sunday_before_open = (df.index.dayofweek == 6) & (df.index.hour < 21)
    is_friday_after_close = (df.index.dayofweek == 4) & (df.index.hour >= 21)
    rows_to_remove = is_artificial & (is_saturday | is_sunday_before_open | is_friday_after_close)
    return df[~rows_to_remove]

def run_healing(raw_folder_path: str, status_callback: Optional[callable] = None) -> Tuple[bool, Optional[str], int, int]:
    """
    Loads raw data, fills gaps to create a continuous timeline, and saves a single 'healed' file.

    The process involves:
    1. Loading all raw data for an asset.
    2. Inferring the data's time frequency (e.g., 1 minute).
    3. Creating a master index with no time gaps.
    4. Reindexing the data to this master index, creating NaN rows for gaps.
    5. Filling price data with forward-fill and volume with 0.
    6. Removing artificial data generated during non-market hours (weekends).
    7. Saving the final, cleaned DataFrame to a single Parquet file.

    Args:
        raw_folder_path: The full path to the folder containing the raw data files.
        status_callback: An optional function for logging progress.

    Returns:
        A tuple containing:
        - bool: True if successful, False otherwise.
        - Optional[str]: The path to the saved healed file, or None on failure.
        - int: The initial count of raw candles.
        - int: The final count of healed candles.
    """
    def log(message: str):
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

    # Create a complete time series index
    master_index = _create_master_index(raw_df, log)
    if master_index is None:
        return False, None, initial_count, 0
    
    # Reindex the raw data to the complete timeline, marking original rows
    raw_df['is_original'] = True
    healed_df = raw_df.reindex(master_index)
    del raw_df, master_index
    healed_df['is_original'] = healed_df['is_original'].fillna(False).astype(bool)
    
    # Fill gaps and clean data
    healed_df = _fill_data_gaps(healed_df, log)
    healed_df = _remove_weekend_data(healed_df, log)
    
    # Final cleanup
    healed_df.drop(columns=['is_original'], inplace=True)
    healed_df.dropna(inplace=True)
    healed_df = healed_df.astype({'open': np.float32, 'high': np.float32, 'low': np.float32, 'close': np.float32, 'volume': np.int32})
    
    final_count = len(healed_df)
    log(f"-> Final healed candle count: {final_count:,}")
    
    log(f"-> Saving healed data file to: {output_filename}")
    healed_df.to_parquet(output_filename, engine='pyarrow', compression='snappy')
    log(f"--- Successfully healed and saved {os.path.basename(output_filename)}. Now, run the resampler ---")

    return True, output_filename, initial_count, final_count