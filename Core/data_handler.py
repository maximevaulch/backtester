# In Core/data_handler.py
import os
import pandas as pd
import numpy as np
import sys # <-- Import sys

def get_data_folder_root():
    """
    Gets the absolute path to the 'Data' directory in the project.
    This function is smart and works both in development (as a .py script)
    and in production (as a PyInstaller .exe).
    """
    # --- THIS IS THE NEW, ROBUST LOGIC ---
    if getattr(sys, 'frozen', False):
        # We are running in a bundle (e.g., PyInstaller .exe)
        # The base path is the directory of the executable
        base_path = os.path.dirname(sys.executable)
    else:
        # We are running in a normal Python environment
        # The base path is the project root (up two levels from here)
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    return os.path.join(base_path, 'Data')

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Robustly cleans a raw candle DataFrame. Converts columns to numeric,
    removes NaNs, and validates OHLC integrity.
    """
    if df.empty:
        return df
    
    # Define expected numeric columns
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    
    for col in numeric_cols:
        if col in df.columns:
            # Force conversion to numeric, invalid values become NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop any row that now contains a NaN value in any column
    initial_rows = len(df)
    df.dropna(inplace=True)
    removed_rows = initial_rows - len(df)
    
    if removed_rows > 0:
        print(f"   -> Data Cleaning: Removed {removed_rows} rows with invalid/missing values.")

    # Cast to optimal types after cleaning
    if 'volume' in df.columns:
        df['volume'] = df['volume'].astype(np.int32)
    for col in ['open', 'high', 'low', 'close']:
        if col in df.columns:
            df[col] = df[col].astype(np.float32)

    # Final integrity check on OHLC data
    if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
        invalid_ohlc_mask = df['high'] < df['low']
        if invalid_ohlc_mask.any():
            print(f"   -> Data Cleaning: Removed {invalid_ohlc_mask.sum()} rows where high < low.")
            df = df[~invalid_ohlc_mask]

    return df

def load_all_asset_data(dataset_name: str) -> pd.DataFrame:
    """
    Loads ALL raw Parquet files for a given asset from its raw data folder,
    cleans, and validates the combined data. Used by the Healer.
    """
    data_folder_root = get_data_folder_root()
    asset_path = os.path.join(data_folder_root, dataset_name)
    if not os.path.isdir(asset_path):
        raise FileNotFoundError(f"Dataset folder not found: {asset_path}")

    all_files = sorted([f for f in os.listdir(asset_path) if f.endswith('.parquet')])
    if not all_files: return pd.DataFrame()

    print(f"--- Data Handler: Loading {len(all_files)} raw files for {dataset_name} ---")
    
    df_list = [pd.read_parquet(os.path.join(asset_path, f)) for f in all_files]
    final_df = pd.concat(df_list, sort=False)
    
    # Remove duplicates before cleaning
    if final_df.index.has_duplicates:
        initial_rows = len(final_df)
        final_df = final_df[~final_df.index.duplicated(keep='first')]
        print(f"-> Note: Removed {initial_rows - len(final_df)} duplicate timestamps.")

    # Use the centralized cleaning function
    final_df = clean_dataframe(final_df)
    
    final_df.sort_index(inplace=True)
    print(f"Successfully loaded {len(final_df):,} total unique, clean rows.")
    return final_df

def load_unified_data(asset_name: str) -> pd.DataFrame:
    """
    Loads all resampled timeframe files for a given asset and merges them
    into a single, unified DataFrame for backtesting.

    Args:
        asset_name (str): The base name of the asset, e.g., 'EUR_USD'.

    Returns:
        pd.DataFrame: A single DataFrame with columns like 'open_1min', 
                      'close_5min', etc., indexed by UTC timestamp.
    """
    print(f"\n--- Loading Unified Data for: {asset_name} ---")
    resampled_folder = os.path.join(get_data_folder_root(), f"{asset_name}_resampled")

    if not os.path.isdir(resampled_folder):
        raise FileNotFoundError(f"Resampled data folder not found: {resampled_folder}")

    all_files = [f for f in os.listdir(resampled_folder) if f.endswith('.parquet')]
    if not all_files:
        print(f"!!! No resampled .parquet files found in {resampled_folder}. Aborting. !!!")
        return pd.DataFrame()

    all_dfs = []
    print(f"-> Found {len(all_files)} timeframe files to unify.")

    # --- Sort files by timeframe duration for consistent merging ---
    # This ensures the lowest timeframe is the base of our join.
    def get_sort_key(filename):
        try:
            timeframe = filename.split('_')[-1].replace('.parquet', '')
            # Use a sanitized version for pd.to_timedelta
            pd_tf = timeframe.upper() if len(timeframe) == 1 else timeframe
            return pd.to_timedelta(pd_tf)
        except (ValueError, IndexError):
            # Return a large delta for files that can't be parsed, pushing them to the end
            return pd.to_timedelta('100D') 
            
    all_files.sort(key=get_sort_key)
    # --- End of sorting logic ---

    for filename in all_files:
        try:
            timeframe = filename.split('_')[-1].replace('.parquet', '')
            df = pd.read_parquet(os.path.join(resampled_folder, filename))

            # Rename columns to include the timeframe suffix
            rename_dict = {col: f"{col}_{timeframe}" for col in df.columns}
            df.rename(columns=rename_dict, inplace=True)
            all_dfs.append(df)
            print(f"  - Loaded and processed {timeframe} data.")
        except Exception as e:
            print(f"  ! Warning: Could not process file '{filename}'. Error: {e}. Skipping.")

    if not all_dfs:
        print("!!! Failed to load any valid data. Aborting. !!!")
        return pd.DataFrame()
    
    print("-> Merging all timeframes into a single unified DataFrame...")
    
    # Start with the first dataframe (which is the lowest timeframe due to sorting)
    unified_df = all_dfs[0]
    # Join the rest of the dataframes to it
    for i in range(1, len(all_dfs)):
        unified_df = unified_df.join(all_dfs[i], how='outer')

    # Forward-fill propagates data from higher TFs down to the base TF rows
    unified_df.ffill(inplace=True)
    # Drop any rows that still have NaNs (e.g., at the very start of the data)
    unified_df.dropna(inplace=True)
    
    unified_df.sort_index(inplace=True)

    print(f"--- Unified data loaded successfully. Total rows: {len(unified_df):,} ---")
    return unified_df