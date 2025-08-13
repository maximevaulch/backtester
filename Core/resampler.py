# In Core/resampler.py
import pandas as pd
import os
import re
from typing import Optional, Tuple, Dict, Any

def run_resampling(healed_file_path: str, status_callback: Optional[callable] = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Loads a 'healed' data file and resamples it to multiple higher timeframes.

    It reads a single Parquet file containing a continuous time series, then
    generates a separate Parquet file for each target timeframe (e.g., 1min,
    5min, 1H, 4H, etc.) in a new '_resampled' directory.

    Args:
        healed_file_path: The full path to the healed Parquet file.
        status_callback: An optional function for logging progress.

    Returns:
        A tuple containing:
        - bool: True if successful, False otherwise.
        - Dict[str, Any]: A report dictionary containing results or an error message.
    """
    def log(message: str):
        if status_callback: status_callback(message)
        else: print(message)

    log("\n=======================================================")
    log(f" STEP 3: RESAMPLING FILE: {os.path.basename(healed_file_path)}")
    log("=======================================================")

    try:
        df = pd.read_parquet(healed_file_path)
        if df.empty:
            log("!!! Healed file is empty. Cannot resample. !!!")
            return False, {"Error": "Healed file is empty."}

        # Define the aggregation rules for resampling OHLCV data.
        agg_rules = { 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum' }

        # Extract the base asset name (e.g., 'EUR_USD') from the healed filename.
        filename = os.path.basename(healed_file_path)
        match = re.match(r"^(.*?)_[SM]\d+_healed\.parquet$", filename)
        if not match:
            raise ValueError(f"Could not parse asset name from filename: {filename}")
        base_name = match.group(1)
        
        target_timeframes = ['30s', '1min', '2min', '3min', '5min', '15min', '30min', '45min', '1H', '4H', 'D']

        data_dir = os.path.dirname(healed_file_path)
        output_dir = os.path.join(data_dir, f"{base_name}_resampled")
        os.makedirs(output_dir, exist_ok=True)
        log(f"-> Output will be saved in: {output_dir}")
        
        report = {}
        log(f"-> Resampling {base_name} from base S30 to target timeframes...")
        
        for tf_str in target_timeframes:
            log(f"  - Processing {tf_str}...")
            
            if tf_str == '30s':
                resampled_df = df.copy()
            else:
                resampled_df = df.resample(tf_str).agg(agg_rules)
            
            resampled_df.dropna(inplace=True)

            if resampled_df.empty:
                log(f"    ! Warning: No data generated for {tf_str}. Skipping.")
                continue
            
            safe_tf_name = tf_str.lower()
            output_filename = f"{base_name}_{safe_tf_name}.parquet"
            full_path = os.path.join(output_dir, output_filename)
            resampled_df.to_parquet(full_path, engine='pyarrow', compression='snappy')
            
            report[output_filename] = f"{len(resampled_df):,} candles"
            log(f"    -> Saved {output_filename} with {len(resampled_df):,} candles.")

        log("\n--- Resampling process finished successfully! ---")
        return True, report

    except Exception as e:
        log(f"!!! An error occurred during resampling: {e} !!!")
        return False, {"Error": str(e)}