# In Core/updater.py
import os
import sys
import re
import time

# Add project root to path to allow importing from Core
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Core.oanda_downloader import run_download, get_data_folder_root
from Core.data_healer import run_healing
from Core.resampler import run_resampling

def get_raw_asset_folders():
    """Finds all existing raw data folders in the Data directory."""
    data_path = get_data_folder_root()
    asset_folders = []
    if not os.path.exists(data_path):
        return []
        
    for item in os.listdir(data_path):
        item_path = os.path.join(data_path, item)
        if os.path.isdir(item_path) and not item.endswith('_resampled') and '_healed' not in item:
            asset_folders.append(item_path)
    return sorted(asset_folders)

def run_full_update(status_callback):
    """
    Orchestrates the entire data update pipeline:
    1. Discovers existing raw data assets.
    2. Updates their raw data via download.
    3. Heals each updated raw data folder.
    4. Resamples each healed file.
    5. Cleans up the temporary healed files.
    """
    try:
        status_callback("INFO: Starting full data update process...")
        time.sleep(1)

        # 1. Discover Assets
        asset_folders = get_raw_asset_folders()
        if not asset_folders:
            status_callback("INFO: No existing raw data sets found. Nothing to update.")
            return

        status_callback(f"INFO: Discovered {len(asset_folders)} asset(s) to update.")
        for folder in asset_folders:
            status_callback(f"  - Found: {os.path.basename(folder)}")
        time.sleep(1)

        # 2. Download Stage
        status_callback("\n--- STAGE 1 of 4: DOWNLOADING ---")
        for folder_path in asset_folders:
            dataset_name = os.path.basename(folder_path)
            status_callback(f"UPDATING: {dataset_name}...")
            try:
                parts = dataset_name.split('_')
                instrument_name = f"{parts[0]}_{parts[1]}"
                granularity_name = parts[2]
                run_download(instrument=instrument_name, granularity=granularity_name, start_date_str=None)
                status_callback(f"SUCCESS: {dataset_name} raw data is now up to date.")
            except Exception as e:
                status_callback(f"ERROR: Failed to download for {dataset_name}. Reason: {e}")
                status_callback("WARNING: Skipping this asset for subsequent stages.")
                asset_folders.remove(folder_path) # Remove from list so we don't try to heal it
        
        # 3. Healing Stage
        status_callback("\n--- STAGE 2 of 4: HEALING ---")
        healed_files_to_process = []
        for folder_path in asset_folders:
            dataset_name = os.path.basename(folder_path)
            status_callback(f"HEALING: {dataset_name}...")
            try:
                success, healed_path, _, _ = run_healing(folder_path)
                if success:
                    healed_files_to_process.append(healed_path)
                    status_callback(f"SUCCESS: {dataset_name} healed successfully.")
                else:
                    status_callback(f"ERROR: Healing failed for {dataset_name}. Check console for details.")
            except Exception as e:
                status_callback(f"ERROR: A critical error occurred during healing for {dataset_name}. Reason: {e}")

        if not healed_files_to_process:
            status_callback("ERROR: No files were successfully healed. Aborting pipeline.")
            return

        # 4. Resampling Stage
        status_callback("\n--- STAGE 3 of 4: RESAMPLING ---")
        for healed_path in healed_files_to_process:
            dataset_name = os.path.basename(healed_path)
            status_callback(f"RESAMPLING: {dataset_name}...")
            try:
                success, _ = run_resampling(healed_path)
                if success:
                    status_callback(f"SUCCESS: {dataset_name} resampled to all timeframes.")
                else:
                    status_callback(f"ERROR: Resampling failed for {dataset_name}. Check console for details.")
            except Exception as e:
                status_callback(f"ERROR: A critical error occurred during resampling for {dataset_name}. Reason: {e}")

        # 5. Cleanup Stage
        status_callback("\n--- STAGE 4 of 4: CLEANUP ---")
        for healed_path in healed_files_to_process:
            if os.path.exists(healed_path):
                status_callback(f"DELETING: Temporary file {os.path.basename(healed_path)}...")
                try:
                    os.remove(healed_path)
                    status_callback(f"SUCCESS: Deleted temporary file.")
                except Exception as e:
                    status_callback(f"ERROR: Could not delete {os.path.basename(healed_path)}. Reason: {e}")

    except Exception as e:
        status_callback(f"\nFATAL ERROR: An unexpected error stopped the pipeline: {e}")
    finally:
        status_callback("\n====== UPDATE PROCESS COMPLETE ======")