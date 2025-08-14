# In Core/oanda_downloader.py
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.exceptions
from dotenv import load_dotenv
import re
import sys

from typing import List, Optional

def get_data_folder_root():
    """Gets the absolute path to the 'Data' directory."""
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, 'Data')

def get_latest_date_from_files(path: str) -> Optional[pd.Timestamp]:
    """
    Finds the most recent date from daily Parquet filenames in a directory.

    Args:
        path: The directory path containing Parquet files named 'YYYY-MM-DD.parquet'.

    Returns:
        A pandas Timestamp of the latest date found, or None if no files exist.
    """
    if not os.path.exists(path): return None
    files = [f for f in os.listdir(path) if f.endswith('.parquet')]
    if not files: return None
    latest_file = sorted(files)[-1]
    return pd.to_datetime(latest_file.split('.')[0], utc=True)

def save_candles_to_daily_files(df: pd.DataFrame, output_path: str):
    """
    Saves candle data into separate daily Parquet files based on UTC date.
    If a file for a given day already exists, it merges the new data.
    """
    if df.empty: return
    df['utc_date'] = df.index.date
    os.makedirs(output_path, exist_ok=True)
    
    for date, daily_df in df.groupby('utc_date'):
        filename = f"{date.strftime('%Y-%m-%d')}.parquet"
        full_path = os.path.join(output_path, filename)
        daily_df = daily_df.drop(columns=['utc_date'])
        
        if os.path.exists(full_path):
            existing_df = pd.read_parquet(full_path)
            combined_df = pd.concat([existing_df, daily_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df.sort_index(inplace=True)
            combined_df.to_parquet(full_path, engine='pyarrow')
        else:
            daily_df.to_parquet(full_path, engine='pyarrow')

def fetch_candles(api: API, instrument: str, granularity: str, from_time: datetime, count: int, status_callback: Optional[callable] = None) -> list:
    """
    A helper function to make a single API request for candles.

    Args:
        api: An initialized oandapyV20 API client.
        instrument: The instrument to fetch (e.g., 'EUR_USD').
        granularity: The candle granularity (e.g., 'M1', 'H4').
        from_time: The start time for the candle request (as a datetime object).
        count: The number of candles to fetch (max 5000).
        status_callback: An optional function to log status messages.

    Returns:
        A list of candle objects from the API response.

    Raises:
        oandapyV20.exceptions.V20Error: If the API request fails, especially on rate limits.
    """
    params = {"granularity": granularity, "count": count, "from": from_time.isoformat().replace('+00:00', 'Z'), "price": "M"}
    r = instruments.InstrumentsCandles(instrument=instrument, params=params)
    try:
        response = api.request(r)
        return response.get('candles', [])
    except oandapyV20.exceptions.V20Error as e:
        msg = ""
        if "ratelimit" in str(e).lower() or "rate limit" in str(e).lower():
            msg = "Rate limit hit. Sleeping for 10 seconds..."
            time.sleep(10)
        else:
            msg = f"OANDA API Error: {e}. Stopping."
        
        if status_callback: status_callback(f"ERROR: {msg}")
        else: print(f"\n{msg}")
        raise

def run_download(instrument: str, granularity: str, start_date_str: Optional[str] = None, status_callback: Optional[callable] = None):
    """
    Main function to download or update historical price data for an instrument.

    It robustly handles gaps in data and resumes from the last downloaded point.
    Candles are saved into daily Parquet files.

    Args:
        instrument: The instrument to download (e.g., 'EUR_USD').
        granularity: The candle granularity (e.g., 'S30', 'M1').
        start_date_str: If provided, starts a fresh download from this date ('YYYY-MM-DD').
                        If None, it attempts to resume from the last downloaded date.
        status_callback: An optional function for logging progress to a GUI or console.
    """
    def log(message: str):
        if status_callback:
            status_callback(message)
        else:
            # Handle console-specific printing like carriage returns for progress updates
            if "Fetched until" in message:
                print(message, end='\r')
            else:
                print(message)

    load_dotenv()
    ACCESS_TOKEN, ENVIRONMENT = os.getenv("OANDA_ACCESS_TOKEN"), os.getenv("OANDA_ENVIRONMENT", "practice")
    if not ACCESS_TOKEN: raise ValueError("OANDA_ACCESS_TOKEN not found in .env file.")

    api = API(access_token=ACCESS_TOKEN, environment=ENVIRONMENT)
    output_path = os.path.join(get_data_folder_root(), f"{instrument}_{granularity}")
    os.makedirs(output_path, exist_ok=True)

    if start_date_str:
        from_time_dt = pd.to_datetime(start_date_str, utc=True)
        log(f"Starting new download for {instrument}_{granularity} from {from_time_dt.strftime('%Y-%m-%d')}")
    else:
        latest_date = get_latest_date_from_files(output_path)
        from_time_dt = (latest_date + timedelta(days=1)) if latest_date else datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        log(f"Resuming download for {instrument}_{granularity} from {from_time_dt.strftime('%Y-%m-%d')}")
    
    end_time_dt = datetime.now(timezone.utc)
    if from_time_dt >= end_time_dt:
        log("Data is up to date. No download needed.")
        return

    last_processed_timestamp = None
    CANDLE_COUNT = 5000

    while from_time_dt < end_time_dt:
        try:
            candles_data = fetch_candles(api, instrument, granularity, from_time_dt, CANDLE_COUNT, status_callback)
            
            if not candles_data:
                log(f"No data at {from_time_dt}. Searching for next market activity...")
                from_time_dt += timedelta(hours=6)
                time.sleep(0.5)
                continue

            last_candle_in_batch = pd.to_datetime(candles_data[-1]['time'])
            if last_candle_in_batch == last_processed_timestamp:
                log("No new candles being returned. Download has caught up. Finishing.")
                break
            last_processed_timestamp = last_candle_in_batch

            records = [{'timestamp': pd.to_datetime(c['time']), 'open': float(c['mid']['o']), 'high': float(c['mid']['h']), 'low': float(c['mid']['l']), 'close': float(c['mid']['c']), 'volume': int(c['volume'])} for c in candles_data if c.get('complete', True)]
            
            if records:
                df = pd.DataFrame(records).set_index('timestamp')
                save_candles_to_daily_files(df, output_path)
                log(f"Fetched until: {last_candle_in_batch.strftime('%Y-%m-%d %H:%M:%S')} UTC | {len(records)} candles")
            
            from_time_dt = last_candle_in_batch + timedelta(microseconds=1)
            time.sleep(0.3 if len(candles_data) == CANDLE_COUNT else 1.5)

        except Exception as e:
            log(f"An unexpected error occurred: {e}. Stopping download.")
            break
            
    log("\nDownload process finished.")

# ... (analyze_raw_data function is unchanged) ...
def analyze_raw_data(raw_folder_path: str) -> str:
    """
    Performs a comprehensive, granularity-aware analysis of raw data in a folder.

    Checks for missing data, invalid candles, and time gaps, returning a formatted string report.

    Args:
        raw_folder_path: The full path to the folder containing raw Parquet files.

    Returns:
        A formatted string containing the full analysis report.
    """
    basename = os.path.basename(raw_folder_path)
    report_lines = [f"Analysis Report for: {basename}\n" + ("-" * 40)]
    
    try:
        granularity_str = basename.split('_')[-1] # e.g., 'S30', 'M1'
        match = re.match(r"([SM])(\d+)", granularity_str)
        if not match: raise ValueError("Granularity format not recognized (e.g., S30, M1).")
        unit, number = match.groups(); number = int(number)
        pd_freq = f"{number}s" if unit == 'S' else f"{number}min"
        expected_delta = pd.to_timedelta(pd_freq)
    except (IndexError, ValueError) as e:
        report_lines.append(f"Could not determine granularity from folder name: {basename}.\nError: {e}")
        return "\n".join(report_lines)

    all_files = [os.path.join(raw_folder_path, f) for f in os.listdir(raw_folder_path) if f.endswith('.parquet')]
    if not all_files: report_lines.append("No .parquet files found."); return "\n".join(report_lines)
    
    df = pd.concat([pd.read_parquet(f) for f in all_files])
    if df.empty: report_lines.append("Data is empty."); return "\n".join(report_lines)

    df.sort_index(inplace=True); df = df[~df.index.duplicated(keep='first')]
    total_candles = len(df)
    report_lines.append(f"Total Unique {granularity_str} Candles: {total_candles:,}")
    report_lines.append(f"Time Range (UTC): {df.index.min()} to {df.index.max()}")
    report_lines.append("\n[Data Integrity]")
    ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']; nan_rows = df[ohlcv_cols].isnull().any(axis=1).sum()
    invalid_ohlc = (df['high'] < df['low']).sum()
    report_lines.append(f"  Rows with Missing Data (NaNs): {nan_rows:,}"); report_lines.append(f"  Invalid Candles (High < Low): {invalid_ohlc:,}")
    report_lines.append("\n[Timestamp Sequence]"); report_lines.append(f"  Is sequential: {'Yes' if df.index.is_monotonic_increasing else 'No'}")
    
    if total_candles > 1:
        time_diffs = df.index.to_series().diff()
        weekend_gaps = time_diffs[time_diffs > timedelta(days=1)]
        intra_week_gaps = time_diffs[(time_diffs > expected_delta) & (time_diffs <= timedelta(days=1))]
        report_lines.append(f"  Weekend/Holiday Gaps (>1 day): {len(weekend_gaps)}")
        report_lines.append(f"  Intra-Week Gaps (> {pd_freq}): {len(intra_week_gaps)}")
        if not intra_week_gaps.empty:
            report_lines.append("    - Top 5 largest intra-week gaps:")
            for gap in intra_week_gaps.nlargest(5): report_lines.append(f"      - {str(gap).split('.')[0]}")
    report_lines.append("\n[Volume Analysis]"); zero_volume_candles = (df['volume'] == 0).sum()
    report_lines.append(f"  Candles with Zero Volume: {zero_volume_candles:,}")
    if total_candles > 0: report_lines.append(f"  Volume (Min/Avg/Max): {df['volume'].min():,} / {df['volume'].mean():,.0f} / {df['volume'].max():,}")
        
    return "\n".join(report_lines)