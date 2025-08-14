# In Core/visualizer.py
import pandas as pd
import os
import subprocess
import sys
from typing import Optional
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Optional

def get_unique_filename(path: str) -> str:
    """
    Checks if a file exists and appends a version number if it does.
    Example: 'chart.html' -> 'chart_1.html'
    """
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base}_{i}{ext}"):
        i += 1
    return f"{base}_{i}{ext}"

def get_timeframe_padding(freq_str: Optional[str]) -> int:
    """Returns a sensible number of padding candles based on the timeframe for smart zooming."""
    if freq_str is None:
        return 20  # Default padding
    
    freq_str = freq_str.lower()
    if 's' in freq_str or any(x in freq_str for x in ['1m', '2m', '3m']):
        return 50 # More detail for low timeframes
    elif any(x in freq_str for x in ['5m', '15m']):
        return 20
    elif any(x in freq_str for x in ['30m', '45m', 'h']):
        return 10
    else: # 4H, D, etc.
        return 5

def _smart_zoom_data(plot_df: pd.DataFrame, trades_df: pd.DataFrame) -> pd.DataFrame:
    """Crops the plotting data to focus on the area with trading activity."""
    if trades_df.empty:
        print("No trades found, showing full day.")
        return plot_df

    print("Trades found, applying smart zoom...")
    freq_str = pd.infer_freq(plot_df['ny_time']) if len(plot_df) > 1 else None

    first_entry_time = trades_df['Entry Time'].min()
    last_exit_time = trades_df['Exit Time'].max()

    first_trade_candle_idx = (plot_df['ny_time'] - first_entry_time).abs().idxmin()
    last_trade_candle_idx = (plot_df['ny_time'] - last_exit_time).abs().idxmin()

    padding = get_timeframe_padding(freq_str)
    start_idx = max(0, first_trade_candle_idx - padding)
    end_idx = min(len(plot_df), last_trade_candle_idx + padding + 1)

    return plot_df.iloc[start_idx:end_idx].copy()

def _add_trade_markers(fig: go.Figure, trades_df: pd.DataFrame, plot_df: pd.DataFrame):
    """Adds shapes and markers for each trade to the plot."""
    # Remap original trade times to the new cropped dataframe's index
    trades_df['entry_idx'] = trades_df['Entry Time'].apply(
        lambda t: (plot_df['ny_time'] - t.tz_convert('America/New_York')).abs().idxmin()
    )
    trades_df['exit_idx'] = trades_df['Exit Time'].apply(
        lambda t: (plot_df['ny_time'] - t.tz_convert('America/New_York')).abs().idxmin()
    )

    for _, trade in trades_df.iterrows():
        is_win, is_loss = trade['R-Multiple'] > 0, trade['R-Multiple'] < 0
        shape_fill = 'rgba(0, 255, 0, 0.2)' if is_win else ('rgba(255, 0, 0, 0.2)' if is_loss else 'rgba(128, 128, 128, 0.2)')
        entry_symbol = 'triangle-up' if trade['Direction'] == 'LONG' else 'triangle-down'
        entry_color = 'limegreen' if trade['Direction'] == 'LONG' else 'red'
        exit_color = 'deepskyblue' if trade.get('Exit Reason') == 'Take Profit' else 'orange'

        fig.add_shape(type="rect", x0=trade['entry_idx'], y0=trade['Entry Price'], x1=trade['exit_idx'], y1=trade['Exit Price'],
                      line=dict(color="rgba(0,0,0,0)"), fillcolor=shape_fill, layer='below')
        fig.add_trace(go.Scatter(x=[trade['entry_idx']], y=[trade['Entry Price']], mode='markers',
                                 marker=dict(symbol=entry_symbol, color=entry_color, size=15, line=dict(color='black', width=1)),
                                 name=f"Entry"))
        fig.add_trace(go.Scatter(x=[trade['exit_idx']], y=[trade['Exit Price']], mode='markers',
                                 marker=dict(symbol='star', color=exit_color, size=15, line=dict(color='black', width=1)),
                                 name=f"Exit"))

def _configure_plot_layout(fig: go.Figure, plot_df: pd.DataFrame, asset_name: str, strategy_name: str, date_str: str):
    """Configures the final layout, title, and axes for the plot."""
    tick_indices, tick_labels = [], []
    if not plot_df.empty:
        for i, row in plot_df.iterrows():
            if i % 15 == 0:  # Add a label every 15 candles for readability
                tick_indices.append(i)
                tick_labels.append(row['ny_time'].strftime('%H:%M'))

    freq_str = pd.infer_freq(plot_df['ny_time']) if len(plot_df) > 1 else 'Unknown'
    chart_title = f"{asset_name} - {strategy_name} (Signal TF: {freq_str or 'Unknown'}) - {date_str}"
    fig.update_layout(title=chart_title, xaxis_title='Time (NY)', yaxis_title='Price',
                      xaxis_rangeslider_visible=False, showlegend=False, template="plotly_dark")
    fig.update_xaxes(tickmode='array', tickvals=tick_indices, ticktext=tick_labels)

def plot_day_summary(plot_df: pd.DataFrame, trades_df: pd.DataFrame, asset_name: str, strategy_name: str, date_str: str) -> Optional[str]:
    """
    Creates a smart-zoomed, interactive chart of price action and trades for a specific day.

    Args:
        plot_df: DataFrame containing the OHLCV data for the plotting period.
        trades_df: DataFrame containing the trades to overlay on the chart.
        asset_name: The name of the asset being plotted.
        strategy_name: The name of the strategy used.
        date_str: The date of the data being plotted, for use in the title.

    Returns:
        The file path to the saved HTML chart, or None if plotting failed.
    """
    if plot_df.empty:
        print("Visualizer Warning: Cannot plot chart, candle data is empty.")
        return None
        
    if 'ny_time' not in plot_df.columns:
        plot_df['ny_time'] = plot_df.index.tz_convert('America/New_York')
    
    plot_df = _smart_zoom_data(plot_df.reset_index(drop=True), trades_df)
    plot_df.reset_index(drop=True, inplace=True)
    
    fig = make_subplots(rows=1, cols=1)

    fig.add_trace(go.Candlestick(
        x=plot_df.index, open=plot_df['open'], high=plot_df['high'], low=plot_df['low'], close=plot_df['close'],
        name='Price',
        increasing=dict(line=dict(color='deepskyblue'), fillcolor='deepskyblue'),
        decreasing=dict(line=dict(color='grey'), fillcolor='grey')
    ))

    if not trades_df.empty:
        _add_trade_markers(fig, trades_df, plot_df)

    _configure_plot_layout(fig, plot_df, asset_name, strategy_name, date_str)
    
    results_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results", "Charts")
    os.makedirs(results_folder, exist_ok=True)
    filename_base = f"{strategy_name}_{asset_name}_{date_str}.html"
    initial_path = os.path.join(results_folder, filename_base)
    
    save_path = get_unique_filename(initial_path)
    print(f"Generating chart: {save_path}")
    fig.write_html(save_path)
    print("Chart saved successfully.")
    return save_path

def open_file(filepath: str):
    """Opens a file using the default application for the current OS."""
    try:
        if sys.platform == "win32":
            os.startfile(os.path.normpath(filepath))
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.call([opener, filepath])
    except Exception as e:
        print(f"Error opening file: {e}")