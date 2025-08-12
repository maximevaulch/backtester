# In Core/visualizer.py
import pandas as pd
import os
import subprocess
import sys
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_unique_filename(path):
    """Checks if a file exists and appends a version number if it does."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base}_{i}{ext}"):
        i += 1
    return f"{base}_{i}{ext}"

def get_timeframe_padding(freq_str: str) -> int:
    """Returns a sensible number of padding candles based on the timeframe."""
    if freq_str is None:
        return 20 # Default padding
    
    freq_str = freq_str.lower()
    if 's' in freq_str or any(x in freq_str for x in ['1m', '2m', '3m']):
        return 50 # More detail for low timeframes
    elif any(x in freq_str for x in ['5m', '15m']):
        return 20
    elif any(x in freq_str for x in ['30m', '45m', 'h']):
        return 10
    else: # 4H, D, etc.
        return 5

def plot_day_summary(plot_df, trades_df, asset_name, strategy_name, date_str):
    """
    Creates a smart-zoomed, interactive chart focusing on price action and trades.
    """
    if plot_df.empty:
        print("Visualizer Warning: Cannot plot chart, candle data is empty.")
        return None
        
    if 'ny_time' not in plot_df.columns:
        plot_df['ny_time'] = plot_df.index.tz_convert('America/New_York')
    
    plot_df.reset_index(drop=True, inplace=True)

    # --- NEW: Smart Zoom Logic ---
    freq_str = pd.infer_freq(plot_df['ny_time']) if len(plot_df) > 1 else None
    
    if not trades_df.empty:
        print("Trades found, applying smart zoom...")
        # 1. Find the time boundaries of all trades
        first_entry_time = trades_df['Entry Time'].min()
        last_exit_time = trades_df['Exit Time'].max()
        
        # 2. Find the candle indices for these boundaries
        first_trade_candle_idx = (plot_df['ny_time'] - first_entry_time).abs().idxmin()
        last_trade_candle_idx = (plot_df['ny_time'] - last_exit_time).abs().idxmin()
        
        # 3. Get dynamic padding and calculate the new window
        padding = get_timeframe_padding(freq_str)
        start_idx = max(0, first_trade_candle_idx - padding)
        end_idx = min(len(plot_df), last_trade_candle_idx + padding + 1)
        
        # 4. Crop the DataFrame and reset its index for plotting
        plot_df = plot_df.iloc[start_idx:end_idx].copy()
        plot_df.reset_index(drop=True, inplace=True)
        
        # 5. The original trade indices need to be re-mapped to the new cropped df
        # We will do this by subtracting the start_idx from the original index.
        trades_df['entry_idx_remapped'] = trades_df['Entry Time'].apply(
            lambda t: (plot_df['ny_time'] - t.tz_convert('America/New_York')).abs().idxmin()
        )
        trades_df['exit_idx_remapped'] = trades_df['Exit Time'].apply(
            lambda t: (plot_df['ny_time'] - t.tz_convert('America/New_York')).abs().idxmin()
        )
    else:
        print("No trades found, showing full day.")

    # --- Create Figure ---
    fig = make_subplots(rows=1, cols=1)

    # --- Add Candlestick Trace ---
    fig.add_trace(go.Candlestick(
        x=plot_df.index, open=plot_df['open'], high=plot_df['high'], low=plot_df['low'], close=plot_df['close'],
        name='Price',
        increasing=dict(line=dict(color='deepskyblue'), fillcolor='deepskyblue'),
        decreasing=dict(line=dict(color='grey'), fillcolor='grey')
    ), row=1, col=1)

    # --- Add Enhanced Trade Markers and Shapes ---
    if not trades_df.empty:
        for _, trade in trades_df.iterrows():
            entry_idx = trade['entry_idx_remapped']
            exit_idx = trade['exit_idx_remapped']

            # Determine color for the trade outcome rectangle
            is_win, is_loss = trade['R-Multiple'] > 0, trade['R-Multiple'] < 0
            shape_fill_color = 'rgba(0, 255, 0, 0.2)' if is_win else ('rgba(255, 0, 0, 0.2)' if is_loss else 'rgba(128, 128, 128, 0.2)')

            fig.add_shape(type="rect", x0=entry_idx, y0=trade['Entry Price'], x1=exit_idx, y1=trade['Exit Price'],
                          line=dict(color="rgba(0,0,0,0)"), fillcolor=shape_fill_color, layer='below')

            # Add Entry Marker
            entry_symbol = 'triangle-up' if trade['Direction'] == 'LONG' else 'triangle-down'
            entry_color = 'limegreen' if trade['Direction'] == 'LONG' else 'red'
            fig.add_trace(go.Scatter(x=[entry_idx], y=[trade['Entry Price']], mode='markers',
                                     marker=dict(symbol=entry_symbol, color=entry_color, size=15, line=dict(color='black', width=1)),
                                     name=f"Entry"), row=1, col=1)

            # Add Exit Marker
            exit_color = 'deepskyblue' if trade.get('Exit Reason') == 'Take Profit' else 'orange'
            fig.add_trace(go.Scatter(x=[exit_idx], y=[trade['Exit Price']], mode='markers',
                                     marker=dict(symbol='star', color=exit_color, size=15, line=dict(color='black', width=1)),
                                     name=f"Exit"), row=1, col=1)

    # --- Create Custom X-Axis Labels ---
    tick_indices, tick_labels = [], []
    if not plot_df.empty:
        for i, row in plot_df.iterrows():
            if i % 15 == 0: # Add a label every 15 candles for readability
                tick_indices.append(i)
                tick_labels.append(row['ny_time'].strftime('%H:%M'))

    # --- Finalize Layout and Save ---
    chart_title = f"{asset_name} - {strategy_name} (Signal TF: {freq_str or 'Unknown'}) - {date_str}"
    fig.update_layout(title=chart_title, xaxis_title='Time (NY)', yaxis_title='Price',
                      xaxis_rangeslider_visible=False, showlegend=False, template="plotly_dark")
    fig.update_xaxes(tickmode='array', tickvals=tick_indices, ticktext=tick_labels, row=1, col=1)
    
    results_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results", "Charts")
    os.makedirs(results_folder, exist_ok=True)
    filename_base = f"{strategy_name}_{asset_name}_{date_str}.html"
    initial_path = os.path.join(results_folder, filename_base)
    
    save_path = get_unique_filename(initial_path)
    
    print(f"Generating chart: {save_path}")
    fig.write_html(save_path)
    print("Chart saved successfully.")
    
    return save_path

def open_file(filepath):
    try:
        if sys.platform == "win32":
            os.startfile(os.path.normpath(filepath))
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.call([opener, filepath])
    except Exception as e:
        print(f"Error opening file: {e}")