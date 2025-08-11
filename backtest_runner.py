
import os
import asyncio
import logging
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from multi_symbol_macd_stream import dfs, compute_macd, handle_bar

import plotly.graph_objs as go
from plotly.subplots import make_subplots

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv('crypto.env')
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# --- Logging setup ---
backtest_log_file = "backtest_log.txt"
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)sZ] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    handlers=[logging.FileHandler(backtest_log_file, mode='w', encoding='utf-8')]
)
bt_logger = logging.getLogger("backtest")
def bt_log(msg: str):
    bt_logger.info(msg)

# --- Override live logger ---
import multi_symbol_macd_stream
multi_symbol_macd_stream.logger = bt_logger
multi_symbol_macd_stream.log = lambda msg: bt_logger.info(msg)

# --- Portfolio tracker ---
class Portfolio:
    def __init__(self, initial_cash=5000):
        self.cash = initial_cash
        self.position = 0
        self.buy_log = []
        self.sell_log = []
        self.last_price = 0

    def buy(self, time, price, quantity):
        cost = price * quantity
        if self.cash >= cost:
            self.cash -= cost
            self.position += quantity
            self.buy_log.append((time, price))
            bt_log(f"📈 Sim BUY {quantity} @ {price} at {time}")

    def sell(self, time, price, quantity):
        if self.position >= quantity:
            self.cash += price * quantity
            self.position -= quantity
            self.sell_log.append((time, price))
            bt_log(f"📉 Sim SELL {quantity} @ {price} at {time}")

    def value(self):
        return self.cash + self.position * self.last_price

portfolio = Portfolio()

# Simulate fake quote for the price logic
multi_symbol_macd_stream.fetch_quote = lambda sym: (portfolio.last_price, portfolio.last_price)

# Patch place_buy/place_sell to simulate and track timestamp
current_bar_time = None
def sim_place_buy(sym, price, dollars):
    qty = int(dollars // price)
    if qty > 0:
        portfolio.buy(current_bar_time, price, qty)
multi_symbol_macd_stream.place_buy = sim_place_buy

def sim_place_sell(sym, price, qty):
    portfolio.sell(current_bar_time, price, qty)
multi_symbol_macd_stream.place_sell = sim_place_sell

# --- Historical data fetch ---
def fetch_historical_bars(symbol: str, date: str):
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    start_dt = datetime.strptime(date + " 09:30:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(date + " 16:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start_dt,
        end=end_dt
    )
    bars = client.get_stock_bars(request).df
    if bars.empty or symbol not in bars.index:
        raise ValueError(f"No bars found for {symbol} on {date}")
    df = bars.loc[symbol].reset_index()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

# --- Simulation loop ---
async def simulate_bar_feed(symbol: str, df: pd.DataFrame):
    global current_bar_time
    for _, row in df.iterrows():
        current_bar_time = row['timestamp']
        portfolio.last_price = row['close']
        bar = {
            'S': symbol,
            't': row['timestamp'].isoformat(),
            'o': row['open'],
            'h': row['high'],
            'l': row['low'],
            'c': row['close'],
            'v': row['volume']
        }
        await handle_bar(bar)
        await asyncio.sleep(0.001)

# --- Interactive dual-axis plot ---
def old_generate_interactive_plot(df: pd.DataFrame, symbol: str, final_val: float, gain: float):
    df.set_index('timestamp', inplace=True)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=df.index, y=df['close'], name='Price', line=dict(color='black')), secondary_y=False)
    fig.add_trace(go.Scatter(x=df.index, y=df['macd'], name='MACD', line=dict(dash='dot')), secondary_y=True)
    fig.add_trace(go.Scatter(x=df.index, y=df['macd_signal'], name='Signal', line=dict(dash='dot')), secondary_y=True)

    if portfolio.buy_log:
        fig.add_trace(go.Scatter(
            x=[x[0] for x in portfolio.buy_log],
            y=[x[1] for x in portfolio.buy_log],
            mode='markers+text',
            name='Buy',
            marker=dict(color='green', symbol='triangle-up', size=16, line=dict(width=2, color='darkgreen')),
            text=["Buy"] * len(portfolio.buy_log),
            textposition="top center",
            showlegend=True
        ), secondary_y=False)

    if portfolio.sell_log:
        fig.add_trace(go.Scatter(
            x=[x[0] for x in portfolio.sell_log],
            y=[x[1] for x in portfolio.sell_log],
            mode='markers+text',
            name='Sell',
            marker=dict(color='red', symbol='triangle-down', size=16, line=dict(width=2, color='darkred')),
            text=["Sell"] * len(portfolio.sell_log),
            textposition="bottom center",
            showlegend=True
        ), secondary_y=False)

    subtitle = f"Final Portfolio: ${final_val:.2f} (Gain: ${gain:+.2f})"
    fig.update_layout(title=f"Backtest for {symbol}<br><sub>{subtitle}</sub>",
                      xaxis_title="Time", yaxis_title="Price", legend_title="Legend")
    fig.update_yaxes(title_text="Price", secondary_y=False)
    fig.update_yaxes(title_text="MACD/Signal", secondary_y=True)
    fig.write_html("backtest_plot.html")

# --- Main runner ---
async def run_backtest(date: str, symbol: str):
    bt_log(f"🧪 Starting backtest for {symbol} on {date}")
    try:
        df = fetch_historical_bars(symbol, date)
        df = compute_macd(df.copy())
        dfs[symbol] = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'macd', 'macd_signal']]
        await simulate_bar_feed(symbol, df)

        # Final liquidation of any open positions
        if portfolio.position > 0:
            bt_log(f"🔁 Liquidating remaining position: {portfolio.position} shares @ {portfolio.last_price}")
            portfolio.sell(current_bar_time, portfolio.last_price, portfolio.position)

        final_val = portfolio.value()
        gain = final_val - 5000
        bt_log(f"💰 Final Portfolio Value: ${final_val:.2f} (Gain: ${gain:.2f})")
        generate_javascript_plot(df, symbol, final_val, gain)

    except Exception as e:
        bt_log(f"⚠️ Error in backtest for {symbol}: {e}")

    bt_log(f"✅ Backtest complete for {symbol}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python backtest_runner.py YYYY-MM-DD SYMBOL")
    else:
        date = sys.argv[1]
        symbol = sys.argv[2].upper()
        asyncio.run(run_backtest(date, symbol))

import json
def generate_javascript_plot(df: pd.DataFrame, symbol: str, final_val: float, gain: float):
    template_path = "backtest_plot_template.html"
    output_path = "backtest_plot.html"
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    df['timestamp'] = df['timestamp'].astype(str)

    html_out = template.replace("PRICE_TIMESTAMPS", json.dumps(df['timestamp'].tolist()))
    html_out = html_out.replace("PRICE_VALUES", json.dumps(df['close'].tolist()))
    html_out = html_out.replace("MACD_VALUES", json.dumps(df['macd'].tolist()))
    html_out = html_out.replace("SIGNAL_VALUES", json.dumps(df['macd_signal'].tolist()))
    html_out = html_out.replace("BUY_TIMES", json.dumps([str(t[0]) for t in portfolio.buy_log]))
    html_out = html_out.replace("BUY_PRICES", json.dumps([t[1] for t in portfolio.buy_log]))
    html_out = html_out.replace("SELL_TIMES", json.dumps([str(t[0]) for t in portfolio.sell_log]))
    html_out = html_out.replace("SELL_PRICES", json.dumps([t[1] for t in portfolio.sell_log]))

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_out)
