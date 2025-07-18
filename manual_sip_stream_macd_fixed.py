import asyncio
import json
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import websockets
from utils import compute_macd

load_dotenv("crypto.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
SYMBOL = os.getenv("CRYPTO_SYMBOL", "AAPL")
WS_URL = "wss://stream.data.alpaca.markets/v2/sip"

df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
last_timestamp = None

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

def bootstrap_history():
    global df, last_timestamp
    log(f"📦 Bootstrapping historical data for {SYMBOL}...")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=60)
    url = f"https://data.alpaca.markets/v2/stocks/{SYMBOL}/bars"

    headers = {
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": SECRET_KEY
    }
    params = {
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "timeframe": "1Min",
        "adjustment": "raw",
        "limit": 60
    }

    resp = requests.get(url, headers=headers, params=params)
    data = resp.json()

    if "bars" not in data or not data["bars"]:
        log("⚠️ No historical data returned.")
        return

    bars = data["bars"]
    df_hist = pd.DataFrame([{
        "timestamp": pd.to_datetime(bar["t"]).replace(tzinfo=timezone.utc),
        "open": bar["o"],
        "high": bar["h"],
        "low": bar["l"],
        "close": bar["c"],
        "volume": bar["v"]
    } for bar in bars])

    df_hist = df_hist.sort_values("timestamp").reset_index(drop=True)
    df_hist = compute_macd(df_hist)
    df[:] = df_hist
    last_timestamp = df_hist["timestamp"].iloc[-1]
    log(f"✅ Bootstrapped {len(df)} bars.")
    print_latest_macd("📊 Initial MACD")

def print_latest_macd(prefix="📈 MACD Update"):
    if df.shape[0] < 35 or "macd" not in df.columns or "macd_signal" not in df.columns:
        log("⏳ Not enough data to compute MACD.")
        return
    last = df.iloc[-1]
    if pd.notna(last.get("macd")) and pd.notna(last.get("macd_signal")):
        log(f"{prefix}: [{last['timestamp']}] {SYMBOL} Close: {last['close']:.2f}, "
            f"MACD: {last['macd']:.4f}, Signal: {last['macd_signal']:.4f}")
    else:
        log("MACD values not ready.")

async def macd_loop():
    global last_timestamp
    while True:
        await asyncio.sleep(60)
        log("⏱️ Timer ticked. Recomputing MACD.")
        if df.empty:
            log("⚠️ DataFrame is empty.")
            continue
        if df["timestamp"].iloc[-1] == last_timestamp:
            log("⚠️ No new bar received in the last minute.")
        else:
            last_timestamp = df["timestamp"].iloc[-1]
        df[:] = compute_macd(df)
        print_latest_macd()

async def handle_bar(bar):
    global df
    ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00")) if isinstance(bar["t"], str)         else datetime.fromtimestamp(bar["t"] / 1e9, tz=timezone.utc)

    new_row = {
        "timestamp": ts,
        "open": bar["o"],
        "high": bar["h"],
        "low": bar["l"],
        "close": bar["c"],
        "volume": bar["v"]
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True).tail(1000)
    df[:] = compute_macd(df)
    log(f"📥 Received new bar: {SYMBOL} {ts} Close={new_row['close']}")
    print_latest_macd()

async def main():
    bootstrap_history()
    log(f"🚀 Starting data stream on {WS_URL}...")
    async with websockets.connect(WS_URL) as ws:
        log("🔌 Connecting to WebSocket...")
        await ws.send(json.dumps({
            "action": "auth",
            "key": API_KEY,
            "secret": SECRET_KEY
        }))
        response = await ws.recv()
        log("✅ Auth response: " + response)

        await asyncio.sleep(1)  # Ensure auth completes before subscribing

        await ws.send(json.dumps({
            "action": "subscribe",
            "bars": [SYMBOL]
        }))
        log(f"📡 Subscribed to 1-min bars for {SYMBOL}")
        asyncio.create_task(macd_loop())

        while True:
            try:
                message = await ws.recv()
                msg_data = json.loads(message)
                if isinstance(msg_data, list):
                    for bar in msg_data:
                        if bar.get("T") == "b":
                            await handle_bar(bar)
                else:
                    log("🟡 Non-bar message: " + str(msg_data))
            except websockets.exceptions.ConnectionClosed:
                log("❌ Connection closed. Reconnecting...")
                break

if __name__ == "__main__":
    asyncio.run(main())
