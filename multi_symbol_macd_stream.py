"""
multi_symbol_macd_stream.py  (per‑symbol budgets)

• Reads STOCK_LIST (comma) and STOCK_BUDGETS (comma, symbol:amount) from crypto.env
• Example:
    STOCK_LIST=AAPL,NVDA,MSFT
    STOCK_BUDGETS=AAPL:500,NVDA:800,MSFT:300
    # Fallback DOLLAR_AMOUNT applies if a symbol has no entry.

Other logic unchanged: BUY when MACD>Signal>0, SELL when MACD stops rising.
"""

import asyncio, json, os, requests, pandas as pd
from datetime import datetime, timezone, timedelta
import websockets
from dotenv import load_dotenv
from utils import compute_macd
from buy_order import place_buy
from sell_order import place_sell

load_dotenv("crypto.env")

API_KEY    = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# Symbols
SYMBOLS = [s.strip().upper() for s in os.getenv("STOCK_LIST", "AAPL").split(",")]

# Default dollars if specific budget missing
DEFAULT_DOLLARS = float(os.getenv("DOLLAR_AMOUNT", "500"))

# Per‑symbol budgets
budget_env = os.getenv("STOCK_BUDGETS", "")
SYMBOL_BUDGETS = {}
if budget_env:
    for pair in budget_env.split(","):
        if ":" in pair:
            sym, amt = pair.split(":")
            SYMBOL_BUDGETS[sym.strip().upper()] = float(amt)

def budget_for(sym):
    return SYMBOL_BUDGETS.get(sym, DEFAULT_DOLLARS)

WS_URL = "wss://stream.data.alpaca.markets/v2/sip"

dfs        = {sym: pd.DataFrame(columns=["timestamp","open","high","low","close","volume"]) for sym in SYMBOLS}
holdings   = {sym: 0 for sym in SYMBOLS}
last_macd  = {sym: None for sym in SYMBOLS}
last_ts    = {sym: None for sym in SYMBOLS}

def log(msg): 
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

def bootstrap_history(symbol):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(minutes=120)
    url   = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": "1Min",
        "start": start.isoformat().replace('+00:00','Z'),
        "end":   end.isoformat().replace('+00:00','Z'),
        "limit": 120,
        "adjustment":"raw"
    }
    hdr = {"APCA-API-KEY-ID":API_KEY,"APCA-API-SECRET-KEY":SECRET_KEY}
    r = requests.get(url, headers=hdr, params=params)
    bars = r.json().get("bars", [])
    if not bars:
        log(f"⚠️ No history for {symbol}")
        return
    df = pd.DataFrame([{ 
        "timestamp": pd.to_datetime(b["t"]).replace(tzinfo=timezone.utc),
        "open":b["o"],"high":b["h"],"low":b["l"],"close":b["c"],"volume":b["v"] 
    } for b in bars])
    dfs[symbol] = compute_macd(df)
    last_ts[symbol] = dfs[symbol]["timestamp"].iloc[-1]
    log(f"✅ {symbol}: bootstrapped {len(df)} bars.")

async def macd_loop():
    while True:
        await asyncio.sleep(60)
        for sym in SYMBOLS:
            if dfs[sym].empty: continue
            dfs[sym] = compute_macd(dfs[sym])
            macd   = dfs[sym].iloc[-1]["macd"]
            signal = dfs[sym].iloc[-1]["macd_signal"]
            if holdings[sym] > 0:
                if macd <= last_macd[sym]:
                    ask = dfs[sym].iloc[-1]["close"]
                    place_sell(sym, ask, holdings[sym])
                    log(f"🔴 SELL {sym} {holdings[sym]} @ ask-0.05  (MACD {macd:.4f} ≤ Prev {last_macd[sym]:.4f})")
                    holdings[sym] = 0
                else:
                    last_macd[sym] = macd

async def handle_bar(bar):
    sym = bar["S"]
    ts  = datetime.fromisoformat(bar["t"].replace("Z","+00:00"))
    new = {"timestamp": ts, "open":bar["o"],"high":bar["h"],"low":bar["l"],"close":bar["c"],"volume":bar["v"]}
    dfs[sym] = pd.concat([dfs[sym], pd.DataFrame([new])], ignore_index=True).tail(1000)
    dfs[sym] = compute_macd(dfs[sym])
    macd   = dfs[sym].iloc[-1]["macd"]
    signal = dfs[sym].iloc[-1]["macd_signal"]
    if holdings[sym]==0 and macd>signal>0:
        bid = bar.get("b", new["close"])
        qty = place_buy(sym, bid, budget_for(sym))
        holdings[sym]=qty
        last_macd[sym]=macd
        log(f"🟢 BUY {sym} {qty} @ bid+0.05  (MACD {macd:.4f} > Signal {signal:.4f})")

async def main():
    for s in SYMBOLS:
        bootstrap_history(s)
    log(f"Connecting to {WS_URL} …")
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({"action":"auth","key":API_KEY,"secret":SECRET_KEY}))
        await ws.recv()
        await ws.send(json.dumps({"action":"subscribe","bars":SYMBOLS}))
        log(f"📡 Subscribed to: {', '.join(SYMBOLS)}")
        asyncio.create_task(macd_loop())
        while True:
            msg = json.loads(await ws.recv())
            if isinstance(msg, list):
                for b in msg:
                    if b.get("T") == "b":
                        await handle_bar(b)

if __name__ == "__main__":
    asyncio.run(main())
