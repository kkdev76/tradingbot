
# FASTAPI log server
from fastapi import FastAPI
from fastapi.responses import FileResponse
import threading

app = FastAPI()

@app.get("/trading_log.txt")
def get_log():
    return FileResponse("trading_log.txt", media_type="text/plain")

def run_dashboard_server():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


"""
multi_symbol_macd_stream.py  (per-symbol dynamic USD budgets)

Console logging via Python's logging module, with UTC timestamps and fancy icons.
Positions are fetched via Alpaca API using get_open_position.

Entry: Buy when MACD > Signal AND |MACD| > PERCENT_THRESHOLD * |Signal|
Exit: Sell when MACD drops to or below tracked MACD value

Additional: Cancels stale pending BUY orders (status=NEW) at the start of each bar.
"""

import os, json, requests, pandas as pd, asyncio, websockets, logging, time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from buy_order import place_buy
from sell_order import place_sell
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderSide
from alpaca.trading.requests import ClosePositionRequest

# Configure logging to console with UTC timestamps
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)sZ] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
logger = logging.getLogger(__name__)

def log(msg: str):
    logger.info(msg)

# Load config
load_dotenv('crypto.env')
API_KEY    = os.getenv('APCA_API_KEY_ID')
SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')

# Alpaca trading client for position and order management
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# Symbols and threshold
SYMBOLS = [s.strip().upper() for s in os.getenv('STOCK_LIST','AAPL').split(',')]
PERCENT_THRESHOLD = float(os.getenv('PERCENT_THRESHOLD','0.1'))

# WebSocket URL
WS_URL = 'wss://stream.data.alpaca.markets/v2/sip'

# State
dfs = {sym: pd.DataFrame(columns=['timestamp','open','high','low','close','volume']) for sym in SYMBOLS}
remaining_budget = {sym: float(os.getenv('DEFAULT_DOLLAR','0')) for sym in SYMBOLS}
tracked_macd = {sym: None for sym in SYMBOLS}  # MACD value at purchase

def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    sig  = macd.ewm(span=9, adjust=False).mean()
    out = df.copy()
    out['macd'] = macd
    out['macd_signal'] = sig
    return out

def fetch_quote(sym: str) -> tuple:
    url = f"https://data.alpaca.markets/v2/stocks/{sym}/quotes/latest"
    r = requests.get(url, headers={
        'APCA-API-KEY-ID': API_KEY,
        'APCA-API-SECRET-KEY': SECRET_KEY
    }).json()
    q = r.get('quote', {})
    return float(q.get('bp', 0.0)), float(q.get('ap', 0.0))

def bootstrap_history(sym: str):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=120)
    bars = requests.get(
        f"https://data.alpaca.markets/v2/stocks/{sym}/bars",
        headers={'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY},
        params={'timeframe':'1Min', 'start':start.isoformat().replace('+00:00','Z'),
                'end':end.isoformat().replace('+00:00','Z'), 'limit':120}
    ).json().get('bars', [])
    if not bars:
        log(f"⚠️ No history for {sym}")
        return
    df = pd.DataFrame([{
        'timestamp': pd.to_datetime(b['t']).replace(tzinfo=timezone.utc),
        'open': b['o'], 'high': b['h'], 'low': b['l'], 'close': b['c'], 'volume': b['v']
    } for b in bars])
    dfs[sym] = compute_macd(df)
    log(f"✅ {sym}: bootstrapped {len(df)} bars.")

async def handle_bar(bar: dict):
    sym = bar['S']
    ts = datetime.fromisoformat(bar['t'].replace('Z','+00:00'))

    # Cancel stale pending BUY orders (status=NEW) for this symbol
    open_orders = trading_client.get_orders()
    for order in open_orders:
        if order.symbol == sym and order.status == OrderStatus.NEW and order.side == OrderSide.BUY:
            trading_client.cancel_order_by_id(order.id)
            log(f"🛑 Cancelled stale BUY order {order.id} for {sym}")
            

    # Append new bar and compute MACD
    row = {'timestamp':ts, 'open':bar['o'], 'high':bar['h'], 'low':bar['l'], 'close':bar['c'], 'volume':bar['v']}
    df = pd.concat([dfs[sym], pd.DataFrame([row])], ignore_index=True).tail(1000)
    dfs[sym] = df = compute_macd(df)
    macd, sig = df.iloc[-1]['macd'], df.iloc[-1]['macd_signal']
    log(f"🔄 {sym} bar at {ts.isoformat()}: MACD={macd:.4f}, Signal={sig:.4f}")

    # Retrieve current position
    try:
        pos_obj = trading_client.get_open_position(sym)
        pos = int(float(pos_obj.qty))
        log(f"ℹ️ Current position for {sym}: {pos} shares")
    except Exception as e:
        err = str(e)
        if '"code":40410000' in err and 'position does not exist' in err:
            log(f"⚠️ Zero Positions for {sym}")
        else:
            log(f"⚠️ Could not fetch position for {sym}: {err}")
        pos = 0

    # SELL/HOLD logic if tracking a buy
    if tracked_macd[sym] is not None:
        if macd > tracked_macd[sym]:
            log(f"🔼 HOLD {sym}: MACD rose {tracked_macd[sym]:.4f} → {macd:.4f}")
            tracked_macd[sym] = macd
        else:
            bid, _ = fetch_quote(sym)
            if pos > 0:
                log(f"🔴 SELL {sym}: MACD dropped to {macd:.4f} ≤ tracked {tracked_macd[sym]:.4f}, selling {pos} @ {bid:.2f}")
                place_sell(sym, bid, pos)
            tracked_macd[sym] = None
        return

    # BUY logic: only if no existing position
    if pos == 0:
        if macd > sig and abs(macd) > abs(sig) * PERCENT_THRESHOLD:
            bid, _ = fetch_quote(sym)
            limit = round(bid + 0.01, 2)
            gap_pct = abs(macd) / abs(sig) * 100 if sig else 0 
            log(f"🟢 BUY {sym}: MACD>{sig:.4f} & gap {gap_pct:.1f}% ≥ {PERCENT_THRESHOLD*100:.1f}% → buying @ {limit:.2f}")
            place_buy(sym, limit, remaining_budget[sym])
            tracked_macd[sym] = macd
        elif macd > sig:
            gap_pct = abs(macd) / abs(sig) * 100 if sig else 0
            log(f"⚪️ {sym}: MACD>sig but gap {gap_pct:.1f}% < {PERCENT_THRESHOLD*100:.1f}%, skipping buy")
    else:
        log(f"⚪️ Skipping buy for {sym}: existing API position of {pos} shares")

async def main():
    for sym in SYMBOLS:
        bootstrap_history(sym)
    log(f"Connecting to {WS_URL}...")
    log("----- Starting Stream -----")
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({'action':'auth','key':API_KEY,'secret':SECRET_KEY}))
        await ws.recv()
        await ws.send(json.dumps({'action':'subscribe','bars':SYMBOLS}))
        log(f"Subscribed to bars: {','.join(SYMBOLS)}")
        while True:
            msg = json.loads(await ws.recv())
            if isinstance(msg, list):
                for m in msg:
                    if m.get('T') == 'b':
                        await handle_bar(m)

if __name__ == '__main__':
    threading.Thread(target=run_dashboard_server, daemon=True).start()
    threading.Thread(target=run_dashboard_server, daemon=True).start()
    asyncio.run(main())
