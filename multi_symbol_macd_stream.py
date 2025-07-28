import os
import json
import requests
import pandas as pd
import asyncio
import websockets
import logging
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from buy_order import place_buy
from sell_order import place_sell
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderSide
from alpaca.trading.requests import ClosePositionRequest

# Logging to file only
log_filename = 'trading_log.txt'
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)sZ] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
)
logger = logging.getLogger(__name__)
def log(msg: str):
    logger.info(msg)

# Load configuration
load_dotenv('crypto.env')
API_KEY = os.getenv('APCA_API_KEY_ID')
SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
TRADE_COOLDOWN_MINUTES = int(os.getenv('MIN_TRADE_COOLDOWN_MINUTES', '9'))
SYMBOLS = [s.strip().upper() for s in os.getenv('STOCK_LIST', 'AAPL').split(',')]
PERCENT_THRESHOLD = float(os.getenv('PERCENT_THRESHOLD', '0.1'))
WS_URL = 'wss://stream.data.alpaca.markets/v2/sip'

# Alpaca client
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# State
dfs = {sym: pd.DataFrame(columns=['timestamp','open','high','low','close','volume']) for sym in SYMBOLS}
remaining_budget = {sym: float(os.getenv('DEFAULT_DOLLAR','0')) for sym in SYMBOLS}
tracked_macd = {sym: None for sym in SYMBOLS}
last_trade_time = {sym: datetime.min.replace(tzinfo=timezone.utc) for sym in SYMBOLS}
# Live quote cache (bid, ask) updated from websocket
latest_quote = {sym: (0.0, 0.0) for sym in SYMBOLS}

# Utility functions
def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    sig = macd.ewm(span=9, adjust=False).mean()
    df['macd'] = macd
    df['macd_signal'] = sig
    return df

def fetch_quote(sym: str) -> tuple:
    """
    Return the most recent bid and ask from the websocket cache.
    """
    return latest_quote.get(sym, (0.0, 0.0))

# Bootstrap historical bars
def bootstrap_history(sym: str):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=120)
    bars = requests.get(
        f"https://data.alpaca.markets/v2/stocks/{sym}/bars",
        headers={'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY},
        params={
            'timeframe': '1Min',
            'start': start.isoformat().replace('+00:00','Z'),
            'end': end.isoformat().replace('+00:00','Z'),
            'limit': 120
        }
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

# Real-time bar handling
async def handle_bar(bar: dict):
    sym = bar['S']
    ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))
    # Append bar and compute MACD
    row = {'timestamp': ts, 'open': bar['o'], 'high': bar['h'], 'low': bar['l'], 'close': bar['c'], 'volume': bar['v']}
    df = pd.concat([dfs[sym], pd.DataFrame([row])], ignore_index=True).tail(1000)
    dfs[sym] = df = compute_macd(df)
    macd, sig = df.iloc[-1]['macd'], df.iloc[-1]['macd_signal']
    log(f"🔄 {sym} bar at {ts.isoformat()}: MACD={macd:.4f}, Signal={sig:.4f}")

    # Position check
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

    # SELL/HOLD logic
    if tracked_macd[sym] is not None:
        log(f"Checking SELL/HOLD for {sym}: current MACD={macd:.4f}, tracked={tracked_macd[sym]:.4f}")
        if macd > tracked_macd[sym]:
            log(f"🔼 HOLD {sym}: MACD rose {tracked_macd[sym]:.4f} → {macd:.4f}")
            tracked_macd[sym] = macd
        else:
            bid, _ = fetch_quote(sym)
            if pos > 0:
                log(f"🔴 SELL {sym}: MACD dropped to {macd:.4f} ≤ tracked {tracked_macd[sym]:.4f}, selling {pos} @ {bid:.2f}")
                place_sell(sym, bid, pos)
                last_trade_time[sym] = datetime.now(timezone.utc)
                log(f"Updated last_trade_time for {sym} to {last_trade_time[sym]}")
            tracked_macd[sym] = None
        return

    # BUY logic with cooldown
    if pos == 0:
        log(f"BUY check for {sym}: MACD={macd:.4f}, Signal={sig:.4f}, Threshold={PERCENT_THRESHOLD}")
        if macd > sig and abs(macd) > abs(sig) * PERCENT_THRESHOLD:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_trade_time[sym]).total_seconds() / 60
            log(f"⏳ Cooldown check for {sym}: elapsed {elapsed:.1f} min (threshold {TRADE_COOLDOWN_MINUTES} min)")
            if elapsed < TRADE_COOLDOWN_MINUTES:
                log(f"⏳ Skipping BUY for {sym}: cooldown active")
                return
            bid, _ = fetch_quote(sym)
            limit = round(bid + 0.01, 2)
            gap_pct = abs(macd) / abs(sig) * 100 if sig else 0
            log(f"🟢 BUY {sym}: MACD>{sig:.4f}, gap {gap_pct:.1f}% ≥ {PERCENT_THRESHOLD*100:.1f}% → buying @ {limit:.2f}")
            place_buy(sym, limit, remaining_budget[sym])
            tracked_macd[sym] = macd
            last_trade_time[sym] = now
            log(f"Updated last_trade_time for {sym} to {last_trade_time[sym]}")
        elif macd > sig:
            gap_pct = abs(macd) / abs(sig) * 100 if sig else 0
            log(f"⚪️ {sym}: MACD>sig but gap {gap_pct:.1f}% < {PERCENT_THRESHOLD*100:.1f}%, skipping buy")
    else:
        log(f"⚪️ Skipping buy for {sym}: existing position of {pos} shares")

# Main loop
async def main():
    for sym in SYMBOLS:
        bootstrap_history(sym)
    log("🚀 Connecting to real-time stream...")
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({'action':'auth','key':API_KEY,'secret':SECRET_KEY}))
        await ws.recv()
        # Subscribe to both bars and quotes for real-time streaming
        await ws.send(json.dumps({'action':'subscribe','bars': SYMBOLS, 'quotes': SYMBOLS}))
        log(f"Subscribed to bars: {','.join(SYMBOLS)}")
        while True:
            msg = await ws.recv()
            data = json.loads(msg) if isinstance(msg, str) else msg
            for m in data:
                msg_type = m.get('T')
                if msg_type == 'b':
                    await handle_bar(m)
                elif msg_type == 'q':  # quote update
                    sym_q = m.get('S')
                    bp = m.get('bp', 0.0)
                    ap = m.get('ap', 0.0)
                    latest_quote[sym_q] = (bp, ap)
                    log(f"💬 QUOTE {sym_q}: bid={bp:.2f}, ask={ap:.2f}")

if __name__ == '__main__':
    asyncio.run(main())
