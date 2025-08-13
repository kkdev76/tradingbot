import os
import json
import requests
import pandas as pd
import asyncio
import websockets
import logging
import time
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from buy_order import place_buy
from sell_order import place_sell
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderSide
from alpaca.trading.requests import ClosePositionRequest

# Logging to file only

from zoneinfo import ZoneInfo

# Logging to file in Pacific Time (handles PST/PDT automatically)
log_filename = 'trading_log.txt'

class PTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, ZoneInfo("America/Los_Angeles"))
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
file_handler.setFormatter(PTFormatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S'))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Avoid duplicate handlers if this module is reloaded
if not logger.handlers:
    logger.addHandler(file_handler)

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

# ===== Risk Guard config =====
def _load_negative_pl_threshold() -> float:
    raw = os.getenv("NEGATIVE_PL_THRESHOLD") or os.getenv("NEGATIVE_PL_LIMIT")
    if raw is None:
        log(f"⚠️ NEGATIVE_PL_THRESHOLD not set; defaulting to -250.00")
        return -250.0
    try:
        val = float(raw)
    except Exception:
        log(f"❌ Invalid NEGATIVE_PL_THRESHOLD value: {raw}. Falling back to -250.00")
        return -250.0
    if val >= 0:
        log(f"⚠️ NEGATIVE_PL_THRESHOLD should be NEGATIVE. Interpreting as negative of provided value.")
        val = -abs(val)
    return val

NEGATIVE_PL_THRESHOLD = _load_negative_pl_threshold()

# Alpaca client
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# State
dfs = {sym: pd.DataFrame(columns=['timestamp','open','high','low','close','volume']) for sym in SYMBOLS}
remaining_budget = {sym: float(os.getenv('DEFAULT_DOLLAR','0')) for sym in SYMBOLS}
tracked_macd = {sym: None for sym in SYMBOLS}
last_trade_time = {sym: datetime.min.replace(tzinfo=timezone.utc) for sym in SYMBOLS}
# Live quote cache (bid, ask) updated from websocket
latest_quote = {sym: (0.0, 0.0) for sym in SYMBOLS}
_macd_neutral_cache = {}

def _get_macd_neutral_threshold(sym: str) -> float:
    """
    Returns the neutral-band threshold for MACD (±threshold) for a given symbol,
    read from environment variables. Expected key: MACD_NEUTRAL_<SYMBOL> (e.g., MACD_NEUTRAL_AMD=0.2).
    Falls back to MACD_NEUTRAL_DEFAULT, else 0.2.
    Cached per symbol to avoid repeated env lookups.
    """
    key_specific = f"MACD_NEUTRAL_{sym.upper()}"
    if sym in _macd_neutral_cache:
        return _macd_neutral_cache[sym]
    raw = os.getenv(key_specific)
    if raw is None:
        raw = os.getenv("MACD_NEUTRAL_DEFAULT", "0.2")
        # Only log once per symbol for missing specific key
        log(f"⚙️ {key_specific} not set; using MACD_NEUTRAL_DEFAULT={raw}")
    try:
        val = float(raw)
    except Exception:
        log(f"⚠️ Invalid neutral threshold for {sym}: '{raw}'. Falling back to 0.2")
        val = 0.2
    val = abs(val)
    _macd_neutral_cache[sym] = val
    return val


# ===== Risk Guard state & helpers =====
STOP_TRADING = False
_last_pl_check_minute = None

def get_daily_pl(client: TradingClient) -> float:
    """Daily P/L since yesterday's close: equity - last_equity (realized + unrealized)."""
    acct = client.get_account()
    return float(acct.equity) - float(acct.last_equity)

def _cancel_all_orders_robust(client: TradingClient):
    """Handle API name differences across versions."""
    try:
        client.cancel_orders()
        return
    except Exception as e:
        log(f"🐞 cancel_orders not available or failed: {e}")
    try:
        # Older/alternate naming
        client.cancel_all_orders()
    except Exception as e:
        log(f"⚠️ Failed to cancel orders via API: {e}")

def _cancel_open_orders_for_symbol(client: TradingClient, sym: str) -> int:
    """
    Cancel any active/unfilled orders for a given symbol.
    Returns the number of orders successfully canceled.
    """
    cancelled = 0
    orders = None
    # Try multiple ways to fetch open/active orders
    try:
        orders = client.get_orders(status=getattr(OrderStatus, "OPEN", "open"))
    except Exception:
        try:
            orders = client.get_orders(status="open")
        except Exception as e2:
            log(f"❌ Unable to fetch open orders for cleanup on {sym}: {e2}")
            return 0

    actionable_statuses = {"new", "accepted", "open", "partially_filled", "pending_new", "accepted_for_bidding"}
    for o in (orders or []):
        try:
            o_symbol = getattr(o, "symbol", getattr(o, "asset_symbol", None))
            if o_symbol != sym:
                continue
            o_status = str(getattr(o, "status", "")).lower()
            if o_status not in actionable_statuses:
                continue

            oid = getattr(o, "id", getattr(o, "order_id", None))
            if not oid:
                continue

            try:
                if hasattr(client, "cancel_order_by_id"):
                    client.cancel_order_by_id(oid)
                elif hasattr(client, "cancel_order"):
                    client.cancel_order(oid)
                else:
                    _cancel_all_orders_robust(client)
                cancelled += 1
                log(f"🧹 Canceled stale open order for {sym} (id={oid}, status={o_status})")
            except Exception as ce:
                log(f"❌ Failed to cancel order {oid} for {sym}: {ce}")
        except Exception as ie:
            log(f"❌ Error while scanning order for {sym}: {ie}")

    if cancelled > 0:
        log(f"🧹 Cleanup complete for {sym}: canceled {cancelled} stale open order(s)")
    return cancelled


def _list_positions_len(client: TradingClient) -> int:
    try:
        poss = client.get_all_positions()
        return len(poss or [])
    except Exception as e:
        log(f"⚠️ list positions failed: {e}")
        return 0

def trigger_global_liquidation_and_exit(client: TradingClient, reason: str = ""):
    """Cancels orders, liquidates all positions, blocks further orders, exits process."""
    global STOP_TRADING
    STOP_TRADING = True
    try:
        log(f"❌ [RiskGuard] *** HALTING TRADES: {reason} ***")
        log(f"ℹ️ [RiskGuard] Cancelling all open orders...")
        _cancel_all_orders_robust(client)

        log(f"ℹ️ [RiskGuard] Closing all positions at market...")
        try:
            client.close_all_positions(cancel_orders=True)
        except TypeError:
            # if older signature without keyword
            client.close_all_positions(True)
        except Exception as e:
            log(f"💥 [RiskGuard] close_all_positions error: {e}")

        deadline = time.time() + 45
        while time.time() < deadline:
            remaining = _list_positions_len(client)
            if remaining == 0:
                log(f"ℹ️ [RiskGuard] All positions closed.")
                break
            log(f"ℹ️ [RiskGuard] Waiting for {remaining} positions to close...")
            time.sleep(3)

        log(f"❌ [RiskGuard] Exiting process due to daily P/L breach.")
    except Exception as e:
        log(f"💥 [RiskGuard] Error during liquidation: {e}")
    finally:
        try:
            for h in logging.getLogger().handlers:
                try:
                    h.flush()
                except Exception:
                    pass
        finally:
            sys.exit(2)

def risk_guard_check(client: TradingClient):
    """Run at most once per minute. If daily P/L <= threshold, liquidate and exit."""
    global _last_pl_check_minute, STOP_TRADING
    if STOP_TRADING:
        return
    now = datetime.now(timezone.utc)
    minute_key = now.strftime("%Y-%m-%d %H:%M")
    if _last_pl_check_minute == minute_key:
        return
    _last_pl_check_minute = minute_key


from zoneinfo import ZoneInfo

_last_shutdown_check_minute = None



def fetch_quote(sym: str) -> tuple:
    """Return the most recent bid and ask from the websocket cache."""
    return latest_quote.get(sym, (0.0, 0.0))

# Bootstrap historical bars
# === Technical Indicators ===
def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute MACD and Signal line on a DataFrame with a 'close' column.
    Adds columns: 'macd' and 'macd_signal' and returns the DataFrame.
    """
    # Use standard 12/26/9 EMA configuration
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    sig = macd.ewm(span=9, adjust=False).mean()
    out = df.copy()
    out['macd'] = macd
    out['macd_signal'] = sig
    return out


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
def scheduled_shutdown_guard_check(client: TradingClient):
    """
    At CRON_SHUTDOWN time (PST/PDT), cancel all orders, liquidate, and exit.
    """
    global _last_shutdown_check_minute, STOP_TRADING

    if STOP_TRADING:
        return  # Already halted

    shutdown_time_str = os.getenv("CRON_SHUTDOWN", "").strip()
    if not shutdown_time_str:
        return  # Disabled if not set

    try:
        shutdown_hour, shutdown_minute = map(int, shutdown_time_str.split(":"))
    except Exception:
        log(f"❌ Invalid CRON_SHUTDOWN value: {shutdown_time_str}")
        return

    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    minute_key = now_pt.strftime("%Y-%m-%d %H:%M")

    # Avoid repeating shutdown check more than once per minute
    if _last_shutdown_check_minute == minute_key:
        return
    _last_shutdown_check_minute = minute_key

    if now_pt.hour == shutdown_hour and now_pt.minute == shutdown_minute:
        log(f"⚠️ [ScheduledShutdown] Triggering daily shutdown at {shutdown_time_str} PT")
        trigger_global_liquidation_and_exit(
            client,
            reason=f"Scheduled shutdown at {shutdown_time_str} PT"
        )

    try:
        daily_pl = get_daily_pl(client)
        log(f"ℹ️ [RiskGuard] Daily P/L: {daily_pl:.2f} (threshold {NEGATIVE_PL_THRESHOLD:.2f})")
        if daily_pl <= NEGATIVE_PL_THRESHOLD:
            log(f"❌ [RiskGuard] BREACH: {daily_pl:.2f} <= {NEGATIVE_PL_THRESHOLD:.2f}")
            trigger_global_liquidation_and_exit(client, reason=f"Daily P/L {daily_pl:.2f} <= {NEGATIVE_PL_THRESHOLD:.2f}")
    except Exception as e:
        log(f"💥 [RiskGuard] Failed to compute daily P/L: {e}")


 

async def handle_bar(bar: dict):
    sym = bar['S']

    # ===== Guards: run once per minute =====
    risk_guard_check(trading_client)
    
    # ===== Scheduled Shutdown: check once per minute =====
    scheduled_shutdown_guard_check(trading_client)
    # ===== Scheduled Shutdown: check once per minute =====


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
                if STOP_TRADING:
                    log(f"[RiskGuard] Blocked SELL {sym} x{pos} (trading halted)")
                    return
                log(f"🔴 SELL {sym}: MACD dropped to {macd:.4f} ≤ tracked {tracked_macd[sym]:.4f}, selling {pos} @ {bid:.2f}")
                place_sell(sym, bid, pos)
                last_trade_time[sym] = datetime.now(timezone.utc)
                log(f"Updated last_trade_time for {sym} to {last_trade_time[sym]}")
            tracked_macd[sym] = None
        return

    # BUY logic with cooldown
    if pos == 0:
        # If a BUY was placed earlier but never filled, there could be open/working orders.
        # Proactively cancel any such orders for this symbol and reset tracking vars.
        stale = _cancel_open_orders_for_symbol(trading_client, sym)
        if stale > 0:
            tracked_macd[sym] = None
            last_trade_time[sym] = datetime.min.replace(tzinfo=timezone.utc)
            log(f"🔄 Reset tracked_macd and last_trade_time after canceling {stale} stale order(s) for {sym}")

        
        gap_pct = (abs(abs(macd)-abs(sig))/abs(sig))*100 if sig else 0
        log(f"BUY check for {sym}: MACD={macd:.4f}, Signal={sig:.4f},Gap={gap_pct:.2f}, Threshold={PERCENT_THRESHOLD}")
        if macd > sig and  gap_pct > PERCENT_THRESHOLD*100:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_trade_time[sym]).total_seconds() / 60
            log(f"⏳ Cooldown check for {sym}: elapsed {elapsed:.1f} min (threshold {TRADE_COOLDOWN_MINUTES} min)")
            if elapsed < TRADE_COOLDOWN_MINUTES:
                log(f"⏳ Skipping BUY for {sym}: cooldown active")
                return
            threshold = _get_macd_neutral_threshold(sym)
            if -threshold < macd < threshold:
                log(f"⏳ MACD in uncertainty zone for {sym}: |MACD|={abs(macd):.4f} < {threshold:.4f}; skipping buy")
                return
            
            bid, _ = fetch_quote(sym)
            limit = round(bid + 0.01, 2)

            if STOP_TRADING:
                log(f"[RiskGuard] Blocked BUY {sym} (trading halted)")
                return
            
            log(f"🟢 BUY {sym}: MACD:{macd}, Signal:{sig}  {macd:.4f}>{sig:.4f}, gap {gap_pct:.1f}% ≥ {PERCENT_THRESHOLD*100:.1f}% → buying @ {limit:.2f}")
            place_buy(sym, limit, remaining_budget[sym])
            tracked_macd[sym] = macd
            last_trade_time[sym] = now
            log(f"Updated last_trade_time for {sym} to {last_trade_time[sym]}")
        elif macd > sig:
             
            log(f"⚪️ {sym}: MACD>sig but gap {gap_pct:.1f}% < {PERCENT_THRESHOLD*100:.1f}%, skipping buy")
    else:
        log(f"⚪️ Skipping buy for {sym}: existing position of {pos} shares")


# Main loop
async def main():
    for sym in SYMBOLS:
        bootstrap_history(sym)
    log(f"🚀 Connecting to real-time stream...")
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
                    # log(f"💬 QUOTE {sym_q}: bid={bp:.2f}, ask={ap:.2f}")
    
if __name__ == '__main__':
    asyncio.run(main())