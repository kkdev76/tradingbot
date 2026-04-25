import os
import csv
import json
import requests
import pandas as pd
import asyncio
import websockets
import logging
import time
import sys
from datetime import datetime, timezone, timedelta, time as datetime_time
from dotenv import load_dotenv
from buy_order import place_buy
from sell_order import place_sell, place_sell_market
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
load_dotenv('keys.env')
load_dotenv('settings.td')
API_KEY = os.getenv('APCA_API_KEY_ID')
SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
TRADE_COOLDOWN_MINUTES = int(os.getenv('MIN_TRADE_COOLDOWN_MINUTES', '9'))
SYMBOLS = [s.strip().upper() for s in os.getenv('STOCK_LIST', 'AAPL').split(',')]
MACD_GAP_PERCENT = float(os.getenv('MACD_GAP_PERCENT', '40'))
MACD_FLATTEN_THRESHOLD = float(os.getenv('MACD_FLATTEN_THRESHOLD', '0.01'))
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '9'))
RSI_BUY_MIN = float(os.getenv('RSI_BUY_MIN', '50'))
RSI_BUY_MAX = float(os.getenv('RSI_BUY_MAX', '65'))
BOOTSTRAP_BAR_COUNT  = int(os.getenv('BOOTSTRAP_BAR_COUNT', '800'))
BOOTSTRAP_MAX_DAYS_BACK = int(os.getenv('BOOTSTRAP_MAX_DAYS_BACK', '5'))
MACD_WARMUP_BARS = int(os.getenv('MACD_WARMUP_BARS', '35'))
WS_URL = 'wss://stream.data.alpaca.markets/v2/sip'

# ===== Risk Guard config =====
def _load_negative_pl_threshold() -> float:
    raw = os.getenv("NEGATIVE_PL_THRESHOLD") or os.getenv("NEGATIVE_PL_LIMIT")
    if raw is None:
        log(f"⚠️ NEGATIVE_PL_THRESHOLD not set; defaulting to -250.00")
        return -250.0
    try:
        val = float(raw)
        log(f"STOP LOSS THRESHOLD RETRIEVED :: {val}")
    except Exception:
        log(f"❌ Invalid NEGATIVE_PL_THRESHOLD value: {raw}. Falling back to -250.00")
        return -250.0
    if val >= 0:
        log(f"⚠️ NEGATIVE_PL_THRESHOLD should be NEGATIVE. Interpreting as negative of provided value.")
        val = -abs(val)
    return val

NEGATIVE_PL_THRESHOLD = _load_negative_pl_threshold()

def _load_stop_loss_percent() -> float:
    raw = os.getenv("STOP_LOSS_PERCENT")
    if raw is None:
        log(f"⚠️ STOP_LOSS_PERCENT not set; defaulting to 0.15%")
        return 0.15
    try:
        val = abs(float(raw))
        log(f"STOP LOSS PERCENT RETRIEVED :: {val}%")
        return val
    except Exception:
        log(f"❌ Invalid STOP_LOSS_PERCENT value: {raw}. Falling back to 0.15%")
        return 0.15

STOP_LOSS_PERCENT = _load_stop_loss_percent()

# Alpaca client
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# State
dfs = {sym: pd.DataFrame(columns=['timestamp','open','high','low','close','volume']) for sym in SYMBOLS}
# 3-bar rolling MACD buffer tracked from the moment a position is opened
_nan = float('nan')
position_macd_buffer = {sym: [_nan, _nan, _nan] for sym in SYMBOLS}
# Parse per-symbol budgets from DOLLAR_BUDGETS (e.g. AMD:10000,TSLA:5000)
# Falls back to DEFAULT_DOLLAR if a symbol is not listed
_default_dollar = float(os.getenv('DEFAULT_DOLLAR', '0'))
_dollar_budgets_raw = os.getenv('DOLLAR_BUDGETS', '')
_dollar_budgets = {}
for _entry in _dollar_budgets_raw.split(','):
    _entry = _entry.strip()
    if ':' in _entry:
        _sym, _val = _entry.split(':', 1)
        try:
            _dollar_budgets[_sym.strip().upper()] = float(_val.strip())
        except ValueError:
            pass
remaining_budget = {sym: _dollar_budgets.get(sym, _default_dollar) for sym in SYMBOLS}
log(f"💵 Budgets loaded: { {s: remaining_budget[s] for s in SYMBOLS} }")
last_trade_time = {sym: datetime.min.replace(tzinfo=timezone.utc) for sym in SYMBOLS}
# Live quote cache (bid, ask) updated from websocket
latest_quote = {sym: (0.0, 0.0) for sym in SYMBOLS}
_macd_neutral_cache = {}
_last_delimiter_minute = None

# MACD buffer for trend analysis - stores last 5 MACD values for each symbol
macd_buffer = {sym: [] for sym in SYMBOLS}
MACD_BUFFER_SIZE = 5
# RSI buffer for trend analysis - stores last 5 RSI values for each symbol
rsi_buffer = {sym: [] for sym in SYMBOLS}
RSI_BUFFER_SIZE = 5

def update_macd_buffer(sym: str, macd_value: float):
    """
    Update the MACD buffer for a symbol with a new value.
    Maintains a fixed-size buffer by evicting oldest value when full.
    """
    if sym not in macd_buffer:
        macd_buffer[sym] = []
    
    buffer = macd_buffer[sym]
    buffer.append(macd_value)
    
    # Keep only the last MACD_BUFFER_SIZE values
    if len(buffer) > MACD_BUFFER_SIZE:
        buffer.pop(0)  # Remove oldest value
    
    macd_buffer[sym] = buffer

def is_macd_monotonically_increasing(sym: str) -> bool:
    """
    Check if all 5 MACD values in the buffer are monotonically increasing.
    Returns True only if each value is strictly greater than the previous.
    Requires at least MACD_BUFFER_SIZE (5) values in buffer to return True.
    """
    buffer = macd_buffer.get(sym, [])
    if len(buffer) < MACD_BUFFER_SIZE:
        return False

    # Check if values are strictly increasing across all 5 bars
    return buffer[4] > buffer[3] > buffer[2] > buffer[1] > buffer[0]

def update_rsi_buffer(sym: str, rsi_value: float):
    """Update the RSI buffer, maintaining a fixed size of RSI_BUFFER_SIZE."""
    if sym not in rsi_buffer:
        rsi_buffer[sym] = []
    buffer = rsi_buffer[sym]
    buffer.append(rsi_value)
    if len(buffer) > RSI_BUFFER_SIZE:
        buffer.pop(0)
    rsi_buffer[sym] = buffer

def is_rsi_monotonically_increasing(sym: str) -> bool:
    """
    Stability check: returns True if all 5 RSI values in the buffer are >= RSI_BUY_MIN (50).
    Ensures RSI has been solidly above the buy floor for 5 consecutive bars, not just a brief spike.
    Requires at least RSI_BUFFER_SIZE (5) values in buffer to return True.
    """
    buffer = rsi_buffer.get(sym, [])
    if len(buffer) < RSI_BUFFER_SIZE:
        return False
    return all(v >= RSI_BUY_MIN for v in buffer)

def update_position_macd_buffer(sym: str, macd: float):
    """
    Update the 3-bar rolling MACD buffer for a held position.
    Fills slots left to right until full, then shifts oldest out.
    """
    buf = position_macd_buffer[sym]
    nan = float('nan')
    if buf[0] != buf[0]:        # buf[0] is NaN — buffer empty
        position_macd_buffer[sym] = [macd, nan, nan]
    elif buf[1] != buf[1]:      # buf[1] is NaN — one value stored
        position_macd_buffer[sym] = [buf[0], macd, nan]
    elif buf[2] != buf[2]:      # buf[2] is NaN — two values stored
        position_macd_buffer[sym] = [buf[0], buf[1], macd]
    else:                        # Buffer full — shift left, append new
        position_macd_buffer[sym] = [buf[1], buf[2], macd]

def reset_position_macd_buffer(sym: str):
    """Reset position MACD buffer when a position is closed."""
    nan = float('nan')
    position_macd_buffer[sym] = [nan, nan, nan]

def check_macd_exit(sym: str) -> tuple:
    """
    Returns (should_sell, reason).
    Requires 3 bars of data. Holds if monotonically increasing.
    Sells if flattening (current ≈ previous) or declining.
    """
    buf = position_macd_buffer[sym]
    if any(v != v for v in buf):   # NaN present — not enough data yet
        return False, ""
    b0, b1, b2 = buf
    if b0 < b1 < b2:               # Monotonically increasing — hold
        return False, ""
    diff = b2 - b1
    if abs(diff) < MACD_FLATTEN_THRESHOLD:
        return True, f"flattening (Δ={diff:.4f}, threshold=±{MACD_FLATTEN_THRESHOLD})"
    else:
        return True, f"declining ({b1:.4f} → {b2:.4f})"

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

_profit_take_cache = {}

def _get_profit_take_percent(sym: str) -> float:
    """
    Returns the take-profit threshold (% gain) for a given symbol,
    read from environment variables. Expected key: PROFIT_TAKE_<SYMBOL> (e.g., PROFIT_TAKE_AMD=1.0).
    Falls back to PROFIT_TAKE_DEFAULT, else 1.0.
    Cached per symbol to avoid repeated env lookups.
    """
    if sym in _profit_take_cache:
        return _profit_take_cache[sym]
    key_specific = f"PROFIT_TAKE_{sym.upper()}"
    raw = os.getenv(key_specific)
    if raw is None:
        raw = os.getenv("PROFIT_TAKE_DEFAULT", "1.0")
        log(f"⚙️ {key_specific} not set; using PROFIT_TAKE_DEFAULT={raw}")
    try:
        val = float(raw)
    except Exception:
        log(f"⚠️ Invalid profit take threshold for {sym}: '{raw}'. Falling back to 1.0")
        val = 1.0
    val = abs(val)
    _profit_take_cache[sym] = val
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
    # Try multiple ways to fetch open/active orders (handle SDK version differences)
    try:
        # Preferred: request-object API (newer Alpaca SDKs)
        try:
            from alpaca.trading.requests import GetOrdersRequest  # type: ignore
            try:
                from alpaca.trading.enums import QueryOrderStatus  # type: ignore
                req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            except Exception:
                # Fallback: some SDKs accept string in request
                req = GetOrdersRequest(status="open")
            orders = client.get_orders(req)
        except Exception:
            # Fallback: older SDKs may not take params; fetch all and filter client-side
            orders = client.get_orders()
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
            # Handle enum status format (e.g., "orderstatus.new" -> "new")
            if "." in o_status:
                o_status = o_status.split(".")[-1]
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
                if '42210000' in str(ce):
                    log(f"⚠️ Order {oid} for {sym} already pending cancel — skipping")
                else:
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

        log(f"❌ [RiskGuard] Exiting process. Reason: {reason or 'N/A'}")
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
    # log(f"🛡️ [RiskGuard] Check start")
    global _last_pl_check_minute, STOP_TRADING
    if STOP_TRADING:
        log(f"🛑 [RiskGuard] Trading halted; skipping risk check")
        return
    now = datetime.now(timezone.utc)
    minute_key = now.strftime("%Y-%m-%d %H:%M")
    if _last_pl_check_minute == minute_key:
        # log(f"⏭️ [RiskGuard] Already checked this minute ({minute_key}); skipping")
        return
    _last_pl_check_minute = minute_key
    # log(f"✅ [RiskGuard] Marked risk check at {minute_key}")


from zoneinfo import ZoneInfo

_last_shutdown_check_minute = None



def fetch_quote(sym: str) -> tuple:
    """Return the most recent bid and ask from the websocket cache."""
    return latest_quote.get(sym, (0.0, 0.0))

# ===== Per-symbol indicator CSV logging =====
_INDICATOR_LOG_DIR = 'indicator_logs'
_indicator_log_writers = {}   # sym -> csv.writer
_indicator_log_files   = {}   # sym -> file handle

def _get_indicator_writer(sym: str):
    """Return (creating if needed) the CSV writer for a symbol."""
    if sym not in _indicator_log_writers:
        os.makedirs(_INDICATOR_LOG_DIR, exist_ok=True)
        date_str = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')
        filepath = os.path.join(_INDICATOR_LOG_DIR, f"{sym}_{date_str}.csv")
        is_new = not os.path.exists(filepath) or os.path.getsize(filepath) == 0
        f = open(filepath, 'a', newline='', encoding='utf-8')
        writer = csv.writer(f)
        if is_new:
            writer.writerow(['timestamp', 'close', 'MACD', 'Signal', 'RSI'])
            f.flush()
        _indicator_log_writers[sym] = writer
        _indicator_log_files[sym] = f
        log(f"📁 Indicator log opened: {filepath}")
    return _indicator_log_writers[sym]

def _log_indicators(sym: str, ts, close: float, macd: float, sig: float, rsi: float):
    """Append one row to the per-symbol indicator CSV."""
    try:
        writer = _get_indicator_writer(sym)
        ts_pt = ts.astimezone(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%dT%H:%M:%S')
        writer.writerow([ts_pt, f'{close:.4f}', f'{macd:.4f}', f'{sig:.4f}', f'{rsi:.2f}'])
        _indicator_log_files[sym].flush()
    except Exception as e:
        log(f"⚠️ Failed to write indicator log for {sym}: {e}")

# Bootstrap historical bars
# === Technical Indicators ===
def _sma_seeded_ema(series: pd.Series, period: int) -> pd.Series:
    """
    EMA with SMA seed — matches ProRealTime and most professional platforms.
    First EMA value = SMA of the first `period` bars; exponential from there.
    pandas ewm(adjust=False) seeds from bar-1 instead, causing divergence.
    """
    k = 2.0 / (period + 1)
    vals = series.to_numpy(dtype=float)
    out = [float('nan')] * len(vals)
    if len(vals) < period:
        return pd.Series(out, index=series.index)
    out[period - 1] = float(sum(vals[:period]) / period)   # SMA seed
    for i in range(period, len(vals)):
        out[i] = vals[i] * k + out[i - 1] * (1 - k)
    return pd.Series(out, index=series.index)


def _wilder_rsi(series: pd.Series, period: int) -> pd.Series:
    """
    RSI using Wilder's smoothing with SMA seed — matches ProRealTime.
    First avg_gain/avg_loss = SMA of first `period` gains/losses.
    Subsequent values use Wilder's: avg = (prev * (period-1) + current) / period.
    """
    delta = series.diff()
    gain = delta.clip(lower=0).to_numpy(dtype=float)
    loss = (-delta.clip(upper=0)).to_numpy(dtype=float)
    n = len(series)
    avg_g = [float('nan')] * n
    avg_l = [float('nan')] * n
    if n < period + 1:
        return pd.Series([float('nan')] * n, index=series.index)
    # SMA seed over first `period` gain/loss values (indices 1..period, since diff NaN at 0)
    avg_g[period] = float(sum(gain[1:period + 1]) / period)
    avg_l[period] = float(sum(loss[1:period + 1]) / period)
    for i in range(period + 1, n):
        avg_g[i] = (avg_g[i - 1] * (period - 1) + gain[i]) / period
        avg_l[i] = (avg_l[i - 1] * (period - 1) + loss[i]) / period
    rs = [ag / al if al else float('nan') for ag, al in zip(avg_g, avg_l)]
    rsi = [100 - (100 / (1 + r)) if r == r else float('nan') for r in rs]
    return pd.Series(rsi, index=series.index)


def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute MACD, Signal line, and RSI on a DataFrame with a 'close' column.
    Uses SMA-seeded EMA to match ProRealTime / TradingView / standard platforms.
    Adds columns: 'macd', 'macd_signal', 'rsi' and returns the DataFrame.
    """
    exp1 = _sma_seeded_ema(df['close'], 12)
    exp2 = _sma_seeded_ema(df['close'], 26)
    macd = exp1 - exp2
    sig  = _sma_seeded_ema(macd.fillna(method='ffill'), 9)
    rsi  = _wilder_rsi(df['close'], RSI_PERIOD)
    out = df.copy()
    out['macd']        = macd
    out['macd_signal'] = sig
    out['rsi']         = rsi
    return out


def bootstrap_history(sym: str):
    """
    Fetch the most recent BOOTSTRAP_BAR_COUNT 1-min bars going back up to
    BOOTSTRAP_MAX_DAYS_BACK calendar days.  Fetching in descending order
    guarantees we always get the newest N bars regardless of market gaps
    (weekends, holidays), then reverse to chronological order for EMA calc.
    """
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=BOOTSTRAP_MAX_DAYS_BACK)
    resp  = requests.get(
        f"https://data.alpaca.markets/v2/stocks/{sym}/bars",
        headers={'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY},
        params={
            'timeframe': '1Min',
            'start':     start.isoformat().replace('+00:00', 'Z'),
            'end':       end.isoformat().replace('+00:00', 'Z'),
            'limit':     BOOTSTRAP_BAR_COUNT,
            'sort':      'desc',   # newest first → we slice head, then reverse
            'feed':      os.getenv('HISTORICAL_FEED', 'sip'),
            'adjustment': os.getenv('HISTORICAL_ADJUSTMENT', 'raw'),
        }
    )
    bars = resp.json().get('bars', [])
    if not bars:
        log(f"⚠️ No history for {sym}")
        return
    # bars are newest-first; reverse to oldest-first for chronological EMA
    bars = list(reversed(bars))

    ET = ZoneInfo("America/New_York")
    regular_open  = datetime_time(9, 30)
    regular_close = datetime_time(16, 0)

    rows = []
    for b in bars:
        ts_utc = pd.to_datetime(b['t']).replace(tzinfo=timezone.utc)
        ts_et  = ts_utc.astimezone(ET)
        # Keep only regular session bars — matches TradingView / ProRealTime defaults
        if regular_open <= ts_et.time() < regular_close:
            rows.append({
                'timestamp': ts_utc,
                'open': b['o'], 'high': b['h'], 'low': b['l'],
                'close': b['c'], 'volume': b['v'],
            })

    if not rows:
        log(f"⚠️ No regular-hours history for {sym}")
        return

    df = pd.DataFrame(rows)
    dfs[sym] = compute_macd(df)
    log(f"✅ {sym}: bootstrapped {len(df)} regular-session bars (back {BOOTSTRAP_MAX_DAYS_BACK}d, max {BOOTSTRAP_BAR_COUNT}).")

# Real-time bar handling
def scheduled_shutdown_guard_check(client: TradingClient):
    """
    At CRON_SHUTDOWN time (PST/PDT), cancel all orders, liquidate, and exit.
    """
    global _last_shutdown_check_minute, STOP_TRADING

    # log(f"🕒 [ShutdownGuard] Check start")

    if STOP_TRADING:
        log(f"🛑 [ShutdownGuard] Trading halted; skipping shutdown check")
        return  # Already halted

    shutdown_time_str = os.getenv("CRON_SHUTDOWN", "").strip()
    if not shutdown_time_str:
        # log(f"⏭️ [ShutdownGuard] CRON_SHUTDOWN not set; skipping")
        return  # Disabled if not set

    try:
        shutdown_hour, shutdown_minute = map(int, shutdown_time_str.split(":"))
        # log(f"✅ [ShutdownGuard] Parsed shutdown time {shutdown_hour:02d}:{shutdown_minute:02d} PT")
    except Exception:
        log(f"❌ Invalid CRON_SHUTDOWN value: {shutdown_time_str}")
        return

    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    minute_key = now_pt.strftime("%Y-%m-%d %H:%M")

    # Avoid repeating shutdown check more than once per minute
    if _last_shutdown_check_minute == minute_key:
        # log(f"⏭️ [ShutdownGuard] Already checked at {minute_key}; skipping")
        return
    _last_shutdown_check_minute = minute_key
    # log(f"✅ [ShutdownGuard] Marked shutdown check at {minute_key}")

    if now_pt.hour == shutdown_hour and now_pt.minute == shutdown_minute:
        log(f"⚠️ [ScheduledShutdown] Triggering daily shutdown at {shutdown_time_str} PT")
        trigger_global_liquidation_and_exit(
            client,
            reason=f"Scheduled shutdown at {shutdown_time_str} PT"
        )
    # else:
        # log(f"🕒 [ShutdownGuard] Not shutdown time yet (now {now_pt.strftime('%H:%M')} PT)")

    try:
        daily_pl = get_daily_pl(client)
        # log(f"ℹ️ [RiskGuard] Daily P/L: {daily_pl:.2f} (threshold {NEGATIVE_PL_THRESHOLD:.2f})")
        if daily_pl <= NEGATIVE_PL_THRESHOLD:
            log(f"❌ [RiskGuard] BREACH: {daily_pl:.2f} <= {NEGATIVE_PL_THRESHOLD:.2f}")
            trigger_global_liquidation_and_exit(client, reason=f"Daily P/L {daily_pl:.2f} <= {NEGATIVE_PL_THRESHOLD:.2f}")
        # else:
            # log(f"✅ [RiskGuard] P/L above threshold; continuing")
    except Exception as e:
        log(f"💥 [RiskGuard] Failed to compute daily P/L: {e}")


 

async def handle_bar(bar: dict):
    global _last_delimiter_minute
    sym = bar['S']

    # Print a minute delimiter once per minute across all symbols
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    minute_key = now_pt.strftime("%Y-%m-%d %H:%M")
    if _last_delimiter_minute != minute_key:
        _last_delimiter_minute = minute_key
        log(f"{'='*10} 📊 {now_pt.strftime('%H:%M')} PT {'='*10}")

    # Entry log for this bar processing
    # try:
    #     ts_dbg = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))
    #     log(f"🧭========  handle_bar start for {sym} at {ts_dbg.isoformat()}")
    # except Exception:
    #     log(f"🧭 handle_bar start for {sym} (timestamp parse failed)")

    # ===== Guards: run once per minute =====
    # log(f"🛡️ Running risk_guard_check for {sym}")
    risk_guard_check(trading_client)
    
    # ===== Scheduled Shutdown: check once per minute =====
    # log(f"🕒 Running scheduled_shutdown_guard_check for {sym}")
    scheduled_shutdown_guard_check(trading_client)
    # ===== Scheduled Shutdown: check once per minute =====


    ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))

    # Drop pre-market / after-hours bars — keeps live indicators aligned with
    # TradingView and ProRealTime which compute on regular session only.
    _ET = ZoneInfo("America/New_York")
    ts_et = ts.astimezone(_ET).time()
    if not (datetime_time(9, 30) <= ts_et < datetime_time(16, 0)):
        log(f"⏭️  {sym}: skipping extended-hours bar at {ts_et}")
        return

    # Append bar and compute MACD
    row = {'timestamp': ts, 'open': bar['o'], 'high': bar['h'], 'low': bar['l'], 'close': bar['c'], 'volume': bar['v']}
    df = pd.concat([dfs[sym], pd.DataFrame([row])], ignore_index=True).tail(1000)
    dfs[sym] = df = compute_macd(df)
    macd, sig = df.iloc[-1]['macd'], df.iloc[-1]['macd_signal']
    rsi_val = df.iloc[-1]['rsi']

    # Update MACD buffer and check trend
    update_macd_buffer(sym, macd)
    is_increasing = is_macd_monotonically_increasing(sym)
    buffer_values = macd_buffer[sym]

    # Update RSI buffer and check trend
    update_rsi_buffer(sym, rsi_val)
    rsi_increasing = is_rsi_monotonically_increasing(sym)
    rsi_in_zone = RSI_BUY_MIN <= rsi_val <= RSI_BUY_MAX

    # Log indicators to per-symbol CSV
    close_price = float(df.iloc[-1]['close'])
    _log_indicators(sym, ts, close_price, macd, sig, rsi_val)

    log(f"🔄 {sym} bar at {ts.isoformat()}: MACD={macd:.4f}, Signal={sig:.4f}, RSI={rsi_val:.2f}")
    # log(f"📊 {sym} MACD Buffer: {[f'{v:.4f}' for v in buffer_values]} | Monotonic Increasing: {is_increasing}")

    # Position check
    pos_obj = None
    try:
        pos_obj = trading_client.get_open_position(sym)
        pos = int(float(pos_obj.qty))
        # log(f"ℹ️ Current position for {sym}: {pos} shares")
    except Exception as e:
        err = str(e)
        if '"code":40410000' in err and 'position does not exist' in err:
            pass  # log(f"⚠️ Zero Positions for {sym}")
        else:
            log(f"⚠️ Could not fetch position for {sym}: {err}")
        pos = 0

    # Update position MACD buffer every bar while holding
    if pos > 0:
        update_position_macd_buffer(sym, macd)
        log(f"📈 {sym} position MACD buffer: {[f'{v:.4f}' if v == v else 'nan' for v in position_macd_buffer[sym]]}")

    # STOP-LOSS: if holding and unrealized P&L has gone negative, market sell immediately
    if pos > 0 and pos_obj is not None:
        try:
            unrealized_plpc = float(pos_obj.unrealized_plpc) * 100
            # log(f"🧮 Stop-loss check for {sym}: unrealized P&L={unrealized_plpc:.2f}% | trigger=<-{STOP_LOSS_PERCENT}%")
            if unrealized_plpc < -STOP_LOSS_PERCENT:
                if STOP_TRADING:
                    log(f"[RiskGuard] Blocked STOP-LOSS SELL {sym} x{pos} (trading halted)")
                    return
                log(f"🛑🛑🛑 STOP-LOSS {sym}: unrealized P&L {unrealized_plpc:.2f}% < -{STOP_LOSS_PERCENT}%, market selling {pos} shares immediately 🛑🛑🛑")
                place_sell_market(sym, pos)
                last_trade_time[sym] = datetime.now(timezone.utc)
                reset_position_macd_buffer(sym)
                log(f"✅ Stop-loss executed for {sym}. Updated last_trade_time to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
                return
            # else:
                # log(f"✅ {sym}: P&L healthy ({unrealized_plpc:.2f}%), no stop-loss triggered")
        except Exception as e:
            log(f"⚠️ Error during stop-loss check for {sym}: {e}")

    # Check for pending sell orders and convert to market sell if needed
    if pos > 0:
        try:
            # Get open orders for THIS symbol only
            orders = None
            try:
                from alpaca.trading.requests import GetOrdersRequest
                try:
                    from alpaca.trading.enums import QueryOrderStatus
                    # Get current date in UTC (Alpaca uses UTC)
                    today = datetime.now(timezone.utc).date()
                    req = GetOrdersRequest(
                        status=QueryOrderStatus.OPEN, 
                        symbol=sym,
                        after=today.isoformat() + "T00:00:00Z",  # Start of today
                        until=today.isoformat() + "T23:59:59Z"   # End of today
                    )
                except Exception:
                    # Get current date in UTC (Alpaca uses UTC)
                    today = datetime.now(timezone.utc).date()
                    req = GetOrdersRequest(
                        status="open", 
                        symbol=sym,
                        after=today.isoformat() + "T00:00:00Z",  # Start of today
                        until=today.isoformat() + "T23:59:59Z"   # End of today
                    )
                orders = trading_client.get_orders(req)
            except Exception:
                # Fallback: try to filter by symbol if the request approach fails
                try:
                    all_orders = trading_client.get_orders()
                    orders = [o for o in all_orders if getattr(o, "symbol", getattr(o, "asset_symbol", None)) == sym]
                except Exception:
                    orders = []
            
            # Look for sell orders for this symbol
            pending_sell_orders = []
            for order in (orders or []):
                try:
                    order_side = getattr(order, "side", "").lower()
                    if order_side == "sell":
                        pending_sell_orders.append(order)
                except Exception:
                    continue
            
            # If we have pending sell orders, cancel them and place market sell
            if pending_sell_orders:
                log(f"🔄 {sym}: Found {len(pending_sell_orders)} pending sell order(s), converting to market sell")
                
                # Cancel all pending sell orders
                for order in pending_sell_orders:
                    try:
                        order_id = getattr(order, "id", getattr(order, "order_id", None))
                        if order_id:
                            if hasattr(trading_client, "cancel_order_by_id"):
                                trading_client.cancel_order_by_id(order_id)
                            elif hasattr(trading_client, "cancel_order"):
                                trading_client.cancel_order(order_id)
                            log(f"🧹 Canceled sell order {order_id} for {sym}")
                    except Exception as e:
                        log(f"❌ Failed to cancel sell order for {sym}: {e}")
                
                # Place market sell order for the current position
                try:
                    bid, _ = fetch_quote(sym)
                    log(f"🔴 Market sell {sym}: {pos} shares @ market")
                    place_sell_market(sym, pos)
                    last_trade_time[sym] = datetime.now(timezone.utc)
                    reset_position_macd_buffer(sym)
                    log(f"✅ Market sell placed for {sym}, updated tracking")
                    return  # Exit early since we've handled the sell
                except Exception as e:
                    log(f"❌ Failed to place market sell for {sym}: {e}")
            
        except Exception as e:
            log(f"⚠️ Error checking pending sell orders on {sym}: {e}")

    # TAKE-PROFIT logic
    if pos > 0 and pos_obj is not None:
        try:
            profit_threshold_pct = _get_profit_take_percent(sym)
            budget = remaining_budget[sym]
            dollar_threshold = (profit_threshold_pct / 100) * budget
            unrealized_pl = float(pos_obj.unrealized_pl)
            # log(f"🧮 Take-profit check for {sym}: unrealized P&L=${unrealized_pl:.2f} | trigger>=${dollar_threshold:.2f} ({profit_threshold_pct:.2f}% of ${budget:.2f})")
            if unrealized_pl >= dollar_threshold:
                if STOP_TRADING:
                    log(f"[RiskGuard] Blocked TAKE-PROFIT SELL {sym} x{pos} (trading halted)")
                    return
                bid, _ = fetch_quote(sym)
                log(f"🔴🔴🔴 TAKE-PROFIT {sym}: P&L ${unrealized_pl:.2f} >= ${dollar_threshold:.2f}, selling {pos} @ {bid:.2f}🔴🔴🔴")
                place_sell(sym, bid, pos)
                last_trade_time[sym] = datetime.now(timezone.utc)
                reset_position_macd_buffer(sym)
                log(f"✅ Take-profit sold {sym}. Updated last_trade_time to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
                return
            # else:
                # log(f"⏳ {sym}: holding position, P&L ${unrealized_pl:.2f} has not reached take-profit threshold ${dollar_threshold:.2f}")
        except Exception as e:
            log(f"⚠️ Error checking profit for {sym}: {e}")

    # MACD EXIT: sell if position MACD buffer is no longer monotonically increasing
    if pos > 0:
        should_sell, reason = check_macd_exit(sym)
        if should_sell:
            if STOP_TRADING:
                log(f"[RiskGuard] Blocked MACD-EXIT SELL {sym} x{pos} (trading halted)")
                return
            log(f"🔴🔴🔴 MACD EXIT {sym}: {reason} | buffer={[f'{v:.4f}' if v == v else 'nan' for v in position_macd_buffer[sym]]} | selling {pos} shares 🔴🔴🔴")
            place_sell_market(sym, pos)
            last_trade_time[sym] = datetime.now(timezone.utc)
            reset_position_macd_buffer(sym)
            log(f"✅ MACD exit sold {sym}. Updated last_trade_time to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
        return

    # BUY logic with cooldown
    if pos == 0:
        # log(f"🧮 Path: BUY evaluation for {sym}")
        # If a BUY was placed earlier but never filled, there could be open/working orders.
        # Proactively cancel any such orders for this symbol and reset tracking vars.
        stale = _cancel_open_orders_for_symbol(trading_client, sym)
        if stale > 0:
            last_trade_time[sym] = datetime.min.replace(tzinfo=timezone.utc)
            log(f"🔄 Reset last_trade_time after canceling {stale} stale order(s) for {sym}")
        # else:
            # log(f"🧹 No stale open orders to cancel for {sym}")

        gap_pct = ((macd - sig) / abs(sig)) * 100 if sig else 0
        # log(f"BUY check for {sym}: MACD={macd:.4f}, Signal={sig:.4f}, Gap={gap_pct:.2f}%")
        if gap_pct > MACD_GAP_PERCENT:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_trade_time[sym]).total_seconds() / 60
            # log(f"⏳ Cooldown check for {sym}: elapsed {elapsed:.1f} min (threshold {TRADE_COOLDOWN_MINUTES} min)")
            if elapsed < TRADE_COOLDOWN_MINUTES:
                # log(f"⏳ Skipping BUY for {sym}: cooldown active")
                return

            if STOP_TRADING:
                log(f"[RiskGuard] Blocked BUY {sym} (trading halted)")
                return

            if not is_increasing:
                log(f"💀 {sym}: Skipping BUY — MACD not monotonically increasing {[f'{v:.4f}' for v in macd_buffer[sym]]}")
            elif not rsi_in_zone:
                log(f"💀 {sym}: Skipping BUY — RSI {rsi_val:.2f} not in zone [{RSI_BUY_MIN}-{RSI_BUY_MAX}]")
            elif not rsi_increasing:
                log(f"💀 {sym}: Skipping BUY — RSI not rising {[f'{v:.2f}' for v in rsi_buffer[sym]]}")
            else:
                bid, _ = fetch_quote(sym)
                limit = round(bid + 0.01, 2)
                log(f"🟢🟢🟢 BUY {sym}: MACD={macd:.4f}, gap={gap_pct:.1f}% ≥ {MACD_GAP_PERCENT}%, RSI={rsi_val:.2f} ∈ [{RSI_BUY_MIN}-{RSI_BUY_MAX}] rising → buying @ {limit:.2f}🟢🟢🟢")
                place_buy(sym, limit, remaining_budget[sym])
                last_trade_time[sym] = now
                position_macd_buffer[sym] = [macd, float('nan'), float('nan')]
                log(f"📊 {sym} position MACD buffer initialized at buy: [{macd:.4f}, nan, nan]")
                log(f"Updated last_trade_time for {sym} to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
        elif macd > sig:
            # log(f"⚪️ {sym}: MACD>sig but conditions not met (MACD={macd:.4f}, gap={gap_pct:.1f}%), skipping buy")
            pass
        else:
            # log(f"⚪️ {sym}: MACD<=Signal ({macd:.4f} <= {sig:.4f}); skipping buy")
            pass
        # log(f"🧭 Path exit: BUY evaluation complete for {sym}")
        pass
    else:
        # log(f"⚪️ Skipping buy for {sym}: existing position of {pos} shares")
        pass
    # log(f"🏁 ========= handle_bar end for {sym}")


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
        _loop_minute_key = None
        _loop_minute_start = None
        _loop_bars_seen = set()

        while True:
            msg = await ws.recv()
            data = json.loads(msg) if isinstance(msg, str) else msg
            for m in data:
                msg_type = m.get('T')
                if msg_type == 'b':
                    # Track which symbols have been processed this minute
                    bar_minute = m.get('t', '')[:16]
                    if bar_minute != _loop_minute_key:
                        _loop_minute_key = bar_minute
                        _loop_minute_start = time.time()
                        _loop_bars_seen = set()

                    await handle_bar(m)

                    _loop_bars_seen.add(m.get('S'))
                    if _loop_bars_seen >= set(SYMBOLS):
                        elapsed_s = time.time() - _loop_minute_start
                        log(f"⏱️ All {len(SYMBOLS)} stocks processed in {elapsed_s:.2f}s")
                        _loop_bars_seen = set()

                elif msg_type == 'q':  # quote update
                    sym_q = m.get('S')
                    bp = m.get('bp', 0.0)
                    ap = m.get('ap', 0.0)
                    latest_quote[sym_q] = (bp, ap)
                    # log(f"💬 QUOTE {sym_q}: bid={bp:.2f}, ask={ap:.2f}")
    
if __name__ == '__main__':
    try:
        asyncio.run(main()) # run the main loop
    except Exception as e:
        log(f"💥 Main loop crashed: {e}")
        sys.exit(1)
    