import os
import csv
import json
import math
import requests
import pandas as pd
import asyncio
import websockets
import logging
import time
import sys
import subprocess
import signal
import threading
import atexit
from datetime import datetime, timezone, timedelta, time as datetime_time
from dotenv import load_dotenv
from buy_order import place_buy
from sell_order import place_sell, place_sell_market
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderSide
from alpaca.trading.requests import ClosePositionRequest, GetCalendarRequest

# Logging to file only

from zoneinfo import ZoneInfo

# Logging to file in Pacific Time (handles PST/PDT automatically)
log_filename = 'trading_log_' + datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%m%d%y') + '.txt'

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
MACD_MIN_VALUE   = float(os.getenv('MACD_MIN_VALUE', '0.05'))
MACD_ABS_GAP_MIN = float(os.getenv('MACD_ABS_GAP_MIN', '0.03'))
SIGNAL_MIN_VALUE = float(os.getenv('SIGNAL_MIN_VALUE', '0.08'))
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '9'))
RSI_BUY_MIN = float(os.getenv('RSI_BUY_MIN', '50'))
RSI_BUY_MAX = float(os.getenv('RSI_BUY_MAX', '65'))
# Price-stall guard (two-window deceleration): MACD is lagging, so it can keep climbing
# after price has stalled — causing entries right as the move exhausts. Over the last
# STALL_WINDOW closes, compare the EARLIER segment's pace to the most-recent STALL_RECENT
# samples: if price was advancing earlier but the recent samples have decelerated below
# STALL_DECEL_RATIO of that pace (or gone flat/negative) while MACD rises, the rise is
# "hollow" — skip the buy. Unit-free (a pace ratio), so no per-symbol/$ tuning.
STALL_WINDOW      = 10    # total samples examined
STALL_RECENT      = 4     # most-recent samples checked for the stall
STALL_DECEL_RATIO = 0.30  # recent pace must be >= this fraction of the earlier pace
BOOTSTRAP_BAR_COUNT  = int(os.getenv('BOOTSTRAP_BAR_COUNT', '800'))
BOOTSTRAP_MAX_DAYS_BACK = int(os.getenv('BOOTSTRAP_MAX_DAYS_BACK', '5'))
MACD_WARMUP_BARS = int(os.getenv('MACD_WARMUP_BARS', '35'))

# Entry-time cutoff: block opening NEW positions after this PT wall-clock time.
# The edge is concentrated in the first trading hour; midday entries bleed
# (2026-06 analysis, 15 clean days). Exits/risk management are unaffected.
# Format "HH:MM" (PT); empty/invalid disables the cutoff.
def _parse_hhmm(raw):
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        h, m = map(int, raw.split(':'))
        return (h, m)
    except Exception:
        return None

NO_NEW_ENTRIES_AFTER = _parse_hhmm(os.getenv('NO_NEW_ENTRIES_AFTER', ''))
_entry_cutoff_logged = False

def entry_cutoff_reached() -> bool:
    """True once the PT wall-clock is at/after NO_NEW_ENTRIES_AFTER — i.e. opening
    new BUYs is blocked. Uses America/Los_Angeles, the same PT approach as the 12:55
    shutdown watchdog, so on a UTC server we never mix time zones. Only gates NEW
    entries; exits, logging, and everything else keep running."""
    if NO_NEW_ENTRIES_AFTER is None:
        return False
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    return (now_pt.hour, now_pt.minute) >= NO_NEW_ENTRIES_AFTER
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
# 5-bar rolling MACD buffer tracked from the moment a position is opened
_nan = float('nan')
position_macd_buffer = {sym: [_nan, _nan, _nan, _nan, _nan] for sym in SYMBOLS}
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
    Returns True if:
      1. All 5 RSI values are >= RSI_BUY_MIN (50) — zone stability
      2. Current RSI >= previous RSI — not falling through the zone from overbought
    """
    buffer = rsi_buffer.get(sym, [])
    if len(buffer) < RSI_BUFFER_SIZE:
        return False
    return all(v >= RSI_BUY_MIN for v in buffer) and buffer[-1] >= buffer[-2]

def price_confirms_macd_rise(df) -> tuple:
    """Two-window deceleration guard for a 'hollow' MACD rise (lagging-indicator trap).

    Splits the last STALL_WINDOW closes into an earlier segment and the most-recent
    STALL_RECENT samples, then compares their per-bar pace:
      earlier_pace = price move over the earlier segment, per bar
      recent_pace  = price move over the recent samples, per bar
    If price was advancing earlier (earlier_pace > 0) but the recent samples have
    stalled — recent_pace <= 0, or below STALL_DECEL_RATIO of earlier_pace — the MACD
    rise is not backed by price and the entry is skipped.

    Returns (confirms, earlier_pace, recent_pace):
      confirms=True  → price still advancing, or no prior up-move to stall from (allow)
      confirms=False → recent stall while MACD rises (block)
    On insufficient data, returns (True, nan, nan) so it never blocks during warmup.
    """
    closes = df['close'].tail(STALL_WINDOW).to_numpy(dtype=float)
    if len(closes) < STALL_WINDOW:
        return True, float('nan'), float('nan')
    earlier_pace = (closes[-STALL_RECENT] - closes[0]) / (STALL_WINDOW - STALL_RECENT)
    recent_pace  = (closes[-1] - closes[-STALL_RECENT]) / (STALL_RECENT - 1)
    if earlier_pace <= 0:
        return True, earlier_pace, recent_pace   # no prior up-move — nothing to stall from
    stalled = recent_pace <= 0 or (recent_pace / earlier_pace) < STALL_DECEL_RATIO
    return (not stalled), earlier_pace, recent_pace


def update_position_macd_buffer(sym: str, macd: float):
    """
    Update the 5-bar rolling MACD buffer for a held position.
    Fills slots left to right until full, then shifts oldest out.
    """
    buf = position_macd_buffer[sym]
    nan = float('nan')
    if buf[0] != buf[0]:        # buffer empty
        position_macd_buffer[sym] = [macd, nan, nan, nan, nan]
    elif buf[1] != buf[1]:
        position_macd_buffer[sym] = [buf[0], macd, nan, nan, nan]
    elif buf[2] != buf[2]:
        position_macd_buffer[sym] = [buf[0], buf[1], macd, nan, nan]
    elif buf[3] != buf[3]:
        position_macd_buffer[sym] = [buf[0], buf[1], buf[2], macd, nan]
    elif buf[4] != buf[4]:
        position_macd_buffer[sym] = [buf[0], buf[1], buf[2], buf[3], macd]
    else:                        # Buffer full — shift left, append new
        position_macd_buffer[sym] = [buf[1], buf[2], buf[3], buf[4], macd]

def reset_position_macd_buffer(sym: str):
    """Reset position MACD buffer when a position is closed."""
    nan = float('nan')
    position_macd_buffer[sym] = [nan, nan, nan, nan, nan]

def check_macd_exit(sym: str) -> tuple:
    """
    Returns (should_sell, reason).
    Requires 5 bars of data. Holds if all 5 bars monotonically increasing.
    Exits when b4 < b2 (current MACD below the window midpoint) AND d2 < 0
    (last bar is actually declining). This avoids false exits on slow-rising
    bars and catches both clean declines and spike-then-crash patterns.
    """
    buf = position_macd_buffer[sym]
    if any(v != v for v in buf):   # NaN present — not enough data yet
        return False, ""
    b0, b1, b2, b3, b4 = buf
    if b0 < b1 < b2 < b3 < b4:    # All 5 strictly increasing — hold
        return False, ""
    d2 = b4 - b3   # last transition
    if b4 < b2 and d2 < 0:
        return True, f"b4({b4:.4f}) < b2({b2:.4f}) and declining ({b3:.4f}→{b4:.4f})"
    return False, ""

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

_profit_take_cache   = {}
_macd_min_cache      = {}
_macd_abs_gap_cache  = {}
_signal_min_cache    = {}

def _get_macd_min_value(sym: str) -> float:
    """Per-symbol MACD_MIN_VALUE. Key: MACD_MIN_VALUE_<SYMBOL>. Falls back to MACD_MIN_VALUE."""
    if sym in _macd_min_cache:
        return _macd_min_cache[sym]
    raw = os.getenv(f"MACD_MIN_VALUE_{sym.upper()}")
    if raw is None:
        val = MACD_MIN_VALUE
    else:
        try:
            val = abs(float(raw))
        except Exception:
            log(f"⚠️ Invalid MACD_MIN_VALUE_{sym}: '{raw}'. Falling back to {MACD_MIN_VALUE}")
            val = MACD_MIN_VALUE
    _macd_min_cache[sym] = val
    return val

def _get_signal_min_value(sym: str) -> float:
    """Per-symbol minimum abs(Signal) required before a buy. Key: SIGNAL_MIN_VALUE_<SYMBOL>."""
    if sym in _signal_min_cache:
        return _signal_min_cache[sym]
    raw = os.getenv(f"SIGNAL_MIN_VALUE_{sym.upper()}")
    if raw is None:
        val = SIGNAL_MIN_VALUE
    else:
        try:
            val = abs(float(raw))
        except Exception:
            log(f"⚠️ Invalid SIGNAL_MIN_VALUE_{sym}: '{raw}'. Falling back to {SIGNAL_MIN_VALUE}")
            val = SIGNAL_MIN_VALUE
    _signal_min_cache[sym] = val
    return val

def _get_macd_abs_gap_min(sym: str) -> float:
    """Per-symbol MACD_ABS_GAP_MIN. Key: MACD_ABS_GAP_MIN_<SYMBOL>. Falls back to MACD_ABS_GAP_MIN."""
    if sym in _macd_abs_gap_cache:
        return _macd_abs_gap_cache[sym]
    raw = os.getenv(f"MACD_ABS_GAP_MIN_{sym.upper()}")
    if raw is None:
        val = MACD_ABS_GAP_MIN
    else:
        try:
            val = abs(float(raw))
        except Exception:
            log(f"⚠️ Invalid MACD_ABS_GAP_MIN_{sym}: '{raw}'. Falling back to {MACD_ABS_GAP_MIN}")
            val = MACD_ABS_GAP_MIN
    _macd_abs_gap_cache[sym] = val
    return val

_gap_pct_cache = {}

def _get_macd_gap_percent(sym: str) -> float:
    """Per-symbol MACD_GAP_PERCENT. Key: MACD_GAP_PERCENT_<SYMBOL>. Falls back to MACD_GAP_PERCENT."""
    if sym in _gap_pct_cache:
        return _gap_pct_cache[sym]
    raw = os.getenv(f"MACD_GAP_PERCENT_{sym.upper()}")
    if raw is None:
        val = MACD_GAP_PERCENT
    else:
        try:
            val = float(raw)
        except Exception:
            log(f"⚠️ Invalid MACD_GAP_PERCENT_{sym}: '{raw}'. Falling back to {MACD_GAP_PERCENT}")
            val = MACD_GAP_PERCENT
    _gap_pct_cache[sym] = val
    return val

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

def trigger_global_liquidation_and_exit(client: TradingClient, reason: str = "", hard_exit: bool = False):
    """Cancels orders, liquidates all positions, blocks further orders, exits process.

    hard_exit=True forces termination via os._exit() — required when called from a
    background thread (e.g. the shutdown watchdog), since sys.exit() only unwinds the
    calling thread and would leave the process alive.
    """
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
            # Move today's log file into indicator_logs/ so the dashboard can sync it
            try:
                import shutil
                src = log_filename
                dst_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), _INDICATOR_LOG_DIR)
                os.makedirs(dst_dir, exist_ok=True)
                dst = os.path.join(dst_dir, os.path.basename(src))
                if os.path.exists(src):
                    shutil.move(src, dst)
            except Exception:
                pass
            if hard_exit:
                os._exit(2)
            else:
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
_indicator_log_writers = {}   # (sym, session_date) -> csv.writer
_indicator_log_files   = {}   # (sym, session_date) -> file handle

def _get_indicator_writer(sym: str, date_str: str):
    """Return (creating if needed) the CSV writer for a symbol on a given session
    date. Keyed by (sym, date) — derived from the BAR's own timestamp, not the
    process start time — so a run that spans midnight routes each bar to the file
    for its own session and one file always holds exactly one session."""
    key = (sym, date_str)
    if key not in _indicator_log_writers:
        os.makedirs(_INDICATOR_LOG_DIR, exist_ok=True)
        filepath = os.path.join(_INDICATOR_LOG_DIR, f"{sym}_{date_str}.csv")
        is_new = not os.path.exists(filepath) or os.path.getsize(filepath) == 0
        f = open(filepath, 'a', newline='', encoding='utf-8')
        writer = csv.writer(f)
        if is_new:
            writer.writerow(['timestamp', 'close', 'MACD', 'Signal', 'RSI'])
            f.flush()
        _indicator_log_writers[key] = writer
        _indicator_log_files[key] = f
        log(f"📁 Indicator log opened: {filepath}")
    return _indicator_log_writers[key]

def _log_indicators(sym: str, ts, close: float, macd: float, sig: float, rsi: float):
    """Append one row to the per-symbol indicator CSV (filed by the bar's PT date)."""
    try:
        ts_pt_dt = ts.astimezone(ZoneInfo("America/Los_Angeles"))
        date_str = ts_pt_dt.strftime('%Y-%m-%d')          # session date = bar's PT date
        writer = _get_indicator_writer(sym, date_str)
        ts_pt = ts_pt_dt.strftime('%Y-%m-%dT%H:%M:%S')
        writer.writerow([ts_pt, f'{close:.4f}', f'{macd:.4f}', f'{sig:.4f}', f'{rsi:.2f}'])
        _indicator_log_files[(sym, date_str)].flush()
    except Exception as e:
        log(f"⚠️ Failed to write indicator log for {sym}: {e}")

# Bootstrap historical bars
# === Technical Indicators ===
def _sma_seeded_ema(series: pd.Series, period: int) -> pd.Series:
    """
    EMA with SMA seed — matches ProRealTime and most professional platforms.
    First EMA value = SMA of the first `period` non-NaN bars; exponential from there.
    Skips leading NaN values so MACD signal (fed a MACD series with 26 leading NaN)
    is seeded correctly from the first valid MACD bar rather than NaN-propagating.
    pandas ewm(adjust=False) seeds from bar-1 instead, causing divergence.
    """
    k = 2.0 / (period + 1)
    vals = series.to_numpy(dtype=float)
    out = [float('nan')] * len(vals)
    # Skip leading NaN — find first valid index
    start = 0
    while start < len(vals) and vals[start] != vals[start]:  # NaN != NaN is True
        start += 1
    if start + period > len(vals):
        return pd.Series(out, index=series.index)
    seed_idx = start + period - 1
    out[seed_idx] = float(sum(vals[start:start + period]) / period)   # SMA seed
    for i in range(seed_idx + 1, len(vals)):
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
    sig  = _sma_seeded_ema(macd, 9)
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
    If the request fails or times out, logs a warning and leaves dfs[sym]
    empty — the bot will warm up from live bars instead (takes ~35 minutes).
    """
    try:
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=BOOTSTRAP_MAX_DAYS_BACK)
        resp  = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{sym}/bars",
            headers={'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY},
            timeout=15,
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
            log(f"⚠️ {sym}: No history returned — will warm up from live bars.")
            return
        # bars are newest-first; reverse to oldest-first for chronological EMA
        bars = list(reversed(bars))

        # Include ALL bars — pre-market, regular session, after-hours.
        # EMA is a continuum: skipping any bars breaks EMA-12/EMA-26 continuity
        # and causes MACD to diverge from TradingView / ProRealTime.
        rows = []
        for b in bars:
            ts_utc = pd.to_datetime(b['t']).replace(tzinfo=timezone.utc)
            rows.append({
                'timestamp': ts_utc,
                'open': b['o'], 'high': b['h'], 'low': b['l'],
                'close': b['c'], 'volume': b['v'],
            })

        if not rows:
            log(f"⚠️ {sym}: No bars in response — will warm up from live bars.")
            return

        df = pd.DataFrame(rows)
        dfs[sym] = compute_macd(df)
        log(f"✅ {sym}: bootstrapped {len(df)} bars incl. pre/after-market (back {BOOTSTRAP_MAX_DAYS_BACK}d, max {BOOTSTRAP_BAR_COUNT}).")

    except requests.exceptions.Timeout:
        log(f"⚠️ {sym}: Bootstrap timed out — will warm up from live bars (~35 min).")
    except requests.exceptions.ConnectionError as e:
        log(f"⚠️ {sym}: Bootstrap connection error ({e}) — will warm up from live bars.")
    except Exception as e:
        log(f"⚠️ {sym}: Bootstrap failed ({e}) — will warm up from live bars.")

# Real-time bar handling
def daily_pl_guard_check(client: TradingClient):
    """
    Bar-driven safety net: if today's P/L breaches NEGATIVE_PL_THRESHOLD, liquidate and exit.

    NOTE: time-based daily shutdown is no longer handled here — it now runs in an
    independent wall-clock watchdog thread (_shutdown_watchdog) so it fires even when
    no market data is flowing (holiday, outage, reconnect storm).
    """
    global _last_shutdown_check_minute, STOP_TRADING

    if STOP_TRADING:
        return  # Already halted

    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    minute_key = now_pt.strftime("%Y-%m-%d %H:%M")

    # Avoid repeating the check more than once per minute
    if _last_shutdown_check_minute == minute_key:
        return
    _last_shutdown_check_minute = minute_key

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

    # (minute delimiter + all per-bar logging moved below the session gate — nothing
    #  is logged or traded before the 06:30 PT open; see the regular-session block.)

    # ===== Guards: run once per minute =====
    # log(f"🛡️ Running risk_guard_check for {sym}")
    risk_guard_check(trading_client)
    
    # ===== Daily P/L breach guard: check once per minute =====
    daily_pl_guard_check(trading_client)
    # ===== Daily P/L breach guard: check once per minute =====


    ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))

    # Determine session — EMA runs on ALL bars (pre-market + after-hours) for
    # continuity matching TradingView.  Trade order placement is still gated to
    # regular session only (checked below after indicators are computed).
    _ET = ZoneInfo("America/New_York")
    ts_et = ts.astimezone(_ET).time()
    is_regular_session = datetime_time(9, 30) <= ts_et < datetime_time(16, 0)

    # Append bar and compute MACD
    row = {'timestamp': ts, 'open': bar['o'], 'high': bar['h'], 'low': bar['l'], 'close': bar['c'], 'volume': bar['v']}
    df = pd.concat([dfs[sym], pd.DataFrame([row])], ignore_index=True).tail(1000)
    dfs[sym] = df = compute_macd(df)
    macd, sig = df.iloc[-1]['macd'], df.iloc[-1]['macd_signal']
    sig_prev  = df.iloc[-2]['macd_signal'] if len(df) >= 2 else float('nan')
    rsi_val = df.iloc[-1]['rsi']

    # Update MACD buffer and check trend
    update_macd_buffer(sym, macd)
    is_increasing = is_macd_monotonically_increasing(sym)
    buffer_values = macd_buffer[sym]

    # Update RSI buffer and check trend
    update_rsi_buffer(sym, rsi_val)
    rsi_increasing = is_rsi_monotonically_increasing(sym)
    rsi_in_zone = rsi_val >= RSI_BUY_MIN

    # Trading, logging, and the minute delimiter all start at the 06:30 PT / 09:30 ET
    # open. Pre-market and after-hours bars updated the EMA/MACD/RSI buffers above for
    # continuity, but are neither logged nor traded — so each CSV / trade log stays a
    # clean regular-session record. Session is keyed off the bar's own ET timestamp,
    # never the server's UTC clock.
    if not is_regular_session:
        return

    # ===== Regular session (06:30–13:00 PT) — everything below starts at the open =====
    # Minute delimiter once per minute across all symbols.
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    minute_key = now_pt.strftime("%Y-%m-%d %H:%M")
    if _last_delimiter_minute != minute_key:
        _last_delimiter_minute = minute_key
        log(f"{'='*10} 📊 {now_pt.strftime('%H:%M')} PT {'='*10}")

    # Log indicators to per-symbol CSV — skip during warmup (NaN values not useful).
    close_price = float(df.iloc[-1]['close'])
    indicators_ready = not (math.isnan(macd) or math.isnan(sig) or math.isnan(rsi_val))
    if indicators_ready:
        _log_indicators(sym, ts, close_price, macd, sig, rsi_val)

    log(f"🔄 {sym} bar at {ts.isoformat()}: MACD={macd:.4f}, Signal={sig:.4f}, RSI={rsi_val:.2f}")

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
                    order_side = str(getattr(order, "side", "")).lower()
                    # Normalise enum format e.g. "orderside.sell" -> "sell"
                    if "." in order_side:
                        order_side = order_side.split(".")[-1]
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
            try:
                place_sell_market(sym, pos)
                last_trade_time[sym] = datetime.now(timezone.utc)
                reset_position_macd_buffer(sym)
                log(f"✅ MACD exit sold {sym}. Updated last_trade_time to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
            except Exception as e:
                log(f"❌ MACD exit sell failed for {sym}: {e}")
        return

    # BUY logic with cooldown
    if pos == 0:
        # Entry-time cutoff FIRST (NO_NEW_ENTRIES_AFTER, PT): once past the cutoff we
        # open no new positions, so for a FLAT symbol there is nothing to do. Return
        # immediately — BEFORE any Alpaca call — so nothing downstream assumes a buy
        # that never happened: no stale-order cancels, no order management, no log
        # spam through the afternoon. (Held positions are handled in the exit block
        # above, which runs before this and acts on the real Alpaca position.)
        buy_blocked = entry_cutoff_reached()
        if buy_blocked:
            global _entry_cutoff_logged
            if not _entry_cutoff_logged:
                log(f"🕒 Entry cutoff {NO_NEW_ENTRIES_AFTER[0]:02d}:{NO_NEW_ENTRIES_AFTER[1]:02d} PT reached — no new entries; managing open positions only.")
                _entry_cutoff_logged = True
            return

        # Before the cutoff: if a BUY was placed earlier but never filled, there could
        # be open/working orders. Proactively cancel any such orders for this symbol
        # and reset tracking vars.
        stale = _cancel_open_orders_for_symbol(trading_client, sym)
        if stale > 0:
            last_trade_time[sym] = datetime.min.replace(tzinfo=timezone.utc)
            log(f"🔄 Reset last_trade_time after canceling {stale} stale order(s) for {sym}")
        # else:
            # log(f"🧹 No stale open orders to cancel for {sym}")

        sig_rising = (not math.isnan(sig_prev)) and (sig > sig_prev)

        # Two-regime momentum gate (thresholds are per-symbol, fall back to global defaults):
        #   Near zero crossover (MACD < min_val): percentage is unreliable because
        #   abs(signal) is tiny — use absolute gap instead.
        #   Trending (MACD >= min_val): signal has real magnitude, percentage gap
        #   correctly measures whether MACD is accelerating away from signal.
        min_val     = _get_macd_min_value(sym)
        abs_gap_min = _get_macd_abs_gap_min(sym)

        if macd <= 0:
            gap_ok = False
            gap_desc = f"MACD={macd:.4f}<=0"
        elif macd < min_val:
            # Hard buy floor: MACD_MIN_VALUE is the minimum to enter, not just
            # a regime boundary. Below it the stock is in a consolidation zone.
            gap_ok = False
            gap_desc = f"MACD={macd:.4f} below floor {min_val} (consolidation zone)"
        else:
            prev_macd = df.iloc[-2]['macd'] if len(df) >= 2 else float('nan')
            if math.isnan(prev_macd) or prev_macd < min_val:
                gap_ok = False
                gap_desc = f"MACD={macd:.4f} first bar above floor {min_val} (prev={prev_macd:.4f}) — waiting for confirmation"
            else:
                gap_pct       = ((macd - sig) / abs(sig)) * 100 if sig else 0
                gap_threshold = _get_macd_gap_percent(sym)
                gap_ok        = gap_pct > gap_threshold
                gap_desc      = f"trending regime: gap={gap_pct:.1f}% vs min={gap_threshold}%"

        sig_min = _get_signal_min_value(sym)
        sig_established = abs(sig) >= sig_min

        if gap_ok and sig_rising and sig_established:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_trade_time[sym]).total_seconds() / 60
            if elapsed < TRADE_COOLDOWN_MINUTES:
                return

            if STOP_TRADING:
                log(f"[RiskGuard] Blocked BUY {sym} (trading halted)")
                return

            price_ok, earlier_pace, recent_pace = price_confirms_macd_rise(df)
            if not is_increasing:
                log(f"💀 {sym}: Skipping BUY — MACD not monotonically increasing {[f'{v:.4f}' for v in macd_buffer[sym]]}")
            elif not rsi_in_zone:
                log(f"💀 {sym}: Skipping BUY — RSI {rsi_val:.2f} below floor {RSI_BUY_MIN}")
            elif not rsi_increasing:
                log(f"💀 {sym}: Skipping BUY — RSI not stable ≥{RSI_BUY_MIN} {[f'{v:.2f}' for v in rsi_buffer[sym]]}")
            elif not price_ok:
                log(f"💀 {sym}: Skipping BUY — price STALLED (recent {STALL_RECENT}-bar pace {recent_pace:+.4f} vs earlier {earlier_pace:+.4f} over {STALL_WINDOW} bars) while MACD rising")
            else:
                bid, _ = fetch_quote(sym)
                if bid <= 0:
                    log(f"⚠️ {sym}: Skipping BUY — no valid quote yet (bid={bid})")
                else:
                    log(f"🟢🟢🟢 BUY {sym}: MACD={macd:.4f}, {gap_desc}, Signal={sig:.4f}≥{sig_min} rising ({sig_prev:.4f}→{sig:.4f}), RSI={rsi_val:.2f}≥{RSI_BUY_MIN} → buying @ market (bid={bid:.2f})🟢🟢🟢")
                    try:
                        place_buy(sym, bid, remaining_budget[sym])
                        last_trade_time[sym] = now
                        position_macd_buffer[sym] = [macd, float('nan'), float('nan'), float('nan'), float('nan')]
                        log(f"📊 {sym} position MACD buffer initialized at buy: [{macd:.4f}, nan, nan, nan, nan]")
                        log(f"Updated last_trade_time for {sym} to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
                    except Exception as e:
                        log(f"❌ BUY order failed for {sym}: {e}")
        elif not gap_ok:
            pass  # gap condition not met for current regime
        elif not sig_rising:
            pass  # signal not rising — trend not yet confirmed
        elif not sig_established:
            log(f"💀 {sym}: Skipping BUY — abs(Signal)={abs(sig):.4f} below floor {sig_min} (no trend context)")
        # log(f"🧭 Path exit: BUY evaluation complete for {sym}")
        pass
    else:
        # log(f"⚪️ Skipping buy for {sym}: existing position of {pos} shares")
        pass
    # log(f"🏁 ========= handle_bar end for {sym}")


# ===== Startup guards (run once, before the reconnect loop) =====
_PIDFILE = "/tmp/macdbot.pid"


def is_trading_day_today() -> bool:
    """True if today (PT) is a market session day, per Alpaca's calendar.

    Prevents the bot from launching on holidays/weekends (a holiday launch with no
    market data was what seeded the duplicate-instance cascade)."""
    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    try:
        cal = trading_client.get_calendar(GetCalendarRequest(start=today, end=today))
        return len(cal) >= 1
    except Exception as e:
        # Don't block trading on a transient calendar API error — fail open, but log loudly.
        log(f"⚠️ [Calendar] check failed ({e}); assuming today IS a trading day.")
        return True


def _proc_is_our_bot(pid: int) -> bool:
    """True only if pid is alive AND its cmdline is this script (guards against PID reuse)."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmd = f.read().replace(b"\x00", b" ").decode("utf-8", "ignore")
        return "multi_symbol_macd_stream" in cmd
    except Exception:
        return False


def _release_singleton():
    """atexit: remove the pidfile only if it's still ours (avoid clobbering a successor)."""
    try:
        if os.path.exists(_PIDFILE):
            with open(_PIDFILE) as f:
                if (f.read().strip() or "0") == str(os.getpid()):
                    os.remove(_PIDFILE)
    except Exception:
        pass


def acquire_singleton():
    """Last-wins: terminate any existing bot instance, then claim the pidfile.

    The freshly-launched process always wins so it runs with today's correct date/state
    and a clean Alpaca connection (the data stream allows only one connection per account)."""
    try:
        if os.path.exists(_PIDFILE):
            with open(_PIDFILE) as f:
                old = int(f.read().strip() or "0")
            if old and old != os.getpid() and _proc_is_our_bot(old):
                log(f"🔫 [Singleton] Existing instance PID {old} found — terminating (last-wins).")
                try:
                    os.kill(old, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                # Wait up to 8s for it to die and release the Alpaca socket.
                deadline = time.time() + 8
                while time.time() < deadline:
                    try:
                        os.kill(old, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.5)
                else:
                    log(f"🔪 [Singleton] PID {old} still alive — SIGKILL.")
                    try:
                        os.kill(old, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    time.sleep(1)
    except Exception as e:
        log(f"⚠️ [Singleton] takeover check failed: {e}")
    try:
        with open(_PIDFILE, "w") as f:
            f.write(str(os.getpid()))
        atexit.register(_release_singleton)
        log(f"🔒 [Singleton] Claimed pidfile {_PIDFILE} (PID {os.getpid()}).")
    except Exception as e:
        log(f"⚠️ [Singleton] could not write pidfile: {e}")


def shutdown_watchdog():
    """Wall-clock daemon thread: liquidate + hard-exit at CRON_SHUTDOWN (PT).

    Independent of the websocket, so it fires even with no market data. Level-triggered
    (>= shutdown time) so a missed minute can't strand the process into the next day."""
    raw = os.getenv("CRON_SHUTDOWN", "").strip()
    if not raw:
        log("⏭️ [Watchdog] CRON_SHUTDOWN not set — daily shutdown watchdog DISABLED.")
        return
    try:
        sh, sm = map(int, raw.split(":"))
    except Exception:
        log(f"❌ [Watchdog] Invalid CRON_SHUTDOWN '{raw}' — watchdog disabled.")
        return
    log(f"🕒 [Watchdog] Armed for {sh:02d}:{sm:02d} PT daily shutdown.")
    while True:
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        if (now.hour, now.minute) >= (sh, sm):
            log(f"⚠️ [Watchdog] Shutdown time reached ({now.strftime('%H:%M')} PT >= "
                f"{sh:02d}:{sm:02d}) — liquidating and exiting.")
            trigger_global_liquidation_and_exit(
                trading_client,
                reason=f"Scheduled shutdown at {sh:02d}:{sm:02d} PT",
                hard_exit=True,
            )
            return  # unreachable: os._exit already fired
        time.sleep(15)


def run_startup_recalibration():
    """Pre-open: refresh per-symbol MACD thresholds from the latest daily close +
    historical indicator logs (recalibrate_settings.py), then reload settings.td so
    the fresh values take effect before the first bar is traded.

    Runs ONLY when launched before the 06:30 PT open, so a mid-session restart
    (auto-reconnect / crash recovery / singleton relaunch) never shifts parameters
    intraday. Never blocks trading: on any failure we log and keep the current
    settings.td unchanged."""
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    if (now_pt.hour, now_pt.minute) >= (6, 30):
        log(f"⏭️ [Recalibrate] Launched {now_pt.strftime('%H:%M')} PT (at/after open) — "
            "skipping recalibration; using existing settings.td.")
        return

    script_dir    = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(script_dir, "settings.td")
    log("🔧 [Recalibrate] Refreshing per-symbol thresholds from the latest daily close before open…")
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(script_dir, "recalibrate_settings.py")],
            cwd=script_dir, capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        log("⚠️ [Recalibrate] Timed out (>300s) — keeping existing settings.td.")
        return
    except Exception as e:
        log(f"⚠️ [Recalibrate] Could not run ({e}) — keeping existing settings.td.")
        return

    if result.returncode != 0:
        log(f"⚠️ [Recalibrate] Exited {result.returncode} — keeping existing settings.td.")
        # Echo full stdout on failure so the skip reasons (e.g. "no indicator logs
        # found") are captured, not just the exit code.
        for line in result.stdout.splitlines():
            s = line.strip()
            if s:
                log(f"   [Recalibrate] {s}")
        if result.stderr:
            log(f"   [Recalibrate] stderr: {result.stderr.strip()[:400]}")
        return

    # Echo the full recalibrate output into the daily log for the record. Log every
    # non-empty line (not just best:/updated) so a no-op run — e.g. every symbol
    # skipped for missing logs — is visible instead of masquerading as success.
    for line in result.stdout.splitlines():
        s = line.strip()
        if s:
            log(f"   [Recalibrate] {s}")

    # Reload the freshly written values and drop cached per-symbol lookups so the
    # getters re-read from the new environment on the first bar.
    load_dotenv(settings_path, override=True)
    for cache in (_macd_min_cache, _signal_min_cache, _macd_abs_gap_cache,
                  _gap_pct_cache, _macd_neutral_cache, _profit_take_cache):
        cache.clear()
    log("✅ [Recalibrate] settings.td refreshed and reloaded into environment.")


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

                elif msg_type in ('error', 'success', 'subscription'):
                    # Surface Alpaca control frames (e.g. {"T":"error","code":406,
                    # "msg":"connection limit exceeded"}) instead of silently dropping them.
                    log(f"📡 [Alpaca] control: {m}")
    
if __name__ == '__main__':
    # ===== Pre-flight (runs once, before the reconnect loop) =====
    # 1) Don't run on market holidays/weekends (the holiday-launch case that seeded
    #    the duplicate-instance cascade).
    if not is_trading_day_today():
        log("🛑 Not a trading session today (holiday/weekend) — exiting.")
        sys.exit(0)
    # 2) Singleton, last-wins: kill any leftover instance and claim the connection.
    acquire_singleton()
    # 2b) Pre-open: recalibrate per-symbol thresholds from the latest close, then
    #     reload settings.td so the fresh values are live before trading begins.
    run_startup_recalibration()
    # 3) Wall-clock shutdown watchdog, independent of market data.
    threading.Thread(target=shutdown_watchdog, name="shutdown-watchdog", daemon=True).start()
    # 3b) Entry-time cutoff (observability).
    if NO_NEW_ENTRIES_AFTER is not None:
        log(f"🕒 Entry cutoff armed: no new entries after {NO_NEW_ENTRIES_AFTER[0]:02d}:{NO_NEW_ENTRIES_AFTER[1]:02d} PT (exits still run).")
    else:
        log("⏭️ NO_NEW_ENTRIES_AFTER not set — entry-time cutoff DISABLED.")

    # Auto-reconnect loop: restarts main() on any WebSocket/network crash.
    # SystemExit (raised by halt_trading → sys.exit(2)) is a BaseException, NOT
    # a subclass of Exception, so it propagates out of the loop cleanly on
    # scheduled shutdown without triggering a reconnect.
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            log(f"💥 Main loop crashed: {e}")
            log(f"🔄 Reconnecting in 10s...")
            time.sleep(10)
    