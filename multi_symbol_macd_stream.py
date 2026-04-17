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
MACD_MIN_THRESHOLD = float(os.getenv('MACD_MIN_THRESHOLD', '0.2'))
MACD_GAP_PERCENT = float(os.getenv('MACD_GAP_PERCENT', '40'))
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

# MACD buffer for trend analysis - stores last 3 MACD values for each symbol
macd_buffer = {sym: [] for sym in SYMBOLS}
MACD_BUFFER_SIZE = 3

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
    Check if the last 3 MACD values in the buffer are monotonically increasing.
    Returns True if current > previous > previous_previous, False otherwise.
    Requires at least 3 values in buffer to return True.
    """
    buffer = macd_buffer.get(sym, [])
    if len(buffer) < MACD_BUFFER_SIZE:
        return False
    
    # Check if values are strictly increasing: buffer[2] > buffer[1] > buffer[0]
    return buffer[2] > buffer[1] > buffer[0]

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
    # Append bar and compute MACD
    row = {'timestamp': ts, 'open': bar['o'], 'high': bar['h'], 'low': bar['l'], 'close': bar['c'], 'volume': bar['v']}
    df = pd.concat([dfs[sym], pd.DataFrame([row])], ignore_index=True).tail(1000)
    dfs[sym] = df = compute_macd(df)
    macd, sig = df.iloc[-1]['macd'], df.iloc[-1]['macd_signal']
    
    # Update MACD buffer and check trend
    update_macd_buffer(sym, macd)
    is_increasing = is_macd_monotonically_increasing(sym)
    buffer_values = macd_buffer[sym]
    
    log(f"🔄 {sym} bar at {ts.isoformat()}: MACD={macd:.4f}, Signal={sig:.4f}")
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
                log(f"✅ Take-profit sold {sym}. Updated last_trade_time to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
                return
            # else:
                # log(f"⏳ {sym}: holding position, P&L ${unrealized_pl:.2f} has not reached take-profit threshold ${dollar_threshold:.2f}")
        except Exception as e:
            log(f"⚠️ Error checking profit for {sym}: {e}")

    # MACD BEARISH CROSSOVER EXIT: sell if MACD has crossed below signal while holding
    if pos > 0:
        if macd < sig:
            if STOP_TRADING:
                log(f"[RiskGuard] Blocked MACD-EXIT SELL {sym} x{pos} (trading halted)")
                return
            log(f"🔴🔴🔴 MACD EXIT {sym}: MACD {macd:.4f} < Signal {sig:.4f}, selling {pos} shares 🔴🔴🔴")
            place_sell_market(sym, pos)
            last_trade_time[sym] = datetime.now(timezone.utc)
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
        if macd > MACD_MIN_THRESHOLD and gap_pct > MACD_GAP_PERCENT:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_trade_time[sym]).total_seconds() / 60
            # log(f"⏳ Cooldown check for {sym}: elapsed {elapsed:.1f} min (threshold {TRADE_COOLDOWN_MINUTES} min)")
            if elapsed < TRADE_COOLDOWN_MINUTES:
                # log(f"⏳ Skipping BUY for {sym}: cooldown active")
                return

            if STOP_TRADING:
                log(f"[RiskGuard] Blocked BUY {sym} (trading halted)")
                return

            if is_increasing:
                bid, _ = fetch_quote(sym)
                limit = round(bid + 0.01, 2)
                log(f"🟢🟢🟢 BUY {sym}: MACD={macd:.4f} > {MACD_MIN_THRESHOLD}, Signal={sig:.4f}, gap {gap_pct:.1f}% ≥ {MACD_GAP_PERCENT}% → buying @ {limit:.2f}🟢🟢🟢")
                place_buy(sym, limit, remaining_budget[sym])
                last_trade_time[sym] = now
                log(f"Updated last_trade_time for {sym} to {last_trade_time[sym].astimezone(ZoneInfo('America/Los_Angeles')).strftime('%H:%M:%S PT')}")
            else:
                log(f"💀💀💀 All Checks Passed for BUY {sym} but MACD is not increasing so skipping buy💀💀💀")
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
    