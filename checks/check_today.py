import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import OrderStatus, QueryOrderStatus

load_dotenv("keys.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

PT = ZoneInfo("America/Los_Angeles")
client = TradingClient(API_KEY, SECRET_KEY, paper=True)

local_start = datetime(2026, 6, 11, 0, 0, 0, tzinfo=PT)
local_end   = datetime(2026, 6, 11, 23, 59, 59, tzinfo=PT)

req = GetOrdersRequest(
    status=QueryOrderStatus.CLOSED,
    after=local_start.astimezone(timezone.utc),
    until=local_end.astimezone(timezone.utc),
    limit=500,
)

orders = client.get_orders(req)
filled = [o for o in orders if o.status == OrderStatus.FILLED]

print("=== All filled orders today (2026-06-11) ===")
buys = {}
sells = {}
for o in sorted(filled, key=lambda x: x.filled_at):
    t = o.filled_at.astimezone(PT).strftime("%H:%M:%S")
    qty = float(o.filled_qty)
    price = float(o.filled_avg_price)
    side = o.side.value
    sym = o.symbol
    print(f"  {t} PT  {sym:5s}  {side:4s}  qty={qty}  avg=${price:.4f}")
    if side == "buy":
        buys[sym] = buys.get(sym, []) + [(qty, price)]
    else:
        sells[sym] = sells.get(sym, []) + [(qty, price)]

print()
print("=== P&L by symbol ===")
total_pnl = 0.0
all_syms = set(list(buys.keys()) + list(sells.keys()))
for sym in sorted(all_syms):
    b = buys.get(sym, [])
    s = sells.get(sym, [])
    buy_total  = sum(q * p for q, p in b)
    sell_total = sum(q * p for q, p in s)
    buy_qty    = sum(q for q, p in b)
    sell_qty   = sum(q for q, p in s)
    if b and s:
        pnl = sell_total - buy_total
        total_pnl += pnl
        avg_buy  = buy_total  / buy_qty
        avg_sell = sell_total / sell_qty
        print(f"  {sym}: bought {buy_qty}sh @ avg ${avg_buy:.4f}, sold {sell_qty}sh @ avg ${avg_sell:.4f}  =>  PnL = ${pnl:.2f}")
    elif b:
        avg_buy = buy_total / buy_qty
        print(f"  {sym}: bought {buy_qty}sh @ avg ${avg_buy:.4f}  (still open)")
    else:
        print(f"  {sym}: only sells recorded today")

print()
print(f"=== NET P&L TODAY: ${total_pnl:.2f} ===")
