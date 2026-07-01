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

days = [(6, 22), (6, 23), (6, 24), (6, 25)]

for month, day in days:
    local_start = datetime(2026, month, day, 0, 0, 0, tzinfo=PT)
    local_end   = datetime(2026, month, day, 23, 59, 59, tzinfo=PT)
    req = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=local_start.astimezone(timezone.utc),
        until=local_end.astimezone(timezone.utc),
        limit=500,
    )
    orders = client.get_orders(req)
    filled = [o for o in orders if o.status == OrderStatus.FILLED]
    print(f"=== June {day} === ({len(filled)} filled orders)")
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
    all_syms = set(list(buys.keys()) + list(sells.keys()))
    total_pnl = 0.0
    print("  P&L:")
    for sym in sorted(all_syms):
        b = buys.get(sym, [])
        s = sells.get(sym, [])
        buy_total = sum(q * p for q, p in b)
        sell_total = sum(q * p for q, p in s)
        buy_qty = sum(q for q, p in b)
        sell_qty = sum(q for q, p in s)
        if buy_qty == sell_qty and b and s:
            pnl = sell_total - buy_total
            total_pnl += pnl
            avg_buy = buy_total / buy_qty
            avg_sell = sell_total / sell_qty
            print(f"    {sym}: {buy_qty}sh  buy=${avg_buy:.4f}  sell=${avg_sell:.4f}  PnL=${pnl:.2f}")
        elif b and s:
            print(f"    {sym}: MISMATCHED  bought={buy_qty}sh  sold={sell_qty}sh  (partial)")
        elif b:
            avg_buy = buy_total / buy_qty
            print(f"    {sym}: bought {buy_qty}sh @ ${avg_buy:.4f}  (no sell this day)")
        else:
            avg_sell = sell_total / sell_qty
            print(f"    {sym}: sold {sell_qty}sh @ ${avg_sell:.4f}  (no buy this day — carry-over)")
    if not all_syms:
        print("    (no trades)")
    else:
        print(f"  Day total (matched pairs only): ${total_pnl:.2f}")
    print()
