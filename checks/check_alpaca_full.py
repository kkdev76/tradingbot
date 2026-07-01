import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest
from alpaca.trading.enums import OrderStatus, QueryOrderStatus

load_dotenv("keys.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

PT = ZoneInfo("America/Los_Angeles")
client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# Account overview
acct = client.get_account()
print("=== ACCOUNT ===")
print(f"  Equity:         ${float(acct.equity):,.2f}")
print(f"  Cash:           ${float(acct.cash):,.2f}")
print(f"  Portfolio value:${float(acct.portfolio_value):,.2f}")
print(f"  Buying power:   ${float(acct.buying_power):,.2f}")
print()

# All orders June 22-24 with ALL statuses
print("=== ALL ORDERS June 22-24 (any status) ===")
for day in [22, 23, 24]:
    local_start = datetime(2026, 6, day, 0, 0, 0, tzinfo=PT)
    local_end   = datetime(2026, 6, day, 23, 59, 59, tzinfo=PT)
    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=local_start.astimezone(timezone.utc),
        until=local_end.astimezone(timezone.utc),
        limit=500,
    )
    orders = client.get_orders(req)
    print(f"\n--- June {day} ({len(orders)} total orders) ---")
    for o in sorted(orders, key=lambda x: x.submitted_at):
        filled_t = o.filled_at.astimezone(PT).strftime("%H:%M:%S") if o.filled_at else "      "
        qty = float(o.filled_qty) if o.filled_qty else 0
        price = float(o.filled_avg_price) if o.filled_avg_price else 0
        print(f"  {filled_t} PT  {o.symbol:5s}  {o.side.value:4s}  status={o.status.value:15s}  filled_qty={qty}  avg=${price:.4f}")
