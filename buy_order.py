import os, time
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest
from dotenv import load_dotenv

load_dotenv("crypto.env")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

def cancel_symbol_orders(symbol: str):
    """Cancel any open orders for this symbol to avoid wash-trade overlap."""
    open_orders = trading_client.get_orders()  # default returns open orders
    for order in open_orders:
        if order.symbol == symbol:
            trading_client.cancel_order_by_id(order.id)

def place_buy(symbol: str, current_price: float, dollars: float) -> int:
    """Buy as many shares as dollars/current_price at LIMIT price (bid+0.05). Returns filled qty."""
    qty = int(dollars // current_price)
    if qty < 1:
        raise ValueError("Budget too small for even 1 share.")
    limit_price = round(current_price + 0.01, 2)
    cancel_symbol_orders(symbol)
    order_req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )
    order = trading_client.submit_order(order_req)
    # Wait 5 seconds before checking fill status
    time.sleep(5)
    # Poll for execution
    for _ in range(5):
        orders = trading_client.get_orders()  # poll open and filled
        resp = next((o for o in orders if o.id == order.id), None)
        if resp:
            filled = int(float(resp.filled_qty))
            if resp.status == 'filled' or filled > 0:
                return filled
        time.sleep(1)
    # Final check
    orders = trading_client.get_orders()
    resp = next((o for o in orders if o.id == order.id), None)
    return int(float(resp.filled_qty)) if resp else 0
