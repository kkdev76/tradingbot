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
    open_orders = trading_client.get_orders()
    for order in open_orders:
        if order.symbol == symbol:
            trading_client.cancel_order_by_id(order.id)

def place_sell(symbol: str, ask_price: float, qty: int = None) -> int:
    """Sell given qty shares (or all if qty=None) at LIMIT price (ask-0.05). Returns filled qty."""
    # Determine qty if not provided
    if qty is None:
        try:
            position = trading_client.get_position(symbol)
            qty = int(float(position.qty))
        except Exception:
            qty = 0
    if qty < 1:
        return 0
    limit_price = round(ask_price - 0.05, 2)
    cancel_symbol_orders(symbol)
    order_req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )
    order = trading_client.submit_order(order_req)
    # Wait 5 seconds before checking fill status
    time.sleep(5)
    # Poll for execution
    for _ in range(5):
        orders = trading_client.get_orders()
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
