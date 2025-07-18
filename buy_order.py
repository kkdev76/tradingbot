import os
from alpaca.trading.client   import TradingClient
from alpaca.trading.enums    import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest
from dotenv import load_dotenv

load_dotenv("crypto.env")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)


def cancel_symbol_orders(symbol: str):
    """Cancel any open orders for this symbol to avoid wash‑trade overlap."""
    for order in trading_client.get_orders():   # returns OPEN orders
        if order.symbol == symbol:
            trading_client.cancel_order_by_id(order.id)


def place_buy(symbol: str, current_price: float, dollars: float) -> int:
    """Buy as many shares as dollars/current_price at LIMIT price (bid+0.05)."""
    qty = int(dollars // current_price)
    if qty < 1:
        raise ValueError("Budget too small for even 1 share.")
    limit_price = round(current_price + 0.05, 2)

    cancel_symbol_orders(symbol)             # ← NEW

    order = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )
    trading_client.submit_order(order)
    return qty
