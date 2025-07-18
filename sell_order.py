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
    for order in trading_client.get_orders():   # returns OPEN orders
        if order.symbol == symbol:
            trading_client.cancel_order_by_id(order.id)

  

def place_sell(symbol: str, ask_price: float, qty: int):
    """Sell qty shares at LIMIT price (ask‑0.05)."""
    limit_price = round(ask_price - 0.05, 2)

    cancel_symbol_orders(symbol)             # ← NEW

    order = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )
    trading_client.submit_order(order)
