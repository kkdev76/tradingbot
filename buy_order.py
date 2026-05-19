import os, time
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest  # switched from LimitOrderRequest
# from alpaca.trading.requests import LimitOrderRequest  # retained for reference
from dotenv import load_dotenv

load_dotenv("keys.env")
load_dotenv("settings.td")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

def cancel_symbol_orders(symbol: str):
    """Cancel any open orders for this symbol to avoid wash-trade overlap."""
    open_orders = trading_client.get_orders()  # default returns open orders
    for order in open_orders:
        if order.symbol == symbol:
            try:
                trading_client.cancel_order_by_id(order.id)
            except Exception as e:
                err = str(e)
                if '42210000' in err:
                    pass  # Order already pending cancel — safe to ignore
                else:
                    raise

def place_buy(symbol: str, current_price: float, dollars: float) -> int:
    """Buy as many shares as dollars/current_price as a MARKET order. Returns filled qty."""
    qty = int(dollars // current_price)
    if qty < 1:
        raise ValueError("Budget too small for even 1 share.")
    cancel_symbol_orders(symbol)
    order_req = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    # # Old limit order approach — filled below ask too rarely, caused stale orders
    # limit_price = round(current_price + 0.01, 2)
    # order_req = LimitOrderRequest(
    #     symbol=symbol,
    #     qty=qty,
    #     side=OrderSide.BUY,
    #     time_in_force=TimeInForce.DAY,
    #     limit_price=limit_price,
    # )
    order = trading_client.submit_order(order_req)
    # Market orders fill in milliseconds — short sleep is sufficient
    time.sleep(1)
    # Poll for fill confirmation
    for _ in range(3):
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
