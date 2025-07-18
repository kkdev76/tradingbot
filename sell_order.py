import os
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
from dotenv import load_dotenv

load_dotenv("crypto.env")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

trading_client = TradingClient(API_KEY, SECRET_KEY, paper=False)

def place_sell(symbol: str, ask_price: float, qty: int):
    """Place a LIMIT SELL at ask_price - $0.05 for the specified quantity."""
    limit_price = round(ask_price - 0.05, 2)
    trading_client.submit_order(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price
    )
