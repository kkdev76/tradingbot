import os
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
from dotenv import load_dotenv

# Load environment variables
load_dotenv("crypto.env")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# Alpaca trading client (live account)
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=False)

def place_buy(symbol: str, bid_price: float, dollars: float) -> int:
    """Place a LIMIT BUY at bid_price + $0.05 for the dollar amount specified.

    Returns
    -------
    int
        Quantity of shares purchased.
    """
    print(symbol)
    limit_price = round(bid_price + 0.05, 2)
    qty = int(dollars // limit_price)
    if qty < 1:
        raise ValueError("Dollar amount too small for even 1 share.")
    order = trading_client.submit_order(
        symbol_or_asset_id=symbol,
        qty=qty,
        side=OrderSide.BUY,
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price
    )
    return qty
