import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
load_dotenv("keys.env")
client = TradingClient(os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY"), paper=True)
positions = client.get_all_positions()
if positions:
    for p in positions:
        sym = p.symbol
        qty = p.qty
        entry = float(p.avg_entry_price)
        current = float(p.current_price)
        upnl = float(p.unrealized_pl)
        print(f"{sym}: {qty}sh  entry=${entry:.4f}  current=${current:.4f}  unrealized_pnl=${upnl:.2f}")
else:
    print("No open positions")
