"""
Recalibrate price-derived thresholds in settings.td using latest closing prices.

Run after market close each day (e.g. 4:15 PM ET).

Updates per-symbol:
  MACD_MIN_VALUE_<SYM>   = price * 0.0005
  MACD_ABS_GAP_MIN_<SYM> = price * 0.0003
  SIGNAL_MIN_VALUE_<SYM> = clamp(price * 0.00034, 0.08, 0.10)
"""

import os
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestBarRequest

load_dotenv("keys.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

SETTINGS_FILE = "settings.td"

def read_settings(path):
    with open(path, "r") as f:
        return f.read()

def get_stock_list(content):
    m = re.search(r"^STOCK_LIST=(.+)$", content, re.MULTILINE)
    if not m:
        raise ValueError("STOCK_LIST not found in settings.td")
    return [s.strip() for s in m.group(1).split(",")]

def fetch_close_prices(symbols):
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    req = StockLatestBarRequest(symbol_or_symbols=symbols)
    bars = client.get_stock_latest_bar(req)
    prices = {}
    for sym, bar in bars.items():
        prices[sym] = bar.close
    return prices

def compute_thresholds(price):
    macd_min   = f"{price * 0.0005:.2f}"
    abs_gap    = f"{price * 0.0003:.2f}"
    signal_min = f"{min(max(price * 0.00034, 0.08), 0.10):.2f}"
    return macd_min, abs_gap, signal_min

def update_setting(content, key, value):
    pattern = rf"^({re.escape(key)}=).*$"
    new_line = rf"\g<1>{value}"
    updated, n = re.subn(pattern, new_line, content, flags=re.MULTILINE)
    if n == 0:
        # Key doesn't exist yet — append before the blank line at end or at end
        updated = content.rstrip() + f"\n{key}={value}\n"
    return updated

def main():
    content = read_settings(SETTINGS_FILE)
    symbols = get_stock_list(content)

    print(f"Fetching closing prices for: {', '.join(symbols)}")
    prices = fetch_close_prices(symbols)

    ET = ZoneInfo("America/New_York")
    now_et = datetime.now(ET)
    date_str = now_et.strftime("%Y-%m-%d")

    print(f"\nRecalibrating settings.td using prices from {date_str}:")
    print(f"{'Symbol':<8} {'Close':>8}  {'MACD_MIN':>10}  {'ABS_GAP':>9}  {'SIG_MIN':>9}")
    print("-" * 55)

    for sym in symbols:
        if sym not in prices:
            print(f"  {sym}: no price data, skipping")
            continue
        price = prices[sym]
        macd_min, abs_gap, signal_min = compute_thresholds(price)
        print(f"{sym:<8} {price:>8.2f}  {macd_min:>10}  {abs_gap:>9}  {signal_min:>9}")

        content = update_setting(content, f"MACD_MIN_VALUE_{sym}", macd_min)
        content = update_setting(content, f"MACD_ABS_GAP_MIN_{sym}", abs_gap)
        content = update_setting(content, f"SIGNAL_MIN_VALUE_{sym}", signal_min)

    # Update the reference comment line
    syms_comment = ", ".join(
        f"{s}~${prices[s]:.0f}" for s in symbols if s in prices
    )
    new_comment = f"# Prices from 4pm ET close {date_str}: {syms_comment}"
    content = re.sub(
        r"^# Prices from 4pm ET close .*$",
        new_comment,
        content,
        flags=re.MULTILINE,
    )
    # If comment line didn't exist, add it before the first MACD_MIN_VALUE_ line
    if "# Prices from 4pm ET close" not in content:
        content = re.sub(
            r"(^MACD_MIN_VALUE_)",
            new_comment + "\n" + r"\1",
            content,
            count=1,
            flags=re.MULTILINE,
        )

    with open(SETTINGS_FILE, "w") as f:
        f.write(content)

    print(f"\nsettings.td updated.")

if __name__ == "__main__":
    main()
