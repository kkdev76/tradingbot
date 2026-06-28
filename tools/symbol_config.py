"""
symbol_config.py — per-symbol settings generator

Run this script when adding a new stock to get the settings.td entries to add.

Usage:
    python symbol_config.py TSLA 250
    python symbol_config.py AMZN 185
"""

import sys


def suggest_signal_min(approx_price: float) -> float:
    """
    Suggest SIGNAL_MIN_VALUE for a new stock symbol.

    The Signal line is the 9-EMA of MACD. When abs(Signal) is near zero, the MACD
    has been oscillating with no directional context — entering is chop, not trend.
    A large negative Signal (recovery from deep sell-off) still passes because we
    use abs(), so genuine reversals are not blocked.

    Calibration anchor: AAPL at $294 → 0.10.
    Formula: price * 0.00034, floored at 0.08, capped at 0.10.

    The cap exists because stocks above ~$300 don't have proportionally larger
    Signal values on good trend days — the relationship breaks down at higher prices.
    Validate on a few backtest days before going live with a new symbol.
    """
    raw = approx_price * 0.00034
    return round(max(0.08, min(raw, 0.10)), 2)


def suggest_macd_min(approx_price: float) -> float:
    """
    Suggest MACD_MIN_VALUE starting point.
    Scales linearly with price: ~0.05% of price, floor 0.05.
    Tune after observing a few live days — this is a starting point only.
    """
    return round(max(0.05, approx_price * 0.0005), 2)


def suggest_macd_abs_gap_min(approx_price: float) -> float:
    """
    Suggest MACD_ABS_GAP_MIN (minimum absolute MACD−Signal spread).
    ~60% of MACD_MIN_VALUE is a reasonable starting point.
    """
    return round(suggest_macd_min(approx_price) * 0.6, 2)


def suggest_all(symbol: str, approx_price: float) -> dict:
    return {
        f"MACD_MIN_VALUE_{symbol.upper()}":    suggest_macd_min(approx_price),
        f"MACD_ABS_GAP_MIN_{symbol.upper()}":  suggest_macd_abs_gap_min(approx_price),
        f"SIGNAL_MIN_VALUE_{symbol.upper()}":  suggest_signal_min(approx_price),
        f"PROFIT_TAKE_{symbol.upper()}":       0.4,
    }


def print_settings(symbol: str, approx_price: float) -> None:
    print(f"\n# --- {symbol.upper()} (~${approx_price:.0f}) ---")
    for key, val in suggest_all(symbol, approx_price).items():
        print(f"{key}={val}")
    print(f"# Add {symbol.upper()} to STOCK_LIST and DOLLAR_BUDGETS in settings.td")
    print(f"# Backtest on indicator logs before going live to validate SIGNAL_MIN_VALUE")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python symbol_config.py <SYMBOL> <approx_price>")
        print("Example: python symbol_config.py TSLA 250")
        print()
        print("Current symbols for reference:")
        for sym, price in [("NVDA", 217), ("AAPL", 294), ("JPM", 300), ("MSFT", 413), ("META", 600)]:
            print_settings(sym, price)
    else:
        symbol = sys.argv[1]
        price  = float(sys.argv[2])
        print_settings(symbol, price)
