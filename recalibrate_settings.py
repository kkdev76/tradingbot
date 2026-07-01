"""
Recalibrate per-symbol MACD/signal thresholds in settings.td by replaying
all available indicator-log CSVs and finding the parameter combination that
maximises cumulative P&L per symbol.

Parameters swept per symbol:
  MACD_MIN_VALUE_<SYM>    floor; entry blocked when MACD < this
  MACD_GAP_PERCENT_<SYM>  % gap (MACD-Signal)/|Signal| required in trending regime
  SIGNAL_MIN_VALUE_<SYM>  minimum |Signal| before entry allowed

Run after market close each trading day (e.g. 4:15 PM ET).
"""

import csv
import math
import os
import re
import sys
from datetime import date, datetime, timezone
from itertools import product
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

load_dotenv("keys.env")
API_KEY    = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

SETTINGS_FILE = "settings.td"
LOG_DIR       = "order_dashboard/indicator_logs"
PT            = ZoneInfo("America/Los_Angeles")

# Only recalibrate on session logs from this date onward. Earlier CSVs are
# shutdown-stamped (one file spanning two sessions) and/or predate the `close`
# column, so they are not trusted for parameter optimization.
EARLIEST_TRUSTED_LOG_DATE = date(2026, 6, 29)

NaN = float("nan")

# ── helpers ──────────────────────────────────────────────────────────────────

def read_settings(path):
    with open(path, "r") as f:
        return f.read()

def get_value(content, key, default=None):
    m = re.search(rf"^{re.escape(key)}=(.+)$", content, re.MULTILINE)
    if m:
        return m.group(1).strip()
    return default

def get_stock_list(content):
    raw = get_value(content, "STOCK_LIST", "")
    return [s.strip() for s in raw.split(",") if s.strip()]

def update_setting(content, key, value):
    pattern  = rf"^({re.escape(key)}=).*$"
    new_line = rf"\g<1>{value}"
    updated, n = re.subn(pattern, new_line, content, flags=re.MULTILINE)
    if n == 0:
        updated = content.rstrip() + f"\n{key}={value}\n"
    return updated

# ── indicator-log loading ─────────────────────────────────────────────────────

def load_csv(path):
    rows = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "close" not in reader.fieldnames:
                return []
            for r in reader:
                try:
                    rows.append({
                        "ts":   datetime.fromisoformat(r["timestamp"]).replace(tzinfo=PT),
                        "px":   float(r["close"]),
                        "macd": float(r["MACD"]),
                        "sig":  float(r["Signal"]),
                        "rsi":  float(r["RSI"]),
                    })
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        pass
    return rows

def load_all_days(sym):
    """Return one rows-list per SESSION DATE for this symbol.

    Rows from all of the symbol's CSVs are merged and regrouped by the bar's own
    timestamp date (deduped by timestamp), so historical shutdown-stamped files —
    where a single file spans two sessions, or two files overlap the same session —
    are split back into correct, independent trading days. Each day is then replayed
    with buffers reset, matching production's per-session behaviour."""
    if not os.path.isdir(LOG_DIR):
        return []
    merged = {}  # timestamp -> row (dedupe; identical bars across files collapse)
    for fname in sorted(os.listdir(LOG_DIR)):
        if fname.startswith(f"{sym}_") and fname.endswith(".csv"):
            for r in load_csv(os.path.join(LOG_DIR, fname)):
                merged[r["ts"]] = r
    by_date = {}
    for r in merged.values():
        d = r["ts"].date()
        if d < EARLIEST_TRUSTED_LOG_DATE:
            continue   # pre-6/29 logs not trusted (shutdown-stamped / no close column)
        by_date.setdefault(d, []).append(r)
    return [sorted(by_date[d], key=lambda x: x["ts"]) for d in sorted(by_date)]

# ── replay engine (mirrors production logic exactly) ─────────────────────────

BUF_SIZE     = 5
RSI_MIN      = 50.0
RSI_BUF_SIZE = 5

def replay(days, budget, stop_loss_pct, profit_take_pct,
           macd_min, gap_pct_thresh, sig_min, cooldown_min=10, entry_cutoff=None):
    """
    Replay all days for one symbol with given params.
    Each day is independent (buffers reset between days).
    entry_cutoff: optional (hour, minute) PT — no NEW entries at/after this time
                  (mirrors production NO_NEW_ENTRIES_AFTER). Exits unaffected.
    Returns total P&L across all days.
    """
    total_pl = 0.0

    for rows in days:
        macd_buf = []
        rsi_buf  = []
        pos_macd = [NaN] * BUF_SIZE   # position MACD buffer (post-entry)

        pos       = 0
        shares    = 0
        entry_px  = 0.0
        last_ts   = datetime.min.replace(tzinfo=PT)
        cooldown  = cooldown_min

        for i, bar in enumerate(rows):
            macd = bar["macd"]
            sig  = bar["sig"]
            rsi  = bar["rsi"]
            px   = bar["px"]
            ts   = bar["ts"]

            macd_buf.append(macd)
            if len(macd_buf) > BUF_SIZE:
                macd_buf.pop(0)
            rsi_buf.append(rsi)
            if len(rsi_buf) > RSI_BUF_SIZE:
                rsi_buf.pop(0)

            prev_macd = rows[i-1]["macd"] if i > 0 else NaN
            prev_sig  = rows[i-1]["sig"]  if i > 0 else NaN

            if pos > 0:
                # update position MACD buffer
                if math.isnan(pos_macd[1]):
                    pos_macd[1] = macd
                elif math.isnan(pos_macd[2]):
                    pos_macd[2] = macd
                elif math.isnan(pos_macd[3]):
                    pos_macd[3] = macd
                elif math.isnan(pos_macd[4]):
                    pos_macd[4] = macd
                else:
                    pos_macd = pos_macd[1:] + [macd]

                pct = (px - entry_px) / entry_px * 100

                # stop-loss
                if pct < -stop_loss_pct:
                    total_pl += (px - entry_px) * shares
                    pos = 0
                    last_ts = ts
                    pos_macd = [NaN] * BUF_SIZE
                    continue

                # take-profit
                if (px - entry_px) * shares >= (profit_take_pct / 100) * budget:
                    total_pl += (px - entry_px) * shares
                    pos = 0
                    last_ts = ts
                    pos_macd = [NaN] * BUF_SIZE
                    continue

                # MACD exit: b4 < b2 AND d2 < 0
                if not any(math.isnan(v) for v in pos_macd):
                    b0, b1, b2, b3, b4 = pos_macd
                    if not (b0 < b1 < b2 < b3 < b4):
                        d2 = b4 - b3
                        if b4 < b2 and d2 < 0:
                            total_pl += (px - entry_px) * shares
                            pos = 0
                            last_ts = ts
                            pos_macd = [NaN] * BUF_SIZE
                continue

            # ── entry checks ────────────────────────────────────────────────

            # Entry-time cutoff (mirror production NO_NEW_ENTRIES_AFTER): no NEW
            # positions at/after this PT time. ts is naive PT wall-clock.
            if entry_cutoff is not None and (ts.hour, ts.minute) >= entry_cutoff:
                continue

            # MACD floor + regime gate
            if macd <= 0 or macd < macd_min:
                continue
            if math.isnan(prev_macd) or prev_macd < macd_min:
                continue   # need confirmation bar above floor

            # gap% in trending regime
            if sig == 0:
                continue
            gap = ((macd - sig) / abs(sig)) * 100
            if gap <= gap_pct_thresh:
                continue

            # signal established and rising
            if abs(sig) < sig_min:
                continue
            if math.isnan(prev_sig) or sig <= prev_sig:
                continue

            # 5-bar monotonic MACD
            if len(macd_buf) < BUF_SIZE:
                continue
            if not (macd_buf[0] < macd_buf[1] < macd_buf[2] < macd_buf[3] < macd_buf[4]):
                continue

            # RSI zone: all 5 bars >= 50 and current >= previous
            if len(rsi_buf) < RSI_BUF_SIZE:
                continue
            if not all(v >= RSI_MIN for v in rsi_buf):
                continue
            prev_rsi = rows[i-1]["rsi"] if i > 0 else rsi
            if rsi < prev_rsi:
                continue

            # cooldown
            if (ts - last_ts).total_seconds() / 60 < cooldown:
                continue

            shares  = int(budget / px)
            if shares == 0:
                continue
            entry_px = px
            pos      = shares
            last_ts  = ts
            pos_macd = [macd, NaN, NaN, NaN, NaN]

        # day ended with open position — mark to close
        if pos > 0:
            total_pl += (rows[-1]["px"] - entry_px) * shares

    return total_pl


# ── candidate generation ──────────────────────────────────────────────────────

def candidates_macd_min(price):
    """7 candidates bracketing price * 0.0005."""
    base = price * 0.0005
    factors = [0.20, 0.30, 0.40, 0.50, 0.65, 0.80, 1.00, 1.20, 1.50]
    vals = sorted({round(base * f, 2) for f in factors})
    return [v for v in vals if v > 0]

def candidates_gap_pct():
    return [20, 30, 40, 50, 60, 70, 80, 90, 100]

def candidates_sig_min(price):
    """6 candidates around price * 0.00034, clamped to [0.06, 0.14]."""
    base = price * 0.00034
    factors = [0.50, 0.65, 0.80, 1.00, 1.20, 1.50]
    raw = sorted({round(base * f, 2) for f in factors})
    return [min(max(v, 0.06), 0.14) for v in raw]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    content = read_settings(SETTINGS_FILE)
    symbols = get_stock_list(content)

    stop_loss_pct   = float(get_value(content, "STOP_LOSS_PERCENT",         "0.35"))
    profit_take_pct = float(get_value(content, "PROFIT_TAKE_DEFAULT",        "0.50"))
    budget          = float(get_value(content, "DEFAULT_DOLLAR",             "5000"))
    cooldown_min    = int(  get_value(content, "MIN_TRADE_COOLDOWN_MINUTES", "10"))

    # Entry-time cutoff — keep the sweep consistent with production's entry window.
    raw_cutoff   = (get_value(content, "NO_NEW_ENTRIES_AFTER", "") or "").strip()
    entry_cutoff = None
    if raw_cutoff:
        try:
            hh, mm = map(int, raw_cutoff.split(":"))
            entry_cutoff = (hh, mm)
        except ValueError:
            entry_cutoff = None

    ET = ZoneInfo("America/New_York")
    print(f"Fetching current (pre-open) prices for: {', '.join(symbols)}")
    data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    feed = get_value(content, "HISTORICAL_FEED", "iex")
    # Use the LATEST TRADE price at recalibration time (runs just before the open),
    # so overnight / pre-market moves are reflected. The prior session's close can be
    # stale if the stock gapped — thresholds are anchored to price, so they must track
    # where the stock actually is right before we start trading it today.
    trades   = data_client.get_stock_latest_trade(
        StockLatestTradeRequest(symbol_or_symbols=symbols, feed=feed))
    prices   = {sym: trades[sym].price for sym in symbols if sym in trades}
    now_et   = datetime.now(ET)
    date_str = now_et.strftime("%Y-%m-%d")

    cutoff_desc = f"{entry_cutoff[0]:02d}:{entry_cutoff[1]:02d} PT" if entry_cutoff else "none"
    print(f"\nRunning sweep on indicator logs in '{LOG_DIR}/'")
    print(f"Fixed params — stop-loss: {stop_loss_pct}%  profit-take: {profit_take_pct}%  "
          f"budget: ${budget:.0f}  entry-cutoff: {cutoff_desc}\n")

    best_params = {}
    skipped = {}

    for sym in symbols:
        price = prices.get(sym)
        if price is None:
            print(f"  {sym}: no price data, skipping")
            skipped[sym] = "no price data"
            continue

        days = load_all_days(sym)
        if not days:
            print(f"  {sym}: no indicator logs found, skipping")
            skipped[sym] = "no indicator logs found"
            continue

        # per-symbol budget override
        sym_budget_raw = get_value(content, "DOLLAR_BUDGETS", "")
        sym_budget = budget
        for part in sym_budget_raw.split(","):
            if ":" in part:
                s, v = part.split(":", 1)
                if s.strip() == sym:
                    sym_budget = float(v.strip())

        # per-symbol profit-take override
        pt = float(get_value(content, f"PROFIT_TAKE_{sym}", str(profit_take_pct)))

        c_macd = candidates_macd_min(price)
        c_gap  = candidates_gap_pct()
        c_sig  = candidates_sig_min(price)
        n_days = len(days)

        print(f"  {sym}  price=${price:.2f}  days={n_days}  "
              f"combos={len(c_macd)}×{len(c_gap)}×{len(c_sig)}={len(c_macd)*len(c_gap)*len(c_sig)}")

        best_pl   = None
        best_combo = None

        for macd_min, gap_pct, sig_min in product(c_macd, c_gap, c_sig):
            pl = replay(days, sym_budget, stop_loss_pct, pt,
                        macd_min, gap_pct, sig_min, cooldown_min, entry_cutoff)
            if best_pl is None or pl > best_pl:
                best_pl    = pl
                best_combo = (macd_min, gap_pct, sig_min)

        macd_min, gap_pct, sig_min = best_combo

        best_params[sym] = {
            "macd_min": f"{macd_min:.2f}",
            "gap_pct":  gap_pct,
            "sig_min":  f"{sig_min:.2f}",
            "best_pl":  best_pl,
        }
        print(f"    best: MACD_MIN={macd_min:.2f}  GAP%={gap_pct}  SIG_MIN={sig_min:.2f}  sim_PL=${best_pl:+.2f}")

    # Refuse to write if NOTHING was optimized. A silent no-op that leaves stale
    # thresholds in place (while reporting "settings.td updated") is worse than an
    # obvious failure — it's exactly how the 2026-07-01 run shipped yesterday's
    # params unnoticed. Exit non-zero so the launcher logs a warning and keeps the
    # existing settings.td unchanged.
    if not best_params:
        reasons = "; ".join(f"{s}: {r}" for s, r in skipped.items()) or "unknown"
        msg = (f"ERROR: Recalibration optimized 0/{len(symbols)} symbols — "
               f"NOT writing {SETTINGS_FILE}. Reasons: {reasons}")
        print(msg)
        print(msg, file=sys.stderr)
        sys.exit(1)

    # Some (but not all) symbols optimized — write the winners, but flag the gap.
    if skipped:
        print(f"WARNING: Recalibration skipped {len(skipped)}/{len(symbols)} symbol(s) "
              f"(kept prior settings for them): "
              + "; ".join(f"{s}: {r}" for s, r in skipped.items()))

    # ── write to settings.td ─────────────────────────────────────────────────
    print(f"\nUpdating {SETTINGS_FILE} ...")

    syms_comment = ", ".join(
        f"{s}~${prices[s]:.0f}" for s in symbols if s in prices
    )
    stamp = now_et.strftime("%Y-%m-%d %H:%M ET")
    new_comment = f"# Prices at pre-open recalibration {stamp}: {syms_comment}"
    content, n = re.subn(r"^# Prices .*$", new_comment, content, count=1, flags=re.MULTILINE)
    if n == 0:
        content = re.sub(r"(^MACD_MIN_VALUE_)", new_comment + "\n" + r"\1",
                         content, count=1, flags=re.MULTILINE)

    for sym, p in best_params.items():
        content = update_setting(content, f"MACD_MIN_VALUE_{sym}",   p["macd_min"])
        content = update_setting(content, f"MACD_GAP_PERCENT_{sym}", p["gap_pct"])
        content = update_setting(content, f"SIGNAL_MIN_VALUE_{sym}", p["sig_min"])

    with open(SETTINGS_FILE, "w") as f:
        f.write(content)

    print(f"\n{'Symbol':<8} {'Price':>8}  {'MACD_MIN':>10}  {'GAP%':>6}  {'SIG_MIN':>8}  {'Sim P&L':>10}")
    print("-" * 60)
    for sym in symbols:
        if sym not in best_params:
            continue
        p = best_params[sym]
        print(f"{sym:<8} {prices[sym]:>8.2f}  {p['macd_min']:>10}  {p['gap_pct']:>6}  "
              f"{p['sig_min']:>8}  ${p['best_pl']:>+9.2f}")

    print(f"\nsettings.td updated.")


if __name__ == "__main__":
    main()
