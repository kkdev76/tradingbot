"""
Replay all NVDA indicator CSVs against the current bot logic and variants.
Does NOT touch any production files. Read-only w.r.t. the live bot.

Variants tested:
  Baseline : RSI_MAX=65, GAP%=40  (current settings)
  A        : RSI_MAX=70, GAP%=40
  B        : RSI_MAX=65, GAP%=25
  C        : RSI_MAX=70, GAP%=25
"""

import csv
import glob
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

PT = ZoneInfo("America/Los_Angeles")
LOG_DIR = "order_dashboard/indicator_logs"

# ── Fixed settings (from settings.td) ──────────────────────────────────────
BUDGET           = 7000.0
MACD_MIN_VALUE   = 0.15        # MACD_MIN_VALUE_NVDA
SIGNAL_MIN_VALUE = 0.10        # SIGNAL_MIN_VALUE_NVDA
MACD_FLATTEN_THRESHOLD = 0.01
STOP_LOSS_PCT    = 0.15        # percent
PROFIT_TAKE_PCT  = 0.4         # percent of budget → $28
COOLDOWN_MIN     = 10
RSI_BUY_MIN      = 50.0
MACD_BUFFER_SIZE = 5
RSI_BUFFER_SIZE  = 5

# ── Variants ────────────────────────────────────────────────────────────────
VARIANTS = [
    {"name": "Baseline (RSI<=65, gap>=40%)", "RSI_MAX": 65.0, "GAP_PCT": 40.0},
    {"name": "A  -- RSI<=70, gap>=40%",      "RSI_MAX": 70.0, "GAP_PCT": 40.0},
    {"name": "B  -- RSI<=65, gap>=25%",      "RSI_MAX": 65.0, "GAP_PCT": 25.0},
    {"name": "C  -- RSI<=70, gap>=25%",      "RSI_MAX": 70.0, "GAP_PCT": 25.0},
]


def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "close" not in (reader.fieldnames or []):
            return []   # older format without price — skip
        for r in reader:
            rows.append({
                "ts":    datetime.fromisoformat(r["timestamp"]).replace(tzinfo=PT),
                "close": float(r["close"]),
                "macd":  float(r["MACD"]),
                "sig":   float(r["Signal"]),
                "rsi":   float(r["RSI"]),
            })
    return rows


def is_monotonic(buf):
    if len(buf) < MACD_BUFFER_SIZE:
        return False
    return buf[4] > buf[3] > buf[2] > buf[1] > buf[0]


def rsi_stable(buf, current, previous):
    if len(buf) < RSI_BUFFER_SIZE:
        return False
    return all(v >= RSI_BUY_MIN for v in buf) and current >= previous


def check_macd_exit(pos_buf):
    if any(math.isnan(v) for v in pos_buf):
        return False, ""
    b0, b1, b2, b3, b4 = pos_buf
    if b0 < b1 < b2 < b3 < b4:
        return False, ""
    d1 = b3 - b2
    d2 = b4 - b3
    if d1 < MACD_FLATTEN_THRESHOLD and d2 < MACD_FLATTEN_THRESHOLD:
        if d2 >= 0:
            return True, f"flattening 2 bars (Δ={d1:.4f}, Δ={d2:.4f})"
        else:
            return True, f"declining 2 bars ({b2:.4f}->{b3:.4f}->{b4:.4f})"
    return False, ""


def replay(rows, RSI_MAX, GAP_PCT):
    nan = float("nan")
    macd_buf  = []
    rsi_buf   = []
    pos_buf   = [nan, nan, nan, nan, nan]
    pos       = 0
    shares    = 0
    entry_px  = 0.0
    last_trade_ts = datetime.min.replace(tzinfo=PT)
    trades    = []

    for i, bar in enumerate(rows):
        macd = bar["macd"]
        sig  = bar["sig"]
        rsi  = bar["rsi"]
        px   = bar["close"]
        ts   = bar["ts"]

        # Update entry buffers
        macd_buf.append(macd)
        if len(macd_buf) > MACD_BUFFER_SIZE:
            macd_buf.pop(0)
        rsi_buf.append(rsi)
        if len(rsi_buf) > RSI_BUFFER_SIZE:
            rsi_buf.pop(0)

        prev_macd = rows[i - 1]["macd"] if i > 0 else nan
        prev_sig  = rows[i - 1]["sig"]  if i > 0 else nan
        prev_rsi  = rows[i - 1]["rsi"]  if i > 0 else nan

        # ── HOLDING: update position MACD buffer ────────────────────────────
        if pos > 0:
            # shift position buffer
            if math.isnan(pos_buf[0]):
                pos_buf = [macd, nan, nan, nan, nan]
            elif math.isnan(pos_buf[1]):
                pos_buf = [pos_buf[0], macd, nan, nan, nan]
            elif math.isnan(pos_buf[2]):
                pos_buf = [pos_buf[0], pos_buf[1], macd, nan, nan]
            elif math.isnan(pos_buf[3]):
                pos_buf = [pos_buf[0], pos_buf[1], pos_buf[2], macd, nan]
            elif math.isnan(pos_buf[4]):
                pos_buf = [pos_buf[0], pos_buf[1], pos_buf[2], pos_buf[3], macd]
            else:
                pos_buf = [pos_buf[1], pos_buf[2], pos_buf[3], pos_buf[4], macd]

            unrealized_pct = (px - entry_px) / entry_px * 100

            # Stop loss
            if unrealized_pct < -STOP_LOSS_PCT:
                pl = (px - entry_px) * shares
                trades.append({"type": "SELL", "reason": "stop-loss",
                               "ts": ts, "px": px, "shares": shares, "pl": pl})
                pos = 0
                shares = 0
                last_trade_ts = ts
                pos_buf = [nan, nan, nan, nan, nan]
                continue

            # Take profit
            profit_target = (PROFIT_TAKE_PCT / 100) * BUDGET
            if (px - entry_px) * shares >= profit_target:
                pl = (px - entry_px) * shares
                trades.append({"type": "SELL", "reason": "take-profit",
                               "ts": ts, "px": px, "shares": shares, "pl": pl})
                pos = 0
                shares = 0
                last_trade_ts = ts
                pos_buf = [nan, nan, nan, nan, nan]
                continue

            # MACD exit
            should_sell, reason = check_macd_exit(pos_buf)
            if should_sell:
                pl = (px - entry_px) * shares
                trades.append({"type": "SELL", "reason": f"macd-exit ({reason})",
                               "ts": ts, "px": px, "shares": shares, "pl": pl})
                pos = 0
                shares = 0
                last_trade_ts = ts
                pos_buf = [nan, nan, nan, nan, nan]
            continue

        # ── FLAT: evaluate buy ───────────────────────────────────────────────
        # gap_ok (two-regime)
        if macd <= 0:
            gap_ok = False
        elif macd < MACD_MIN_VALUE:
            gap_ok = False
        elif math.isnan(prev_macd) or prev_macd < MACD_MIN_VALUE:
            gap_ok = False   # need 2 consecutive bars above floor
        else:
            gap_pct = ((macd - sig) / abs(sig)) * 100 if sig else 0
            gap_ok  = gap_pct > GAP_PCT

        sig_rising     = (not math.isnan(prev_sig)) and sig > prev_sig
        sig_established = abs(sig) >= SIGNAL_MIN_VALUE
        increasing     = is_monotonic(macd_buf)
        rsi_in_zone    = RSI_BUY_MIN <= rsi <= RSI_MAX
        rsi_ok         = rsi_stable(rsi_buf, rsi, prev_rsi if not math.isnan(prev_rsi) else rsi)
        cooldown_ok    = (ts - last_trade_ts).total_seconds() / 60 >= COOLDOWN_MIN

        if gap_ok and sig_rising and sig_established and increasing and rsi_in_zone and rsi_ok and cooldown_ok:
            shares   = int(BUDGET / px)
            entry_px = px
            pos      = shares
            last_trade_ts = ts
            pos_buf  = [macd, nan, nan, nan, nan]
            trades.append({"type": "BUY", "reason": f"gap={gap_pct:.1f}% RSI={rsi:.1f}",
                           "ts": ts, "px": px, "shares": shares, "pl": None})

    # Force-close any open position at last bar
    if pos > 0:
        last = rows[-1]
        pl = (last["close"] - entry_px) * shares
        trades.append({"type": "SELL", "reason": "EOD close",
                       "ts": last["ts"], "px": last["close"], "shares": shares, "pl": pl})

    return trades


def print_day(date_str, rows, variant_trades_map):
    """Print per-day trade detail for all variants."""
    first_px = rows[0]["close"]
    last_px  = rows[-1]["close"]
    day_move = last_px - first_px
    print(f"\n{'='*65}")
    print(f"  {date_str}   {len(rows)} bars   open ${first_px:.2f}  close ${last_px:.2f}  day move {day_move:+.2f}")
    print(f"{'='*65}")
    for v in VARIANTS:
        trades = variant_trades_map[v["name"]]
        day_pl = sum(t["pl"] for t in trades if t["type"] == "SELL")
        print(f"\n  {v['name']}   day P&L: ${day_pl:+.2f}")
        for t in trades:
            ts_str = t["ts"].strftime("%H:%M")
            if t["type"] == "BUY":
                print(f"    {ts_str}  BUY   {t['shares']}sh @ ${t['px']:.4f}  [{t['reason']}]")
            else:
                print(f"    {ts_str}  SELL  {t['shares']}sh @ ${t['px']:.4f}  [{t['reason']}]  ${t['pl']:+.2f}")


def main():
    csv_files = sorted(
        f for f in os.listdir(LOG_DIR)
        if f.startswith("NVDA_") and f.endswith(".csv")
    )

    if not csv_files:
        print(f"No NVDA CSV files found in {LOG_DIR}")
        return

    # cumulative P&L per variant
    cumulative = {v["name"]: 0.0 for v in VARIANTS}
    # wins/losses per variant
    win_loss = {v["name"]: {"wins": 0, "losses": 0, "scratch": 0} for v in VARIANTS}

    print(f"\nNVDA multi-day replay across {len(csv_files)} trading days")

    for fname in csv_files:
        date_str = fname.replace("NVDA_", "").replace(".csv", "")
        rows = load_csv(os.path.join(LOG_DIR, fname))
        if not rows:
            continue

        variant_trades_map = {}
        for v in VARIANTS:
            trades = replay(rows, v["RSI_MAX"], v["GAP_PCT"])
            variant_trades_map[v["name"]] = trades
            day_pl = sum(t["pl"] for t in trades if t["type"] == "SELL")
            cumulative[v["name"]] += day_pl
            if day_pl > 0.50:
                win_loss[v["name"]]["wins"] += 1
            elif day_pl < -0.50:
                win_loss[v["name"]]["losses"] += 1
            else:
                win_loss[v["name"]]["scratch"] += 1

        print_day(date_str, rows, variant_trades_map)

    # Final summary table
    print(f"\n{'='*65}")
    print(f"  CUMULATIVE SUMMARY  ({len(csv_files)} days)")
    print(f"{'='*65}")
    print(f"  {'Variant':<35}  {'Total P&L':>10}  {'W/L/S':>8}")
    print(f"  {'-'*35}  {'-'*10}  {'-'*8}")
    for v in VARIANTS:
        name = v["name"]
        pl   = cumulative[name]
        wl   = win_loss[name]
        wls  = f"{wl['wins']}/{wl['losses']}/{wl['scratch']}"
        print(f"  {name:<35}  ${pl:>+9.2f}  {wls:>8}")
    print()


if __name__ == "__main__":
    main()

