"""
Sweep stop-loss values (0.15%, 0.25%, 0.35%, 0.50%) against actual Alpaca P&L.
All other settings kept at current baseline (RSI<=65, gap>=40%).
Read-only -- does not touch any production files.
"""

import csv, math, os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import OrderStatus, QueryOrderStatus

load_dotenv("keys.env")
PT      = ZoneInfo("America/Los_Angeles")
LOG_DIR = "order_dashboard/indicator_logs"

client = TradingClient(os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY"), paper=True)

# Per-symbol settings (baseline — unchanged from settings.td)
SYM_CFG = {
    "NVDA": {"budget": 7000, "macd_min": 0.15, "sig_min": 0.10, "profit_pct": 0.4},
    "AAPL": {"budget": 7000, "macd_min": 0.08, "sig_min": 0.10, "profit_pct": 0.4},
    "META": {"budget": 7000, "macd_min": 0.20, "sig_min": 0.10, "profit_pct": 0.4},
    "MSFT": {"budget": 5000, "macd_min": 0.10, "sig_min": 0.10, "profit_pct": 0.4},
    "JPM":  {"budget": 5000, "macd_min": 0.07, "sig_min": 0.10, "profit_pct": 0.4},
}

# Baseline entry parameters (current settings)
RSI_MIN       = 50.0
RSI_MAX       = 65.0
GAP_PCT       = 40.0
MACD_FLATTEN  = 0.01
COOLDOWN_MIN  = 10
MACD_BUF_SIZE = 5
RSI_BUF_SIZE  = 5

STOP_LOSS_VARIANTS = [0.15, 0.25, 0.35, 0.50]


def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "close" not in (reader.fieldnames or []):
            return []
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
    if len(buf) < MACD_BUF_SIZE:
        return False
    return buf[4] > buf[3] > buf[2] > buf[1] > buf[0]


def rsi_stable(buf, cur, prev):
    if len(buf) < RSI_BUF_SIZE:
        return False
    return all(v >= RSI_MIN for v in buf) and cur >= prev


def macd_exit(pb):
    if any(math.isnan(v) for v in pb):
        return False
    b0, b1, b2, b3, b4 = pb
    if b0 < b1 < b2 < b3 < b4:
        return False
    return (b3 - b2) < MACD_FLATTEN and (b4 - b3) < MACD_FLATTEN


def replay_sym(rows, cfg, stop_loss_pct):
    nan = float("nan")
    macd_buf, rsi_buf = [], []
    pb = [nan] * 5
    pos, shares, entry_px = 0, 0, 0.0
    last_ts = datetime.min.replace(tzinfo=PT)
    total_pl = 0.0

    for i, bar in enumerate(rows):
        macd, sig, rsi, px, ts = bar["macd"], bar["sig"], bar["rsi"], bar["close"], bar["ts"]
        macd_buf.append(macd); macd_buf = macd_buf[-MACD_BUF_SIZE:]
        rsi_buf.append(rsi);   rsi_buf  = rsi_buf[-RSI_BUF_SIZE:]
        prev_macd = rows[i-1]["macd"] if i > 0 else nan
        prev_sig  = rows[i-1]["sig"]  if i > 0 else nan
        prev_rsi  = rows[i-1]["rsi"]  if i > 0 else nan

        if pos > 0:
            for slot in range(5):
                if math.isnan(pb[slot]):
                    pb[slot] = macd; break
            else:
                pb = pb[1:] + [macd]

            pct = (px - entry_px) / entry_px * 100

            if pct < -stop_loss_pct:
                total_pl += (px - entry_px) * shares
                pos = 0; last_ts = ts; pb = [nan] * 5
                continue

            if (px - entry_px) * shares >= (cfg["profit_pct"] / 100) * cfg["budget"]:
                total_pl += (px - entry_px) * shares
                pos = 0; last_ts = ts; pb = [nan] * 5
                continue

            if macd_exit(pb):
                total_pl += (px - entry_px) * shares
                pos = 0; last_ts = ts; pb = [nan] * 5
            continue

        # buy evaluation
        if macd <= 0 or macd < cfg["macd_min"]:
            gap_ok = False
        elif math.isnan(prev_macd) or prev_macd < cfg["macd_min"]:
            gap_ok = False
        else:
            gap_ok = ((macd - sig) / abs(sig) * 100 if sig else 0) > GAP_PCT

        if (gap_ok
                and not math.isnan(prev_sig) and sig > prev_sig
                and abs(sig) >= cfg["sig_min"]
                and is_monotonic(macd_buf)
                and RSI_MIN <= rsi <= RSI_MAX
                and rsi_stable(rsi_buf, rsi, prev_rsi if not math.isnan(prev_rsi) else rsi)
                and (ts - last_ts).total_seconds() / 60 >= COOLDOWN_MIN):
            shares   = int(cfg["budget"] / px)
            entry_px = px; pos = shares; last_ts = ts
            pb = [macd, nan, nan, nan, nan]

    if pos > 0:
        total_pl += (rows[-1]["close"] - entry_px) * shares

    return total_pl


def actual_pnl(date_str):
    local_start = datetime(*[int(x) for x in date_str.split("-")], 0, 0, 0, tzinfo=PT)
    local_end   = datetime(*[int(x) for x in date_str.split("-")], 23, 59, 59, tzinfo=PT)
    req = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=local_start.astimezone(timezone.utc),
        until=local_end.astimezone(timezone.utc),
        limit=500,
    )
    orders = [o for o in client.get_orders(req) if o.status == OrderStatus.FILLED]
    buys, sells = {}, {}
    for o in orders:
        sym = o.symbol
        qty = float(o.filled_qty); px = float(o.filled_avg_price)
        if o.side.value == "buy":
            buys[sym] = buys.get(sym, 0.0) + qty * px
        else:
            sells[sym] = sells.get(sym, 0.0) + qty * px
    return sum(sells[s] - buys.get(s, 0.0) for s in sells)


def available_dates():
    dates = set()
    for f in os.listdir(LOG_DIR):
        if f.endswith(".csv"):
            dates.add(f.rsplit("_", 1)[1].replace(".csv", ""))
    valid = []
    for d in sorted(dates):
        if all(
            os.path.exists(os.path.join(LOG_DIR, f"{sym}_{d}.csv")) and
            load_csv(os.path.join(LOG_DIR, f"{sym}_{d}.csv"))
            for sym in SYM_CFG
        ):
            valid.append(d)
    return valid


def actual_pnl_by_sym(date_str):
    local_start = datetime(*[int(x) for x in date_str.split("-")], 0, 0, 0, tzinfo=PT)
    local_end   = datetime(*[int(x) for x in date_str.split("-")], 23, 59, 59, tzinfo=PT)
    req = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=local_start.astimezone(timezone.utc),
        until=local_end.astimezone(timezone.utc),
        limit=500,
    )
    orders = [o for o in client.get_orders(req) if o.status == OrderStatus.FILLED]
    buys, sells = {}, {}
    for o in orders:
        sym = o.symbol
        qty = float(o.filled_qty); px = float(o.filled_avg_price)
        if o.side.value == "buy":
            buys[sym] = buys.get(sym, 0.0) + qty * px
        else:
            sells[sym] = sells.get(sym, 0.0) + qty * px
    result = {}
    for sym in set(list(buys.keys()) + list(sells.keys())):
        result[sym] = sells.get(sym, 0.0) - buys.get(sym, 0.0)
    return result


def main():
    dates = available_dates()

    sl_headers = "  ".join(f"SL={sl:.2f}%" for sl in STOP_LOSS_VARIANTS)
    sep_cols   = "  ".join("-" * 9 for _ in STOP_LOSS_VARIANTS)

    totals_actual = 0.0
    totals_sl = [0.0] * len(STOP_LOSS_VARIANTS)

    for date_str in dates:
        actual_by_sym = actual_pnl_by_sym(date_str)
        ap = sum(actual_by_sym.values())
        totals_actual += ap

        # Per-symbol replay for each stop-loss
        sym_sl_pl = {}   # sym -> [pl per sl variant]
        for sym, cfg in SYM_CFG.items():
            rows = load_csv(os.path.join(LOG_DIR, f"{sym}_{date_str}.csv"))
            sym_sl_pl[sym] = [replay_sym(rows, cfg, sl) for sl in STOP_LOSS_VARIANTS]

        sl_day_totals = [sum(sym_sl_pl[sym][j] for sym in SYM_CFG) for j in range(len(STOP_LOSS_VARIANTS))]
        for j in range(len(STOP_LOSS_VARIANTS)):
            totals_sl[j] += sl_day_totals[j]

        # Print day header
        print(f"\n{'='*75}")
        print(f"  {date_str}   Actual: ${ap:+.2f}")
        print(f"  {'Sym':<6}  {'Actual':>8}  {sl_headers}")
        print(f"  {'-'*6}  {'-'*8}  {sep_cols}")

        for sym in SYM_CFG:
            a_sym = actual_by_sym.get(sym, 0.0)
            sl_cols = "  ".join(f"${v:>+7.2f}" for v in sym_sl_pl[sym])
            # only print rows where something happened
            if a_sym != 0.0 or any(v != 0.0 for v in sym_sl_pl[sym]):
                print(f"  {sym:<6}  ${a_sym:>+6.2f}  {sl_cols}")

        sl_cols = "  ".join(f"${v:>+7.2f}" for v in sl_day_totals)
        print(f"  {'TOTAL':<6}  ${ap:>+6.2f}  {sl_cols}")

    # Summary
    print(f"\n{'='*75}")
    print(f"  CUMULATIVE ({len(dates)} days)")
    print(f"  {'Date':<12}  {'Actual':>8}  {sl_headers}")
    print(f"  {'-'*12}  {'-'*8}  {sep_cols}")

    # reprint totals line
    sl_totals = "  ".join(f"${v:>+7.2f}" for v in totals_sl)
    print(f"  {'ALL DAYS':<12}  ${totals_actual:>+6.2f}  {sl_totals}")
    print()


if __name__ == "__main__":
    main()
