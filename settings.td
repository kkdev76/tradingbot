STOCK_LIST=AMD,NFLX,GOOGL,AMZN,GS,AVGO,CAT,NVDA,META,MU

# Default USD budget per symbol if not overridden below
DEFAULT_DOLLAR=5000

# Per-symbol USD budgets — all new symbols start at $5000
DOLLAR_BUDGETS=AMD:5000,NFLX:5000,GOOGL:5000,AMZN:5000,GS:5000,AVGO:5000,CAT:5000,NVDA:5000,META:5000,MU:5000

# MACD buy conditions — two-regime momentum gate:
#   Crossover regime (0 < MACD < MACD_MIN_VALUE): use absolute gap  (macd - signal) > MACD_ABS_GAP_MIN
#   Trending regime  (MACD >= MACD_MIN_VALUE):     use percentage gap (macd - signal) / signal > MACD_GAP_PERCENT
# Global defaults (used if no per-symbol override is set)
MACD_MIN_VALUE=0.05
MACD_ABS_GAP_MIN=0.03
MACD_GAP_PERCENT=40
# Per-symbol overrides — formula: macd_min = price * 0.0005, abs_gap = macd_min * 0.6
# Prices verified via Alpaca live quotes 2026-05-20:
# AMD~$440, NFLX~$88, GOOGL~$388, AMZN~$264, GS~$983, AVGO~$417, CAT~$869, NVDA~$221, META~$603, MU~$726
MACD_MIN_VALUE_AMD=0.22
MACD_MIN_VALUE_NFLX=0.05
MACD_MIN_VALUE_GOOGL=0.19
MACD_MIN_VALUE_AMZN=0.13
MACD_MIN_VALUE_GS=0.49
MACD_MIN_VALUE_AVGO=0.21
MACD_MIN_VALUE_CAT=0.43
MACD_MIN_VALUE_NVDA=0.11
MACD_MIN_VALUE_META=0.30
MACD_MIN_VALUE_MU=0.36
MACD_ABS_GAP_MIN_AMD=0.13
MACD_ABS_GAP_MIN_NFLX=0.03
MACD_ABS_GAP_MIN_GOOGL=0.11
MACD_ABS_GAP_MIN_AMZN=0.08
MACD_ABS_GAP_MIN_GS=0.29
MACD_ABS_GAP_MIN_AVGO=0.13
MACD_ABS_GAP_MIN_CAT=0.26
MACD_ABS_GAP_MIN_NVDA=0.07
MACD_ABS_GAP_MIN_META=0.18
MACD_ABS_GAP_MIN_MU=0.22

# MACD sell condition: how close two consecutive MACD values need to be to count as "flattening"
MACD_FLATTEN_THRESHOLD=0.01

# RSI buy conditions
RSI_PERIOD=9
RSI_BUY_MIN=50
RSI_BUY_MAX=65

# Cooldown in minutes between trades on same symbol
MIN_TRADE_COOLDOWN_MINUTES=10

# Signal line floor: abs(Signal) must exceed this before a buy is allowed.
# When Signal is near zero the MACD has no directional history — entering is chop, not trend.
# A large negative Signal (recovery from sell-off) still passes because abs() is used,
# so genuine reversals are not blocked.
# Formula for a new symbol: max(0.08, min(price * 0.00034, 0.10))
# Reference: AAPL $294 → 0.10, NVDA $217 → 0.08 (floored), META $600 → 0.10 (capped)
SIGNAL_MIN_VALUE=0.08
# formula: sig_min = min(max(price * 0.00034, 0.08), 0.15)
SIGNAL_MIN_VALUE_AMD=0.10
SIGNAL_MIN_VALUE_NFLX=0.08
SIGNAL_MIN_VALUE_GOOGL=0.10
SIGNAL_MIN_VALUE_AMZN=0.09
SIGNAL_MIN_VALUE_GS=0.10
SIGNAL_MIN_VALUE_AVGO=0.10
SIGNAL_MIN_VALUE_CAT=0.10
SIGNAL_MIN_VALUE_NVDA=0.08
SIGNAL_MIN_VALUE_META=0.10
SIGNAL_MIN_VALUE_MU=0.10

NEGATIVE_PL_THRESHOLD=-100.0
STOP_LOSS_PERCENT=0.35
CRON_SHUTDOWN=12:55
MACD_NEUTRAL_DEFAULT=0.20

# ===== Historical data and MACD warm-up configuration =====
# Match historical feed to streaming feed (sip or iex)
HISTORICAL_FEED=sip
# Use raw adjustments for intraday indicator consistency (raw, split, dividend, all)
HISTORICAL_ADJUSTMENT=raw
# Number of most recent 1-min bars to bootstrap MACD.
# All bars (pre-market, regular, after-hours) are included — EMA is a continuum.
# EMA-26 seed influence decays as (1-k)^N where k=2/27≈0.074:
#   N=65  → ~0.8% seed remaining
#   N=100 → ~0.06% seed remaining (effectively zero)
# 200 bars = ~3.3 hrs, fully within today's pre-market at a typical 9AM ET startup.
# 50-bar safety margin above the ~150 needed for complete convergence.
BOOTSTRAP_BAR_COUNT=200
# Maximum days to look back when fetching historical bars in one request
BOOTSTRAP_MAX_DAYS_BACK=2
# Minimum bars required before enabling trading decisions (26+9=35 with SMA-seeded EMA)
MACD_WARMUP_BARS=35

# Take-profit: sell when unrealized P&L % reaches this threshold (per-symbol)
PROFIT_TAKE_AMD=0.5
PROFIT_TAKE_NFLX=0.5
PROFIT_TAKE_GOOGL=0.5
PROFIT_TAKE_AMZN=0.5
PROFIT_TAKE_GS=0.5
PROFIT_TAKE_AVGO=0.5
PROFIT_TAKE_CAT=0.5
PROFIT_TAKE_NVDA=0.5
PROFIT_TAKE_META=0.5
PROFIT_TAKE_MU=0.5
PROFIT_TAKE_DEFAULT=0.5
