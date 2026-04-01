STOCK_LIST=AMD,TSLA,MSFT,INTC

# Default USD budget per symbol if not overridden below
DEFAULT_DOLLAR=10000

# Per‑symbol USD budgets (SYMBOL:budget_in_USD comma‑separated)
# Any symbol listed here will use its own budget; others fall back to DEFAULT_DOLLAR
DOLLAR_BUDGETS=AMD:10000,MSFT:10000,TSLA:5000,INTC:5000,ARM:5000,MU:5000,AVGO:5000,   # per‑symbol overrides

# MACD entry threshold (MACD − Signal)
PERCENT_THRESHOLD=0.25

# Cooldown in minutes between trades on same symbol
MIN_TRADE_COOLDOWN_MINUTES=10

NEGATIVE_PL_THRESHOLD=-50.0
CRON_SHUTDOWN=12:55
MACD_NEUTRAL_AMD=0.2
MACD_NEUTRAL_TSLA=0.25
MACD_NEUTRAL_MSFT=0.25
MACD_NEUTRAL_DEFAULT=0.20

# ===== Historical data and MACD warm-up configuration =====
# Match historical feed to streaming feed (sip or iex)
HISTORICAL_FEED=sip
# Use raw adjustments for intraday indicator consistency (raw, split, dividend, all)
HISTORICAL_ADJUSTMENT=raw
# Number of most recent 1-min bars to bootstrap MACD
BOOTSTRAP_BAR_COUNT=120
# Maximum days to look back when fetching historical bars in one request
BOOTSTRAP_MAX_DAYS_BACK=5
# Minimum bars required before enabling trading decisions (warm-up)
MACD_WARMUP_BARS=78

# Take-profit: sell when unrealized P&L % reaches this threshold (per-symbol)
PROFIT_TAKE_AMD=0.4
PROFIT_TAKE_TSLA=0.8
PROFIT_TAKE_MSFT=0.4
PROFIT_TAKE_INTC=0.4
PROFIT_TAKE_ARM=0.5
PROFIT_TAKE_MU=0.4
PROFIT_TAKE_AVGO=0.3
PROFIT_TAKE_DEFAULT=0.35
