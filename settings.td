STOCK_LIST=NVDA,AAPL,META,MSFT,JPM

# Default USD budget per symbol if not overridden below
DEFAULT_DOLLAR=5000

# Per‑symbol USD budgets (SYMBOL:budget_in_USD comma‑separated)
# Any symbol listed here will use its own budget; others fall back to DEFAULT_DOLLAR
DOLLAR_BUDGETS=NVDA:7000,AAPL:7000,META:7000,MSFT:5000,JPM:5000   # per‑symbol overrides

# MACD buy conditions
MACD_GAP_PERCENT=40

# MACD sell condition: how close two consecutive MACD values need to be to count as "flattening"
MACD_FLATTEN_THRESHOLD=0.01

# RSI buy conditions
RSI_PERIOD=9
RSI_BUY_MIN=50
RSI_BUY_MAX=65

# Cooldown in minutes between trades on same symbol
MIN_TRADE_COOLDOWN_MINUTES=10

NEGATIVE_PL_THRESHOLD=-100.0
STOP_LOSS_PERCENT=0.15
CRON_SHUTDOWN=12:55
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
PROFIT_TAKE_NVDA=0.4
PROFIT_TAKE_AAPL=0.4
PROFIT_TAKE_META=0.4
PROFIT_TAKE_MSFT=0.4
PROFIT_TAKE_JPM=0.4
PROFIT_TAKE_DEFAULT=0.35
