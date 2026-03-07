FULL_PROJECT_SCHEMA_DESCRIPTION = {

    # ===============================
    # Identity
    # ===============================
    "SYMBOL": "Ticker symbol identifying the company traded on the market.",
    "DATE": "Trading date corresponding to the market session and associated events.",

    # ===============================
    # Core Market Data (Silver)
    # ===============================
    "OPEN_PRICE": "Price at which the security opened during the trading session.",
    "HIGH_PRICE": "Highest traded price of the security during the trading session.",
    "LOW_PRICE": "Lowest traded price of the security during the trading session.",
    "CLOSE_PRICE": "Final traded price of the security at the end of the trading session.",
    "CLOSE_ADJUSTED_PRICE": "Closing price adjusted for corporate actions such as splits and dividends.",
    "VOLUME": "Total number of shares traded during the trading session.",
    "SPLIT_COEFFICIENT": "Factor representing stock split adjustments applied on that date.",

    # ===============================
    # Corporate Events (Silver)
    # ===============================
    "DIVIDENDS": "Dividend amount distributed per share on the given date.",
    "QUARTER": "Fiscal quarter associated with the earnings report.",
    "EPS": "Reported earnings per share announced by the company.",
    "EPS_ESTIMATE": "Analysts' estimated earnings per share for the same reporting period.",
    "RELEASE_TIME": "Timestamp indicating when earnings were publicly released.",

    # ===============================
    # Daily Price Metrics (Gold Layer 1)
    # ===============================
    "PREV_CLOSE_PRICE": "Closing price of the previous trading day (t-1).",
    "DAILY_RETURN_PCT": "Percentage change between previous close (t-1) and current close (t).",
    "GAP_AMOUNT": "Difference between current open price and previous close price.",
    "GAP_DIRECTION": "Indicates whether the opening price gapped UP, DOWN, or showed NO_GAP relative to the previous close.",
    "DAILY_DIRECTION": "Indicates whether the closing price moved UP, DOWN, or remained FLAT compared to the previous close.",
    "INTRADAY_RANGE_AMT": "Difference between the highest and lowest traded prices during the session.",
    "INTRADAY_RANGE_PCT": "Intraday price range expressed as a percentage of the closing price.",
    "20_DAY_AVG_INTRADAY_RANGE_PCT": "Rolling 20-day average of intraday range percentage, representing typical short-term volatility.",

    # ===============================
    # Daily Volume & Liquidity (Gold Layer 2)
    # ===============================
    "PREV_VOLUME": "Volume traded during the previous trading day (t-1).",
    "VOLUME_CHANGE_PCT": "Percentage change in volume relative to the previous trading day.",
    "VOLUME_DIRECTION": "Indicates whether trading volume increased, decreased, or remained FLAT compared to the prior day.",
    "5_DAY_AVG_VOLUME": "Rolling 5-day average trading volume.",
    "20_DAY_AVG_VOLUME": "Rolling 20-day average trading volume, representing the typical participation level.",

    # ===============================
    # Earnings Surprise Metrics (Gold Layer 3)
    # ===============================
    "EPS_SURPRISE_AMT": "Numerical difference between reported earnings per share and the analysts' estimated earnings per share.",
    "EPS_SURPRISE_PCT": "Percentage difference between reported EPS and estimated EPS.",
    "SURPRISE_DIRECTION": "Categorical indicator showing whether the earnings surprise was POSITIVE, NEGATIVE, or NO_SURPRISE.",

    # ===============================
    # Earnings Event Window Context
    # ===============================
    "PRE_EARNINGS_CLOSE": "Closing price of the trading day immediately preceding the earnings announcement (t-1).",
    "PRE_EARNINGS_VOLUME": "Trading volume of the day immediately preceding the earnings announcement (t-1).",
    "POST_EARNINGS_OPEN": "Opening price of the trading day immediately following the earnings announcement (t+1).",
    "POST_EARNINGS_CLOSE": "Closing price of the trading day immediately following the earnings announcement (t+1).",
    "POST_EARNINGS_VOLUME": "Trading volume of the day immediately following the earnings announcement (t+1).",

    # ===============================
    # Earnings Reaction Metrics
    # ===============================
    "EARNING_DIRECTION": "Direction of price movement between pre-earnings close (t-1) and post-earnings close (t+1).",
    "EVENT_WINDOW_RETURN_PCT": "Percentage return between pre-earnings close (t-1) and post-earnings close (t+1).",
    "EARNINGS_DAY_RETURN_PCT": "Percentage return between pre-earnings close (t-1) and earnings-day close (t).",
    "NEXT_DAY_RETURN_PCT": "Percentage return between earnings-day close (t) and next trading day close (t+1).",

    # ===============================
    # Volatility & Normalization
    # ===============================
    "VOLATILITY_SPIKE_FLAG": "Indicates whether intraday volatility exceeded 1.5x its 20-day average baseline.",
    "VOLATILITY_MULTIPLIER": "Ratio of event-day intraday volatility to its 20-day average volatility baseline.",
    "EARNINGS_REACTION_STRENGTH": "Normalized earnings reaction calculated as the event window return divided by the typical 20-day intraday volatility.",

    # ===============================
    # Volume Confirmation
    # ===============================
    "VOLUME_SPIKE_FLAG": "Indicates whether trading volume exceeded 1.5x the typical 20-day average volume.",
    "VOLUME_CONFIRMATION_FLAG": "Classifies whether price movement is CONFIRMED, WEAK, or NEUTRAL based on abnormal volume participation.",

    # ===============================
    # Alignment & Drift Analysis
    # ===============================
    "REACTION_ALIGNMENT_FLAG": "Indicates whether the market reaction direction aligns with the earnings surprise direction.",
    "DRIFT_DIRECTION": "Direction of price movement from earnings-day close (t) to the next trading day close (t+1).",
    "DRIFT_ALIGNMENT_FLAG": "Indicates whether post-earnings price drift continues (CONTINUATION) or reverses (REVERSAL) the initial earnings reaction.",

    # ===============================
    # Risk Metrics (Gold Layer 4)
    # ===============================
    "ROLLING_RETURN_20D": "Total return over the past 20 trading days calculated as the percentage change between the current closing price and the closing price 20 days earlier.",
    "ROLLING_VOLATILITY_20D": "Rolling 20-day standard deviation of daily returns, representing short-term price instability.",
    "MAX_DRAWDOWN_LOOKBACK": "Maximum peak-to-trough percentage decline observed within the past 20 trading days.",
    "RISK_ADJUSTED_RETURN_20D": "Risk efficiency metric calculated as the 20-day return divided by the 20-day rolling volatility.",

    # ===============================
    # Relative Strength (Gold Layer 5)
    # ===============================
    "INDEX_RETURN_20D": "Total return of the benchmark market index (SPY) over the past 20 trading days.",
    "RELATIVE_STRENGTH_20D": "Difference between the stock's 20-day return and the market index's 20-day return, measuring whether the stock outperformed or underperformed the market.",
    "RELATIVE_STRENGTH_DIRECTION": "Categorical interpretation of relative strength indicating whether the stock OUTPERFORMED, UNDERPERFORMED, or matched the market.",
    "RS_TREND": "Change in relative strength compared to the previous trading day, indicating whether relative performance is STRENGTHENING, WEAKENING, or STABLE."
}