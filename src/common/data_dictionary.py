DATA_DICTIONARY = {

    # =========================
    # CORE IDENTIFIERS
    # =========================
    "SYMBOL": "Stock ticker symbol. Example: AAPL, MSFT, TSLA.",

    "DATE": "Trading date. This is the calendar day when the market data or earnings event occurred.",

    "QUARTER": "Financial quarter of the earnings report. Example: Q1, Q2, Q3, Q4.",

    "RELEASE_TIME": "Time of earnings release. Example: before market open or after market close.",

    # =========================
    # PRICE DATA
    # =========================
    "OPEN_PRICE": "Price of the stock at the start of the trading day.",

    "HIGH_PRICE": "Highest price the stock reached during the day.",

    "LOW_PRICE": "Lowest price the stock reached during the day.",

    "CLOSE_PRICE": "Final traded price at the end of the trading day.",

    "CLOSE_ADJUSTED_PRICE": (
        "Closing price adjusted for stock splits and dividends. "
        "Used to calculate true historical returns."
    ),

    "SPLIT_COEFFICIENT": (
        "Indicates if a stock split occurred. "
        "Example: 2.0 means 2-for-1 split."
    ),

    # =========================
    # VOLUME DATA
    # =========================
    "VOLUME": "Total number of shares traded during the day.",

    "PREV_VOLUME": "Previous day's traded volume.",

    "VOLUME_CHANGE_PCT": (
        "Percentage change in trading volume compared to previous day."
    ),

    "VOLUME_DIRECTION": (
        "Shows if volume increased (UP), decreased (DOWN), or stayed same (FLAT) compared to previous day."
    ),

    "5_DAY_AVG_VOLUME": "Average trading volume over the past 5 trading days.",

    "20_DAY_AVG_VOLUME": "Average trading volume over the past 20 trading days.",

    # =========================
    # DAILY PRICE METRICS
    # =========================
    "PREV_CLOSE_PRICE": "Closing price from the previous trading day.",

    "DAILY_RETURN_PCT": (
        "Percentage price change from previous close to today's close."
    ),

    "GAP_AMOUNT": (
        "Difference between today's opening price and yesterday's closing price."
    ),

    "GAP_DIRECTION": (
        "Indicates whether the stock opened higher (UP), lower (DOWN), or equal (NO_GAP) compared to yesterday's close."
    ),

    "DAILY_DIRECTION": (
        "Indicates whether the stock closed higher (UP), lower (DOWN), or equal (FLAT) compared to yesterday's close."
    ),

    "INTRADAY_RANGE_AMT": (
        "Difference between highest and lowest price during the day."
    ),

    "INTRADAY_RANGE_PCT": (
        "Intraday range expressed as percentage of closing price."
    ),

    "20_DAY_AVG_INTRADAY_RANGE_PCT": (
        "Average daily volatility over the past 20 trading days."
    ),

    "VOLATILITY_SPIKE_FLAG": (
        "Marks whether today's volatility is unusually high compared to normal recent volatility."
    ),

    # =========================
    # EARNINGS DATA
    # =========================
    "EPS": "Actual earnings per share reported by the company.",

    "EPS_ESTIMATE": "Analysts' expected earnings per share.",

    "EPS_SURPRISE_AMT": (
        "Difference between actual earnings and expected earnings."
    ),

    "EPS_SURPRISE_PCT": (
        "Percentage difference between actual earnings and expected earnings."
    ),

    # =========================
    # EARNINGS REACTION METRICS
    # =========================
    "PRE_EARNINGS_CLOSE": (
        "Closing price before the earnings announcement."
    ),

    "POST_EARNINGS_CLOSE": (
        "Closing price after the earnings announcement."
    ),

    "POST_EARNINGS_OPEN": (
        "Opening price on the first trading day after earnings release."
    ),

    "PRE_EARNINGS_VOLUME": (
        "Trading volume before earnings release."
    ),

    "POST_EARNINGS_VOLUME": (
        "Trading volume after earnings release."
    ),

    "EARNINGS_RETURN_PCT": (
        "Percentage price change caused by earnings release."
    ),

    "EARNING_DIRECTION": (
        "Indicates whether stock moved UP, DOWN, or FLAT after earnings."
    ),

    "EARNINGS_REACTION_STRENGTH": (
        "Measures how large the earnings price move was compared to normal daily volatility."
    ),

    # =========================
    # DIVIDENDS
    # =========================
    "DIVIDENDS": (
        "Cash payment distributed to shareholders per share."
    )
}
