import hashlib
import pickle
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests


class EconomicDataFetcher:
    """
    Unified client for FRED economic data, VoteHub approval polls, and GDELT media sentiment.

    GDELT responses are disk-cached to respect their strict rate limits.
    All API endpoints and hyperparameters are configurable via the constructor.

    Usage:
        fetcher = EconomicDataFetcher(fred_api_key="YOUR_KEY")
        df      = fetcher.get_approval_predictors(start_date="2017-01-01")
        gdelt   = fetcher.fetch_gdelt_sentiment()
        polls   = fetcher.fetch_trump_sentiment()
    """

    FRED_BASE_URL        = "https://api.stlouisfed.org/fred"
    VOTEHUB_URL          = "https://api.votehub.com/polls"
    GDELT_URL            = "https://api.gdeltproject.org/api/v2/doc/doc"
    GDELT_DEFAULT_QUERY  = '"Trump" sourcecountry:US sourcelang:English'

    SERIES_IDS = {
        # --- Original Series ---
        "gdp":                    "GDP",           # Nominal GDP
        "gdp_real":               "GDPC1",         # Real GDP (Chained 2012 Dollars)
        "cpi":                    "CPIAUCSL",      # Consumer Price Index for All Urban Consumers
        "unemployment":           "UNRATE",        # Unemployment Rate
        "federal_funds_rate":     "FEDFUNDS",      # Effective Federal Funds Rate
        "exports":                "EXPGS",         # Exports of Goods and Services
        "imports":                "IMPGS",         # Imports of Goods and Services
        "trade_balance":          "NETEXP",        # Net Exports of Goods and Services
        "10yr_treasury":          "DGS10",         # Daily 10-Year Treasury Yield
        "3mo_treasury":           "DTB3",          # Daily 3-Month Treasury Yield
        "mortgage_rate_30yr":     "MORTGAGE30US",  # 30-Year Mortgage Rate
        "michigan_sentiment":     "UMCSENT",       # University of Michigan: Consumer Sentiment
        # --- Inflation & Prices ---
        "pce_inflation":          "PCEPI",         # PCE Price Index (Fed's preferred inflation measure)
        "core_cpi":               "CPILFESL",      # Core CPI (excludes food & energy)
        "gas_prices":             "GASREGW",       # Weekly retail gasoline prices
        "ppi":                    "PPIACO",        # Producer Price Index (leading indicator for CPI)
        # --- Labor Market ---
        "labor_participation":    "CIVPART",       # Labor Force Participation Rate
        "u6_unemployment":        "U6RATE",        # U-6 Broader Underemployment Measure
        "avg_hourly_earnings":    "CES0500000003", # Average Hourly Earnings
        "jobless_claims":         "ICSA",          # Weekly Initial Jobless Claims
        "nonfarm_payrolls":       "PAYEMS",        # Total Nonfarm Payroll Employment
        # --- Consumer & Spending ---
        "real_disposable_income": "DSPIC96",       # Real Disposable Personal Income
        "personal_savings_rate":  "PSAVERT",       # Personal Savings Rate
        "retail_sales":           "RSAFS",         # Retail & Food Services Sales
        "consumer_credit":        "TOTALSL",       # Total Consumer Credit Outstanding
        # --- Markets & Financial Conditions ---
        "sp500":                  "SP500",         # S&P 500
        "vix":                    "VIXCLS",        # CBOE Volatility Index
        "credit_spread":          "BAA10Y",        # Baa Corporate Yield minus 10-yr Treasury
        # --- Trade & Manufacturing (Trump-specific) ---
        "trade_deficit_goods":    "BOPGSTB",       # Goods-only trade balance
        "china_imports":          "IMPCH",         # Imports from China
        "manufacturing_output":   "IPMAN",         # Industrial Production: Manufacturing
        # --- Fiscal & Debt ---
        "federal_debt_pct_gdp":   "GFDEGDQ188S",  # Federal Debt as % of GDP
        "budget_deficit":         "MTSDS133FMS",   # Monthly Federal Surplus/Deficit
    }

    def __init__(
        self,
        fred_api_key: str,
        gdelt_query: Optional[str] = None,
        cache_dir: str = ".cache",
        gdelt_delay: float = 10.0,
    ):
        """
        Args:
            fred_api_key: FRED API key (https://fred.stlouisfed.org/docs/api/api_key.html).
            gdelt_query:  GDELT full-text search query. Defaults to GDELT_DEFAULT_QUERY.
            cache_dir:    Directory for GDELT disk cache. Created automatically if missing.
            gdelt_delay:  Seconds to wait between live GDELT API calls to avoid 429s.
        """
        self.fred_api_key = fred_api_key
        self.gdelt_query  = gdelt_query or self.GDELT_DEFAULT_QUERY
        self.cache_dir    = Path(cache_dir)
        self.gdelt_delay  = gdelt_delay
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── FRED internals ────────────────────────────────────────────────────────

    def _fetch_fred(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
        units: Optional[str] = None,
    ) -> pd.DataFrame:
        params = {"series_id": series_id, "api_key": self.fred_api_key, "file_type": "json"}
        if start_date: params["observation_start"] = start_date
        if end_date:   params["observation_end"]   = end_date
        if frequency:  params["frequency"]         = frequency
        if units:      params["units"]             = units
        try:
            r = requests.get(f"{self.FRED_BASE_URL}/series/observations", params=params)
            r.raise_for_status()
            data = r.json()
            if "observations" not in data:
                raise ValueError(f"No data for series {series_id}")
            df = pd.DataFrame(data["observations"])
            df["date"]  = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            return df[["date", "value"]].dropna()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {series_id}: {e}")
            return pd.DataFrame()

    def _fred(
        self,
        key: str,
        label: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        units: Optional[str] = None,
    ) -> pd.DataFrame:
        df = self._fetch_fred(self.SERIES_IDS[key], start_date, end_date, units=units)
        if not df.empty:
            df["indicator"] = label
        return df

    # ── FRED: GDP & Macro ─────────────────────────────────────────────────────

    def get_gdp(self, real=False, start_date=None, end_date=None) -> pd.DataFrame:
        # GDP growth is one of the most cited predictors of presidential approval.
        # Voters reward presidents when the economy is expanding and punish them during
        # contractions. Real GDP (inflation-adjusted) is especially important since it
        # reflects actual output growth rather than price-driven nominal gains.
        key   = "gdp_real" if real else "gdp"
        label = "Real GDP (Chained 2012 Dollars)" if real else "Nominal GDP (Current Dollars)"
        return self._fred(key, label, start_date, end_date)

    def get_cpi(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # CPI is a direct measure of the cost of living. Rising inflation erodes
        # purchasing power and is consistently one of the top financial concerns
        # cited by voters in polls. High inflation has historically been one of
        # the strongest negative predictors of presidential approval.
        return self._fred("cpi", "Consumer Price Index (All Urban Consumers)", start_date, end_date, units)

    def get_unemployment_rate(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The unemployment rate is the single most-studied economic predictor of
        # presidential approval. A rising unemployment rate consistently correlates
        # with falling approval, as it is highly visible and personally felt by voters.
        return self._fred("unemployment", "Unemployment Rate (%)", start_date, end_date)

    def get_federal_funds_rate(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The federal funds rate reflects the Fed's monetary policy stance. Rapid rate
        # hikes increase borrowing costs for consumers and businesses, which can dampen
        # economic sentiment and, indirectly, presidential approval.
        return self._fred("federal_funds_rate", "Effective Federal Funds Rate (%)", start_date, end_date)

    def get_interest_rates(self, rate_type="all", start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        # Treasury yields and the yield curve shape carry signals about economic
        # expectations. An inverted yield curve (3-month > 10-year) is a well-known
        # recession predictor. The spread between these two rates (10yr - 3mo) can
        # anticipate downturns before they show up in GDP or unemployment data.
        rates = {}
        if rate_type in ("all", "10yr"):
            rates["10yr_treasury"] = self._fred("10yr_treasury", "10-Year Treasury Rate (%)", start_date, end_date)
        if rate_type in ("all", "3mo"):
            rates["3mo_treasury"]  = self._fred("3mo_treasury",  "3-Month Treasury Rate (%)", start_date, end_date)
        if rate_type in ("all", "mortgage"):
            rates["mortgage_30yr"] = self._fred("mortgage_rate_30yr", "30-Year Mortgage Rate (%)", start_date, end_date)
        return rates

    def get_international_trade(self, trade_type="all", start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        # Trade data is especially important for Trump's approval given his tariff
        # policies and "America First" trade rhetoric. Voters who feel the trade
        # deficit is improving may reward him, while import-driven price increases
        # from tariffs could suppress approval through higher consumer prices.
        trade = {}
        if trade_type in ("all", "exports"):
            trade["exports"] = self._fred("exports",       "Exports (Billions $)",       start_date, end_date)
        if trade_type in ("all", "imports"):
            trade["imports"] = self._fred("imports",       "Imports (Billions $)",       start_date, end_date)
        if trade_type in ("all", "balance"):
            trade["balance"] = self._fred("trade_balance", "Trade Balance (Billions $)", start_date, end_date)
        return trade

    def get_michigan_sentiment(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Michigan Consumer Sentiment is one of the strongest single predictors of
        # presidential approval in the academic literature. It captures how people
        # *feel* about the economy — which often diverges from objective indicators —
        # and has been shown to mediate the relationship between economic data and
        # approval ratings. High-frequency and forward-looking.
        return self._fred("michigan_sentiment", "Consumer Sentiment Index", start_date, end_date)

    # ── FRED: Inflation & Prices ──────────────────────────────────────────────

    def get_pce_inflation(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # The PCE Price Index is the Federal Reserve's preferred inflation measure and
        # tends to be less volatile than CPI. Fed policy decisions are explicitly tied
        # to PCE targets, so it is a more direct signal of monetary conditions.
        return self._fred("pce_inflation", "PCE Price Index", start_date, end_date, units)

    def get_core_cpi(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Core CPI strips out volatile food and energy prices, revealing the underlying
        # inflation trend. Helps isolate whether inflation is structurally elevated —
        # which has a stickier negative effect on approval than short-term price spikes.
        return self._fred("core_cpi", "Core CPI (Ex Food & Energy)", start_date, end_date, units)

    def get_gas_prices(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Gas prices are one of the most psychologically salient economic indicators
        # for voters — displayed publicly on street corners and felt at every fill-up.
        # Research shows gas prices have an outsized effect on presidential approval
        # relative to their actual macroeconomic weight.
        return self._fred("gas_prices", "Regular Gasoline Price ($/gallon)", start_date, end_date)

    def get_ppi(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # The Producer Price Index is a leading indicator of consumer inflation — when
        # input costs rise for producers, those costs are eventually passed to consumers.
        # Including PPI helps the model anticipate future CPI moves.
        return self._fred("ppi", "Producer Price Index (All Commodities)", start_date, end_date, units)

    # ── FRED: Labor Market ────────────────────────────────────────────────────

    def get_labor_participation(self, start_date=None, end_date=None) -> pd.DataFrame:
        # A falling UNRATE alongside a falling LFPR suggests people are dropping out of
        # the workforce rather than finding jobs — a misleadingly rosy headline number.
        return self._fred("labor_participation", "Labor Force Participation Rate (%)", start_date, end_date)

    def get_u6_unemployment(self, start_date=None, end_date=None) -> pd.DataFrame:
        # U-6 is the broadest official measure of labor underutilization — includes
        # part-time workers who want full-time jobs and discouraged workers who've
        # stopped looking. Better captures economic precarity felt by working-class voters.
        return self._fred("u6_unemployment", "U-6 Underemployment Rate (%)", start_date, end_date)

    def get_avg_hourly_earnings(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Average hourly earnings measure wage growth experienced directly in paychecks.
        # The relationship between this series and CPI (real wages = earnings - inflation)
        # is one of the most actionable derived features in the model.
        return self._fred("avg_hourly_earnings", "Average Hourly Earnings (All Employees, $)", start_date, end_date, units)

    def get_jobless_claims(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Initial jobless claims are released weekly — one of the highest-frequency
        # economic signals available. Useful for capturing near-real-time economic
        # shocks (e.g. tariff-driven layoffs, recession onset).
        return self._fred("jobless_claims", "Initial Jobless Claims (Weekly)", start_date, end_date)

    def get_nonfarm_payrolls(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Monthly nonfarm payrolls are the headline jobs number that dominates media
        # coverage every first Friday of the month and directly shapes the narrative
        # around a president's economic stewardship.
        return self._fred("nonfarm_payrolls", "Total Nonfarm Payrolls (Thousands)", start_date, end_date, units)

    # ── FRED: Consumer & Spending ─────────────────────────────────────────────

    def get_real_disposable_income(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Real disposable income measures what households actually have to spend after
        # taxes and adjusted for inflation. Helps resolve the "why is approval low if
        # unemployment is low?" puzzle when real incomes are falling.
        return self._fred("real_disposable_income", "Real Disposable Personal Income (Billions $, Chained 2017)", start_date, end_date, units)

    def get_personal_savings_rate(self, start_date=None, end_date=None) -> pd.DataFrame:
        # A falling savings rate can indicate households are drawing down savings to
        # maintain spending — a sign of financial strain even if consumption looks healthy.
        return self._fred("personal_savings_rate", "Personal Savings Rate (%)", start_date, end_date)

    def get_retail_sales(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Retail sales capture consumer spending in real time and serve as a direct
        # proxy for economic confidence.
        return self._fred("retail_sales", "Retail & Food Services Sales (Millions $)", start_date, end_date, units)

    def get_consumer_credit(self, start_date=None, end_date=None, units="lin") -> pd.DataFrame:
        # Rising consumer credit signals that households are borrowing more to maintain
        # spending — a warning sign of financial stress if paired with falling incomes.
        return self._fred("consumer_credit", "Total Consumer Credit Outstanding (Billions $)", start_date, end_date, units)

    # ── FRED: Markets & Financial Conditions ─────────────────────────────────

    def get_sp500(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The S&P 500 captures the "wealth effect." Trump in particular has repeatedly
        # cited the stock market as a scorecard of his presidency, making it especially
        # salient for his approval dynamics.
        return self._fred("sp500", "S&P 500 Index", start_date, end_date)

    def get_vix(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The VIX ("fear index") spikes correspond to financial stress events that
        # typically coincide with sharp drops in presidential approval.
        return self._fred("vix", "CBOE Volatility Index (VIX)", start_date, end_date)

    def get_credit_spread(self, start_date=None, end_date=None) -> pd.DataFrame:
        # A widening Baa-10yr spread signals rising default risk and tightening financial
        # conditions, typically preceding recessions by 6–12 months.
        return self._fred("credit_spread", "Baa-10yr Treasury Credit Spread (%)", start_date, end_date)

    # ── FRED: Trade & Manufacturing ───────────────────────────────────────────

    def get_trade_deficit_goods(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The goods-only trade balance is the figure Trump has most frequently cited and
        # campaigned on — isolates physical trade most directly affected by tariffs.
        return self._fred("trade_deficit_goods", "Goods Trade Balance (Millions $)", start_date, end_date)

    def get_china_imports(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Imports from China are the direct target of Trump's tariff policy in both his
        # first and second terms — uniquely relevant to his approval given the centrality
        # of China trade to his economic platform.
        return self._fred("china_imports", "U.S. Imports from China (Millions $)", start_date, end_date)

    def get_manufacturing_output(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Manufacturing output acts as a direct scorecard for Trump's promise to revive
        # American manufacturing, tracking blue-collar industrial employment in the
        # Rust Belt and Midwest.
        return self._fred("manufacturing_output", "Industrial Production: Manufacturing (Index 2017=100)", start_date, end_date)

    # ── FRED: Fiscal & Debt ───────────────────────────────────────────────────

    def get_federal_debt_pct_gdp(self, start_date=None, end_date=None) -> pd.DataFrame:
        # Federal debt as a share of GDP becomes more salient during debt-ceiling
        # standoffs or credit-rating events, shaping media narratives about fiscal
        # responsibility.
        return self._fred("federal_debt_pct_gdp", "Federal Debt as % of GDP", start_date, end_date)

    def get_budget_deficit(self, start_date=None, end_date=None) -> pd.DataFrame:
        # The monthly federal surplus/deficit provides a near-real-time fiscal pulse
        # that quarterly debt-to-GDP data misses.
        return self._fred("budget_deficit", "Federal Budget Surplus/Deficit (Millions $)", start_date, end_date)

    # ── FRED: Aggregate fetchers ──────────────────────────────────────────────

    def get_all_economic_data(self, start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        """Fetch all series and return as a dict of DataFrames keyed by indicator name."""
        kw = dict(start_date=start_date, end_date=end_date)
        return {
            "gdp_nominal":            self.get_gdp(real=False, **kw),
            "gdp_real":               self.get_gdp(real=True,  **kw),
            "cpi":                    self.get_cpi(**kw),
            "unemployment":           self.get_unemployment_rate(**kw),
            "fed_funds_rate":         self.get_federal_funds_rate(**kw),
            **self.get_interest_rates(**kw),
            **self.get_international_trade(**kw),
            "michigan_sentiment":     self.get_michigan_sentiment(**kw),
            "pce_inflation":          self.get_pce_inflation(**kw),
            "core_cpi":               self.get_core_cpi(**kw),
            "gas_prices":             self.get_gas_prices(**kw),
            "ppi":                    self.get_ppi(**kw),
            "labor_participation":    self.get_labor_participation(**kw),
            "u6_unemployment":        self.get_u6_unemployment(**kw),
            "avg_hourly_earnings":    self.get_avg_hourly_earnings(**kw),
            "jobless_claims":         self.get_jobless_claims(**kw),
            "nonfarm_payrolls":       self.get_nonfarm_payrolls(**kw),
            "real_disposable_income": self.get_real_disposable_income(**kw),
            "personal_savings_rate":  self.get_personal_savings_rate(**kw),
            "retail_sales":           self.get_retail_sales(**kw),
            "consumer_credit":        self.get_consumer_credit(**kw),
            "sp500":                  self.get_sp500(**kw),
            "vix":                    self.get_vix(**kw),
            "credit_spread":          self.get_credit_spread(**kw),
            "trade_deficit_goods":    self.get_trade_deficit_goods(**kw),
            "china_imports":          self.get_china_imports(**kw),
            "manufacturing_output":   self.get_manufacturing_output(**kw),
            "federal_debt_pct_gdp":   self.get_federal_debt_pct_gdp(**kw),
            "budget_deficit":         self.get_budget_deficit(**kw),
        }

    def get_approval_predictors(self, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Fetches all series and merges them into a single wide-format DataFrame indexed
        by month, ready for model training.

        All series are resampled to monthly frequency (end-of-month) using the mean
        to harmonize high-frequency series (weekly jobless claims, daily VIX) with
        lower-frequency series (quarterly GDP).

        Derived features added:
          - yield_curve_spread : 10yr Treasury minus 3mo Treasury
          - real_wage_growth   : avg_hourly_earnings YoY% minus CPI YoY%
          - misery_index       : unemployment + CPI YoY%
        """
        raw = self.get_all_economic_data(start_date=start_date, end_date=end_date)
        frames = {
            key: df.set_index("date")["value"].resample("ME").mean()
            for key, df in raw.items()
            if not df.empty
        }
        wide = pd.DataFrame(frames)
        wide.index.name = "date"

        if "10yr_treasury" in wide.columns and "3mo_treasury" in wide.columns:
            wide["yield_curve_spread"] = wide["10yr_treasury"] - wide["3mo_treasury"]

        if "avg_hourly_earnings" in wide.columns and "cpi" in wide.columns:
            wide["real_wage_growth"] = (
                wide["avg_hourly_earnings"].pct_change(12) * 100
                - wide["cpi"].pct_change(12) * 100
            )

        if "unemployment" in wide.columns and "cpi" in wide.columns:
            wide["misery_index"] = wide["unemployment"] + wide["cpi"].pct_change(12) * 100

        return wide

    # ── Approval Polls ────────────────────────────────────────────────────────

    def fetch_trump_sentiment(
        self,
        min_sample_size: int = 500,
        from_date: str = "2025-02-01",
        to_date: str = "2026-03-20",
    ) -> pd.DataFrame:
        """
        Fetch Trump approval polls from VoteHub filtered by date range and sample size.

        Args:
            min_sample_size: Drop polls with fewer respondents than this threshold.
            from_date:       Earliest poll end date to include (YYYY-MM-DD).
            to_date:         Latest poll end date to include (YYYY-MM-DD).
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept":  "application/json",
            "Referer": "https://votehub.com/",
        }
        try:
            r = requests.get(
                self.VOTEHUB_URL,
                headers=headers,
                params={"subject": "Trump", "to_date": to_date},
                timeout=15,
            )
            if r.status_code != 200:
                print(f"VoteHub returned {r.status_code}")
                return pd.DataFrame()

            records = []
            for poll in r.json():
                end_date    = poll.get("end_date")
                sample_size = poll.get("sample_size", 0) or 0
                if not end_date or end_date < from_date or sample_size < min_sample_size:
                    continue
                record = {"date": pd.to_datetime(end_date), "sample_size": sample_size}
                for answer in poll.get("answers", []):
                    if answer.get("choice") == "Approve":
                        record["approval"] = answer.get("pct")
                    elif answer.get("choice") == "Disapprove":
                        record["disapproval"] = answer.get("pct")
                if "approval" in record or "disapproval" in record:
                    records.append(record)

            return pd.DataFrame(records).sort_values("date")

        except Exception as e:
            print(f"Error fetching VoteHub data: {e}")
            return pd.DataFrame()

    # ── GDELT ─────────────────────────────────────────────────────────────────

    def _gdelt_cache_path(self, mode: str, start: str, end: str) -> Path:
        key = hashlib.md5(f"{self.gdelt_query}|{mode}|{start}|{end}".encode()).hexdigest()
        return self.cache_dir / f"gdelt_{key}.pkl"

    def _fetch_gdelt_mode(self, mode: str, start: str, end: str) -> list:
        """Fetch a single GDELT timeline mode, loading from disk cache when available."""
        cache_path = self._gdelt_cache_path(mode, start, end)
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                return pickle.load(f)

        print(f"Fetching GDELT {mode}...")
        r = requests.get(
            self.GDELT_URL,
            params={
                "query":         self.gdelt_query,
                "mode":          mode,
                "startdatetime": start,
                "enddatetime":   end,
                "format":        "json",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json().get("timeline", [{}])[0].get("data", [])

        with open(cache_path, "wb") as f:
            pickle.dump(data, f)

        return data

    def fetch_gdelt_sentiment(
        self,
        start: str = "20250101000000",
        end: str = "20260328000000",
        rolling_window: int = 20,
    ) -> pd.DataFrame:
        """
        Fetch GDELT tone and volume timelines for the configured query.

        Results are disk-cached — repeated calls with the same parameters return
        instantly without hitting the API. A configurable delay (gdelt_delay) is
        inserted between live API calls to avoid 429 rate-limit errors.

        Args:
            start:          Start datetime in GDELT format (YYYYMMDDHHmmss).
            end:            End datetime in GDELT format (YYYYMMDDHHmmss).
            rolling_window: Periods for centred rolling mean applied to both series.

        Returns:
            DataFrame with columns: date, tone, volume.
        """
        try:
            tone_data = self._fetch_gdelt_mode("TimelineTone", start, end)

            # Only sleep if the volume data also requires a live API call
            if not self._gdelt_cache_path("TimelineVolRaw", start, end).exists():
                time.sleep(self.gdelt_delay)

            vol_data = self._fetch_gdelt_mode("TimelineVolRaw", start, end)

            def _to_df(data, col):
                return pd.DataFrame(
                    [{"date": pd.to_datetime(p["date"]), col: float(p["value"])} for p in data]
                )

            df = (
                _to_df(tone_data, "tone")
                .merge(_to_df(vol_data, "volume"), on="date")
                .sort_values("date")
            )
            df["tone"]   = df["tone"].rolling(window=rolling_window, min_periods=1, center=True).mean()
            df["volume"] = df["volume"].rolling(window=rolling_window, min_periods=1, center=True).mean()
            return df

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(
                    "GDELT rate limit hit (429). Try again later or from a different IP.\n"
                    "In shared environments (e.g. Colab), the IP may be rate-limited by other users."
                )
            else:
                print(f"HTTP error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching GDELT data: {e}")
            return pd.DataFrame()

    def clear_gdelt_cache(self) -> None:
        """Remove all cached GDELT responses from disk."""
        removed = list(self.cache_dir.glob("gdelt_*.pkl"))
        for f in removed:
            f.unlink()
        print(f"Removed {len(removed)} cached GDELT file(s) from {self.cache_dir}")
