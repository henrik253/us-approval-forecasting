import logging
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class FREDFetcher:
    """
    Fetches economic time-series data from the FRED API (St. Louis Fed).

    All individual series methods return a list of ``{"date": str, "value": float,
    "indicator": str}`` records, or an empty list when the request fails.
    ``fetch_panel`` returns a flat list across all series with an added ``series``
    key, ready for JSON serialisation and S3 storage.
    """

    BASE_URL = "https://api.stlouisfed.org/fred"

    SERIES_IDS = {
        # --- GDP & Macro ---
        "gdp":                    "GDP",
        "gdp_real":               "GDPC1",
        "cpi":                    "CPIAUCSL",
        "unemployment":           "UNRATE",
        "federal_funds_rate":     "FEDFUNDS",
        "exports":                "EXPGS",
        "imports":                "IMPGS",
        "trade_balance":          "NETEXP",
        "10yr_treasury":          "DGS10",
        "3mo_treasury":           "DTB3",
        "mortgage_rate_30yr":     "MORTGAGE30US",
        "michigan_sentiment":     "UMCSENT",
        # --- Inflation & Prices ---
        "pce_inflation":          "PCEPI",
        "core_cpi":               "CPILFESL",
        "gas_prices":             "GASREGW",
        "ppi":                    "PPIACO",
        # --- Labor Market ---
        "labor_participation":    "CIVPART",
        "u6_unemployment":        "U6RATE",
        "avg_hourly_earnings":    "CES0500000003",
        "jobless_claims":         "ICSA",
        "nonfarm_payrolls":       "PAYEMS",
        # --- Consumer & Spending ---
        "real_disposable_income": "DSPIC96",
        "personal_savings_rate":  "PSAVERT",
        "retail_sales":           "RSAFS",
        "consumer_credit":        "TOTALSL",
        # --- Markets & Financial Conditions ---
        "sp500":                  "SP500",
        "vix":                    "VIXCLS",
        "credit_spread":          "BAA10Y",
        # --- Trade & Manufacturing ---
        "trade_deficit_goods":    "BOPGSTB",
        "china_imports":          "IMPCH",
        "manufacturing_output":   "IPMAN",
        # --- Fiscal & Debt ---
        "federal_debt_pct_gdp":   "GFDEGDQ188S",
        "budget_deficit":         "MTSDS133FMS",
    }

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError(
                "FRED API key is required. Pass it via api_key= or set FRED_API_KEY."
            )
        self.api_key = api_key

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _fetch_series(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        units: Optional[str] = None,
    ) -> List[dict]:
        params: dict = {
            "series_id": series_id,
            "api_key":   self.api_key,
            "file_type": "json",
        }
        if start_date: params["observation_start"] = start_date
        if end_date:   params["observation_end"]   = end_date
        if units:      params["units"]             = units
        try:
            r = requests.get(f"{self.BASE_URL}/series/observations", params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if "observations" not in data:
                logger.warning("FRED: no observations for series %s", series_id)
                return []
            records = []
            for obs in data["observations"]:
                try:
                    value = float(obs["value"])
                except (ValueError, TypeError):
                    continue
                records.append({"date": obs["date"], "value": value})
            return records
        except requests.exceptions.HTTPError as e:
            logger.error("FRED HTTP error for %s: %s", series_id, e)
        except requests.exceptions.RequestException as e:
            logger.error("FRED request failed for %s: %s", series_id, e)
        except Exception as e:
            logger.error("FRED unexpected error for %s: %s", series_id, e)
        return []

    def _fred(
        self,
        key: str,
        label: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        units: Optional[str] = None,
    ) -> List[dict]:
        records = self._fetch_series(self.SERIES_IDS[key], start_date, end_date, units=units)
        for r in records:
            r["indicator"] = label
        return records

    # ── GDP & Macro ───────────────────────────────────────────────────────────

    def get_gdp(self, real: bool = False, start_date=None, end_date=None) -> List[dict]:
        key   = "gdp_real" if real else "gdp"
        label = "Real GDP (Chained 2012 Dollars)" if real else "Nominal GDP (Current Dollars)"
        return self._fred(key, label, start_date, end_date)

    def get_cpi(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("cpi", "Consumer Price Index (All Urban Consumers)", start_date, end_date, units)

    def get_unemployment_rate(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("unemployment", "Unemployment Rate (%)", start_date, end_date)

    def get_federal_funds_rate(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("federal_funds_rate", "Effective Federal Funds Rate (%)", start_date, end_date)

    def get_interest_rates(self, rate_type: Optional[str] = "all", start_date=None, end_date=None) -> Dict[str, List[dict]]:
        rates = {}
        if rate_type in ("all", "10yr"):
            rates["10yr_treasury"] = self._fred("10yr_treasury", "10-Year Treasury Rate (%)", start_date, end_date)
        if rate_type in ("all", "3mo"):
            rates["3mo_treasury"]  = self._fred("3mo_treasury",  "3-Month Treasury Rate (%)", start_date, end_date)
        if rate_type in ("all", "mortgage"):
            rates["mortgage_30yr"] = self._fred("mortgage_rate_30yr", "30-Year Mortgage Rate (%)", start_date, end_date)
        return rates

    def get_international_trade(self, trade_type: Optional[str] = "all", start_date=None, end_date=None) -> Dict[str, List[dict]]:
        trade = {}
        if trade_type in ("all", "exports"):
            trade["exports"] = self._fred("exports",       "Exports (Billions $)",       start_date, end_date)
        if trade_type in ("all", "imports"):
            trade["imports"] = self._fred("imports",       "Imports (Billions $)",       start_date, end_date)
        if trade_type in ("all", "balance"):
            trade["balance"] = self._fred("trade_balance", "Trade Balance (Billions $)", start_date, end_date)
        return trade

    def get_michigan_sentiment(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("michigan_sentiment", "Consumer Sentiment Index", start_date, end_date)

    # ── Inflation & Prices ────────────────────────────────────────────────────

    def get_pce_inflation(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("pce_inflation", "PCE Price Index", start_date, end_date, units)

    def get_core_cpi(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("core_cpi", "Core CPI (Ex Food & Energy)", start_date, end_date, units)

    def get_gas_prices(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("gas_prices", "Regular Gasoline Price ($/gallon)", start_date, end_date)

    def get_ppi(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("ppi", "Producer Price Index (All Commodities)", start_date, end_date, units)

    # ── Labor Market ──────────────────────────────────────────────────────────

    def get_labor_participation(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("labor_participation", "Labor Force Participation Rate (%)", start_date, end_date)

    def get_u6_unemployment(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("u6_unemployment", "U-6 Underemployment Rate (%)", start_date, end_date)

    def get_avg_hourly_earnings(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("avg_hourly_earnings", "Average Hourly Earnings (All Employees, $)", start_date, end_date, units)

    def get_jobless_claims(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("jobless_claims", "Initial Jobless Claims (Weekly)", start_date, end_date)

    def get_nonfarm_payrolls(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("nonfarm_payrolls", "Total Nonfarm Payrolls (Thousands)", start_date, end_date, units)

    # ── Consumer & Spending ───────────────────────────────────────────────────

    def get_real_disposable_income(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("real_disposable_income", "Real Disposable Personal Income (Billions $)", start_date, end_date, units)

    def get_personal_savings_rate(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("personal_savings_rate", "Personal Savings Rate (%)", start_date, end_date)

    def get_retail_sales(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("retail_sales", "Retail & Food Services Sales (Millions $)", start_date, end_date, units)

    def get_consumer_credit(self, start_date=None, end_date=None, units: Optional[str] = "lin") -> List[dict]:
        return self._fred("consumer_credit", "Total Consumer Credit Outstanding (Billions $)", start_date, end_date, units)

    # ── Markets & Financial Conditions ────────────────────────────────────────

    def get_sp500(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("sp500", "S&P 500 Index", start_date, end_date)

    def get_vix(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("vix", "CBOE Volatility Index (VIX)", start_date, end_date)

    def get_credit_spread(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("credit_spread", "Baa-10yr Treasury Credit Spread (%)", start_date, end_date)

    # ── Trade & Manufacturing ─────────────────────────────────────────────────

    def get_trade_deficit_goods(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("trade_deficit_goods", "Goods Trade Balance (Millions $)", start_date, end_date)

    def get_china_imports(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("china_imports", "U.S. Imports from China (Millions $)", start_date, end_date)

    def get_manufacturing_output(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("manufacturing_output", "Industrial Production: Manufacturing (Index 2017=100)", start_date, end_date)

    # ── Fiscal & Debt ─────────────────────────────────────────────────────────

    def get_federal_debt_pct_gdp(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("federal_debt_pct_gdp", "Federal Debt as % of GDP", start_date, end_date)

    def get_budget_deficit(self, start_date=None, end_date=None) -> List[dict]:
        return self._fred("budget_deficit", "Federal Budget Surplus/Deficit (Millions $)", start_date, end_date)

    # ── Aggregate fetchers ────────────────────────────────────────────────────

    def fetch_all_series(self, start_date=None, end_date=None) -> Dict[str, List[dict]]:
        """Fetch all series at their native FRED frequency. Returns a dict of series name → list of records."""
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

    def fetch_panel(self, start_date=None, end_date=None) -> List[dict]:
        """
        Fetch all series and return as a flat list of records.

        Each record contains ``date``, ``value``, ``indicator``, and ``series`` keys.
        """
        all_series = self.fetch_all_series(start_date=start_date, end_date=end_date)
        records = []
        for series_name, series_records in all_series.items():
            for r in series_records:
                records.append({**r, "series": series_name})
        if not records:
            logger.warning("FRED: all series returned empty — check API key and connectivity")
        return records
