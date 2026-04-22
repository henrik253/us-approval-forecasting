# Dataset Description

## 1. FRED — Federal Reserve Economic Data
**Source:** St. Louis Fed API (`api.stlouisfed.org`)
**Fetcher:** `pipelines/lambda/fetch_sources/fred.py`
**Auth:** API key (`FRED_API_KEY`)

Economic time-series covering 30+ indicators across six categories:

| Category | Series examples |
|---|---|
| GDP & Macro | Nominal GDP, Real GDP, CPI, Unemployment Rate, Federal Funds Rate, Michigan Consumer Sentiment |
| Inflation & Prices | PCE Inflation, Core CPI, Gas Prices, PPI |
| Labor Market | Labor Participation Rate, U-6 Underemployment, Avg Hourly Earnings, Nonfarm Payrolls, Initial Jobless Claims |
| Consumer & Spending | Real Disposable Income, Personal Savings Rate, Retail Sales, Consumer Credit |
| Markets & Financial | S&P 500, VIX, Baa-10yr Credit Spread |
| Trade & Manufacturing | Goods Trade Balance, U.S. Imports from China, Manufacturing Output |
| Fiscal & Debt | Federal Debt as % of GDP, Federal Budget Surplus/Deficit |

**Record shape:** `{date, value, indicator, series}` — flat list, one row per series per observation date.
**Frequency:** Varies by series (monthly, quarterly, daily).

---

## 2. VoteHub — Presidential Approval Polls
**Source:** VoteHub API (`api.votehub.com/polls`)
**Fetcher:** `pipelines/lambda/fetch_sources/votehub.py`
**Auth:** None (public API, browser headers used)

Aggregated presidential approval polls filtered by subject (default: Trump), date range, and minimum sample size (≥ 500 respondents).

**Record shape:** `{date, sample_size, approval, disapproval}`
**Frequency:** Irregular (poll end dates).

---

## 3. GDELT — Media Sentiment
**Source:** GDELT 2.0 DOC API (`api.gdeltproject.org`)
**Fetcher:** `pipelines/lambda/fetch_sources/gdelt.py`
**Auth:** None (public API)

News media tone and article volume for English-language US sources mentioning "Trump". Disk-cached to avoid rate limits (HTTP 429 is a known issue with this API).

- **Tone** (`TimelineTone`): average emotional tone of articles (negative = negative sentiment)
- **Volume** (`TimelineVolRaw`): raw count of matching articles

**Record shape:** `{date, tone, volume}`
**Frequency:** Daily (15-minute buckets aggregated).
