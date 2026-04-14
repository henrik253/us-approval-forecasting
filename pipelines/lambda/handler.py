"""
AWS Lambda handler — fetches all data sources and returns a combined result.

Each source is fetched independently so a failure in one does not prevent the
others from running. Errors are collected in the ``errors`` key of the response
rather than propagating as exceptions.

Environment variables
---------------------
FRED_API_KEY   : Required. FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html
START_DATE     : Optional. Earliest observation date (YYYY-MM-DD). Falls back to event payload.
END_DATE       : Optional. Latest observation date (YYYY-MM-DD). Falls back to event payload.
GDELT_CACHE_DIR: Optional. Cache directory for GDELT responses (default: /tmp/gdelt).
"""

import logging
import os

from dotenv import load_dotenv

from fetch_sources.fred import FREDFetcher
from fetch_sources.gdelt import GDELTFetcher
from fetch_sources.votehub import VoteHubFetcher

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.

    Args:
        event:   Lambda event dict. Supports optional keys:
                   - ``start_date`` (str, YYYY-MM-DD)
                   - ``end_date``   (str, YYYY-MM-DD)
        context: Lambda context object (unused).

    Returns:
        dict with keys:
          ``statusCode`` : 200
          ``data``       : dict of source name → list of records (JSON-serialisable)
          ``errors``     : dict of source name → error message for any failed fetch
    """
    event = event or {}
    start_date = event.get("start_date") or os.getenv("START_DATE")
    end_date   = event.get("end_date")   or os.getenv("END_DATE")

    data   = {}
    errors = {}

    # ── FRED economic data ────────────────────────────────────────────────────
    try:
        fred_api_key = os.getenv("FRED_API_KEY", "")
        if not fred_api_key:
            raise EnvironmentError("FRED_API_KEY environment variable is not set")
        fred   = FREDFetcher(api_key=fred_api_key)
        panel  = fred.fetch_panel(start_date=start_date, end_date=end_date)
        if panel.empty:
            raise ValueError("FRED fetch returned an empty panel")
        data["economic"] = panel.reset_index().to_dict(orient="records")
        logger.info("FRED: fetched %d rows x %d columns", *panel.shape)
    except Exception as e:
        logger.error("FRED fetch failed: %s", e)
        errors["economic"] = str(e)

    # ── VoteHub approval polls ────────────────────────────────────────────────
    try:
        polls = VoteHubFetcher().fetch()
        if polls.empty:
            raise ValueError("VoteHub fetch returned no records")
        data["approval"] = polls.to_dict(orient="records")
        logger.info("VoteHub: fetched %d poll records", len(polls))
    except Exception as e:
        logger.error("VoteHub fetch failed: %s", e)
        errors["approval"] = str(e)

    # ── GDELT media sentiment ─────────────────────────────────────────────────
    try:
        cache_dir = os.getenv("GDELT_CACHE_DIR", "/tmp/gdelt")
        sentiment = GDELTFetcher(cache_dir=cache_dir).fetch()
        if sentiment.empty:
            raise ValueError("GDELT fetch returned no records")
        data["media_sentiment"] = sentiment.to_dict(orient="records")
        logger.info("GDELT: fetched %d sentiment records", len(sentiment))
    except Exception as e:
        logger.error("GDELT fetch failed: %s", e)
        errors["media_sentiment"] = str(e)

    # ── Response ──────────────────────────────────────────────────────────────
    if errors:
        logger.warning("Completed with errors in sources: %s", list(errors.keys()))

    return {
        "statusCode": 200,
        "data":       data,
        "errors":     errors,
    }
