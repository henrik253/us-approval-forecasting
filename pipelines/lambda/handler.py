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
S3_BUCKET      : Required. S3 bucket name where fetched data is stored as JSON.
S3_KEY_PREFIX  : Optional. Key prefix inside the bucket (default: "raw").
S3_REGION      : Optional. AWS region for the S3 client (default: "us-east-1").
"""

import json
import logging
import os

import boto3
from dotenv import load_dotenv

from fetch_sources.fred import FREDFetcher
from fetch_sources.gdelt import GDELTFetcher
from fetch_sources.votehub import VoteHubFetcher

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Debug dummy data ──────────────────────────────────────────────────────────
_DEBUG_DATA = {
    "economic": [
        {"date": "2024-01-01", "UNRATE": 3.7, "CPIAUCSL": 314.0, "GDP": 27360.0},
        {"date": "2024-02-01", "UNRATE": 3.9, "CPIAUCSL": 315.2, "GDP": 27360.0},
        {"date": "2024-03-01", "UNRATE": 3.8, "CPIAUCSL": 316.1, "GDP": 27500.0},
    ],
    "approval": [
        {"date": "2024-01-15", "president": "Biden", "approve": 42.1, "disapprove": 53.4, "pollster": "Gallup"},
        {"date": "2024-02-15", "president": "Biden", "approve": 41.8, "disapprove": 54.0, "pollster": "Gallup"},
        {"date": "2024-03-15", "president": "Biden", "approve": 40.5, "disapprove": 55.1, "pollster": "Gallup"},
    ],
    "media_sentiment": [
        {"date": "2024-01-01", "avg_tone": -2.3, "num_articles": 1520, "theme": "PRESIDENT"},
        {"date": "2024-02-01", "avg_tone": -1.8, "num_articles": 1340, "theme": "PRESIDENT"},
        {"date": "2024-03-01", "avg_tone": -2.9, "num_articles": 1610, "theme": "PRESIDENT"},
    ],
}


def _upload_to_s3(data: dict) -> dict:
    """Upload each source's records to S3 as a JSON file.

    Args:
        data: dict of source name → list of records.

    Returns:
        dict of source name → S3 URI of the uploaded object.
    """
    bucket = os.environ["S3_BUCKET"]
    prefix = os.getenv("S3_KEY_PREFIX", "raw").rstrip("/")
    region = os.getenv("S3_REGION", "us-east-1")

    s3 = boto3.client("s3", region_name=region)
    uploaded = {}
    for source, records in data.items():
        key = f"{prefix}/{source}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(records, default=str),
            ContentType="application/json",
        )
        uri = f"s3://{bucket}/{key}"
        uploaded[source] = uri
        logger.info("Uploaded %s → %s", source, uri)
    return uploaded


def lambda_handler(event: dict, context) -> dict:
    """
    Lambda entry point.

    Args:
        event:   Lambda event dict. Supports optional keys:
                   - ``debug``      (bool) — skip real fetches, use hardcoded dummy data
                   - ``start_date`` (str, YYYY-MM-DD)
                   - ``end_date``   (str, YYYY-MM-DD)
        context: Lambda context object (unused).

    Returns:
        dict with keys:
          ``statusCode`` : 200
          ``data``       : dict of source name → list of records (JSON-serialisable)
          ``s3``         : dict of source name → S3 URI of the stored JSON object
          ``errors``     : dict of source name → error message for any failed fetch
    """
    event = event or {}
    debug      = bool(event.get("debug", False))
    start_date = event.get("start_date") or os.getenv("START_DATE")
    end_date   = event.get("end_date")   or os.getenv("END_DATE")

    data   = {}
    errors = {}

    if debug:
        # ── Debug mode: use hardcoded dummy data, no real API calls ───────────
        print("DEBUG MODE: skipping real fetches, using hardcoded dummy data")

        print("     FETCHING FRED DATA")
        data["economic"] = _DEBUG_DATA["economic"]
        logger.info("DEBUG: loaded %d dummy economic records", len(data["economic"]))

        print("     FETCHING VOTEHUB DATA")
        data["approval"] = _DEBUG_DATA["approval"]
        logger.info("DEBUG: loaded %d dummy approval records", len(data["approval"]))

        print("     FETCHING GDELT DATA")
        data["media_sentiment"] = _DEBUG_DATA["media_sentiment"]
        logger.info("DEBUG: loaded %d dummy media_sentiment records", len(data["media_sentiment"]))

    else:
        # ── Production mode: fetch from real sources ──────────────────────────
        print("FETCHING: ")

        print("     FETCHING FRED DATA")
        try:
            fred_api_key = os.getenv("FRED_API_KEY", "")
            if not fred_api_key:
                raise EnvironmentError("FRED_API_KEY environment variable is not set")
            records = FREDFetcher(api_key=fred_api_key).fetch_panel(start_date=start_date, end_date=end_date)
            if not records:
                raise ValueError("FRED fetch returned an empty panel")
            data["economic"] = records
            logger.info("FRED: fetched %d records", len(records))
        except Exception as e:
            logger.error("FRED fetch failed: %s", e)
            errors["economic"] = str(e)

        print("     FETCHING VOTEHUB DATA")
        try:
            polls = VoteHubFetcher().fetch()
            if not polls:
                raise ValueError("VoteHub fetch returned no records")
            data["approval"] = polls
            logger.info("VoteHub: fetched %d poll records", len(polls))
        except Exception as e:
            logger.error("VoteHub fetch failed: %s", e)
            errors["approval"] = str(e)

        print("     FETCHING GDELT DATA")
        try:
            cache_dir = os.getenv("GDELT_CACHE_DIR", "/tmp/gdelt")
            sentiment = GDELTFetcher(cache_dir=cache_dir).fetch()
            if not sentiment:
                raise ValueError("GDELT fetch returned no records")
            data["media_sentiment"] = sentiment
            logger.info("GDELT: fetched %d sentiment records", len(sentiment))
        except Exception as e:
            logger.error("GDELT fetch failed: %s", e)
            errors["media_sentiment"] = str(e)

    # ── Upload to S3 ──────────────────────────────────────────────────────────
    uploaded = {}
    try:
        uploaded = _upload_to_s3(data)
    except Exception as e:
        logger.error("S3 upload failed: %s", e)
        errors["s3_upload"] = str(e)

    # ── Response ──────────────────────────────────────────────────────────────
    if errors:
        logger.warning("Completed with errors in sources: %s", list(errors.keys()))

    return {
        "statusCode": 200,
        "data":       data,
        "s3":         uploaded,
        "errors":     errors,
    }
