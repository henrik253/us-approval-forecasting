import hashlib
import logging
import pickle
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class GDELTFetcher:
    """
    Fetches media tone and article-volume timelines from the GDELT 2.0 DOC API.

    Results are disk-cached so repeated calls with the same parameters return
    instantly without hitting the API. When running in AWS Lambda use
    ``cache_dir="/tmp/gdelt"`` (the only writable path in that environment).
    """

    BASE_URL      = "https://api.gdeltproject.org/api/v2/doc/doc"
    DEFAULT_QUERY = '"Trump" sourcecountry:US sourcelang:English'

    def __init__(
        self,
        query: Optional[str] = None,
        cache_dir: str = ".cache",
        delay: float = 10.0,
    ):
        """
        Args:
            query:     GDELT full-text search query. Defaults to DEFAULT_QUERY.
            cache_dir: Directory for disk cache. Created automatically if missing.
                       Use ``/tmp/gdelt`` when running inside AWS Lambda.
            delay:     Seconds to wait between live API calls to avoid 429 errors.
        """
        self.query     = query or self.DEFAULT_QUERY
        self.cache_dir = Path(cache_dir)
        self.delay     = delay
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Cache helpers ─────────────────────────────────────────────────────────

    def _cache_path(self, mode: str, start: str, end: str) -> Path:
        key = hashlib.md5(f"{self.query}|{mode}|{start}|{end}".encode()).hexdigest()
        return self.cache_dir / f"gdelt_{key}.pkl"

    def _fetch_mode(self, mode: str, start: str, end: str) -> list:
        """Fetch a single GDELT timeline mode, using disk cache when available."""
        cache_path = self._cache_path(mode, start, end)
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                return pickle.load(f)

        logger.info("Fetching GDELT %s ...", mode)
        try:
            r = requests.get(
                self.BASE_URL,
                params={
                    "query":         self.query,
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
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                logger.warning(
                    "GDELT rate limit (429) for mode %s. Try again later or from a different IP.", mode
                )
            else:
                logger.error("GDELT HTTP error for mode %s: %s", mode, e)
        except requests.exceptions.RequestException as e:
            logger.error("GDELT request failed for mode %s: %s", mode, e)
        except Exception as e:
            logger.error("GDELT unexpected error for mode %s: %s", mode, e)
        return []

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch(
        self,
        start: str = "20250101000000",
        end: str = "20260328000000",
        rolling_window: int = 20,
    ) -> pd.DataFrame:
        """
        Fetch tone and volume timelines for the configured query.

        Args:
            start:          Start datetime in GDELT format (YYYYMMDDHHmmss).
            end:            End datetime in GDELT format (YYYYMMDDHHmmss).
            rolling_window: Periods for centred rolling mean applied to both series.

        Returns:
            DataFrame with columns: date, tone, volume. Empty DataFrame on failure.
        """
        try:
            tone_data = self._fetch_mode("TimelineTone", start, end)

            if not self._cache_path("TimelineVolRaw", start, end).exists():
                time.sleep(self.delay)

            vol_data = self._fetch_mode("TimelineVolRaw", start, end)

            if not tone_data and not vol_data:
                logger.warning("GDELT returned no data for query: %s", self.query)
                return pd.DataFrame()

            def _to_df(data, col):
                return pd.DataFrame(
                    [{"date": pd.to_datetime(p["date"]), col: float(p["value"])} for p in data]
                )

            df = (
                _to_df(tone_data, "tone")
                .merge(_to_df(vol_data, "volume"), on="date")
                .sort_values("date")
                .reset_index(drop=True)
            )
            df["tone"]   = df["tone"].rolling(window=rolling_window, min_periods=1, center=True).mean()
            df["volume"] = df["volume"].rolling(window=rolling_window, min_periods=1, center=True).mean()
            return df

        except Exception as e:
            logger.error("GDELT fetch failed: %s", e)
            return pd.DataFrame()

    def clear_cache(self) -> None:
        """Remove all cached GDELT responses from disk."""
        removed = list(self.cache_dir.glob("gdelt_*.pkl"))
        for f in removed:
            f.unlink()
        logger.info("Removed %d cached GDELT file(s) from %s", len(removed), self.cache_dir)
