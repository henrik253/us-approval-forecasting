import logging
from typing import List

import requests

logger = logging.getLogger(__name__)


class VoteHubFetcher:
    """
    Fetches presidential approval poll data from the VoteHub API.

    Filters by date range and minimum sample size to exclude low-quality polls.
    Returns an empty list (never raises) so callers can handle missing data
    gracefully.
    """

    BASE_URL = "https://api.votehub.com/polls"

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":  "application/json",
        "Referer": "https://votehub.com/",
    }

    def __init__(
        self,
        subject: str = "Trump",
        from_date: str = "2025-02-01",
        to_date: str = "2026-03-20",
        min_sample_size: int = 500,
    ):
        """
        Args:
            subject:         Poll subject name passed to the VoteHub API.
            from_date:       Earliest poll end date to include (YYYY-MM-DD).
            to_date:         Latest poll end date to include (YYYY-MM-DD).
            min_sample_size: Drop polls with fewer respondents than this threshold.
        """
        self.subject         = subject
        self.from_date       = from_date
        self.to_date         = to_date
        self.min_sample_size = min_sample_size

    def fetch(self) -> List[dict]:
        """
        Fetch approval polls filtered by the instance's date range and sample size.

        Returns:
            List of ``{"date": str, "sample_size": int, "approval": float,
            "disapproval": float}`` records, sorted by date. Empty list on failure.
        """
        try:
            r = requests.get(
                self.BASE_URL,
                headers=self._HEADERS,
                params={"subject": self.subject, "to_date": self.to_date},
                timeout=15,
            )
            if r.status_code != 200:
                logger.error("VoteHub returned HTTP %d", r.status_code)
                return []

            records = []
            for poll in r.json():
                end_date    = poll.get("end_date")
                sample_size = poll.get("sample_size", 0) or 0
                if not end_date or end_date < self.from_date or sample_size < self.min_sample_size:
                    continue
                record: dict = {"date": end_date, "sample_size": sample_size}
                for answer in poll.get("answers", []):
                    if answer.get("choice") == "Approve":
                        record["approval"] = answer.get("pct")
                    elif answer.get("choice") == "Disapprove":
                        record["disapproval"] = answer.get("pct")
                if "approval" in record or "disapproval" in record:
                    records.append(record)

            if not records:
                logger.warning(
                    "VoteHub: no polls matched the filters (from=%s, min_n=%d)",
                    self.from_date, self.min_sample_size,
                )
                return []

            return sorted(records, key=lambda rec: rec["date"])

        except requests.exceptions.RequestException as e:
            logger.error("VoteHub request failed: %s", e)
        except Exception as e:
            logger.error("VoteHub unexpected error: %s", e)
        return []
