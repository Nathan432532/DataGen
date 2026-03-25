"""Extract — download Vlaanderen realtime solar/wind per-day CSV files.

Data source: https://admiring-leakey-15793c.netlify.app/data/realtime
File naming:
  realtime_solar_{YYYYMMDD}.csv
  realtime_wind_{YYYYMMDD}.csv

Each file has a `datetime` column and NIS-code columns (5-digit municipality
codes) with MW values for that hour. An extra `source_file_date` column is
added to record which file the row came from.
"""

from __future__ import annotations

import csv
import io
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("vlaanderen_energy.extract")

BASE_URL = "https://admiring-leakey-15793c.netlify.app/data/realtime"
DEFAULT_START = date(2025, 3, 1)
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_solar_data(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> pd.DataFrame:
    """Download realtime solar CSV files and return a combined DataFrame."""
    return _fetch_kind("realtime_solar", start_date, end_date)


def extract_wind_data(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> pd.DataFrame:
    """Download realtime wind CSV files and return a combined DataFrame."""
    return _fetch_kind("realtime_wind", start_date, end_date)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _fetch_kind(
    kind: str,
    start_date: date | str | None,
    end_date: date | str | None,
) -> pd.DataFrame:
    start = _parse_date(start_date) if start_date else DEFAULT_START
    end = _parse_date(end_date) if end_date else date.today()

    frames: list[pd.DataFrame] = []
    missing = 0

    for day in _daterange(start, end):
        df = _fetch_daily_csv(kind, day)
        if df is None:
            missing += 1
            continue
        frames.append(df)

    logger.info(
        "%s: fetched %d files, %d missing (range %s → %s)",
        kind,
        len(frames),
        missing,
        start,
        end,
    )

    if not frames:
        raise ValueError(
            f"No {kind} data found for range {start} → {end}. "
            "All files were missing or empty."
        )

    df = pd.concat(frames, ignore_index=True)
    if "datetime" in df.columns:
        df = df.sort_values("datetime").reset_index(drop=True)
    return df


def _fetch_daily_csv(kind: str, day: date) -> pd.DataFrame | None:
    ymd = day.strftime("%Y%m%d")
    url = f"{BASE_URL}/{kind}_{ymd}.csv"

    response: requests.Response | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 404:
                logger.debug("404 for %s", url)
                return None
            response.raise_for_status()
            break
        except requests.Timeout as exc:
            if attempt == MAX_RETRIES:
                logger.warning("Timeout after %d attempts for %s: %s", MAX_RETRIES, url, exc)
                return None
            sleep_sec = RETRY_BACKOFF_SECONDS ** (attempt - 1)
            logger.warning(
                "Timeout for %s (attempt %d/%d), retrying in %ds",
                url, attempt, MAX_RETRIES, sleep_sec,
            )
            time.sleep(sleep_sec)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            return None

    if response is None:
        return None

    text = response.text.strip()
    if not text:
        return None

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return None

    rows = []
    for row in reader:
        row["source_file_date"] = ymd
        rows.append(row)

    if not rows:
        return None

    df = pd.DataFrame(rows)
    logger.debug("Fetched %d rows from %s", len(df), url)
    return df


def _daterange(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value[:10], "%Y-%m-%d").date()
