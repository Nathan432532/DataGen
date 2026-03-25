"""Extract — fetch ELIA wind and solar data via REST API.

Data source: ELIA Open Data Platform
https://opendata.elia.be/

Datasets:
- ods031: Wind power production (measured, Flanders)
- ods032: Solar/photovoltaic power production (measured, Flanders)
"""

import os
from datetime import datetime, timezone
from urllib.parse import quote

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("elia.extract")

ELIA_API_BASE = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"
ELIA_WIND_DATASET = "ods031"
ELIA_SOLAR_DATASET = "ods032"
ELIA_LIMIT = int(os.environ.get("ELIA_RECORD_LIMIT", "200000"))
PAGE_SIZE = 100  # ELIA max per request


def _as_iso_day_start(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if len(value) == 10:
        return f"{value}T00:00:00+00:00"
    return value


def _as_iso_day_end(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if len(value) == 10:
        return f"{value}T23:59:59+00:00"
    return value


def _build_where_clause(
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    region: str | None = None,
) -> str:
    """Build ODSQL where clause with correctly quoted datetime literals."""
    clauses = []

    if start_date:
        clauses.append(f'datetime >= "{_as_iso_day_start(start_date)}"')

    if end_date:
        clauses.append(f'datetime <= "{_as_iso_day_end(end_date)}"')

    if region:
        clauses.append(f'region = "{region}"')

    return " AND ".join(clauses) if clauses else ""


def _fetch_elia_dataset(
    dataset_id: str,
    dataset_name: str,
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    region: str | None = None,
) -> pd.DataFrame:
    """Paginate through the ELIA API and return a DataFrame for a specific dataset.

    Args:
        dataset_id: ELIA dataset ID (e.g., 'ods086')
        dataset_name: Human-readable name (e.g., 'wind')
        start_date: Start date (ISO format string or datetime)
        end_date: End date (ISO format string or datetime)
    """
    if start_date:
        end = end_date or datetime.now(timezone.utc).isoformat()
        frames: list[pd.DataFrame] = []
        for win_start, win_end in _iter_month_windows(start_date, end):
            logger.info("%s window %s -> %s", dataset_name, win_start, win_end)
            frame = _fetch_elia_window(
                dataset_id, dataset_name, win_start, win_end, region
            )
            if not frame.empty:
                frames.append(frame)

        if not frames:
            raise ValueError(
                f"No records from ELIA dataset {dataset_id} ({dataset_name})"
            )

        df = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .reset_index(drop=True)
        )
        logger.info(
            "Fetched %d records from ELIA %s (%s) across %d windows",
            len(df),
            dataset_name,
            dataset_id,
            len(frames),
        )
        return df

    frame = _fetch_elia_window(dataset_id, dataset_name, None, end_date, region)
    if frame.empty:
        raise ValueError(f"No records from ELIA dataset {dataset_id} ({dataset_name})")
    return frame


def _fetch_elia_window(
    dataset_id: str,
    dataset_name: str,
    start_date: datetime | str | None,
    end_date: datetime | str | None,
    region: str | None,
) -> pd.DataFrame:
    """Fetch one date window using offset pagination."""
    all_records: list[dict] = []
    offset = 0
    where_clause = _build_where_clause(start_date, end_date, region)
    where_param = f"&where={quote(where_clause, safe='')}" if where_clause else ""

    while offset < ELIA_LIMIT:
        batch = min(PAGE_SIZE, ELIA_LIMIT - offset)
        url = (
            f"{ELIA_API_BASE}/{dataset_id}/records"
            f"?limit={batch}&offset={offset}"
            f"&order_by=datetime"
            f"{where_param}"
        )

        logger.info("GET %s (%s) offset=%d", dataset_name, dataset_id, offset)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("ELIA %s request failed: %s", dataset_name, exc)
            if all_records:
                logger.warning(
                    "Returning %d partial records for %s",
                    len(all_records),
                    dataset_name,
                )
                break
            raise

        results = data.get("results", [])
        if not results:
            logger.info("No more records at offset %d for %s", offset, dataset_name)
            break

        all_records.extend(results)
        offset += batch

        total = data.get("total_count", 0)
        if offset >= total:
            break

    if not all_records:
        return pd.DataFrame()

    df = pd.json_normalize(all_records)
    logger.info(
        "Fetched %d records from ELIA %s (%s)", len(df), dataset_name, dataset_id
    )
    return df


def _iter_month_windows(
    start_date: datetime | str,
    end_date: datetime | str,
) -> list[tuple[str, str]]:
    """Return inclusive month windows as ISO datetime strings."""
    start = pd.Timestamp(_as_iso_day_start(start_date))
    end = pd.Timestamp(_as_iso_day_end(end_date))

    windows: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        next_month = (cursor + pd.offsets.MonthBegin(1)).normalize()
        window_end = min(next_month - pd.Timedelta(seconds=1), end)
        windows.append((cursor.isoformat(), window_end.isoformat()))
        cursor = next_month
    return windows


def extract_elia_wind_data(
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    region: str | None = None,
) -> pd.DataFrame:
    """Fetch wind power forecast data from ELIA (ods086).

    Args:
        start_date: Start date (ISO format string or datetime)
        end_date: End date (ISO format string or datetime)
    """
    return _fetch_elia_dataset(ELIA_WIND_DATASET, "wind", start_date, end_date, region)


def extract_elia_solar_data(
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    region: str | None = None,
) -> pd.DataFrame:
    """Fetch solar (photovoltaic) power forecast data from ELIA (ods087).

    Args:
        start_date: Start date (ISO format string or datetime)
        end_date: End date (ISO format string or datetime)
    """
    return _fetch_elia_dataset(
        ELIA_SOLAR_DATASET, "solar", start_date, end_date, region
    )
