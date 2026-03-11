"""Extract — fetch ELIA wind and solar forecast data via REST API.

Data source: ELIA Open Data Platform
https://opendata.elia.be/

Datasets:
- ods086: Wind power production estimation and forecast
- ods087: Photovoltaic power production estimation and forecast
"""

import os

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("elia.extract")

ELIA_API_BASE = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"
ELIA_WIND_DATASET = "ods086"
ELIA_SOLAR_DATASET = "ods087"
ELIA_LIMIT = int(os.environ.get("ELIA_RECORD_LIMIT", "500"))
PAGE_SIZE = 100  # ELIA max per request


def _fetch_elia_dataset(dataset_id: str, dataset_name: str) -> pd.DataFrame:
    """Paginate through the ELIA API and return a DataFrame for a specific dataset."""
    all_records: list[dict] = []
    offset = 0

    while offset < ELIA_LIMIT:
        batch = min(PAGE_SIZE, ELIA_LIMIT - offset)
        url = (
            f"{ELIA_API_BASE}/{dataset_id}/records"
            f"?limit={batch}&offset={offset}"
            f"&order_by=datetime%20DESC"
        )
        logger.info("GET %s (%s)", url, dataset_name)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("ELIA %s request failed: %s", dataset_name, exc)
            if all_records:
                logger.warning("Returning %d partial records for %s", len(all_records), dataset_name)
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
        raise ValueError(f"No records from ELIA dataset {dataset_id} ({dataset_name})")

    df = pd.json_normalize(all_records)
    logger.info("Fetched %d records from ELIA %s (%s), cols=%s",
                len(df), dataset_name, dataset_id, list(df.columns))
    return df


def extract_elia_wind_data() -> pd.DataFrame:
    """Fetch wind power forecast data from ELIA (ods086)."""
    return _fetch_elia_dataset(ELIA_WIND_DATASET, "wind")


def extract_elia_solar_data() -> pd.DataFrame:
    """Fetch solar (photovoltaic) power forecast data from ELIA (ods087)."""
    return _fetch_elia_dataset(ELIA_SOLAR_DATASET, "solar")
