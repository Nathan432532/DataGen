"""Extract — fetch ELIA electricity generation data via REST API.

Data source: ELIA Open Data Platform
https://opendata.elia.be/
"""

import os

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("elia.extract")

ELIA_API_BASE = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"
ELIA_DATASET = os.environ.get("ELIA_DATASET_ID", "ods001")
ELIA_LIMIT = int(os.environ.get("ELIA_RECORD_LIMIT", "500"))
PAGE_SIZE = 100  # ELIA max per request


def extract_elia_data() -> pd.DataFrame:
    """Paginate through the ELIA API and return a DataFrame."""
    all_records: list[dict] = []
    offset = 0

    while offset < ELIA_LIMIT:
        batch = min(PAGE_SIZE, ELIA_LIMIT - offset)
        url = (
            f"{ELIA_API_BASE}/{ELIA_DATASET}/records"
            f"?limit={batch}&offset={offset}"
            f"&order_by=datetime%20DESC"
        )
        logger.info("GET %s", url)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("ELIA request failed: %s", exc)
            if all_records:
                logger.warning("Returning %d partial records", len(all_records))
                break
            raise

        results = data.get("results", [])
        if not results:
            logger.info("No more records at offset %d", offset)
            break

        all_records.extend(results)
        offset += batch

        total = data.get("total_count", 0)
        if offset >= total:
            break

    if not all_records:
        raise ValueError(f"No records from ELIA dataset {ELIA_DATASET}")

    df = pd.json_normalize(all_records)
    logger.info("Fetched %d records from ELIA (%s), cols=%s", len(df), ELIA_DATASET, list(df.columns))
    return df
