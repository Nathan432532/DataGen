"""Extract — download Vlaanderen solar & wind generation CSVs.

Data source: ELIA Open Data exports for Belgian solar (ods032) and
wind (ods031) generation, which cover the Flemish region.
"""

import io
import os

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("vlaanderen_energy.extract")

ELIA_EXPORTS = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"

SOLAR_DATASET = os.environ.get("VLAANDEREN_SOLAR_DATASET", "ods032")
WIND_DATASET = os.environ.get("VLAANDEREN_WIND_DATASET", "ods031")
RECORD_LIMIT = int(os.environ.get("VLAANDEREN_RECORD_LIMIT", "500"))


# ------------------------------------------------------------------
# Solar
# ------------------------------------------------------------------
def extract_solar_data() -> pd.DataFrame:
    """Download solar generation CSV from ELIA."""
    return _download_csv(SOLAR_DATASET, "solar")


# ------------------------------------------------------------------
# Wind
# ------------------------------------------------------------------
def extract_wind_data() -> pd.DataFrame:
    """Download wind generation CSV from ELIA."""
    return _download_csv(WIND_DATASET, "wind")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _download_csv(dataset_id: str, label: str) -> pd.DataFrame:
    """Try CSV export first, fall back to JSON API."""
    csv_url = f"{ELIA_EXPORTS}/{dataset_id}/exports/csv?limit={RECORD_LIMIT}"
    logger.info("Downloading %s CSV: %s", label, csv_url)

    try:
        resp = requests.get(csv_url, timeout=120)
        resp.raise_for_status()
        # ELIA CSV exports use semicolons
        df = pd.read_csv(io.StringIO(resp.text), sep=";")
        if len(df.columns) <= 1:
            df = pd.read_csv(io.StringIO(resp.text), sep=",")
        logger.info("%s CSV → %d rows, cols=%s", label, len(df), list(df.columns))
        return df
    except Exception as exc:
        logger.warning("CSV export failed for %s (%s), trying JSON API", label, exc)

    return _fallback_api(dataset_id, label)


def _fallback_api(dataset_id: str, label: str) -> pd.DataFrame:
    url = f"{ELIA_EXPORTS}/{dataset_id}/records?limit=100&order_by=datetime%20DESC"
    logger.info("JSON fallback for %s: %s", label, url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("results", [])
    if not records:
        raise ValueError(f"No records for {label} dataset {dataset_id}")
    df = pd.json_normalize(records)
    logger.info("%s fallback → %d rows", label, len(df))
    return df
