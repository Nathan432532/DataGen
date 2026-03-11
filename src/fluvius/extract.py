"""Extract — download Fluvius energy data (parquet / JSON fallback).

Data source: Fluvius Open Data — energy consumption by municipality.
"""

import io
import os

import pandas as pd
import requests

from src.common.logging_config import setup_logging

logger = setup_logging("fluvius.extract")

# Primary: parquet export
FLUVIUS_PARQUET_URL = os.environ.get(
    "FLUVIUS_PARQUET_URL",
    "https://opendata.fluvius.be/api/explore/v2.1/catalog/datasets/"
    "energieverbruik-per-gemeente-per-jaar/exports/parquet?limit=-1",
)

# Fallback: JSON records
FLUVIUS_API_URL = os.environ.get(
    "FLUVIUS_API_URL",
    "https://opendata.fluvius.be/api/explore/v2.1/catalog/datasets/"
    "energieverbruik-per-gemeente-per-jaar/records?limit=100",
)


def extract_fluvius_data() -> pd.DataFrame:
    """Download Fluvius energy data — prefers parquet, falls back to JSON API."""
    logger.info("Attempting parquet download: %s", FLUVIUS_PARQUET_URL)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        resp = requests.get(FLUVIUS_PARQUET_URL, timeout=120, headers=headers)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))
        logger.info("Parquet download OK → %d rows, cols=%s", len(df), list(df.columns))
        return df
    except Exception as exc:
        logger.warning("Parquet download failed (%s), trying JSON API …", exc)

    logger.info("Forcing JSON API fallback for testing ...")
    return _extract_via_api()


def _extract_via_api() -> pd.DataFrame:
    """Fallback: fetch records from the Fluvius JSON API."""
    logger.info("JSON API: %s", FLUVIUS_API_URL)
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(FLUVIUS_API_URL, timeout=60, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    records = data.get("results", data.get("records", []))
    if not records:
        raise ValueError("No records returned from Fluvius API")

    df = pd.json_normalize(records)
    logger.info("JSON API → %d rows, cols=%s", len(df), list(df.columns))
    return df
