"""Extract — download Kaggle weather dataset for Antwerp.

Authenticates via the ``KAGGLE_API_TOKEN`` environment variable.
Supports both:
- New-style tokens:  ``KGAT_…``
- Legacy JSON tokens: ``{"username":"…","key":"…"}``
"""

import json
import os

import kagglehub
import pandas as pd

from src.common.logging_config import setup_logging

logger = setup_logging("weather_kaggle.extract")

DATASET_SLUG = "ramima/weather-dataset-in-antwerp-belgium"


# ------------------------------------------------------------------
# Kaggle auth
# ------------------------------------------------------------------
def setup_kaggle_auth() -> None:
    """Configure kagglehub credentials from ``KAGGLE_API_TOKEN``."""
    token = os.environ.get("KAGGLE_API_TOKEN", "")
    if not token:
        raise ValueError("KAGGLE_API_TOKEN environment variable is not set")

    # New-style token (KGAT_…) — set KAGGLE_TOKEN for kagglehub
    if token.startswith("KGAT_"):
        os.environ["KAGGLE_TOKEN"] = token
        logger.info("Kaggle authentication configured (new-style KGAT_ token)")
        return

    # Legacy JSON format — extract username/key
    try:
        creds = json.loads(token)
        os.environ["KAGGLE_USERNAME"] = creds["username"]
        os.environ["KAGGLE_KEY"] = creds["key"]
        logger.info("Kaggle authentication configured (legacy JSON token)")
    except (json.JSONDecodeError, KeyError) as exc:
        raise ValueError(
            "KAGGLE_API_TOKEN must be a KGAT_ token or JSON with 'username' and 'key'"
        ) from exc


# ------------------------------------------------------------------
# Extract
# ------------------------------------------------------------------
def extract_weather_data() -> pd.DataFrame:
    """Download CSV files from Kaggle, concatenate, and return a DataFrame."""
    setup_kaggle_auth()

    logger.info("Downloading dataset %s …", DATASET_SLUG)
    path = kagglehub.dataset_download(DATASET_SLUG)
    logger.info("Dataset path: %s", path)

    csv_files = sorted(f for f in os.listdir(path) if f.lower().endswith(".csv"))
    logger.info("CSV files found: %s", csv_files)

    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {path}")

    frames: list[pd.DataFrame] = []
    for fname in csv_files:
        fpath = os.path.join(path, fname)
        # Original dataset uses semicolons; fall back to comma
        df = pd.read_csv(fpath, sep=";")
        if len(df.columns) <= 1:
            df = pd.read_csv(fpath, sep=",")
        logger.info("  %s → %d rows, cols=%s", fname, len(df), list(df.columns))
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Combined: %d rows", len(combined))
    return combined
