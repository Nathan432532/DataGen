"""Load — insert ELIA wind and solar data into the *raw* layer."""

import uuid
from datetime import datetime, timezone

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("elia.load")

RAW_WIND_TABLE = "raw_elia_wind"
RAW_SOLAR_TABLE = "raw_elia_solar"


def load_raw_elia_wind(df: pd.DataFrame, run_id: str | None = None) -> str:
    """Write wind data to ``raw.raw_elia_wind``."""
    run_id = run_id or str(uuid.uuid4())

    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"] = "elia_wind_ods086"
    df["run_id"] = run_id

    engine = get_engine()
    df.to_sql(RAW_WIND_TABLE, engine, schema="raw", if_exists="replace", index=False)
    logger.info("Loaded %d rows → raw.%s  (run_id=%s)", len(df), RAW_WIND_TABLE, run_id)
    return run_id


def load_raw_elia_solar(df: pd.DataFrame, run_id: str | None = None) -> str:
    """Write solar data to ``raw.raw_elia_solar``."""
    run_id = run_id or str(uuid.uuid4())

    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"] = "elia_solar_ods087"
    df["run_id"] = run_id

    engine = get_engine()
    df.to_sql(RAW_SOLAR_TABLE, engine, schema="raw", if_exists="replace", index=False)
    logger.info("Loaded %d rows → raw.%s  (run_id=%s)", len(df), RAW_SOLAR_TABLE, run_id)
    return run_id
