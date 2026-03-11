"""Load — insert Vlaanderen solar/wind data into the *raw* layer."""

import uuid
from datetime import datetime, timezone

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("vlaanderen_energy.load")


def load_raw_solar(df: pd.DataFrame, run_id: str | None = None) -> str:
    """Write solar data to ``raw.raw_vlaanderen_solar``."""
    return _load(df, "raw_vlaanderen_solar", "elia_solar_ods032", run_id)


def load_raw_wind(df: pd.DataFrame, run_id: str | None = None) -> str:
    """Write wind data to ``raw.raw_vlaanderen_wind``."""
    return _load(df, "raw_vlaanderen_wind", "elia_wind_ods031", run_id)


def _load(df: pd.DataFrame, table: str, source: str, run_id: str | None) -> str:
    run_id = run_id or str(uuid.uuid4())

    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"] = source
    df["run_id"] = run_id

    engine = get_engine()
    df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
    logger.info("Loaded %d rows → raw.%s  (run_id=%s)", len(df), table, run_id)
    return run_id
