"""Load — insert ELIA data into the *raw* layer."""

import uuid
from datetime import datetime, timezone

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("elia.load")

RAW_TABLE = "raw_elia_generation"


def load_raw_elia(df: pd.DataFrame, run_id: str | None = None) -> str:
    """Write *df* to ``raw.raw_elia_generation``."""
    run_id = run_id or str(uuid.uuid4())

    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"] = "elia_opendata"
    df["run_id"] = run_id

    engine = get_engine()
    df.to_sql(RAW_TABLE, engine, schema="raw", if_exists="replace", index=False)
    logger.info("Loaded %d rows → raw.%s  (run_id=%s)", len(df), RAW_TABLE, run_id)
    return run_id
