"""Data-quality validation helpers.

Each function raises ``ValueError`` when a check fails so that the
calling Airflow task is marked **FAILED** automatically.
"""

import logging
from sqlalchemy import text
from src.common.db import get_engine

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Table existence
# ------------------------------------------------------------------
def validate_table_exists(schema: str, table: str) -> bool:
    """Raise if *schema.table* does not exist."""
    engine = get_engine()
    query = text(
        "SELECT EXISTS ("
        "  SELECT FROM information_schema.tables"
        "  WHERE table_schema = :schema AND table_name = :table"
        ")"
    )
    with engine.connect() as conn:
        exists = conn.execute(query, {"schema": schema, "table": table}).scalar()
    if not exists:
        raise ValueError(f"Table {schema}.{table} does not exist")
    logger.info("validated: %s.%s exists", schema, table)
    return True


# ------------------------------------------------------------------
# Row count
# ------------------------------------------------------------------
def validate_row_count(schema: str, table: str, min_rows: int = 1) -> int:
    """Raise if the table has fewer than *min_rows* rows."""
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar()
    if count < min_rows:
        raise ValueError(
            f"Table {schema}.{table} has {count} rows (expected >= {min_rows})"
        )
    logger.info("validated: %s.%s has %d rows", schema, table, count)
    return count


# ------------------------------------------------------------------
# Column presence
# ------------------------------------------------------------------
def validate_columns(schema: str, table: str, expected_columns: list) -> bool:
    """Raise if any *expected_columns* are missing from the table."""
    engine = get_engine()
    query = text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table"
    )
    with engine.connect() as conn:
        actual = {row[0] for row in conn.execute(query, {"schema": schema, "table": table})}
    missing = set(expected_columns) - actual
    if missing:
        raise ValueError(f"Table {schema}.{table} is missing columns: {missing}")
    logger.info("validated: %s.%s has expected columns", schema, table)
    return True


# ------------------------------------------------------------------
# Duplicate timestamp check
# ------------------------------------------------------------------
def validate_no_duplicate_timestamps(
    schema: str, table: str, timestamp_col: str = "timestamp"
) -> bool:
    """Raise if duplicate values exist in *timestamp_col*."""
    engine = get_engine()
    query = text(
        f'SELECT "{timestamp_col}", COUNT(*) AS cnt '
        f'FROM "{schema}"."{table}" '
        f'GROUP BY "{timestamp_col}" '
        f"HAVING COUNT(*) > 1 LIMIT 5"
    )
    with engine.connect() as conn:
        dupes = conn.execute(query).fetchall()
    if dupes:
        raise ValueError(
            f"Table {schema}.{table} has duplicate timestamps: {dupes}"
        )
    logger.info("validated: %s.%s has no duplicate timestamps", schema, table)
    return True
