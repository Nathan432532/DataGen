"""Database connection utilities.

Every pipeline uses ``get_engine()`` to obtain a SQLAlchemy engine that
points at the *data* database (not the Airflow metadata DB).
"""

import os
from sqlalchemy import create_engine


def get_engine():
    """Return a SQLAlchemy engine for the project data database."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ.get("POSTGRES_DB", "bank")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
