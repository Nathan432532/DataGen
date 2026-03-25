-- =============================================================
-- Database initialisation — executed once on first container start
-- =============================================================

-- Airflow metadata schema
CREATE SCHEMA IF NOT EXISTS airflow;
GRANT ALL ON SCHEMA airflow TO PUBLIC;

-- Set default search path so Airflow finds its schema
ALTER DATABASE bank SET search_path TO airflow, public;

-- Data-layer schemas
CREATE SCHEMA IF NOT EXISTS raw;
GRANT ALL ON SCHEMA raw TO PUBLIC;
