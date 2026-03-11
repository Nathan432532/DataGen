-- =============================================================
-- Database initialisation — executed once on first container start
-- =============================================================

-- Data-layer schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;

-- Grant access (default user already owns the DB, but be explicit)
GRANT ALL ON SCHEMA raw   TO PUBLIC;
GRANT ALL ON SCHEMA clean TO PUBLIC;
