CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    records_processed INT DEFAULT 0,
    error_message TEXT NULL
);

CREATE TABLE IF NOT EXISTS audit.ingestion_log (
    id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    file_name TEXT NOT NULL,
    landed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count INT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.raw_file_registry (
    id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    object_path TEXT NOT NULL,
    file_format TEXT NOT NULL,
    ingest_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);