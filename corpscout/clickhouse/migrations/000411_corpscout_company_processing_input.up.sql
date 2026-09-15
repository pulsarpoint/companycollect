CREATE DATABASE IF NOT EXISTS corpscout;

-- Populate a fixed selection with INSERT SELECT before starting its task.
-- Retain this queue unchanged until processing and any retries have finished.
CREATE TABLE IF NOT EXISTS corpscout.company_processing_input
(
    input_id String,
    company_id String,
    company_name String,
    country_code LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY input_id;
