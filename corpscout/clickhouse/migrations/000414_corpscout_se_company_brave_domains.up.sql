CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE corpscout.se_company_brave_domains
(
    result_id String,
    task_id String,
    input_id String,
    work_key String,
    attempt UInt32,
    status LowCardinality(String),
    export_batch_id String,
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    query String,
    query_type LowCardinality(String),
    processor_version LowCardinality(String),
    answer_text String,
    route LowCardinality(String),
    source_url String,
    error_type LowCardinality(String),
    source_run_id String,
    completed_at DateTime64(6, 'UTC'),
    archive_path String,
    CONSTRAINT swedish_company CHECK country_code = 'SE',
    CONSTRAINT successful_answer CHECK status = 'success'
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (company_id, query_type);

-- Full immutable responses live in S3. Provision brave_history before this migration.
CREATE TABLE corpscout.se_company_brave_domains_history
(
    result_id String,
    task_id String,
    input_id String,
    work_key String,
    attempt UInt32,
    status LowCardinality(String),
    export_batch_id String,
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    query String,
    query_type LowCardinality(String),
    processor_version LowCardinality(String),
    answer_text String,
    route LowCardinality(String),
    source_url String,
    error_type LowCardinality(String),
    source_run_id String,
    completed_at DateTime64(6, 'UTC')
)
ENGINE = S3(brave_history, filename='v1/country=SE/batch_id=*/*.parquet', format='Parquet');
