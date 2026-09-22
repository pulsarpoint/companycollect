CREATE DATABASE IF NOT EXISTS corpscout;

-- S3 files are unaffected by dropping their external table definition.
-- Disable the query condition cache for correct filtered reads on ClickHouse 26.5.1.
DROP TABLE IF EXISTS corpscout.se_company_brave_search_results_s3_archive;

CREATE OR REPLACE VIEW corpscout.se_company_brave_search_results_s3_archive SQL SECURITY INVOKER AS
SELECT *, _path FROM s3(brave_history,
    filename='v1/country=SE/batch_id=*/*.parquet', format='Parquet',
    structure='result_id String, task_id String, input_id String, work_key String, attempt UInt32, status LowCardinality(String), export_batch_id String, country_code LowCardinality(String), company_id String, company_name String, query String, query_type LowCardinality(String), processor_version LowCardinality(String), answer_text String, route LowCardinality(String), source_url String, error_type LowCardinality(String), source_run_id String, completed_at DateTime64(6, \'UTC\')')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;
