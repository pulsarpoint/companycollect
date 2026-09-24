SELECT throwIf(count() > 0, 'webtech_scan_input must be empty before its layout changes')
FROM corpscout.webtech_scan_input;

DROP TABLE IF EXISTS corpscout.webtech_scan_input;

CREATE TABLE corpscout.webtech_scan_input
(
    task_id String,
    input_id String,
    root_domain String,
    website_origin String,
    page_url String,
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(root_domain) % 128),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    INDEX task_id_index task_id TYPE set(0) GRANULARITY 1,
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND length(input_id) = 64,
    CONSTRAINT valid_target CHECK notEmpty(root_domain)
        AND protocol(page_url) IN ('http', 'https') AND notEmpty(domain(page_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
