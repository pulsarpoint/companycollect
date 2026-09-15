-- Apply after old Brave workers finish. Responses remain until verified archival.
ALTER TABLE processing.export_batches
    ADD COLUMN archive_manifest jsonb,
    ADD COLUMN archived_at timestamptz;

CREATE INDEX processing_unarchived_batches ON processing.export_batches (task_id, created_at)
    WHERE archived_at IS NULL;

UPDATE processing.export_batches SET destination='country_brave_domains_v1'
WHERE destination='company_brave_info_v1';
