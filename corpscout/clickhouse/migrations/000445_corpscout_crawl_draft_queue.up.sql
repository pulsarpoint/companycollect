-- Domain membership remains the queue. Store the selected URL and provenance here,
-- separately from recurring per-domain presets. Legacy rows keep empty values.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.website_crawl_task_domains
    ADD COLUMN IF NOT EXISTS website_url String DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_name String DEFAULT '',
    ADD COLUMN IF NOT EXISTS submission_id String DEFAULT '';
ALTER TABLE corpscout.website_crawl_task_domains
    MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation=1;
