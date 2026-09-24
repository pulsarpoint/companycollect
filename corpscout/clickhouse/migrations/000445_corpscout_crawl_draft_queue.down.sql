ALTER TABLE corpscout.website_crawl_task_domains
    DROP COLUMN IF EXISTS website_url,
    DROP COLUMN IF EXISTS source_name,
    DROP COLUMN IF EXISTS submission_id;
ALTER TABLE corpscout.website_crawl_task_domains
    RESET SETTING number_of_free_entries_in_pool_to_execute_mutation;
