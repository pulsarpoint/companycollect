CREATE DATABASE IF NOT EXISTS corpscout;

-- Only the exact original official-website question belongs to the seeded search.
-- Defaults also make existing answers available to its normal freshness check.
ALTER TABLE corpscout.company_brave_search_results
    ADD COLUMN search_id String DEFAULT if(query_type = 'official_website' AND query = concat('Find the official website of ', company_name, '.'), '54d90187-85d5-45dc-9603-cd7c4a7d31d1', ''),
    ADD COLUMN search_revision UInt32 DEFAULT if(search_id = '54d90187-85d5-45dc-9603-cd7c4a7d31d1', 1, 0),
    ADD COLUMN search_name String DEFAULT if(search_id = '54d90187-85d5-45dc-9603-cd7c4a7d31d1', 'Official website', '');
