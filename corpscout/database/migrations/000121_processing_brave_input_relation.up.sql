-- Apply after ClickHouse migration 000412 while Brave tasks are idle.
-- Membership, the table UUID, admission cursor and saved outcomes are unchanged.
UPDATE processing.tasks
SET source_info = jsonb_set(source_info, '{relation}', '"corpscout.company_brave_search_input"'::jsonb)
WHERE source_info->>'relation' = 'corpscout.company_processing_input';

UPDATE processing.tasks
SET config = jsonb_set(config, '{input_relation}', '"corpscout.company_brave_search_input"'::jsonb)
WHERE config->>'input_relation' = 'corpscout.company_processing_input';
