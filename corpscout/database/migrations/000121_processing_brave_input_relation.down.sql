UPDATE processing.tasks
SET source_info = jsonb_set(source_info, '{relation}', '"corpscout.company_processing_input"'::jsonb)
WHERE source_info->>'relation' = 'corpscout.company_brave_search_input';

UPDATE processing.tasks
SET config = jsonb_set(config, '{input_relation}', '"corpscout.company_processing_input"'::jsonb)
WHERE config->>'input_relation' = 'corpscout.company_brave_search_input';
