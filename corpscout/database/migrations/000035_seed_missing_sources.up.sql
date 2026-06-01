-- Seed sources that exist in the crawler but were not included in migration 000017.
-- Uses ON CONFLICT (name) DO UPDATE so this is safe to re-run.
INSERT INTO data_sources (
    name, display_name, description, source_group, input_table_name,
    pull_task_type, processor_task_type, enabled,
    schedule_kind, schedule_expression, config
)
VALUES
    ('cvr',
     'CVR (Denmark)',
     'Danish Central Business Register',
     'registry',
     'cvr_company_raw_inputs',
     'source_pull', 'source_process', false,
     'manual', NULL,
     '{
       "api_url":   "https://cvrapi.dk/api",
       "docs_url":  "https://cvrapi.dk/documentation",
       "protocol":  "REST/JSON",
       "page_size": 100,
       "fields":    ["name", "country", "registration_number", "status", "website"],
       "auth_env":  "CVR_API_TOKEN",
       "notes":     "Danish Central Business Register. API token required. API does not return a total record count."
     }'::jsonb),

    ('ariregister',
     'Ariregister (Estonia)',
     'Estonian Business Register',
     'registry',
     'ariregister_company_raw_inputs',
     'source_pull', 'source_process', false,
     'manual', NULL,
     '{
       "api_url":   "https://ariregister.rik.ee/api/1/",
       "docs_url":  "https://ariregister.rik.ee/eng/api",
       "protocol":  "REST/JSON",
       "page_size": 200,
       "fields":    ["name", "country", "registration_number", "status"],
       "auth_env":  null,
       "notes":     "Estonian Business Register (Ariregister). Public open data, no auth required. Uses offset-based pagination."
     }'::jsonb),

    ('wikidata',
     'Wikidata',
     'Wikidata global company data via SPARQL',
     'other',
     'wikidata_company_raw_inputs',
     'source_pull', NULL, false,
     'manual', NULL,
     '{
       "api_url":   "https://query.wikidata.org/sparql",
       "docs_url":  "https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service",
       "protocol":  "SPARQL",
       "page_size": 500,
       "fields":    ["name", "country", "website"],
       "auth_env":  null,
       "notes":     "SPARQL query for company entities (Q4830453, Q783794, Q891723). No date-based incremental sync; always full scan."
     }'::jsonb),

    ('opencorporates',
     'OpenCorporates',
     'Global company aggregator',
     'other',
     'opencorporates_company_raw_inputs',
     'source_pull', 'source_process', false,
     'manual', NULL,
     '{
       "api_url":   "https://api.opencorporates.com/v0.4/companies/search",
       "docs_url":  "https://api.opencorporates.com/documentation/API-Reference",
       "protocol":  "REST/JSON",
       "page_size": 100,
       "fields":    ["name", "country", "registration_number", "status", "lei"],
       "auth_env":  "CRAWLER_OPENCORPORATES_API_KEY",
       "notes":     "Global company aggregator. API key is optional but strongly recommended — anonymous rate limits are very low."
     }'::jsonb)

ON CONFLICT (name) DO UPDATE SET
    display_name        = EXCLUDED.display_name,
    description         = EXCLUDED.description,
    source_group        = EXCLUDED.source_group,
    input_table_name    = EXCLUDED.input_table_name,
    pull_task_type      = EXCLUDED.pull_task_type,
    processor_task_type = EXCLUDED.processor_task_type,
    schedule_kind       = EXCLUDED.schedule_kind,
    schedule_expression = EXCLUDED.schedule_expression,
    config              = EXCLUDED.config,
    updated_at          = now();
