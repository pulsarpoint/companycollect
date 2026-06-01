-- Populate config with API metadata for each known source.
UPDATE data_sources SET config = '{
  "api_url":   "https://api.gleif.org/api/v1/lei-records",
  "docs_url":  "https://www.gleif.org/en/lei-data/gleif-api",
  "protocol":  "REST/JSON",
  "page_size": 200,
  "fields":    ["name", "country", "lei", "status"],
  "auth_env":  null,
  "notes":     "Global LEI database. Supports incremental sync via filter[lastUpdateTime]. No auth required."
}'::jsonb WHERE name = 'gleif';

UPDATE data_sources SET config = '{
  "api_url":   "https://query.wikidata.org/sparql",
  "docs_url":  "https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service",
  "protocol":  "SPARQL",
  "page_size": 500,
  "fields":    ["name", "country", "website"],
  "auth_env":  null,
  "notes":     "SPARQL query for company entities (Q4830453, Q783794, Q891723). No date-based incremental sync; always full scan."
}'::jsonb WHERE name = 'wikidata';

UPDATE data_sources SET config = '{
  "api_url":   "https://api.opencorporates.com/v0.4/companies/search",
  "docs_url":  "https://api.opencorporates.com/documentation/API-Reference",
  "protocol":  "REST/JSON",
  "page_size": 100,
  "fields":    ["name", "country", "registration_number", "status", "lei"],
  "auth_env":  "CRAWLER_OPENCORPORATES_API_KEY",
  "notes":     "Global company aggregator. API key is optional but strongly recommended — anonymous rate limits are very low."
}'::jsonb WHERE name = 'opencorporates';

UPDATE data_sources SET config = '{
  "api_url":   "https://api.company-information.service.gov.uk/advanced-search/companies",
  "docs_url":  "https://developer.company-information.service.gov.uk/api/docs/",
  "protocol":  "REST/JSON",
  "page_size": 100,
  "fields":    ["name", "country", "registration_number", "status"],
  "auth_env":  "COMPANIES_HOUSE_API_KEY",
  "notes":     "UK Companies House official registry. API key required (HTTP Basic auth, key as username, empty password). Without key returns empty results."
}'::jsonb WHERE name = 'companies_house';

UPDATE data_sources SET config = '{
  "api_url":   "https://data.brreg.no/enhetsregisteret/api/enheter",
  "docs_url":  "https://data.brreg.no/enhetsregisteret/api/",
  "protocol":  "REST/JSON",
  "page_size": 200,
  "fields":    ["name", "country", "registration_number", "status", "website"],
  "auth_env":  null,
  "notes":     "Norwegian Entity Register (Brønnøysundregistrene). Public open data, no auth required. Supports date filter via fraRegistreringsdato."
}'::jsonb WHERE name = 'brreg';

UPDATE data_sources SET config = '{
  "api_url":   "https://cvrapi.dk/api",
  "docs_url":  "https://cvrapi.dk/documentation",
  "protocol":  "REST/JSON",
  "page_size": 100,
  "fields":    ["name", "country", "registration_number", "status", "website"],
  "auth_env":  "CVR_API_TOKEN",
  "notes":     "Danish Central Business Register. API token required. API does not return a total record count."
}'::jsonb WHERE name = 'cvr';

UPDATE data_sources SET config = '{
  "api_url":   "https://ariregister.rik.ee/api/1/",
  "docs_url":  "https://ariregister.rik.ee/eng/api",
  "protocol":  "REST/JSON",
  "page_size": 200,
  "fields":    ["name", "country", "registration_number", "status"],
  "auth_env":  null,
  "notes":     "Estonian Business Register (Ariregister). Public open data, no auth required. Uses offset-based pagination."
}'::jsonb WHERE name = 'ariregister';
