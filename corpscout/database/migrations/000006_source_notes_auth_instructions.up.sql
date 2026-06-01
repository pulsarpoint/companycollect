-- Expand source notes with API key registration instructions so administrators
-- know exactly how to obtain credentials when a probe returns an auth error.

UPDATE data_sources SET config = config || '{
  "notes": "UK Companies House official registry. Requires a free API key (HTTP Basic auth: key as username, empty password). Register at https://developer.company-information.service.gov.uk/ — sign in, create an application, and generate a live key. Set env var COMPANIES_HOUSE_API_KEY. Free tier allows 600 req/min, no approval needed."
}'::jsonb WHERE name = 'companies_house';

UPDATE data_sources SET config = config || '{
  "notes": "Danish Central Business Register. Token is optional — unauthenticated requests work but with stricter rate limits. To get a token, register at https://cvrapi.dk/documentation. Set env var CVR_API_TOKEN if you have one. API does not return a total record count."
}'::jsonb WHERE name = 'cvr';
