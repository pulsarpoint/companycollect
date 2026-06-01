UPDATE data_sources SET config = '{
  "api_url":   "https://ariregister.rik.ee/api/1/",
  "docs_url":  "https://ariregister.rik.ee/eng/api",
  "protocol":  "REST/JSON",
  "page_size": 200,
  "fields":    ["name", "country", "registration_number", "status"],
  "auth_env":  null,
  "notes":     "Estonian Business Register (Ariregister). Public open data, no auth required. Uses offset-based pagination."
}'::jsonb WHERE name = 'ariregister';
