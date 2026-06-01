-- Fix ariregister config: the previous entry pointed at a non-existent REST
-- endpoint. The register exposes company data as a daily-refreshed public bulk
-- CSV download; no authentication is required.
UPDATE data_sources SET config = '{
  "api_url":   "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
  "docs_url":  "https://avaandmed.ariregister.rik.ee",
  "protocol":  "Bulk CSV download (ZIP)",
  "page_size": null,
  "fields":    ["name", "country", "registration_number", "status"],
  "auth_env":  null,
  "notes":     "Daily open-data ZIP from the Estonian Business Register. No auth required. Contains all registered companies. Incremental sync is not supported — each crawl downloads the full dataset (~5-15 MB compressed)."
}'::jsonb WHERE name = 'ariregister';
