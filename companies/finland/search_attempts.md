# Finland — Search Attempts Log

## Attempt 1

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `Finland company register API PRH Business Information System open data`
- Language: English
- Why: locate the official national company registry and its API.
- Top relevant URLs:
  - https://avoindata.prh.fi/en  (PRH Open Data)
  - https://www.prh.fi/en/companiesandorganisations/tietopalvelut/prhopendata.html
  - https://avoindata.suomi.fi/data/en_GB/dataset/prh-avoin-data
  - https://www.ytj.fi/en/index/opendata.html
- Result: Identified PRH as the official registry; open data via avoindata.prh.fi; daily updates; no email/phone.
- Decision: Treat PRH Open Data as primary candidate; dig into the API.

## Attempt 2

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `avoindata.fi yritykset open data company register bulk download`
- Language: English
- Why: find a bulk (CSV/JSON) download of the full register.
- Top relevant URLs:
  - https://www.avoindata.fi/data/en_GB/dataset/yritykset
  - https://www.avoindata.fi/data/en_GB/dataset/yritykset/resource/ac409ad1-3183-4174-9317-a13c16511ab1 (full_prh_data.csv)
  - https://avoindata.prh.fi/en/ytj/swagger-ui
- Result: Found references to a monthly `full_prh_data.csv` and to fetching all companies as JSON via the API/Swagger.
- Decision: Verify both the CSV resource and the API directly.

## Attempt 3

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `Finland kaupparekisteri avoin data API yritystietojärjestelmä YTJ` (local language)
- Language: Finnish
- Why: confirm local terminology and find Finnish-language portal pages.
- Top relevant URLs:
  - https://avoindata.prh.fi/fi , https://avoindata.prh.fi/ytj.html
  - https://avoindata.prh.fi/en/ytj/swagger-ui
  - https://avoindata.suomi.fi/data/fi/dataset/yritykset
- Result: Confirmed YTJ = joint PRH + Tax Administration system; API returns all registered + pending companies as JSON; daily; sole traders excluded.
- Decision: Test the v3 API endpoints directly with curl.

## Attempt 4 (direct API verification)

- Date/time: 2026-06-06T13:41Z
- Source: curl against `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
- Why: confirm the API is live, key-less, and inspect schema + pagination.
- Result:
  - `?totalResults=true&maxResults=1` → HTTP 200, `totalResults: 819096`, returned 100 companies (page size fixed at 100).
  - `?businessId=0100002-9` → HTTP 200, full single record (2 KB).
  - `?page=2` → HTTP 200, next 100 companies.
  - `/v3/api-docs`, `/v3/swagger-config` → HTTP 404 (raw OpenAPI not exposed at those paths).
- Decision: API is the canonical bulk + lookup path. Saved samples to `raw/api/`.

## Attempt 5 (portal metadata + bulk CSV check)

- Date/time: 2026-06-06T13:41Z
- Source: avoindata.suomi.fi CKAN action API
- Why: read machine-readable dataset/resource metadata; confirm CSV bulk existence and license.
- Result:
  - `package_show?id=yritykset` → HTTP 200; license **CC-BY-4.0**; describes daily JSON via API.
  - `package_show?id=prh-avoin-data` → HTTP 200; license **CC-BY-4.0**; resource = PRH API.
  - `resource_show?id=ac409ad1-...` (legacy full_prh_data.csv) → **HTTP 404** (removed).
  - Human portal pages (`/data/en_GB/...`) return HTTP 403 to fetchers; use CKAN API instead.
- Decision: Drop the legacy CSV; standardize on the PRH JSON API. License confirmed CC-BY-4.0.
