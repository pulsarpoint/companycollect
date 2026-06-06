# Finland — Company Open Data Investigation

Date: 2026-06-06
Country: Finland (FI)
Languages considered: Finnish, Swedish, English

## Summary

Finland has one of the cleanest official company open-data setups in Europe.
The single authoritative source is the **Finnish Patent and Registration Office (PRH)**,
which — together with the Tax Administration — operates the **Business Information System
(YTJ, *Yritys- ja yhteisötietojärjestelmä*)**. PRH publishes the whole Trade Register
as open data under CC-BY-4.0 through a public, key-less REST API at `avoindata.prh.fi`.

No scraping is needed. No authentication is needed. The data is official, structured,
multilingual (fi/sv/en descriptions embedded), and refreshed daily.

## What was found

### 1. PRH Open Data — YTJ API v3 (RECOMMENDED)

- Base: `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
- Verified live during this investigation:
  - `?totalResults=true&maxResults=1` → `{"totalResults":819096, ...}`
  - `?businessId=0100002-9` → single full company record (HTTP 200)
  - `?page=2` → next page (HTTP 200)
- Page size is fixed at **100 records/page**; `maxResults` is effectively ignored
  (a request for 5 still returned 100). Iterate with `page=1..N`.
- Total register ≈ **819,096** entities → ≈ **8,191 pages** for a full crawl.
- No auth, no registration, no API key observed.
- Daily updates (per PRH).
- Swagger UI exists at `https://avoindata.prh.fi/en/ytj/swagger-ui` (human page;
  the raw OpenAPI JSON was not exposed at the common `/v3/api-docs` path — 404).

Other PRH API surfaces referenced by the portal (not exhaustively tested here):
- Registered notifications API (*kaupparekisterin rekisteröidyt ilmoitukset*)
- Digital financial statement information API

### 2. avoindata.suomi.fi — national open data portal (catalog/metadata)

- Dataset "Yritys- ja yhteisötietojärjestelmän (YTJ) avoimet tiedot JSON-tiedostona"
  (`/data/.../dataset/yritykset`) — points to the PRH JSON API. License **CC-BY-4.0**.
- Dataset "API: YTJ-tiedot kaupparekisteriin merkityistä yrityksistä"
  (`/data/.../dataset/prh-avoin-data`) — the API dataset. License **CC-BY-4.0**.
- The portal's human pages return HTTP 403 to automated fetchers, but the
  **CKAN action API** (`/data/api/3/action/package_show?id=...`) works and is the
  machine-readable way to read dataset/resource metadata. Note the canonical host is
  now `avoindata.suomi.fi` (old `avoindata.fi` 301-redirects there).

### 3. Legacy CSV bulk dump (NO LONGER AVAILABLE)

- Older documentation and search results reference a single `full_prh_data.csv`
  (community-assembled monthly dump) under avoindata.fi dataset `yritykset`,
  resource `ac409ad1-3183-4174-9317-a13c16511ab1`.
- `resource_show` for that id now returns **HTTP 404** — the resource was removed
  during the portal migration. Do not rely on it; use the API.

## What was NOT found / limitations

- No open data for **sole traders / private entrepreneurs** (*toiminimi*).
- No **email / phone** contact data in open data.
- Municipalities, wellbeing services counties, and tax partnerships are excluded.
- No official single-file "download everything" archive currently published; the
  full-register bulk path is the paginated API.

## Recommendation

Ingest via the PRH YTJ API v3:
1. **Initial backfill** — crawl `?page=1..8191` (100/page), persist raw JSON per page.
2. **Daily incremental** — re-crawl or use a date filter to capture daily changes.
3. Normalize to the internal company model (`schema_notes.md`).
4. Carry CC-BY-4.0 attribution ("Source: Finnish Patent and Registration Office").

For richer firmographics (financials), the digital financial statement API is a
useful secondary source once the base register is loaded.
