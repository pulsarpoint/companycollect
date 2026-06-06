# Company data sources for Finland

## Status

- Official bulk data: **found** (full register retrievable as JSON via official API; legacy CSV dump removed)
- Official API: **found** (PRH Open Data YTJ API v3 — REST, no auth, no registration)
- Open data portal: **found** (avoindata.suomi.fi / avoindata.prh.fi)
- License: **known** — Creative Commons Attribution 4.0 (CC-BY-4.0)
- Recommended ingestion path: **API (paginated full crawl) + daily incremental**

## Best source

**PRH Open Data — YTJ API v3** (`https://avoindata.prh.fi/opendata-ytj-api/v3/companies`).

The Finnish Patent and Registration Office (PRH) and the Tax Administration jointly run
the Business Information System (YTJ / *Yritys- ja yhteisötietojärjestelmä*). PRH publishes
the full Trade Register as open data through a public REST API:

- Verified live: returns **819,096** companies total.
- No authentication, no API key, no registration.
- Updated **daily**.
- Licensed **CC-BY-4.0** (attribution to PRH required, redistribution allowed).
- Rich nested JSON: business ID (Y-tunnus), names + history, company form,
  industry classification (TOL/NACE), registered entries, addresses, status,
  and digital financial-statement availability.

The full register can be walked via the same API using the `page` parameter
(100 records/page → ~8,191 pages). A separate legacy single-file CSV dump
(`full_prh_data.csv`) that older docs referenced is **no longer available** (404)
after the portal migrated to avoindata.suomi.fi — the API is the current canonical bulk path.

## Excluded from this data

- Private traders / sole proprietors (*toiminimi*) — not in open data
- Email addresses and phone numbers
- Municipalities, wellbeing services counties, tax partnerships

## Next action

Implement a paginated crawler against `…/v3/companies?page=N` to fetch the full
register (~8,191 pages), then a daily delta job. Map the nested JSON to the internal
company model (see `schema_notes.md`). Add "Source: PRH, CC-BY 4.0" attribution.
