# New York — Active Corporations (data.ny.gov) Field Catalog

## Source Summary

- Country: United States
- Source type: official_registry
- Organization: New York Department of State, Division of Corporations (via data.ny.gov / Socrata)
- URL: https://data.ny.gov/resource/n9v6-gdp6.json (CSV: rows.csv?accessType=DOWNLOAD)
- License: Open data (data.ny.gov terms)
- Access: public (Socrata API + CSV; no key for modest use)
- Freshness: periodic (state refresh)
- Record shape: Socrata JSON / CSV, one row per entity
- Primary keys: `dos_id`
- Join keys: `dos_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| dos_id | dos_id | NY DOS filing id | string | identifier | 4424185 | NY-scoped id |
| current_entity_name | current_entity_name | Name | string | legal_name | BUTCHY'S WINE & SPIRITS, INC. | |
| entity_type | entity_type | Entity type | string | legal_form | DOMESTIC BUSINESS CORPORATION | |
| jurisdiction | jurisdiction | Formation jurisdiction | string | geography | New York / Delaware | |
| initial_dos_filing_date | initial_dos_filing_date | Initial filing | date | date | 2013-06-27 | ≈ registration |
| county | county | County | string | geography | Westchester | |
| dos_process_* | dos_process_* | Service-of-process | string | address | | agent name possible — redact |

## Interpretation Notes

- A second **concrete free/open state registry** (alongside Colorado) — New York's
  Active Corporations via **data.ny.gov Socrata** (verified live: real record DOS
  id 4424185). Demonstrates the per-state pattern for a major state.
- **No principal-office street address** in this dataset (only county + service-of-
  process). `dos_process_*` may carry a **person/agent name** → redact.
- **State-scoped id** (`dos_id`) — there is no national US company id, so NY
  records do not join to other states except by name/fuzzy match.
- Sibling open states with the same pattern: **Washington** (data.wa.gov "Active
  Corporations: Beginning 1800", verified present), **Colorado**, Oregon,
  Connecticut, Iowa, Minnesota. Each is a separate Socrata/CSV endpoint.
- `sample_record.json` is a real company-level NY record (process fields omitted).
