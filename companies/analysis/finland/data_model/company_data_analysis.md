# Company Data Analysis For Finland

## Summary

Finland supports a **rich, single-source company profile** built entirely from one
official, free, daily-updated, CC-BY-4.0 API: the **PRH Open Data YTJ API v3**
(Business Information System, jointly run by the Finnish Patent and Registration
Office and the Tax Administration). From this one source we can populate national
identifiers, current + historical names and legal forms, trade-register status,
industry classification, bilingual addresses with municipality codes, website, and —
notably — **tax registration flags** (VAT / employer / prepayment register) derived
from the `registeredEntries` array.

The profile is genuinely country-shaped: it is organized around the **Y-tunnus
(Business ID)**, the **Trade Register status**, and the Finnish **Tax Administration
sub-registers**. It deliberately does **not** include officers, beneficial owners, or
financial figures, because those are not in this endpoint.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| prh_ytj_v3 | PRH Open Data — YTJ API v3 (companies) | recommended | public, no auth | CC-BY-4.0 | **Primary — everything in the profile** |
| prh_financial_statements | PRH digital financial statement API | useful_secondary | public | CC-BY-4.0 | Planning-only — financials, not yet fetched |
| avoindata.suomi.fi datasets | National open-data catalog | useful_secondary | public | CC-BY-4.0 | Metadata/license confirmation only |
| legacy full_prh_data.csv | Community CSV dump | unavailable (404) | — | CC-BY-4.0 | Skipped — removed from portal |

Only `prh_ytj_v3` carries company field data and was cataloged in full. The financial
statement API is referenced as a planning-only future source (no records fetched, so
no fields invented).

## What Each Source Contributes

- **prh_ytj_v3 (companies endpoint)** — the entire profile: Business ID + BRIS EUID,
  name history (primary/auxiliary), legal form history, TOL/NACE activity,
  visiting/postal addresses with Statistics Finland municipality codes, registered
  website, trade-register status, and the register-entry history that yields
  VAT/employer/prepayment registration flags. Daily `lastModified` enables delta
  ingestion.
- **prh_financial_statements (planning-only)** — would add `financial_statements[]`
  (digital financial statement data) keyed by Business ID. Not analyzed here; no
  fields asserted.

## Proposed Country Company Profile

`country_company_profile.schema.json` (JSON Schema, Draft 2020-12) defines these
top-level sections, each carrying `x-source`/`x-source-path` provenance:

- `registration` — business_id, business_id_registration_date, eu_id, derived vat_id
- `legal_identity` — current legal_name, auxiliary_names, full name_history, legal_form
- `status` — **is_active (derived from tradeRegisterStatus)**, trade_register_status_code,
  raw_status_code (kept verbatim), incorporation/dissolution dates, special_situations
- `activity` — TOL/NACE code, code_set, bilingual labels
- `addresses[]` — visiting/postal, street, postcode, bilingual city, municipality_code
- `tax_registrations` — derived vat / employer / prepayment_register flags
- `register_entries[]` — raw register-entry history preserved verbatim
- `online_presence` — website (only contact-type field available)
- `financial_statements[]` — **planning-only** placeholder for the future API
- `record_metadata.last_modified`, `source_provenance[]`

`country_company_profile.example.json` is a real worked example for **Dynava Oy
(0100130-4)** — an active, VAT-registered limited company with 10 names, two
addresses, and a website — produced directly from the saved raw record.

## Join And Precedence Rules

- **Primary key / join key:** `business_id` (Y-tunnus). Secondary cross-EU key:
  `eu_id` (BRIS EUID, `FIFPRO.<business_id>`). Geography join key:
  `addresses[].municipality_code` (Statistics Finland).
- **Single authoritative source**, so there are no cross-source value conflicts.
- **Liveness must be derived from `tradeRegisterStatus`** (1=active, 4=ceased,
  3=intermediate), confirmed by null `endDate`. The raw `status` field is a constant
  `'2'` for both active and ceased entities and must never be used as a liveness flag —
  this is the single most important interpretation pitfall in the dataset.
- **Current vs historical:** for names and forms, pick the array element with a null
  `endDate` (latest `registrationDate` if several).
- **Tax flags:** "registered" = a `registeredEntries` element in that register
  (5/6/7) with a null `endDate`.
- **Freshness:** daily; use `lastModified` for incremental crawls.

## Missing Or Restricted Data

Not available from the open companies endpoint:

- **Officers / board / representatives** — not present.
- **Beneficial owners** — not present in PRH open data.
- **Financial figures / share capital** — only via the separate PRH digital financial
  statement API (planning-only here).
- **Email / phone** — explicitly excluded from open data (website *is* available).
- **Sole traders (toiminimi)** — excluded from open data entirely.

Restricted/paid sources: none required — the official source is fully open. No data
was taken from any restricted source.

Confidence caveats:

- `companySituations` (bankruptcy/liquidation) was empty across the whole sample, so
  its sub-schema is unconfirmed — verify on a known-distressed entity before relying
  on it.
- Numeric `register`/`authority`/`source` code meanings are inferred from sample
  descriptions; confirm against official PRH code lists before hard-coding.

## Common Mapper Notes

A future cross-country mapper can map Finland cleanly for `company_id`,
`registration_number`, `tax_id`, `vat_id`, `legal_name`, `status`, `legal_form`,
`incorporation_date`, `dissolution_date`, `registered_address`, and `activity_code`
(TOL→NACE crosswalk). It must mark `officers`, `owners`, and `financials` as
`not_available_in_open_sources` for the companies endpoint (financials pending the
financial-statement API). See `common_field_mapping_suggestions.md`. That file is a
suggestion layer only and does not constrain Finland's country-specific profile.
