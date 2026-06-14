# DRCIP — Registrar of Companies (open CSV + eSearch) Field Catalog

## Source Summary

- Country: Cyprus
- Source type: official_registry
- Organization: Department of Registrar of Companies and Intellectual Property (DRCIP)
- URL: https://www.companies.gov.cy/en/ (open CSV via https://www.data.gov.cy/en/group/30 ; free eSearch https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx)
- License: Open data (data.gov.cy — confirm exact licence per dataset)
- Access: public (free)
- Freshness: regular open-data refresh; eSearch real-time
- Record shape: one row per organisation (companies CSV); officers in a related CSV keyed by `registration_number`
- Primary keys: `registration_number` (HE…)
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| registration_number | registration_number | Company id; prefix encodes entity type (HE=company) | string | identifier | HE123456 | join key |
| name | name | Registered organisation name (Greek/English) | string | legal_name | Example Holdings Ltd | both scripts where available |
| type | type | Entity type (company/business name/partnership/overseas) | string | legal_form | company | correlates with prefix |
| status | status | Lifecycle status | string | status | operational | normalise on ingest |
| registration_date | registration_date | Registration / incorporation date | date | date | 2014-03-10 | format unconfirmed |
| registered_address | registered_address | Registered office address | string | address | 1 Example Street, 1010 Nicosia | parse municipality/district |
| officers[].name | officers | Director/secretary name [PII] | string | person | John Doe | GDPR; not shareholders |
| officers[].role | officers | Officer role | string | relationship | director / secretary | normalise vocabulary |

## Interpretation Notes

- **Single join key.** The DRCIP `registration_number` (the `HE…` form for companies) is the spine for every
  other Cyprus source. The **prefix encodes the entity type** (HE = limited company, BN = business name,
  EE = partnership, AE = overseas) — documented behaviour, not a published code table.
- **Bilingual names.** Names are held in Greek and/or English. Keep both forms where present; do not assume
  one canonical script.
- **Officers, not shareholders.** The open set **names officers** (directors and the secretary) but does
  **not** include shareholders or beneficial owners. Officer coverage was confirmed via the OpenSanctions
  `cy_companies` mirror (~567,536 companies, ~2.75M entities). Officer names are **personal data** — apply a
  GDPR lawful basis and a retention policy before persisting, and do not reuse for direct marketing.
- **No activity code.** The open register carries no public NACE/activity code — `activity_code` is
  `not_available` for Cyprus from open data.
- **Address is free text.** Expect a single registered-office line; municipality and district
  (Nicosia/Limassol/Larnaca/Paphos/Famagusta) must be parsed, not read from dedicated columns.
- **Transport caveat.** The exact data.gov.cy CSV resource URL was not resolved during discovery
  (non-standard CKAN path: `/api/3/action/*` → HTTP 404; group page JS-rendered). Field semantics above come
  from the documented eSearch/CSV schema and the OpenSanctions mirror, not from a downloaded file.
- A `sample_record.json` is included as a **schematic** illustration (placeholder values, not a real company),
  because no per-company open record was downloadable here.
