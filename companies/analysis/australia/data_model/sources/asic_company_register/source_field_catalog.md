# ASIC Company Register (ASIC Connect) Field Catalog

> **PLANNING-ONLY / PAID.** The authoritative company register, but extracts are
> bought per item via ASIC Connect. Cataloged from public docs; no records
> retrieved. Officeholders are personal data. (ASIC also publishes some FREE
> datasets on data.gov.au — e.g. Business Names register — but not the company
> register detail.)

## Source Summary

- Country: Australia
- Source type: official_registry
- Organization: ASIC
- URL: https://asic.gov.au/online-services/search-asic-registers/company-and-organisation-registers/
- License: paid extracts
- Access: paid (per extract)
- Freshness: real-time
- Record shape: planning-only
- Primary keys: `ACN`
- Join keys: `ACN`, `ABN`

## Fields (the open gaps)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| ACN | ACN | Company number | string | identifier | join to ABR ASICNumber |
| registered_office_address | registered office | Full street address | address | address | planning-only; ABR lacks it |
| date_of_registration | registration date | Incorporation date | date | date | planning-only; ABR lacks it |
| company_status_type | status/type/class | ASIC status + type | string | status | planning-only; precise status |
| officeholders[] | officeholders | Directors/secretaries | array | person | planning-only; PII |

## Interpretation Notes

- Documents the fields the **open ABR extract lacks**: **full registered-office
  address**, **incorporation date**, precise **company status/type**, and
  **officeholders** — all **paid** via ASIC Connect. Join on **ACN** (= ABR
  `ASICNumber`). Keep planning-only; redact officeholder PII.
