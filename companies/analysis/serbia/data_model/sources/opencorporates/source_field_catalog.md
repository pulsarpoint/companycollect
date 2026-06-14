# OpenCorporates — Serbia (register 224) Field Catalog

> **PLANNING-ONLY / RESTRICTED.** Aggregator mirror of APR data with an English
> UI. Search is public; bulk/API is restricted (agreement + paid tier). Cataloged
> from public docs; no records retrieved. Use only as a cross-check — prefer the
> primary APR open data.

## Source Summary

- Country: Serbia
- Source type: aggregator
- Organization: OpenCorporates
- URL: https://opencorporates.com/registers/224
- License: restricted (OpenCorporates terms; bulk requires agreement)
- Access: public (search) / restricted (bulk/API)
- Freshness: varies (may lag APR)
- Record shape: planning-only
- Primary keys: `company_number`
- Join keys: `company_number` (↔ APR maticni_broj)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_number | company_number | Registration number | string | identifier | mirrors maticni_broj |
| name | name | Name | string | legal_name | English/transliterated |
| current_status | current_status | Status | string | status | normalised; may lag |
| officers[] | officers | Officers | array | person | coverage varies; PII |

## Interpretation Notes

- Adds **no authoritative fields** beyond APR (it mirrors APR), plus an English UI
  and occasional officer data. The primary APR open data supersedes it for
  identity/status/financials.
- Keep **planning-only**; its bulk/API terms restrict redistribution.
