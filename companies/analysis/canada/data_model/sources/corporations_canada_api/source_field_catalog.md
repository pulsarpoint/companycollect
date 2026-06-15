# Corporations Canada — Real-time API Field Catalog

> **DOCUMENTED-ONLY.** Same OGL data as the bulk dataset, per corporation, plus
> **director names** and corporate history. Cataloged from public docs; no records
> pulled. Director names are personal data — redact.

## Source Summary

- Country: Canada
- Source type: official_registry
- Organization: ISED — Corporations Canada
- URL: https://ised-isde.canada.ca/site/corporations-canada/en/data-services
- License: Open Government Licence – Canada (OGL)
- Access: public
- Freshness: real-time
- Record shape: JSON per corporation number
- Primary keys: `corporationNumber`
- Join keys: `corporationNumber`, `businessNumber`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| directors[] | directors | Director names + addresses | array | person | **PII — redact**; not in bulk |
| status | status | Real-time status | string | status | |
| registeredOfficeAddress | registered office | Current address | string | address | |
| corporateHistory[] | history/filings | Filings/amalgamations | array | filing | |

## Interpretation Notes

- The **real-time per-corporation** counterpart to the bulk dataset (same OGL
  licence, federal corporations only). Its key add over the bulk CSV is **director
  names** (and corporate history). Join on **corporation number**. Treat director
  data per privacy law (PIPEDA) — redact.
