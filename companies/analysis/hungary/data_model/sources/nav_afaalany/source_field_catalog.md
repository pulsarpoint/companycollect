# NAV VAT-subjects database (Áfaalanyok adatbázis) Field Catalog

## Source Summary

- Country: Hungary
- Source type: official_tax
- Organization: Nemzeti Adó- és Vámhivatal (NAV)
- URL: https://nav.gov.hu/adatbazisok/adatbleker/afaalanyok (single + group/batch query)
- License: public (NAV közadat)
- Access: public
- Freshness: daily
- Record shape: query/batch result per adószám; some downloadable lists (CSV)
- Primary keys: `adoszam`
- Join keys: `adoszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| adoszam | adószám | Tax number | string | identifier | (not copied) | 8-digit base = stem |
| name | név | Taxpayer name | string | legal_name | (not copied) | cross-check |
| afa_status | áfaalany státusz | VAT status | string | status | (not copied) | daily |
| tax_number_cancelled | adószám törlés | Tax number cancelled | boolean | status | (not copied) | risk flag |

## Interpretation Notes

- **Tax/VAT validation, daily.** NAV publishes the VAT-subjects (áfaalanyok) database, updated **daily**, with
  a **single** lookup and a **group/batch** query (upload a list of adószám → statuses). Some lists are
  downloadable (CSV, e.g. excise subjects). Use to **validate/enrich** adószám ↔ name ↔ VAT status and to flag
  **cancelled tax numbers** (a strong distress/dissolution signal complementing the register status).
- Not a company master; enrichment keyed on **adószám** (8-digit base join stem).
