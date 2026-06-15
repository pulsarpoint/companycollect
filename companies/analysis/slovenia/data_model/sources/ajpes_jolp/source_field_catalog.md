# AJPES JOLP — Annual Reports Field Catalog

> **PLANNING-ONLY / VIEW-ONLY.** JOLP publishes filed annual reports **free to
> view per company**, but there is **no open bulk download or API** and the reuse
> terms differ from the CC-BY open datasets. Cataloged from public docs; no data
> retrieved; no scraping.

## Source Summary

- Country: Slovenia
- Source type: official_registry
- Organization: AJPES
- URL: https://www.ajpes.si/jolp/
- License: public view; reuse terms unclear
- Access: public (view-only)
- Freshness: annual (~last 5 business years)
- Record shape: per-company web pages / PDF
- Primary keys: `Matična številka`
- Join keys: `Matična številka`

## Fields (from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| bilanca_stanja | Bilanca stanja | Balance sheet | object | financial | planning-only; EUR |
| izkaz_poslovnega_izida | Izkaz poslovnega izida | Income statement | object | financial | planning-only; EUR |
| leto | poslovno leto | Fiscal year | integer | date | planning-only; ~5 yrs |
| letno_porocilo | Letno poročilo | Annual report doc | document | document | planning-only; HTML/PDF |

## Interpretation Notes

- The **only free route to financials** in Slovenia, but **view-only** per
  company (balance sheet + income statement, ~last 5 years). **No open
  bulk/API** — do not mass-scrape; treat as planning-only.
- For structured/bulk financials, the paid **Fi=Po** product is the route
  (see `ajpes_fipo`). Join on Matična številka.
