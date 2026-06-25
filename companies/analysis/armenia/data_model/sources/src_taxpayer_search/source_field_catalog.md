# State Revenue Committee (SRC) — taxpayer search Field Catalog

## Source Summary

- Country: Armenia
- Source type: tax_register
- Organization: State Revenue Committee of the Republic of Armenia (SRC)
- URL: https://www.src.am/en/search
- License: restricted
- Access: **browser-public per-TIN search** (no bulk/API)
- Freshness: live
- Record shape: per-TIN search result (browser-public)
- Primary keys: tin_hvhh
- Join keys: tin_hvhh, taxpayer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| tin_hvhh | ՀՎՀՀ (TIN) | 8-digit taxpayer id | string | identifier |  | **join key to register** |
| taxpayer_name | Հարկ վճարողի անվանում | Taxpayer name | string | legal_name |  | individuals = personal data |
| taxpayer_status | Կարգավիճակ | Active/inactive | string | status |  | tax status (not registration) |
| vat_status | ԱԱՀ կարգավիճակ | VAT status | string | status |  | VAT registration indicator |

## Interpretation Notes

- The SRC website hosts a **public taxpayer search** (`/en/search`; internal endpoint
  `/searchTaxpayerData`, `/singleSearchResult`). It returns a taxpayer **name** and **status**
  (and VAT status) by **TIN (ՀՎՀՀ)** / name — **browser-public, per-TIN AJAX**, **not** a bulk
  download or documented open API. All fields here are documented from the public search; no
  per-TIN values were captured.
- **Identifier**: the **TIN (ՀՎՀՀ / HVHH, 8-digit)** is the SRC key and the **join key** to the
  State Register. The search covers **companies and individuals** — **individual names are
  personal data** — redact.
- **`taxpayer_status`** is **tax status** (active/inactive taxpayer), not company registration
  status — keep distinct from the State Register `status`.
- No `sample_record.json`: per-TIN browser search, nothing captured.
