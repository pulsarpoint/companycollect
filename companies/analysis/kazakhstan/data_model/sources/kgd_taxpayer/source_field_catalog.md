# State Revenue Committee (KGD) — taxpayer search / lists Field Catalog

## Source Summary

- Country: Kazakhstan
- Source type: tax_register
- Organization: State Revenue Committee, Ministry of Finance (kgd.gov.kz)
- URL: https://kgd.gov.kz/ru
- License: restricted
- Access: **browser-public search + published lists** (some downloadable XLSX)
- Freshness: periodic
- Record shape: per-BIN search result + published lists
- Primary keys: bin_iin
- Join keys: bin_iin, taxpayer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| bin_iin | БИН/ИИН | BIN (legal) / IIN (individual) | string | identifier |  | **join key (BIN) to gbd_ul** |
| taxpayer_name | Наименование налогоплательщика | Taxpayer name | string | legal_name |  | individuals = personal data |
| vat_registration | Регистрация по НДС | VAT status | string | status |  | НДС = VAT |
| taxpayer_status | Статус налогоплательщика | Taxpayer status | string | status |  | from KGD lists |

## Interpretation Notes

- The **State Revenue Committee** (`kgd.gov.kz`) hosts a **taxpayer search** and publishes
  **lists** — VAT (НДС) payers, **inactive** taxpayers, **pseudo-enterprises**, tax-debtors,
  deregistered — browser-public, some as downloadable **XLSX**. Lookup by **BIN/IIN** returns
  taxpayer name and VAT/status. No single clean open API; per-search or per-list. Documented
  from the public services; no per-BIN values were captured.
- **Identifier**: **BIN** (legal entities) joins to `gbd_ul`; **IIN** for individuals.
  **`taxpayer_status`** is **tax status** (active/inactive/etc.), not company registration
  status — keep distinct from `gbd_ul` registration data. **`vat_registration`** indicates VAT
  registration. **Language**: Russian + Kazakh.
- **Personal data**: individual taxpayers (IIN, name) are personal data — redact individuals.
- No `sample_record.json`: per-BIN browser search / list, nothing captured.
