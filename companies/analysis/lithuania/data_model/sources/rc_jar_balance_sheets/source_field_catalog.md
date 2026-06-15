# Registrų centras JAR — Balance sheets (BalansoAtaskaita) Field Catalog

## Source Summary

- Country: Lithuania
- Source type: financial_statements
- Organization: Registrų centras via data.gov.lt
- URL: https://get.data.gov.lt/datasets/gov/rc/jar/balanso_ataskaitos/BalansoAtaskaita
- License: CC-BY 4.0 (open data)
- Access: public, **no API key**
- Freshness: annual filings (regularly updated)
- Record shape: Spinta JSON rows — **one row per balance-sheet line item**
- Primary keys: `_id` (Spinta UUID per line)
- Join keys: `juridinis_asmuo._id` (→ company)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| juridinis_asmuo._id | juridinis_asmuo | Company ref | string | identifier | 6e956d29-… | → JuridinisAsmuo._id |
| template_id / template_name | template_* | Statement set | string | metadata | FS0422 | reporting regime |
| standard_id / standard_name | standard_* | Statement standard | string | metadata | BST124 / BALANSAS (Sutrumpintas) | balance sheet |
| line_type_id | line_type_id | Line code | string | financial | BSLT00021 | stable account code |
| line_name | line_name | Line name | string | financial | TRUMPALAIKIS TURTAS | = current assets |
| reiksme | reiksme | Value (EUR) | decimal | financial | 13532 | amount |
| laikotarpis_nuo / iki | laikotarpis_* | Period from/to | date | date | 2023-01-01 / 2023-12-31 | fiscal period |
| reg_date | reg_date | Filing date | date | date | 2024-04-26 | when filed |

## Interpretation Notes

- **Verified from real data**: line item `TRUMPALAIKIS TURTAS` (current assets)
  `reiksme` 13532, period 2023-01-01…2023-12-31, standard `BALANSAS (Sutrumpintas)`.
- **Granularity**: each row is **one balance-sheet account line**, not a whole
  statement. To build a company's balance sheet, **group rows by
  `juridinis_asmuo._id` + period (`laikotarpis_nuo`/`iki`)** and pivot on
  `line_type_id` / `line_name`.
- **Join to company**: resolve `juridinis_asmuo._id` against `JuridinisAsmuo._id`
  to get `ja_kodas` (the 9-digit company code).
- **Currency**: **EUR**. **Reporting regime**: `template_name` / `standard_name`
  indicate the statement set (full / abbreviated / micro / small-partnership), so
  the available line items vary by regime.
- **Coverage**: depends on filing compliance; the `fa_veluojantys` /
  `fa_dokumentu_nepateike` models list late / non-filers.
- Keyless Spinta API; cursor pagination via `_page.next`.
