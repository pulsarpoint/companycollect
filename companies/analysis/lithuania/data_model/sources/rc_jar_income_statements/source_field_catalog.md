# Registrų centras JAR — Profit & loss (PelnoAtaskaita) Field Catalog

## Source Summary

- Country: Lithuania
- Source type: financial_statements
- Organization: Registrų centras via data.gov.lt
- URL: https://get.data.gov.lt/datasets/gov/rc/jar/pelno_ataskaitos/PelnoAtaskaita
- License: CC-BY 4.0 (open data)
- Access: public, **no API key**
- Freshness: annual filings (regularly updated)
- Record shape: Spinta JSON rows — **one row per P&L line item**
- Primary keys: `_id`
- Join keys: `juridinis_asmuo._id` (→ company)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| juridinis_asmuo._id | juridinis_asmuo | Company ref | string | identifier | 85f86af4-… | → JuridinisAsmuo._id |
| template_id / template_name | template_* | Statement set | string | metadata | FS0128 | reporting regime (micro etc.) |
| standard_id / standard_name | standard_* | Statement standard | string | metadata | IST023 / PELNO (NUOSTOLIŲ) ATASKAITA (Trumpa) | P&L |
| line_type_id | line_type_id | Line code | string | financial | ISLT00345 | stable account code |
| line_name | line_name | Line name | string | financial | PARDAVIMO PAJAMOS | = sales revenue |
| reiksme | reiksme | Value (EUR) | decimal | financial | 58708 | amount |
| laikotarpis_nuo / iki | laikotarpis_* | Period from/to | date | date | 2021-01-01 / 2021-12-31 | fiscal period |
| reg_date | reg_date | Filing date | date | date | 2022-05-27 | when filed |

## Interpretation Notes

- **Verified from real data**: line item `PARDAVIMO PAJAMOS` (sales revenue)
  `reiksme` 58708, period 2021, standard `PELNO (NUOSTOLIŲ) ATASKAITA (Trumpa)`.
- **Same shape as the balance-sheet model** (`BalansoAtaskaita`): one row per
  income-statement account line. To build a P&L, **group by `juridinis_asmuo._id` +
  period** and pivot on `line_type_id` / `line_name`.
- **Join to company**: `juridinis_asmuo._id` → `JuridinisAsmuo._id` → `ja_kodas`.
- **Currency EUR**. Available lines vary by `template`/`standard` (micro / short /
  full P&L).
- Keyless Spinta API; cursor pagination via `_page.next`.
