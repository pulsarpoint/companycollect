# Company data sources for Latvia

## Status

- Official bulk data: **found** (UR Register of Enterprises open data — CSV/XLSX/JSON/Parquet/SQLite/Postgres)
- Official API: **found** (CKAN API at data.gov.lv)
- Open data portal: **found** (data.gov.lv; UR org = ~35 datasets)
- License: **known — CC0-1.0 (public domain)**
- Recommended ingestion path: **bulk download** (register + structured financials + beneficial owners + officers)

## Best source

The **Register of Enterprises (Uzņēmumu reģistrs / UR)** publishes everything as **CC0-1.0 (public domain)**
open data on **data.gov.lv**, keyed on the **regcode** (11-digit registration number). Verified by real
download:

- **Company register** — **485,134 entities**: name, legal form (SIA/AS/IK…), register type, registered/
  terminated dates, address + postcode + ATVK territory code, SEPA id.
- **Structured financial statements** — **1,970,094 annual reports** across four joined CSVs:
  `financial_statements` (report metadata + employees + currency), `balance_sheets`, `income_statements`,
  `cash_flow_statements`. Genuine structured figures (not PDF).
- **Beneficial owners** (`patiesie labuma guvēji`) as open CSV — unusual in the EU.
- Plus officers, members/shareholders, equity capital, insolvency, liquidations, historical names (~35 datasets).

So Latvia is **best-in-class fully-open** (like Estonia) but under **CC0** — no attribution required, commercial
use allowed.

## Next action

Ingest the register (keyed on regcode) + the four financial-statement CSVs (join on
`legal_entity_registration_number` → `regcode`; balance/income via `statement_id`/`file_id`) + beneficial owners
+ officers. Treat owner/officer personal data under GDPR. VAT (`LV`+regcode) via VIES/VID.
