# Latvia — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **UR Register of Enterprises** | Official registry | Free | **CSV/XLSX/JSON/Parquet/SQLite/PG** | CC0-1.0 | **recommended** (open spine) |
| **Gada pārskatu finanšu dati** | Official financials | Free | **CSV (4 files)** | CC0-1.0 | **recommended** (**structured financials**) |
| **Patiesie labuma guvēji (beneficial owners)** | BO register | Free | CSV | CC0-1.0 | **recommended** (open BO) |
| UR officers / members / equity / events | Official registry | Free | CSV | CC0-1.0 | useful secondary (governance + events) |
| VID / VIES (LV VAT) | Official tax | Free | SOAP/CSV | Validation/open | useful secondary |
| data.gov.lv | Open data portal | Free | CSV/JSON/… | CC0 (UR) | useful secondary (CKAN hub) |

## Access points

- CKAN: https://data.gov.lv/dati/lv/api/3/action/ (UR org slug `ur`, ~35 datasets)
- Register: `.../resource/25e80bf3-.../download/register.csv`
- Financials: `.../download/financial_statements.csv`, `balance_sheets.csv`, `income_statements.csv`, `cash_flow_statements.csv`
- Beneficial owners: `.../download/beneficial_owners.csv`
- Dataset pages: https://data.gov.lv/dati/lv/dataset/uz ; .../gada-parskatu-finansu-dati ; .../patiesie-labuma-guveji
- VIES: https://ec.europa.eu/taxation_customs/vies/ ; UR site: https://www.ur.gov.lv/en/

## Key facts

- **Single join key**: **regcode** (11-digit registration number; = financials `legal_entity_registration_number`). VAT = `LV` + regcode (VIES/VID).
- **Fully open under CC0-1.0 (public domain)** — no attribution required, commercial OK: register + **structured financial statements** + **beneficial owners** + officers/members/equity/events.
- **Financials are STRUCTURED** (not PDF): financial_statements + balance_sheets + income_statements + cash_flow_statements; includes **employee counts**; EUR (historical LVL). Join via statement_id/file_id.
- **Verified live**: register 485,134 entities; financial_statements 1,970,094 reports; BO open.
- **GDPR**: beneficial owners / officers / members are personal data.

See `source_inventory.json` for the machine-readable version.
