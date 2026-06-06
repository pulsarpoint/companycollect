# Serbia — Source Inventory

| Source | Type | Access | Format | License | Records | Status |
|---|---|---|---|---|---|---|
| **APR Companies Open Data API** (`openapi.apr.gov.rs/api/opendata/companies`) | Official registry | Public, no auth | JSON | public_domain | 133,357 | **recommended** |
| **APR Financial Statements API** (`.../companies/financial-statements`) | Official registry | Public, no auth | JSON | public_domain | 123,455 | **recommended** |
| APR NGO API (`.../opendata/ngo`) | Official registry | Public, no auth | JSON | public_domain | 40,547 | useful_secondary_source |
| APR Web-Service (veb-servis) | Official registry | Auth + **paid** | web-service | contract | all registers | blocked_by_payment |
| OpenCorporates (register 224) | Aggregator | Search free / API paid | JSON, HTML | restricted | — | useful_secondary_source |
| RZS Statistical Office Open Data | Statistical office | Public | JSON/CSV/OData | open | aggregate | not_company_data |

## Key facts

- **Publisher of all open APIs:** Agencija za privredne registre (APR) — the
  official Serbian Business Registers Agency.
- **Catalog:** national open data portal `data.gov.rs` (udata/etalab platform).
- **Update frequency:** monthly. Snapshot date is carried in each payload as
  `DatumPreseka` (2026-05-31 at time of download).
- **Primary key across datasets:** `matični broj` (8-digit registration number).
- **Gaps:** no PIB/VAT, no directors/shareholders, no entrepreneurs (preduzetnici),
  no beneficial ownership in the open feeds — those require the paid web-service.
