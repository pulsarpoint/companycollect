# Kazakhstan Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| data.egov.kz gbd_ul (State DB of Legal Entities) | egov_gbd_ul | Open Data Portal (MDD) | open API, free API key required | JSON/XML/CSV | recommended | Open register: BIN, name, reg date, address, OKED, director |
| KGD taxpayer search / lists | kgd_taxpayer | State Revenue Committee | browser-public search/lists | HTML/XLSX | useful_secondary_source | Tax/VAT status by BIN/IIN |
| Kazakhstan Stock Exchange | kase_listed | KASE | browser-public (SPA) | HTML | useful_secondary_source | Listed companies (ISIN) |
| Bureau of National Statistics | stat_gov_kz | BNS | public | HTML/XLSX | not_company_data | Statistics, not a per-company register |

## Notes

- **`gbd_ul`** is the authoritative **open** legal-entities register on data.egov.kz: BIN,
  name (RU/KZ), registration date, legal address, activity (OKED), director name. Served via
  the data.egov.kz API (`/api/v4/gbd_ul/<version>?apiKey=…`) — **verified 403 without a key**;
  a **free API key (registration)** is required.
- **KGD** (`kgd.gov.kz`) adds **tax/VAT status** via browser-public taxpayer search and
  published lists (by **BIN/IIN**).
- **KASE** lists companies/securities (browser-public SPA; ISIN `KZxxxxxxxxxx`).
- **stat.gov.kz** is statistics, not a register.
- **Identifier**: the **12-digit BIN** is the universal key (gbd_ul + KGD); ISIN for listed.
- The **director's name** (gbd_ul) is personal data — redact.
