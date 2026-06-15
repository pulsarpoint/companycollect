# United Kingdom — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| Free Company Data Product (basic) | ch_basic_company_data | official_registry | public | OGL | csv/zip | recommended |
| Accounts Bulk Data (iXBRL) | ch_accounts_bulk | official_financial | public | OGL | ixbrl/xbrl | recommended |
| PSC snapshot (beneficial owners) | ch_psc_snapshot | beneficial_ownership | public | OGL | json/zip | recommended |
| REST API (officers/filing history) | ch_rest_api | official_registry | public (free key) | OGL | json | blocked_by_authentication |

## Best combination

**Basic Company Data** (register: identity, address, status, SIC) + **Accounts
Bulk Data** (iXBRL financials) + **PSC** (beneficial owners), joined on **company
number**; **REST API** (free key) for officers/filing history. All OGL.

## Downloaded (real)

- `raw/bulk/BasicCompanyData-part1_7.zip` — 73 MB, **849,999** companies (×7 ≈ 5.9M) + metadata
- `raw/bulk/Accounts_Bulk_Data-2026-06-10.zip` — 77 MB, **9,717** iXBRL filings + metadata
- `raw/samples/basic_company_sample.json` — real register row (! LTD, 08209948)
- `normalized/companies.sample.jsonl` — real record (00009604) with **real iXBRL financials**
