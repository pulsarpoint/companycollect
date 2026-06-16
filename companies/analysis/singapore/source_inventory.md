# Singapore — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| ACRA Information on Corporate Entities | ACRA via data.gov.sg | official registry | public, no key | CSV | Singapore Open Data Licence | **recommended** |
| ACRA BizFile+ (profiles & financials) | ACRA | financial disclosure | paid per-document | PDF, XBRL | restricted | blocked_by_payment |
| SGX listed disclosures | Singapore Exchange | financial disclosure | public (exchange terms) | PDF, XLSX | exchange terms | useful_secondary_source |
| data.gov.sg | GovTech | open data portal | public, no key | CSV, JSON | Singapore Open Data Licence | useful_secondary_source |

## Roles

- **acra_entities** — the authoritative open **registry list** keyed on the UEN:
  name, type, status, registration date, address, SSIC activity, former names,
  audit firms, officer **count**. A–Z CSV family on data.gov.sg. Verified live
  ('B' = 93,896 entities). No financials, no officer names.
- **acra_bizfile_financials** — paid business profiles (officers, shareholders,
  share capital) and financial statements (XBRL). The authoritative private-company
  financial/officer source, but pay-per-document.
- **sgx_listed_financials** — open listed-company financials (issuers only).
- **data_gov_sg** — the portal + keyless search/poll-download APIs.

## Join key

**UEN (Unique Entity Number)** across all sources. The UEN is also the entity tax
reference; Singapore has GST (not VAT) and the GST reg is generally the UEN, so
there is no separate VAT number.
