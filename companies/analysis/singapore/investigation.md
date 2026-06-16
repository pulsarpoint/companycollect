# Singapore Company Data Investigation

## Conclusion

Singapore has a **fully-open company/entity registry list** plus **paid financials**
(listed-only via SGX):

- **Identity (open bulk):** **ACRA Information on Corporate Entities**, published on
  **data.gov.sg** as a family of CSV datasets split by first letter (A–Z + others).
  Every entity is keyed by its **UEN (Unique Entity Number)**. 53 columns: name,
  entity type, business constitution, status, registration/incorporation date, full
  address, primary & secondary SSIC activity, number of officers, up to 15 former
  names, and up to 5 audit firms. Fully open (Singapore Open Data Licence).
- **Financials (paid / listed-only):** ACRA financial statements (filed in **XBRL**
  via BizFinx) are sold per-document on **BizFile+** — not open. **Listed**
  companies' financials are public via **SGX**.

## What was verified live

- **ACRA bulk works** via the data.gov.sg poll-download API: dataset 'B'
  (`d_3a3807c023c61ddfba947dc069eb53f2`) → signed S3 CSV, **30.6 MB**, **93,896
  entities**, 53 columns. Real records: UEN `191900023K` **BRIDGESTONE SINGAPORE PTE
  LTD** (Live Company), `193100019E` **BATA SHOE (SINGAPORE) PRIVATE LIMITED** (Live),
  plus historical sole proprietorships.
- **data.gov.sg** dataset search + poll-download APIs work with **no key**.
- **ACRA BizFile+** and **SGX** reachable (HTTP 200); BizFile financial statements
  are paid per-document.

## Identifiers

- **UEN (Unique Entity Number)** — the universal entity identifier (company id,
  registration number, and the entity's tax reference). Format varies by class:
  - **Businesses** (sole proprietorships / partnerships): 9 chars `nnnnnnnnX`.
  - **Local companies**: `yyyynnnnnX` (year + serial + check).
  - **Other entities** (societies, LLPs, etc.): `TyyPQnnnnX` (the "T"/"S"/"R"
    prefix form).
- Singapore has **GST** (not VAT). The GST registration number for a local entity is
  generally its **UEN**; there is no separate VAT number. Map `tax_id` to the UEN.

## ACRA entities schema (53 columns, verified)

`uen`, `issuance_agency_id` (ACRA), `entity_name`, `entity_type_description`,
`business_constitution_description`, `company_type_description`,
`paf_constitution_description`, `entity_status_description`,
`registration_incorporation_date`, `uen_issue_date`, address (`address_type`,
`block`, `street_name`, `level_no`, `unit_no`, `building_name`, `postal_code`,
`other_address_line1/2`), `account_due_date`, `annual_return_date`,
`primary_ssic_code` + `primary_ssic_description` + `primary_user_described_activity`,
`secondary_ssic_*`, `no_of_officers`, `former_entity_name1..15`,
`uen_of_audit_firm1..5` + `name_of_audit_firm1..5`.

> The dataset gives the **count** of officers (`no_of_officers`) but **not their
> names** — officer/shareholder identities are in the paid BizFile profile (personal
> data, PDPA).

## What is NOT openly available

- **Financial statements** (private companies) — paid (BizFile XBRL); listed via SGX.
- **Officer / shareholder names** and **share capital** — paid BizFile profile.
- **A separate VAT number** — GST country; GST reg = UEN.

## Recommended ingestion

1. **ACRA entities** A–Z CSV datasets via the data.gov.sg poll-download API, keyed
   on UEN (resolve SSIC code lists).
2. **SGX** for listed-company financials (join by UEN/name).
3. Treat **BizFile+** financials/profiles as a paid enrichment (officers,
   shareholders, private financials).
4. Officer/shareholder data is personal data (PDPA) — not in the open dataset; only
   the count is open.
