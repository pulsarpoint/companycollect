# ACRA Information on Corporate Entities Field Catalog

## Source Summary

- Country: Singapore
- Source type: official_registry
- Organization: ACRA via data.gov.sg / GovTech
- URL: https://data.gov.sg/datasets/d_3a3807c023c61ddfba947dc069eb53f2/view
- License: Singapore Open Data Licence (free reuse, attribution)
- Access: public, **no key** (data.gov.sg poll-download API)
- Freshness: updated periodically (monthly-ish)
- Record shape: flat CSV, 53 columns, one row per entity; **split A–Z** (+ others)
- Primary keys: `uen`
- Join keys: `uen`

## Fields (modeled subset)

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| uen | uen | Unique Entity Number | string | identifier | 191900023K | key + tax ref |
| entity_name | entity_name | Entity name | string | legal_name | BRIDGESTONE SINGAPORE PTE LTD | |
| entity_type_description | entity_type_description | Entity type | string | legal_form | Local Company | company/sole prop/partnership/LLP |
| business_constitution_description | … | Constitution | string | legal_form | Sole-Proprietor | businesses |
| company_type_description | … | Company type | string | legal_form | | companies |
| entity_status_description | … | Status | string | status | Live Company / Terminated | Live = active |
| registration_incorporation_date | … | Incorporation date | date | date | 1974-09-27 | |
| address (block/street/…/postal_code) | … | Registered address | string | address | | concatenate |
| annual_return_date | … | Last annual return | date | filing | | compliance |
| primary_ssic_code / description | … | Primary activity (SSIC) | string | activity | 46473 | |
| secondary_ssic_code | … | Secondary activity | string | activity | 46541 | |
| no_of_officers | no_of_officers | Officer **count** | integer | metadata | 2 | names NOT open |
| former_entity_name1..15 | … | Former names | array | legal_name | | name history |
| name_of_audit_firm1..5 | … | Audit firms | array | relationship | | UEN + name |

(Full 53-column layout: see `schema_notes.md` / `source_field_catalog.json`.)

## Interpretation Notes

- **Verified from real data**: the 'B' dataset (`d_3a3807c023c61ddfba947dc069eb53f2`),
  **93,896 entities**, 53 columns. Real records: UEN `191900023K` BRIDGESTONE
  SINGAPORE PTE LTD (Live Company), `193100019E` BATA SHOE (SINGAPORE) PRIVATE
  LIMITED.
- **UEN** is the universal id: company id, registration number, and the entity's
  tax reference. Format varies by class (business 9-char / company `yyyynnnnnX` /
  other `TyyPQnnnnX`). Keep as a string.
- **Coverage**: ALL ACRA-registered entities — local companies, sole
  proprietorships, partnerships, LLPs, etc. The dataset is **split by first letter**
  (A–Z + others); ingest each data.gov.sg dataset in the family.
- **Status**: `entity_status_description` "Live Company"/"Live" ⇒ active; Terminated
  / Cancelled / Ceased Registration / Struck Off / In Liquidation ⇒ inactive.
- **Officers**: only the **count** (`no_of_officers`) is open — officer/director
  **names** are personal data (PDPA), available only in the paid BizFile profile.
- **Former names** (up to 15) and **audit firms** (up to 5, with their own UEN)
  are valuable: collect non-empty `former_entity_name*` into an array; each audit
  firm UEN joins back to this dataset.
- Encoding UTF-8, comma-delimited, header row; empty values often `na`.
- **No financials** in this dataset.
