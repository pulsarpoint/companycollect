# Singapore — Schema Notes

## Identifiers

- **UEN (Unique Entity Number)** — the universal entity identifier: company id,
  registration number, and the entity's **tax reference**. Format varies by class:
  - **Businesses** (sole proprietorships / partnerships): **9 chars** `nnnnnnnnX`
    (serial + check letter), e.g. `00010100A`.
  - **Local companies**: **10 chars** `yyyynnnnnX` (4-digit year + 5-digit serial +
    check), e.g. `191900023K`.
  - **Other entities** (LLPs, societies, etc.): **10 chars** `TyyPQnnnnX` (prefix
    `T`/`S`/`R` + year + entity-type code + serial + check).
- Singapore has **GST** (not VAT). The GST registration number for a local entity is
  generally its **UEN**; no separate VAT number.

## ACRA entities CSV (53 columns, verified)

| Column | Meaning |
|---|---|
| uen | Unique Entity Number (key) |
| issuance_agency_id | Issuing agency (ACRA) |
| entity_name | Registered entity name |
| entity_type_description | Local Company / Sole Proprietorship/Partnership / LLP / … |
| business_constitution_description | Sole-Proprietor / Partnership / … |
| company_type_description | Company type (e.g. private/public) |
| paf_constitution_description | Public Accounting Firm constitution (if applicable) |
| entity_status_description | Live / Terminated / Cancelled / Ceased Registration / … |
| registration_incorporation_date | Registration / incorporation date (YYYY-MM-DD) |
| uen_issue_date | UEN issue date |
| address_type | LOCAL / FOREIGN |
| block / street_name / level_no / unit_no / building_name / postal_code | Registered address components |
| other_address_line1 / other_address_line2 | Free-text address (foreign) |
| account_due_date | Accounts due date |
| annual_return_date | Last annual-return date |
| primary_ssic_code / primary_ssic_description | Primary activity (SSIC) |
| primary_user_described_activity | Free-text primary activity |
| secondary_ssic_code / secondary_ssic_description | Secondary activity |
| no_of_officers | **Count** of officers (names NOT included) |
| former_entity_name1..15 | Up to 15 former names (name history) |
| uen_of_audit_firm1..5 / name_of_audit_firm1..5 | Up to 5 audit firms (UEN + name) |

- Encoding **UTF-8**, comma-delimited, header row. Empty values often `na`.
- The dataset family is split by first letter (A–Z + others); each is a data.gov.sg
  dataset with its own `datasetId`.

## Access (data.gov.sg APIs, no key)

- **Search**: `GET https://api-production.data.gov.sg/v2/public/api/datasets?query=...`
  → dataset list with `datasetId`.
- **Download**: `GET https://api-open.data.gov.sg/v1/public/api/datasets/{datasetId}/poll-download`
  → `data.url` = a signed (time-limited) S3 CSV URL; then GET that URL.

## Financials (not in the open dataset)

- **ACRA BizFile+**: business profiles (officers, shareholders, share capital) and
  **financial statements (XBRL via BizFinx)** — pay-per-document, SGD.
- **SGX**: listed-company results / annual reports (PDF/Excel), SGD.

## Dates, money, encoding

- Dates: `YYYY-MM-DD`.
- Money: **SGD** (financials, when obtained).
- Encoding: UTF-8 CSV.

## Internal model mapping

```text
company_id          <- uen
registration_number <- uen
tax_id              <- uen (entity tax reference)
vat_id              <- null (GST country; GST reg = UEN, no separate VAT)
legal_name          <- entity_name
former_names        <- former_entity_name1..15
company_type        <- entity_type_description (+ company_type_description)
status              <- entity_status_description (Live -> active; Terminated/Cancelled/Ceased -> closed)
incorporation_date  <- registration_incorporation_date
registered_address  <- block/street_name/level_no/unit_no/building_name/postal_code
activity_code       <- primary_ssic_code (SSIC) + secondary_ssic_code
officers            <- no_of_officers (COUNT only; names via paid BizFile, PDPA)
auditors            <- name_of_audit_firm1..5
financials          <- BizFile XBRL (paid) / SGX (listed), SGD
```
