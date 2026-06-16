# Company Data Analysis For Singapore

## Summary

Singapore offers a **fully-open company/entity registry list** plus **paid
financials** (listed-only via SGX). The **ACRA Information on Corporate Entities**
dataset on **data.gov.sg** (split A–Z) gives, openly and keyed on the **UEN**:
identity, entity type, status, registration date, address, primary/secondary SSIC
activity, up to 15 former names, up to 5 audit firms, and the officer **count** —
for **all** ACRA-registered entities. Officer/shareholder **names**, share
capital, and private-company financial statements are paid (ACRA BizFile+, XBRL);
listed-company financials are open via SGX. The example uses real ACRA data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| acra_entities | ACRA Information on Corporate Entities | recommended | public, no key | Singapore Open Data Licence | Authoritative open registry list |
| acra_bizfile_financials | ACRA BizFile+ (profiles & financials) | blocked_payment | paid per-document | restricted | Officers/shareholders/capital + private financials |
| sgx_listed_financials | SGX listed disclosures | planning_only | exchange terms | exchange terms | Listed-company financials |

## What Each Source Contributes

- **acra_entities** — the open registry layer keyed on the UEN: entity name, type,
  business constitution, status, registration/incorporation date, full address,
  primary & secondary SSIC activity, officer **count**, up to 15 former names, and
  up to 5 audit firms. Verified live (the 'B' dataset: 93,896 entities, 53 cols).
  No financials, no officer names.
- **acra_bizfile_financials** — the authoritative source of officer/shareholder
  **names**, share capital, and **financial statements (XBRL)**; pay-per-document.
  Planning-only; personal data (PDPA).
- **sgx_listed_financials** — open financial results / annual reports for **listed**
  issuers (SGD). The only open financial route; listed-only.

## Proposed Country Company Profile

A single object keyed on `registration.uen`:

- `registration` — UEN (+ issuance agency).
- `tax_identifiers` — tax_id = UEN; vat_id null (no separate VAT).
- `legal_identity` — legal name, former names, entity type, company type.
- `status` — Live → active.
- `incorporation` — registration/incorporation date.
- `activity` — SSIC primary/secondary.
- `registered_location` — address, postal code.
- `officers_summary` — officer count (open); `auditors[]` — audit firms.
- `officers[]` — names (paid BizFile, PDPA, planning-only).
- `financial_statements[]` — paid (BizFile) / listed (SGX), SGD, planning-only.
- `source_provenance[]`.

## Join And Precedence Rules

- **Join key**: the UEN everywhere (audit-firm UENs join back to the dataset; SGX by
  UEN/ticker/name).
- **Precedence**: ACRA entities (open registry) > SGX (open listed financials) >
  BizFile (paid officers/financials).
- **UEN is also the tax reference**; no separate VAT id.

## Missing Or Restricted Data

- **Officer/shareholder names** + share capital — paid BizFile (PDPA).
- **Private-company financials** — paid (BizFile XBRL); listed via SGX.
- **No separate VAT id** — GST registration is the UEN.

## Common Mapper Notes

- Map `company_id`, `registration_number`, and `tax_id` all to the UEN; mark
  `vat_id` as not available.
- Map `financials` from SGX (listed) or BizFile (paid), SGD.
- The open dataset gives only the officer **count**; redact any officer/shareholder
  names obtained from BizFile (PDPA).
