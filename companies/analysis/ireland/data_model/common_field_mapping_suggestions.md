# Ireland — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Ireland profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Ireland source | Ireland path | Notes |
|---|---|---|---|
| company_id | cro_company_records | company_num | CRO number |
| registration_number | cro_company_records | company_num | same |
| tax_id | not_available_in_open_sources | — | no tax id in CRO data |
| vat_id | vies_vat | IE + 7 digits + 1-2 letters | not in CRO data; validate via VIES |
| legal_name | cro_company_records | company_name | trim spaces |
| status | cro_company_records | company_status | Normal/Dissolved/Strike off/… |
| legal_form | cro_company_records | company_type (+ code) | LTD/PLC/DAC/CLG/Unlimited |
| incorporation_date | cro_company_records | company_reg_date | |
| dissolution_date | cro_company_records | comp_dissolved_date | |
| registered_address | cro_company_records | company_address_1..4 (+ eircode) | concat |
| activity_code | cro_company_records | nace_v2_code | NACE Rev.2; strip trailing .0 |
| financials | cro_financial_statements (index, open) / cro_document_retrieval (figures, paid) | index + PDF | figures behind pay-per-call PDF; EUR |
| officers | cro_document_retrieval (paid) | directors'/auditor's report | PII; not in open data |
| owners | rbo_register (restricted) | beneficial_owners[] | restricted post-CJEU |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Ireland is **fully-open for identity** (CRO Company Records, CC-BY 4.0, 817k companies) with an **open
  financial-filings index**, but the **financial figures are paid** (document retrieval PDF), **officers** come
  only from those paid documents, and **VAT** + **beneficial ownership** are external/restricted.
- For a cross-country `financials` field, map to the open **index** for "which accounts were filed when", and to
  the **paid PDF** (or a vendor) for the actual numbers — not a structured open feed.
- `tax_id` is `not_available_in_open_sources`; `vat_id` must be sourced from VIES/Revenue (no open CRO↔VAT
  crosswalk).
- `officers` and `owners` are paid/restricted — mark accordingly; only the CRO number + identity are open.
