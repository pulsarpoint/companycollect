# Ireland Company Profile — Mapping Report

One authoritative open source (CRO Open Data Portal, CC-BY 4.0), keyed on the **CRO number** (`company_num`).
Identity + filings index are open; financial figures + officers are paid; VAT and beneficial ownership are
separate/restricted.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.cro_number | cro_company_records | company_num | self | daily | public / CC-BY 4.0 | authoritative | id |
| tax_identifiers.vat_id | vies_vat | vatNumber | (name match) | real-time | public / validation | external | not in CRO data |
| legal_identity.legal_name | cro_company_records | company_name | company_num | daily | public / CC-BY 4.0 | authoritative | trim spaces |
| legal_identity.company_type | cro_company_records | company_type(+_code) | company_num | daily | public | authoritative | LTD/PLC/DAC/CLG |
| status.* | cro_company_records | company_status(_code/_date) | company_num | daily | public | authoritative | trim |
| activity.nace_v2_code | cro_company_records | nace_v2_code | company_num | daily | public | authoritative | strip trailing .0 |
| incorporation.incorporation_date | cro_company_records | company_reg_date | company_num | daily | public | authoritative | |
| incorporation.dissolution_date | cro_company_records | comp_dissolved_date | company_num | daily | public | authoritative | |
| registered_location.* | cro_company_records | company_address_1..4 / eircode | company_num | daily | public | authoritative | concat |
| compliance.* | cro_company_records | last_ar_date / nard / last_accounts_date | company_num | daily | public | authoritative | filing signals |
| financial_filings[] | cro_financial_statements | submission_num / file_name / dates | company_num | annual files | public / CC-BY 4.0 | authoritative | open INDEX |
| financial_statements[] | cro_document_retrieval | balance_sheet / P&L (PDF) | company_num/submission_num | per filing | **paid** | planning-only | OCR; EUR; abridged |
| officers[] | cro_document_retrieval | directors'/auditor's report | company_num | per filing | **paid** | planning-only | **PII (GDPR)** |
| beneficial_owners[] | rbo_register | beneficial_owners[] | company_num | continuous | **restricted** | planning-only | **PII (GDPR)** |
| (discovery) | data_gov_ie | resource url | — | mirror | CC-BY 4.0 | mirror | use opendata.cro.ie |

## Precedence Rules

1. **CRO Company Records is authoritative** for identity, status, type, dates, address/eircode, NACE and filing
   signals — open, daily, CC-BY 4.0.
2. **Financial Statements (open)** is the **filings index**; the **figures** come only from the **paid** PDF
   (document retrieval) or a commercial provider.
3. **Officers** are **not** in open data — derive from the **paid** filed documents (directors'/auditor's report).
4. **VAT** is not in the CRO data → VIES/Revenue (external; no open crosswalk — match by name).
5. **Beneficial ownership (RBO)** is **restricted** (post-CJEU) — planning-only.
6. **CORE** is for manual lookups/filing; **data.gov.ie** is a mirror — use **opendata.cro.ie** directly.

## Missing-Data Notes

- **Financial figures are paid** (document retrieval pay-per-call); the open dataset is the filings index.
- **Officers/directors** are not in open data (paid documents); **beneficial ownership** restricted.
- **VAT** not in the CRO open data (no open CRO↔VAT crosswalk).
- **Employee count** not in the open data.
- **GDPR**: officers (from documents) and beneficial owners are personal data. Normalize `nace_v2_code`
  (trailing .0); trim name/status spaces.
