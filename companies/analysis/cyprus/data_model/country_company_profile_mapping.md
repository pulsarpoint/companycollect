# Cyprus Company Profile — Mapping Report

Join everything on the **DRCIP `registration_number`** (the `HE…` form for companies). The open register is the
spine; tax/VAT enrich per company; financials, shareholders and beneficial owners are paid/restricted
(planning-only).

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.registration_number | drcip_register | registration_number | self (HE…) | regular | public / open data | authoritative | company id; prefix = entity type |
| registration.entity_type | drcip_register | type | registration_number | regular | public | authoritative | company/business name/partnership/overseas |
| legal_identity.legal_name | drcip_register | name | registration_number | regular | public | authoritative | keep Greek + English |
| legal_identity.company_type | drcip_register | type + name | registration_number | regular | public | authoritative | Ltd/Plc from name suffix |
| status.value | drcip_register | status | registration_number | regular | public | authoritative | normalise (operational→active) |
| incorporation.registration_date | drcip_register | registration_date | registration_number | regular | public | authoritative | normalise to ISO 8601 |
| registered_location.registered_address | drcip_register | registered_address | registration_number | regular | public | authoritative | free-text line |
| registered_location.municipality / region | drcip_register | registered_address (parsed) | registration_number | regular | public | derived | district = Nicosia/Limassol/Larnaca/Paphos/Famagusta |
| officers[] | drcip_register | officers (name, role) | registration_number | regular | public / open data | authoritative (open) | **PII (GDPR)**; directors/secretary, NOT shareholders |
| tax_identifiers.tic | tax_department | TIC | registration_number | per lookup | public / validation | authoritative | tax id; not in open CSV |
| tax_identifiers.vat_number | tax_department | VAT number | registration_number | per lookup | public / VIES | authoritative | CY+8 digits+letter |
| tax_identifiers.vat_status | tax_department | VAT status | registration_number | per lookup | public / VIES | evidence | point-in-time |
| activity.activity_code | — | not_available | — | — | — | none | **no public NACE/activity code** in Cyprus open data |
| financial_statements[] | he32_financial_statements | financial_statements.* | registration_number | annual | **paid** / PDF | planning-only | EUR; scanned PDF → OCR/parse, or commercial provider |
| financial_statements[].share_capital | he32_financial_statements | annual_return.share_capital | registration_number | annual | paid | planning-only | EUR |
| shareholders[] | he32_financial_statements | annual_return.shareholders[] | registration_number | annual | **paid** / PDF | planning-only | PII; not in open register |
| beneficial_owners[] | ubo_register | beneficial_owners[] | registration_number | continuous | **restricted** | planning-only | PII; conditions/fee (post-CJEU) |
| financial_statements[] (alt) | commercial_aggregators | company.financials[] | registration_number | vendor | **paid** / contract | planning-only fallback | scalable structured financials |
| (cross-reference / QA) | opensanctions_mirror | properties.* / Directorship | registrationNumber | from CSV | CC-BY-NC | QA only | non-commercial; use data.gov.cy for commercial reuse |

## Precedence Rules

1. **Open official first.** `drcip_register` (data.gov.cy CSV + eSearch) is authoritative for identity, status,
   address, dates, and officers.
2. **Tax/VAT** comes from `tax_department` (TIC) / VIES (VAT) per company — there is no bulk list.
3. **Financials**: prefer the official `he32_financial_statements` (paid PDF, OCR) for fidelity; use
   `commercial_aggregators` as the **scalable structured** alternative. Both are planning-only.
4. **Ownership layers are distinct** — do not conflate: open **officers** (DRCIP CSV) ≠ **shareholders**
   (paid HE32) ≠ **beneficial owners** (restricted UBO).
5. **opensanctions_mirror** is **QA/cross-reference only** (CC-BY-NC); for commercial reuse take the company
   list from `data.gov.cy` directly.

## Missing-Data Notes

- **No activity/NACE code** in open data (`activity_code` = not_available).
- **No structured financials** in open data — figures are inside paid scanned PDFs.
- **Shareholders/beneficial owners** are not open (paid HE32 / restricted UBO).
- **Exact open CSV resource URL** still to be resolved via data.gov.cy/en/group/30; the dataset's exact open
  licence must be confirmed there before redistribution.
