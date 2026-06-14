# Company Data Analysis For Ireland

## Summary

Ireland is **fully-open for company identity** and **open for the financial-filings index**, via the **CRO Open
Data Portal** (`opendata.cro.ie`, launched late 2024 under **CC-BY 4.0**), keyed on the **CRO number**
(`company_num`). **Company Records** gives a rich identity for **817,068 companies** (name, status, type, dates,
address + eircode, NACE Rev.2, filing signals). **Financial Statements** is an **open index** of filed accounts
(121,387 filings in 2023) — but the **figures** are inside the filed **PDFs**, retrieved **pay-per-call**.
**Officers** are not in open data (only in the paid documents), **VAT** (`IE…`) is not in the CRO data, and
**beneficial ownership (RBO)** is restricted. So the open profile is strong on identity + filing history, with
financial figures / officers / VAT / ownership behind a fee or restriction.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| cro_company_records | CRO Company Records | recommended | public | CC-BY 4.0 | **identity spine** |
| cro_financial_statements | CRO Financial Statements (index) | recommended | public | CC-BY 4.0 | **filings index** |
| cro_document_retrieval | CRO document retrieval (PDFs) | blocked_by_payment | paid | public doc | financial figures + officers |
| cro_core_search | CORE company search | useful_secondary | public | public | manual lookups + filing |
| rbo_register | RBO beneficial ownership | blocked_by_authentication | restricted | restricted | beneficial owners |
| vies_vat | Revenue / VIES (IE VAT) | useful_secondary | public | validation | VAT (not in CRO data) |
| data_gov_ie | data.gov.ie | useful_secondary | public | CC-BY 4.0 | mirror / discovery |

## What Each Source Contributes

- **cro_company_records** — the open spine (817,068 companies): CRO number, name, status (code/text/date), legal
  form (code/text), incorporation + dissolution dates, registered address + **eircode**, **NACE Rev.2**, and
  filing signals (`last_ar_date`, `nard`, `last_accounts_date`). Daily, CC-BY 4.0.
- **cro_financial_statements** — the open **filings index** (per-year): `company_num`, `submission_num`, PDF
  `file_name`, filing dates, **accounts-to date**. Tells you which accounts were filed for which period.
- **cro_document_retrieval** — the actual financial-statement **PDFs** (balance sheet + P&L + notes; directors'
  report) retrieved **pay-per-call**; the only route to figures and to **officers**. Abridged for small/micro.
- **cro_core_search** — free real-time manual lookups + filing; alternative document purchase.
- **rbo_register** — beneficial owners; **restricted** (post-CJEU). Planning-only.
- **vies_vat** — validates the Irish VAT number (not in the CRO data; no open crosswalk).
- **data_gov_ie** — mirror of the CRO datasets for discovery / EU federation.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.cro_number` and groups: `tax_identifiers` (VAT,
external), `legal_identity`, `status`, `activity` (NACE), `incorporation`, `registered_location` (eircode),
`compliance` (annual-return + accounts signals), **`financial_filings[]`** (the open index — submission + PDF
pointer + accounts-to date), and planning-only **`financial_statements[]`** (paid PDF figures, EUR),
**`officers[]`** (paid documents, PII) and **`beneficial_owners[]`** (restricted RBO, PII). Every section carries
`source_provenance`. The example uses real Company Records values (CRO 784992) + the real filings-index shape;
figures/officers/owners are empty (paid/restricted).

## Join And Precedence Rules

- **Single join key:** CRO number (`company_num`) across all sources.
- **Precedence:** Company Records authoritative for identity; the Financial-Statements **index** authoritative
  for "what was filed"; the **paid PDF** is the only source of figures/officers; VAT external; RBO restricted.
- **Single authoritative open source** (CRO) — `data.gov.ie` is just a mirror.

## Missing Or Restricted Data

- **Financial figures are paid** (document retrieval pay-per-call) — open data is the index, not the numbers.
- **Officers/directors** not in open data (paid documents); **beneficial ownership** restricted (RBO).
- **VAT/tax id** not in the CRO open data (no open crosswalk); **employee count** not present.
- **GDPR**: officers (from documents) and beneficial owners are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← CRO number, legal_name/status/legal_form/
incorporation_date/dissolution_date/registered_address ← Company Records, activity_code ← nace_v2_code (strip
.0). Map `financials` to the open **index** (filed-when) + the **paid PDF** (figures); `officers` to the paid
documents; `owners` to the restricted RBO. Mark `tax_id` and `owners` and detailed `financials` as paid/
restricted/`not_available_in_open_sources` as appropriate; `vat_id` via VIES.
