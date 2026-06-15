# Company Data Analysis For Lithuania

## Summary

Lithuania is a **fully-open tier-1 source**: both the **company register** and
**financial statements** are openly available with **no API key**, through the
data.gov.lt Spinta REST API (Registrų centras JAR). A rich company profile can be
built — identity, legal form, status, dates, address, and **structured balance
sheet + P&L line items in EUR** — all keyed on the **company code (įmonės kodas,
9-digit)**, which is also the legal-entity taxpayer code. The example uses real
data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| rc_jar_legal_entities | RC JAR — Register of Legal Entities (JuridinisAsmuo) | recommended | public, no key | CC-BY 4.0 | Authoritative identity |
| rc_jar_balance_sheets | RC JAR — Balance sheets (BalansoAtaskaita) | recommended | public, no key | CC-BY 4.0 | Open balance-sheet financials |
| rc_jar_income_statements | RC JAR — Profit & loss (PelnoAtaskaita) | recommended | public, no key | CC-BY 4.0 | Open P&L financials |

(Supplementary JAR models — `buveines` addresses, `ja_kapitalas` capital,
`valdymo_organai` directors, `formos_statusai` code lists, late/non-filer and NGO
models — are all keyless and enrich the profile; directors are personal data.)

## What Each Source Contributes

- **rc_jar_legal_entities** — the authoritative identity layer: 9-digit company
  code, legal name, legal-form reference (→ Forma, 168 forms, LT+EN), status
  reference (→ Statusas, 31 statuses, LT+EN), registration and deregistration
  dates. Verified live (e.g. `ja_kodas` 110000291). Address is usually in the
  `Buveine` model.
- **rc_jar_balance_sheets** — open balance-sheet line items (e.g. current assets
  €13,532, 2023), one row per account, linked to the company, in EUR.
- **rc_jar_income_statements** — open P&L line items (e.g. sales revenue €58,708,
  2021), same shape, EUR.

## Proposed Country Company Profile

A single object keyed on `registration.company_code`:

- `registration` — company code.
- `tax_identifiers` — tax_id = company code; vat_id null (VIES).
- `legal_identity` — name, legal form (resolved code).
- `status` — status (resolved code) + date.
- `incorporation` — registration / deregistration dates.
- `registered_location` — address (Buveine).
- `financial_statements[]` — balance sheet + P&L, aggregated per fiscal period from
  line items (EUR).
- `officers[]` — directors (valdymo_organai); personal data (GDPR), planning-only.
- `source_provenance[]`.

## Join And Precedence Rules

- **Join keys**: company code (`ja_kodas`) is the business key; the Spinta `_id`
  UUID is the internal join (financial models → `juridinis_asmuo._id`; `forma._id` →
  Forma; `statusas._id` → Statusas).
- **Precedence**: all data comes from the same official register via one API — no
  conflicting sources. Resolve code-list references; aggregate financial line items
  per company + period.
- **No VAT in the register** — obtain PVM kodas via EU VIES.

## Missing Or Restricted Data

- **VAT number** — separate (VIES).
- **Beneficial owners** — JANGIS register is access-controlled; not in this set.
- **Activity code** — no NACE/EVRK code observed in the JAR base models.
- **Directors** — available but personal data (GDPR), redact.
- **Financials are line items** — aggregate per period; coverage depends on filing
  compliance.

## Common Mapper Notes

- Map `company_id`/`registration_number`/`tax_id` all to the company code; mark
  `vat_id` as `not_available_in_open_register` (VIES).
- Map `financials` by aggregating BalansoAtaskaita + PelnoAtaskaita line items per
  company + period (EUR).
- Resolve `forma`/`statusas` via the Forma/Statusas code lists (LT + EN labels).
- Redact director personal data (GDPR) in any committed output.
