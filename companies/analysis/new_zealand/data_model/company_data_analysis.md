# Company Data Analysis For New Zealand

## Summary

New Zealand has an **authoritative machine-readable identity API** but **no free
bulk dump** and **public financials only for FMC reporting entities**. The
**NZBN API** (Companies Office / MBIE) is the anchor: every NZ business entity has
a **13-digit NZBN** (a GS1 GLN), and the API returns the publicly available data —
name, type, status, registration date, source register + company number,
addresses, trading names, ANZSIC industry. It requires a **free subscription key**
(verified HTTP 401 without one), so the open layer is key-gated/planning-only here.

NZ has **GST, not VAT**; IRD/GST numbers are not public. Financial statements
exist only for **FMC reporting entities** (issuers, large/overseas-owned
companies, managed investment schemes), as documents on the **Companies Register**
and the **FMA Disclose Register**. The example is schematic (key-gated source).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| nzbn_api | NZBN API | blocked_authentication | free subscription key | Crown copyright (public data) | Authoritative identity |
| companies_register | NZ Companies Register | insufficient_transport_info | public search (no bulk/API) | public register | Company number, directors, documents |
| disclose_register | Disclose Register (FMA) | insufficient_transport_info | public search | public register | FMC financial statements |

## What Each Source Contributes

- **nzbn_api** — the authoritative entity layer keyed on the 13-digit NZBN: name,
  entity type, status, registration date, source register + company number,
  addresses (registered/service/postal), trading names, contacts, ANZSIC industry,
  and a `companyDetails` block (company number, NZSX listing, insolvency flags).
  Free subscription key; schema cataloged from public docs (HTTP 401 without key).
- **companies_register** — the company number, **directors/shareholders** (personal
  data) and **filed documents**, including financial statements for entities
  required to file. Search-only; no free bulk/API.
- **disclose_register** — FMA register of FMC offers / managed investment schemes;
  **financial statements** + offer documents for the FMC-reporting subset. The open
  route to NZ company financials.

## Proposed Country Company Profile

A single object keyed on `registration.nzbn` (+ `company_number`):

- `registration` — NZBN, company number, source register.
- `tax_identifiers` — ird/gst/vat all null (not public; no VAT).
- `legal_identity` — name, entity type, trading names.
- `status` — Registered/Removed/In liquidation.
- `incorporation` — registration date.
- `activity` — ANZSIC industry classifications.
- `registered_location` — registered/service addresses.
- `financial_statements[]` — planning-only (FMC reporting entities; NZD).
- `officers[]` — planning-only (directors; personal data).
- `source_provenance[]`.

## Join And Precedence Rules

- **Join key:** NZBN (13-digit) everywhere; the company number links the Companies
  Register and the Disclose issuer.
- **Precedence:** NZBN API (identity) > Companies Register (directors/documents) >
  Disclose Register (FMC financials).
- **No VAT/IRD/GST** in the open layer — do not synthesize.

## Missing Or Restricted Data

- **Free bulk** — none; NZBN API per-entity/search (free key).
- **Financial statements** — FMC reporting entities only.
- **IRD/GST numbers** — not public; **no VAT**.
- **Directors/shareholders** — Companies Register; personal data (Privacy Act 2020).

## Common Mapper Notes

- Map `company_id` to NZBN and `registration_number` to the company number; mark
  `tax_id`/`vat_id` as `not_available_in_open_sources`.
- Map `financials` only from the Disclose/Companies registers for FMC entities.
- Redact director/shareholder/contact personal data (Privacy Act) in any committed
  output.
