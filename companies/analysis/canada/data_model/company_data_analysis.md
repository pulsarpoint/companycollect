# Company Data Analysis For Canada

## Summary

Canada has **no single national company register** — incorporation is split
between the **federal** level (**Corporations Canada**, under the CBCA) and **13
provinces/territories**, each with its own registry. The federal layer is **open
and rich**, but a **subset** of all Canadian companies:

- **Corporations Canada — Federal Corporations** (ISED, open.canada.ca, **OGL**) —
  CSVs split by active/inactive × CBCA/non-CBCA × EN/FR. The active CBCA
  business-corporations file = **642,720** corporations. Per record: **corporation
  number** (federal id), **Business Number / BN** (CRA tax id), corporate name
  (EN + FR), governing legislation, status, anniversary date, **full registered
  address**, last annual filing/meeting, and director counts. Verified live (real
  record: MINDANGLER CAPITAL INC., corp # 8660115, BN 835752437, Ottawa ON).
- **Provincially-incorporated companies are NOT in the federal dataset** — they
  require the provincial registries (Québec **REQ** and BC **OrgBook** are open;
  Ontario/Alberta and others vary, some paid).
- **Financials** are open only for **reporting issuers** (public companies / funds)
  via **SEDAR+** (the CSA's national system); **private-company financials are not
  public**.

Identifiers: **corporation number** (federal) + **BN** (CRA 9-digit, the
cross-source join key); Canada has **no separate VAT number** — GST/HST
registration is the BN + RT program account. Director names (federal API /
provincial) are personal data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| corporations_canada_federal | Corporations Canada — Federal Corporations | ready | public | OGL Canada | Open federal identity backbone |
| corporations_canada_api | Corporations Canada — real-time API | insufficient_transport_info | public | OGL Canada | Director names + history (federal) |
| sedar_plus | SEDAR+ reporting-issuer filings | planning_only | public (per-issuer) | public disclosure | Financials (reporting issuers) |
| provincial_registries | Provincial registries (REQ, OrgBook, …) | insufficient_transport_info | mixed | varies | Provincial companies (coverage gap) |

## What Each Source Contributes

- **corporations_canada_federal** — corporation number, BN, name (EN/FR),
  legislation, status, anniversary date, full address, annual filing info, director
  counts. The free federal identity layer (federal corporations only).
- **corporations_canada_api** — director **names** + corporate history (planning-
  only; PII).
- **sedar_plus** — reporting-issuer financial statements (CAD, IFRS/ASPE).
- **provincial_registries** — provincial corporations (Québec NEQ open, BC OrgBook
  open; others vary). The coverage layer the federal dataset lacks.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.corporation_number`**
(+ business_number, and a provincial_registry_number slot) and groups fields by
real concepts: registration (federal id + BN + provincial id), tax_identifiers
(tax_id = BN; vat_id not available), legal_identity (bilingual names + governing
legislation), status, incorporation, registered_location (full address), directors
(counts open; names planning-only/PII), and financial_statements[] (reporting
issuers only). The `example.json` uses the **real** federal record for MINDANGLER
CAPITAL INC., with director names and financials left planning-only/redacted.

## Join And Precedence Rules

- **Corporation number** (federal id) + **BN** (CRA, the cross-source join key:
  federal ↔ provincial ↔ SEDAR+). Provincial companies use a provincial registry
  number (e.g. Québec NEQ). Precedence: federal dataset (identity) > federal API
  (real-time/directors) > provincial (coverage) > SEDAR+ (financials). No VAT id.

## Missing Or Restricted Data

- **Provincial companies** — not in the federal dataset (provincial registries,
  mixed/paid).
- **Director names** — federal API / provincial (PII; planning-only).
- **NAICS / activity code, financials (non-issuer), beneficial owners** — not in
  the federal open data.

## Common Mapper Notes

Canada is a **multi-jurisdiction** country (federal + 13 provinces) with **two
federal identifiers** (corporation number + BN) and **no VAT id**. Map
`company_id`←corporation number, `tax_id`←BN; treat the federal dataset as a
**subset** (add provinces for full coverage); map `financials` only for reporting
issuers (SEDAR+); mark NAICS / officers / owners / non-issuer financials
`not_available` for an open-only pipeline. See
`common_field_mapping_suggestions.md`.
