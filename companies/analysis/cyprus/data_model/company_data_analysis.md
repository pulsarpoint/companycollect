# Company Data Analysis For Cyprus

## Summary

Cyprus is a **partial-open** country. A solid open company profile can be built from the **DRCIP Registrar**
open data published on **data.gov.cy** (companies + **officers**, ~567,536 companies / ~2.75M entities), keyed
on the **HE registration number**, plus free **eSearch** for single lookups. Tax and VAT identifiers (TIC, VAT)
are added per company via the Tax Department / VIES. What is **not** open: **financial statements** (public but
**paid** — EUR 10 detailed search, delivered as **scanned PDFs**, no structured figures), **shareholders**
(on the paid HE32 annual return) and **beneficial owners** (restricted UBO register, post-CJEU). So the open
profile is rich on **identity + status + officers**, but **financials and ownership-beyond-officers are
planning-only**.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| drcip_register | DRCIP Registrar (open CSV + eSearch) | recommended | public | Open data (confirm exact) | **Open spine** (identity, status, officers) |
| he32_financial_statements | HE32 + audited financial statements | blocked_by_payment | paid | Public/paid (EUR 10, PDF) | Financials + shareholders (planning-only) |
| data_gov_cy | data.gov.cy open data portal | useful_secondary | public | Per dataset | Discovery / resolve CSV URL + licence |
| ubo_register | UBO beneficial ownership register | blocked_by_authentication | restricted | Conditions/fee (post-CJEU) | Beneficial owners (planning-only) |
| tax_department | Tax Department — TIC / VAT | useful_secondary | public | Validation only | TIC + VAT enrichment |
| opensanctions_mirror | OpenSanctions cy_companies | useful_secondary | public | CC-BY-NC 4.0 | QA cross-reference (non-commercial) |
| commercial_aggregators | CyprusRegistry, Kyckr, … | useful_secondary | paid | Commercial | Scalable structured financials (planning-only) |

## What Each Source Contributes

- **drcip_register** — the authoritative open register: HE registration number (the join key; prefix encodes
  entity type), name (Greek/English), entity type, status, registration date, registered address, and —
  distinctively — **named officers** (directors/secretary) in the open CSV. Confirmed via OpenSanctions.
- **he32_financial_statements** — the official financials: balance sheet, income statement, notes, auditor's
  report, plus share capital and **shareholders** on the HE32 annual return. Public but **paid** and
  **document-based** (scanned PDF) — no XBRL/CSV; needs OCR/parsing. Planning-only.
- **data_gov_cy** — the open-data portal; used to resolve the DRCIP CSV resource URL and confirm its exact open
  licence (CKAN-like API on a non-standard path — resolve via the UI).
- **ubo_register** — beneficial owners; restricted access (conditions/fee post-CJEU). Planning-only.
- **tax_department** — TIC (tax id) and VAT (CY+8 digits+letter) per company; VIES validates VAT. Not a master,
  not redistributable as a list.
- **opensanctions_mirror** — a FollowTheMoney mirror of the open CSV; **QA/cross-reference only** (CC-BY-NC).
  For commercial reuse, take the company list from data.gov.cy directly.
- **commercial_aggregators** — vendors that pre-parse the paid PDFs into **structured** financials; the
  realistic route to financials at scale. Paid, per contract. Planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.registration_number` (HE…) and groups:
`registration`, `tax_identifiers` (TIC/VAT), `legal_identity`, `status`, `activity` (explicitly
`activity_code = not_available`), `incorporation`, `registered_location` (with parsed municipality/region),
and the open **`officers[]`** array. Paid/restricted concepts are kept as clearly marked **planning-only**
sections: `financial_statements[]` (EUR, x-access: paid), `shareholders[]` (paid HE32), and
`beneficial_owners[]` (restricted UBO). Every section carries `source_provenance`. The example record is
schematic (placeholder values; empty paid/restricted sections) because no per-company open record was
downloadable during discovery.

## Join And Precedence Rules

- **Single join key:** DRCIP `registration_number` (HE…) across all sources.
- **Three identifiers, kept separate:** HE (registry), TIC (tax), VAT (CY+8+letter).
- **Three person/ownership layers, kept separate:** open **officers** (DRCIP CSV) ≠ **shareholders** (paid
  HE32) ≠ **beneficial owners** (restricted UBO). Never conflate.
- **Precedence:** open official `drcip_register` is authoritative for identity/status/officers; financials
  prefer official `he32_financial_statements` for fidelity, with `commercial_aggregators` as the scalable
  structured alternative; `opensanctions_mirror` is QA only (CC-BY-NC).

## Missing Or Restricted Data

- **Unavailable in open data:** activity/NACE code (`activity_code` = not_available); explicit dissolution date
  (only implied via status); structured financial figures; shareholders; beneficial owners.
- **Paid:** financial statements + shareholders (HE32, EUR 10, scanned PDF); structured financials via
  commercial vendors.
- **Restricted:** beneficial ownership register (conditions/fee, post-CJEU).
- **Open with care:** officer names are **personal data** — apply a GDPR lawful basis + retention policy, no
  direct-marketing reuse.

## Common Mapper Notes

A future cross-country mapper can map company_id/registration_number ← HE number, tax_id ← TIC, vat_id ← VAT,
legal_name/status/legal_form/incorporation_date/registered_address ← DRCIP, and **officers ← open DRCIP
officers**. Map `financials` and `owners` only to the paid/restricted Cyprus sources, and mark `activity_code`
and `dissolution_date` as `not_available_in_open_sources`. The ambiguous common `owners` field must resolve
explicitly to shareholders (paid) or beneficial owners (restricted), never to the open officers list.
