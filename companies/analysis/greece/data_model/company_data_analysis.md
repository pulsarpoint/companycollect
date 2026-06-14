# Company Data Analysis For Greece

## Summary

Greece is a **partial-open / automation-blocked** country. The authoritative register, **GEMI** (Γενικό
Εμπορικό Μητρώο), holds the full company identity (name EL/EN, legal form, status, seat, ΚΑΔ activity,
incorporation, directors) and filed financial statements, and is **free to search manually** — but its
underlying `/api` is **undocumented, rate-limited and reCAPTCHA-protected** (no open bulk; not bypassable), and
financial statements are **document-based PDF**. Everything joins on the **ΑΦΜ** (AFM, 9-digit tax id) and the
**GEMI number** (VAT = `EL` + ΑΦΜ). A lawful automated profile therefore needs **AADE credentials** (per-ΑΦΜ
tax-side data) or a **commercial provider** (structured financials at scale); open data only supports
**manual lookups** plus ΑΦΜ↔name cross-references from Diavgeia/procurement.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| gemi_portal | GEMI publicity portal | blocked_by_authentication (automation); recommended (manual) | public | public (terms unclear) | **identity spine** (manual) |
| gemi_financial_statements | GEMI financial statements | blocked_by_authentication | public | public docs | **financials** (PDF) |
| aade_rgwspublic | AADE RgWsPublic | blocked_by_authentication | restricted | credentials | tax-side basic data (primary ΚΑΔ, status) |
| vies_vat | VIES (EL VAT) | useful_secondary | public | validation | VAT validation |
| data_gov_gr | data.gov.gr | useful_secondary | public | open | not the company register |
| diavgeia | Diavgeia transparency | useful_secondary | public | open | ΑΦΜ↔name cross-ref |
| procurement_kimdis | KIMDIS/ΕΣΗΔΗΣ procurement | useful_secondary | public | open | supplier ΑΦΜ cross-ref |
| commercial_aggregators | ICAP/CRIF, Kyckr | useful_secondary | paid | commercial | structured financials at scale |

## What Each Source Contributes

- **gemi_portal** — the authoritative identity: GEMI number, ΑΦΜ, name (EL/EN), legal form, status, seat, ΚΑΔ,
  incorporation, directors. Free **manual** search; automated `/api` blocked (reCAPTCHA + 429).
- **gemi_financial_statements** — annual financial statements (ΕΛΠ/IFRS) + balance sheets, as **PDF** on the
  company page. Document-based; OCR/parse or a provider for structured figures. EUR.
- **aade_rgwspublic** — tax-side company basic data by ΑΦΜ (name, address, **primary ΚΑΔ**, ΔΟΥ, active/ceased);
  requires registered TaxisNet credentials. Planning-only.
- **vies_vat** — validates EL VAT (EL + ΑΦΜ); may return name/address. Enrichment only.
- **data_gov_gr** — curated statistical open data; **not** the company register.
- **diavgeia** / **procurement_kimdis** — open government data referencing company **ΑΦΜ + name**; cross-reference.
- **commercial_aggregators** — vendors reselling GEMI/AADE + **parsed financials** + credit; the realistic route
  to structured financials at scale. Paid, planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.gemi_number` with `tax_identifiers.afm` as the
universal cross-source key. It groups `legal_identity`, `status` (+ tax status), `activity` (ΚΑΔ + primary),
`incorporation`, `registered_location`, `officers[]`, `financial_statements[]` (planning-only, PDF/vendor, EUR),
and `public_sector_links[]` (Diavgeia/procurement ΑΦΜ cross-references). Every section carries
`source_provenance`. The example record is **schematic** (placeholder values; financials/officers empty/redacted)
because no per-company open record was lawfully downloadable.

## Join And Precedence Rules

- **Keys:** GEMI number (register) + **ΑΦΜ** (universal cross-source key); VAT = EL + ΑΦΜ.
- **Precedence:** GEMI authoritative for identity/status/ΚΑΔ/directors (manual); AADE (credentialed) adds primary
  ΚΑΔ + tax status; VIES validates VAT; financials from GEMI PDFs or a vendor; Diavgeia/procurement cross-ref.
- **Automation:** GEMI is blocked (reCAPTCHA + rate limits) — manual, AADE credentials, or a provider only.

## Missing Or Restricted Data

- **No open bulk** export; **no structured open financials** (PDF only).
- **Automated GEMI access blocked** (reCAPTCHA + rate limits).
- **Beneficial ownership** (Μητρώο Πραγματικών Δικαιούχων) is access-controlled — `not_available_in_open_sources`.
- **GEMI reuse terms unclear**; confirm before redistribution.
- **GDPR**: directors/representatives are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← GEMI number, tax_id ← ΑΦΜ, vat_id ← EL+ΑΦΜ,
legal_name/status/legal_form/incorporation_date/registered_address/activity_code ← GEMI, officers ← GEMI
representatives. Map `financials` to GEMI PDFs (OCR) or a vendor (parsed) — not a structured feed. Mark
`owners` (beneficial ownership) and `dissolution_date` as `not_available_in_open_sources`. Treat Greece as
requiring manual/credentialed/commercial access rather than open bulk.
