# Company Data Analysis For Vietnam

## Summary

Vietnam is a **portal-gated, no-open-bulk** country. An authoritative national
register exists with a **free per-company public search**, but there is **no open
API or bulk download**, and **financial statements are open only for listed
companies**:

- **Register** — the **National Business Registration Portal (NBRP)**,
  `dangkykinhdoanh.gov.vn` (Business Registration Authority, MPI). Free
  per-company search returning name, **enterprise code = tax code (mã số doanh
  nghiệp = mã số thuế, 10–13 digits)**, head-office address, business lines
  (VSIC), legal representative, and legal status — but **Vietnamese-only,
  CAPTCHA-gated on submit, no open API/bulk**. Full coverage only via a **paid
  MOU**.
- **Tax** — the GDT lookup (`tracuunnt.gdt.gov.vn`) confirms a tax code + status
  per company (also CAPTCHA-gated).
- **Open data portal** — `data.gov.vn` has **no enterprise-registration dataset**;
  GSO's enterprise survey is statistical and access-controlled.
- **Financials** — open only for **listed companies** via **HOSE / HNX / SSC**
  (per issuer, VND, VAS, no clean open bulk). Non-listed financials are **not
  published**.

Everything keys on the **enterprise code, which IS the tax code** (Vietnam has no
separate VAT number). The combined profile is therefore **largely planning-only**:
the company id is derivable per company, but bulk identity is paid and most
financials/ownership are unavailable. Legal-representative names are personal data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| nbrp_search | NBRP per-company search | blocked_authentication | public (CAPTCHA) | restricted/unclear | Authoritative identity (gated) |
| nbrp_bulk_mou | NBRP bulk database (MOU) | blocked_payment | paid | contract | Full coverage (planning-only) |
| gdt_taxpayer_lookup | GDT taxpayer lookup | blocked_authentication | public (CAPTCHA) | restricted/unclear | Tax status confirmation (gated) |
| hose_hnx_ssc_disclosure | HOSE/HNX/SSC disclosure | planning_only | public (per-issuer) | issuer disclosure | Listed-company financials |
| vn_aggregators | Commercial aggregators | blocked_license | search/paid | vendor terms | License-uncertain cross-check |

(GSO's Vietnam Enterprise Survey is a statistical, access-controlled survey —
not a per-company register — and is excluded from the model.)

## What Each Source Contributes

- **nbrp_search** — the authoritative company record (enterprise code/tax code,
  name, legal form, status, establishment date, head-office address, VSIC business
  lines, legal representative). Gated per-company.
- **nbrp_bulk_mou** — the same fields in **bulk**, via a **paid MOU** — the only
  full-coverage route.
- **gdt_taxpayer_lookup** — tax-code validation + status, by the same number.
- **hose_hnx_ssc_disclosure** — listed-company balance sheet / income statement /
  cash flow (VND, VAS); listed only.
- **vn_aggregators** — convenience repackaging; not official; license-uncertain.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.enterprise_code`**
(= tax code) and groups fields by real concepts: registration, tax_identifiers
(tax_code/vat_id = same number; tax status), legal_identity, status, activity
(VSIC), incorporation, registered_location, officers[] (legal representative only,
PII-flagged), and financial_statements[] (listed-only, planning-only). The
`example.json` is **schematic** — the NBRP/GDT are CAPTCHA-gated so no real
per-company record was copied; the enterprise code 0100000000 is a placeholder,
officers are redacted, and financials are empty (non-listed).

## Join And Precedence Rules

- **Enterprise code = tax code** is the single universal key (register ↔ tax ↔
  listed financials via ticker). No separate VAT number. Precedence: NBRP
  (identity) > GDT (tax status) > NBRP MOU (bulk; planning-only) > HOSE/HNX/SSC
  (financials; planning-only) > aggregators (cross-check).

## Missing Or Restricted Data

- **All bulk identity** — gated (per-company CAPTCHA) or paid (MOU).
- **Financials** — listed-only; non-listed not published.
- **Shareholders / beneficial owners** — not available (only the legal
  representative; personal data).
- **No open re-use licence**; aggregators license-uncertain.

## Common Mapper Notes

Vietnam is a **single-number** country (`company_id = tax_id = vat_id =` enterprise
code) with **no open bulk** — treat as **manual/licensed-first**. Map identity from
the gated NBRP (or paid MOU), tax status from GDT, financials from HOSE/HNX/SSC
(listed only), `officers` from the legal representative (redacted), and mark
`owners` and non-listed `financials` `not_available`. See
`common_field_mapping_suggestions.md`.
