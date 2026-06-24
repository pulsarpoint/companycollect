# Company Data Analysis For Montenegro

## Summary

Montenegro's authoritative company register is **CRPS** (Centralni registar
privrednih subjekata), run by the **Revenue and Customs Administration**
(`tax.gov.me`). A full company profile is **designable** keyed on the **PIB**
(8-digit tax id = company id) + CRPS registration number, with the **PDV** (VAT)
number separate. But at investigation time CRPS had **no working open access**: the
legacy domain **`crps.me` is parked** and the current portal
**`eprijava.tax.gov.me/TaxisPortal` returned HTTP 503 (down)** — and there is **no
open bulk/API**. So the CRPS section is documented from public knowledge with **no
captured values** (`insufficient_transport_info`).

The **only working open dataset** is **data.gov.me "Javna preduzeća"** — a real,
openly-licensed list of **public/state enterprises** (name/status/type/founder/
address/website, no PIB). **Financial statements** are filed at CRPS but **not
published openly**. Currency **EUR**.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| crps_business_register | CRPS — Central Registry of Business Entities | insufficient_transport_info | portal 503 / legacy parked | not stated | Primary identity (unavailable) |
| datagovme_javna_preduzeca | data.gov.me — Javna preduzeća | ready | public XLSX/CKAN | open data | Public-enterprise anchor |

(data.gov.me portal and MONSTAT are recorded in discovery as aggregate/statistical
secondary sources, not modelled here.)

## What Each Source Contributes

- **crps_business_register** — the full company record model: PIB, registration
  number, PDV (VAT), name, legal form (DOO/AD/OD/KD), status, registration date,
  activity (KD), address, founders, and filed financial statements. Documented from
  public knowledge; **unavailable** (portal down) so no live values.
- **datagovme_javna_preduzeca** — a working open dataset of **public enterprises**
  with real names/status/type/founder/address/website. No PIB; joins to CRPS by
  name. Used as the example anchor (Investiciono-razvojni fond Crne Gore A.D.).

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **PIB** with sections:
`registration` (pib/registration_number), `tax_identifiers` (tax_id/vat_id),
`legal_identity`, `status`, `activity`, `registered_location`, `public_enterprise`
(open dataset), `owners` (redacted), `financial_statements[]` (planning-only, EUR),
and `source_provenance[]`. The example uses a real public enterprise with
CRPS-held identifiers null (portal down).

## Join And Precedence Rules

- **PIB** is the universal key (held by CRPS). Public-enterprise rows join by
  **name** until CRPS is back.
- **CRPS** authoritative for identity/status/activity/ownership/financials;
  **Javna preduzeća** authoritative for the public-enterprise sub-section.

## Missing Or Restricted Data

- **CRPS unavailable** (portal 503; legacy domain parked) — no open bulk/API, no
  register values captured.
- **No open financials** — filed at CRPS, not published.
- **No full open register** — data.gov.me has only public enterprises + statistics.
- **Officers/dissolution date** not in the open model.
- **Owners** redacted as personal data.

## Common Mapper Notes

`company_id == tax_id == PIB`; `vat_id` separate. The blocker is **availability** of
CRPS, not schema. A future implementation must re-probe the CRPS portal. See
`common_field_mapping_suggestions.md`.
