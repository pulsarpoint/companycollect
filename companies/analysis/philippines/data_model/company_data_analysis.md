# Company Data Analysis For Philippines

## Summary

The Philippines' official corporate register is the **SEC**, keyed on the **SEC
Registration Number**, but its company documents are **paid**: the **General
Information Sheet (GIS)** (directors/officers/stockholders/capital) and **Audited
Financial Statements (AFS)** are filed via **eFAST** (login) and obtained through the
**SEC Express System** (paid per document); the main site is WAF-blocked. The **TIN**
(BIR) is the tax id; VAT-registered businesses use the TIN (no separate VAT number).

The one genuinely **open** source is **PSE EDGE** for **listed companies** — verified
live (PLDT Inc. / TEL / Services-Telecommunications / listed 1953-09-17) — including
disclosures and financial reports. **Sole proprietorships** are registered with
**DTI BNRS** (free name search). **data.gov.ph** has no accessible company dataset.
So there is **no open bulk corporate register and no open private financials** —
ingestion is `blocked_payment` (SEC) + open-for-listed (PSE). Currency **PHP**; GIS
officers/stockholders are personal data (Data Privacy Act 2012). No SEC per-company
values were captured (paywall not bypassed).

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| pse_edge | PSE EDGE — listed directory + disclosures | ready | **open** | public disclosure | Listed identity + financials |
| sec_express | SEC Express — GIS/AFS/Articles | blocked_payment | paid; eFAST login | paid | Corporate identity + financials |
| dti_bnrs | DTI BNRS — sole-prop business names | insufficient_transport_info | free search | not stated | Sole proprietors |

(data.gov.ph is recorded in discovery as unavailable — JS SPA, no accessible dataset.)

## What Each Source Contributes

- **pse_edge** — open listed-company directory (name, symbol, sector, subsector,
  listing date) + disclosures/financial reports (PHP). Verified live (PLDT/TEL).
- **sec_express** — the canonical corporate record (SEC reg number, type, status,
  incorporation date, address, capital, directors/officers/stockholders, AFS), paid.
  Field model from public product descriptions.
- **dti_bnrs** — sole-proprietor business-name existence + BN number (free search).

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **sec_registration_number** (with
`dti_bn_number` for sole props) and sections: `tax_identifiers` (tin; no VAT),
`legal_identity`, `status`, `activity` (primary purpose / PSE sector),
`registered_location`, `capital` (PHP, paid), `owners`/`officers` (redacted, paid),
`listing` (PSE, open), `financial_statements[]` (AFS paid / PSE listed), and
`source_provenance[]`. The example uses the PSE-verified **PLDT Inc.** (TEL) with SEC
identifiers null.

## Join And Precedence Rules

- **SEC Registration Number** is the corporate key; **TIN** links tax; **DTI BN** for
  sole props; **PSE symbol** keys the listed entity (join to SEC by name).
- **SEC** authoritative for corporate identity + financials (paid); **PSE** for
  listed (open); **DTI** for sole proprietors.

## Missing Or Restricted Data

- **No open bulk corporate register; no open private financials** — SEC paid; only
  PSE EDGE (listed) is open.
- **No company dataset on data.gov.ph** (JS SPA).
- **No VAT number** (TIN-based).
- **Directors/officers/stockholders** redacted as personal data (Data Privacy Act
  2012).

## Common Mapper Notes

`company_id == SEC Registration Number` (corporations) / **DTI BN** (sole props);
`tax_id == TIN`; no `vat_id`. The blocker is **paid SEC documents**; the open path is
**PSE EDGE** (listed). Currency **PHP**. See `common_field_mapping_suggestions.md`.
