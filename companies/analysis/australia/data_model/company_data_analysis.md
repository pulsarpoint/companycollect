# Company Data Analysis For Australia

## Summary

Australia has an **open identity backbone** but **paid company-register detail and
mostly-paid financials**. The **Australian Business Register (ABR) / ABN Lookup
Bulk Extract** (Australian Taxation Office, via data.gov.au) is a free, **CC-BY
3.0 AU**, **weekly** bulk of **every ABN holder** — ABN, ACN (companies), entity
type, legal name, trading/business names, state + postcode, GST registration.
Verified live (real record: QBE Insurance (International) Ltd, ABN 11000000948,
ACN 000000948). Everything keys on the **ABN** (11-digit, also the tax id);
companies also carry an **ACN** (9-digit). Australia has **no separate VAT
number** — GST registration is the indirect-tax flag.

The open extract **lacks** street address, incorporation date, ANZSIC activity,
officers, and financials — those require **paid ASIC** (company register +
financial reports). **Financials** are essentially **listed-only** (free via the
**ASX**) or **paid** (ASIC); **most small proprietary companies do not lodge**
financials publicly. So the open profile is strong on identity but thin on detail/
financials. Individual/sole-trader names (ABR) and officeholders (ASIC) are
personal data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| abr_bulk_extract | ABR / ABN Lookup Bulk Extract | ready | public | CC-BY 3.0 AU | Open identity backbone |
| abn_lookup_api | ABN Lookup web services | blocked_authentication | free GUID | CC-BY 3.0 AU | Per-ABN enrichment |
| asic_company_register | ASIC company register | blocked_payment | paid | paid extracts | Address, incorporation date, officers |
| asic_financial_reports | ASIC financial reports | blocked_payment | paid | paid per document | Financials (lodging companies) |
| asx_listed | ASX listed-company financials | planning_only | public (listed) | issuer disclosure | Financials (listed only) |

## What Each Source Contributes

- **abr_bulk_extract** — ABN, ACN (ASICNumber), entity type, legal name,
  trading/business names, state + postcode, GST registration, status. The free
  identity layer for the whole ABN population (filter by ACN/entity type for
  companies).
- **abn_lookup_api** — the same public fields per ABN, real-time (free GUID).
- **asic_company_register** — full registered address, **incorporation date**,
  precise company status, **officeholders** → paid.
- **asic_financial_reports** — financials for lodging companies → paid.
- **asx_listed** — listed-issuer financials (AUD, AASB/IFRS) → the free financial
  route, listed-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.abn`** (+ acn) and
groups fields by real concepts: registration, tax_identifiers (tax_id = ABN,
gst_registered; vat_id not available), legal_identity (name, entity type, trading
names), status (ABN status open; ASIC status paid), registered_location
(state+postcode open; full address paid), incorporation (paid), officers[]
(paid, PII), and financial_statements[] (listed/paid). The `example.json` uses
**real** ABR identity for QBE Insurance (International) Ltd, with paid-ASIC fields
(incorporation date, full address, officers, status) and financials left null.

## Join And Precedence Rules

- **ABN** is the universal key + tax id; **ACN** (= ABR `ASICNumber`) joins
  companies to ASIC; ASX ticker links listed financials to ACN/ABN. Precedence:
  ABR (identity) > ABN Lookup API (refresh) > ASIC (address/date/officers/status;
  paid) > ASX/ASIC financials (listed/paid). No VAT id to derive.

## Missing Or Restricted Data

- **Street address, incorporation date, ANZSIC, officers, ownership** — not in the
  open ABR; **paid ASIC**.
- **Financials** — listed-only (free ASX) or paid (ASIC); most small companies
  don't lodge.
- **Personal data** — sole-trader/individual names (ABR), officeholders (ASIC).

## Common Mapper Notes

Australia is a **two-identifier** country (ABN + ACN) with **open identity** but
**no VAT id** and **paid detail/financials**. Map `company_id`/`tax_id`←ABN,
`registration_number`←ACN, derive nothing for `vat_id` (GST flag), and mark
street address / incorporation date / activity / officers / non-listed financials
`not_available` for an open-only pipeline. See
`common_field_mapping_suggestions.md`.
