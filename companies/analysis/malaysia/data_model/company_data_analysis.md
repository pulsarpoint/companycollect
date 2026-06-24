# Company Data Analysis For Malaysia

## Summary

Malaysia's official register is **SSM** (Companies Commission), keyed on the
company **registration number** — the new **12-digit** format (since 2019), with the
old **NNNNNNN-A** format as a cross-reference. A rich profile is **designable**
(identity, status, MSIC activity, capital, directors, shareholders, financials),
but SSM **commercially distributes** its data: company profiles and **financial
statements** are **paid products** via **SSM e-Info** (SAML login) and **MyData-SSM**,
with only a free **e-Search** for existence. **TIN** (LHDN) is the tax id; Malaysia
uses **SST** (no VAT/GST since 2018 — no separate VAT number).

**Listed-company financials** come from **Bursa Malaysia** (public via browser;
WAF-blocked for automation here). **data.gov.my** hosts **no company register**
(DOSM statistics). So there is **no open bulk register and no open financials** —
ingestion is `blocked_payment` (SSM) + listed-only (Bursa). Currency **MYR**;
directors/shareholders are personal data (PDPA 2010). No per-company values were
captured (paywall/WAF not bypassed).

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ssm_einfo | SSM e-Info — Company Profile + Financials | blocked_payment | paid, SAML login | commercial | Primary identity + financials |
| mydata_ssm | MyData-SSM — Buy SSM Report | blocked_payment | paid | commercial | Alternative SSM channel |
| bursa_listed | Bursa Malaysia — listed financials | blocked_authentication | browser; WAF here | public disclosure | Listed financials |

(data.gov.my is recorded in discovery as a statistics-only secondary source.)

## What Each Source Contributes

- **ssm_einfo** — the canonical SSM Company Profile + Financial Comparison/Historical
  (identity, status, MSIC, paid-up capital, directors, shareholders, financials),
  paid. Field model from published sample templates.
- **mydata_ssm** — the same SSM register via a second paid channel (Company Profile +
  Financial Report); no new fields (prefer e-Info).
- **bursa_listed** — listed-company financials/announcements (MYR), public; joins on
  the SSM number. WAF-blocked here.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **registration_number_new** with
sections: `registration` (new + old), `tax_identifiers` (tin/sst; no VAT),
`legal_identity`, `status`, `activity` (MSIC), `registered_location`, `capital`
(MYR, paid), `owners`/`officers` (redacted, paid), `listing` (Bursa),
`financial_statements[]` (paid/listed), and `source_provenance[]`. The example uses
a real public-knowledge listed company (Malayan Banking Berhad, Bursa 1155) with SSM
identifiers null.

## Join And Precedence Rules

- The **SSM 12-digit number** is the universal key; **TIN** links tax; **Bursa stock
  code** keys the listed entity.
- **SSM** authoritative for identity + financials (paid); **Bursa** for listed.

## Missing Or Restricted Data

- **No open bulk register; no open financials** — SSM paid; only e-Search open.
- **No company register on data.gov.my** (DOSM statistics).
- **No VAT** (SST; no separate VAT number).
- **Directors/shareholders** redacted as personal data (PDPA 2010).

## Common Mapper Notes

`company_id == registration_number == SSM 12-digit`; `tax_id == TIN`; no `vat_id`
(SST). The blocker is **commercial distribution** of SSM data. Currency **MYR**. See
`common_field_mapping_suggestions.md`.
