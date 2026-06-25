# Company Data Analysis For United Arab Emirates

## Summary

The UAE has **no single national company register**. Company registration is split
across **emirate-level DEDs** (trade-license registries — Dubai DET / Invest in Dubai,
Abu Dhabi ADDED, Sharjah SEDD, …), **free-zone registrars** (DIFC and ADGM public
registers, plus DMCC/JAFZA and ~40 others), and the federal **National Economic
Register (NER)** unified search (Ministry of Economy). A company profile is
**designable** keyed on the **trade/commercial license number** (per emirate) or the
**free-zone registration number**, with the **NER economic register number** as the
national unified id and the **TRN** (Tax Registration Number, 15-digit FTA, also the
VAT id) as the tax key.

But **every registry layer is gated** (login/WAF/rate-limited) from this environment,
and the **open-data portals (bayanat.ae / data.gov.ae) were unreachable**. Listed-
company financials come from **DFM/ADX** (public via the browser but WAF/auth-gated).
So there is **no open company register and no open programmatic financials**.
Currency **AED**; owners/managers are personal data (PDPL; DIFC/ADGM DP laws). No
registry per-company values were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| national_economic_register | NER — Ministry of Economy | blocked_authentication | login-gated | restricted | Unified company id |
| emirate_deds | Emirate DEDs (Dubai/Abu Dhabi/…) | blocked_authentication | per-emirate WAF/login | restricted | Mainland trade licenses |
| freezone_public_registers | DIFC & ADGM public registers | blocked_authentication | browser; WAF/rate-limited | public register | Free-zone entities |
| dfm_adx_listed | DFM & ADX — listed financials | blocked_authentication | browser; WAF/auth-gated | public disclosure | Listed identity + financials |

(bayanat.ae / data.gov.ae is recorded in discovery as unavailable.)

## What Each Source Contributes

- **national_economic_register** — the unified company number + licensing authority
  routing + legal form/status/activity/emirate. Login-gated.
- **emirate_deds** — mainland trade-license records (trade name, license number/type,
  legal form, status, expiry, activities, owners). Per-emirate, WAF/login.
- **freezone_public_registers** — DIFC/ADGM entity records (name, registration number,
  type, status, address, incorporation date). Browser-public, WAF-gated.
- **dfm_adx_listed** — listed-company profiles + financial statements (AED), keyed on
  the exchange symbol. Browser-public, WAF/auth-gated.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on the **trade_license_number** (with
`economic_register_number` as the unified id and `license_authority` routing) and has
sections: `tax_identifiers` (trn = vat_id), `legal_identity`, `status` (incl. license
expiry), `activity` (DED/ISIC + exchange sector), `registered_location` (emirate +
free-zone address), `owners` (redacted, gated), `listing` (DFM/ADX), and
`financial_statements[]` (DFM/ADX listed). The example uses the public-knowledge
DFM-listed **Emaar Properties PJSC (EMAAR)** with registry identifiers null.

## Join And Precedence Rules

- **No single national id** — `company_id` is the per-authority license/registration
  number; the **NER economic register number** unifies; **TRN** links tax; **DFM/ADX
  symbol** keys the listed entity.
- The **issuing authority** is authoritative for identity; the **NER** unifies;
  **DFM/ADX** for listed financials. All gated.

## Missing Or Restricted Data

- **No open register; no open programmatic financials** — every layer login/WAF/
  rate-limited.
- **No company dataset on data.gov.ae / bayanat.ae** (unreachable).
- **No separate VAT number** (VAT = the TRN).
- **Incorporation dates, owners/managers** are in gated records — redacted as personal
  data (PDPL; DIFC/ADGM DP laws).

## Common Mapper Notes

The UAE has **no single national company id** — map `company_id` to the per-authority
license/registration number and keep the NER unified number + TRN. The blocker is
**end-to-end gating**; the only browser-public source is **DFM/ADX** (listed).
Currency **AED**. See `common_field_mapping_suggestions.md`.
