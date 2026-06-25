# Company Data Analysis For Armenia

## Summary

Armenia's company data hinges on the **8-digit TIN (ՀՎՀՀ / HVHH)** — shared by the State
Register and the State Revenue Committee — but, contrary to Armenia's open-data reputation,
the sources are gated from this environment. The authoritative **State Register of Legal
Entities** (`e-register.am`, Ministry of Justice) protects its free company search behind
**Radware Bot Manager** (no open bulk/API; planning-only). The **SRC taxpayer search**
(`src.am`) is **browser-public per-TIN**, returning a taxpayer **name** and **status** (and
VAT status). The **Armenia Securities Exchange** lists securities by **ISIN** but is a JS SPA
with no clean public API. The national portal `data.gov.am` did not resolve, and the civic
**Open Data Armenia** (CKAN) carries **no company register**. A company profile can be
**modelled** around the TIN, but no registry per-company values were captured (and none were
fabricated).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| state_register_eregister | State Register (e-register.am) | blocked_authentication | public search, Radware bot-protected | restricted | Authoritative register: reg. number, legal form, status, address, officers/owners |
| src_taxpayer_search | SRC taxpayer search | insufficient_transport_info | browser-public per-TIN | restricted | Tax identity: TIN, name, taxpayer/VAT status |
| amx_listed | Armenia Securities Exchange | insufficient_transport_info | browser-public SPA | public disclosure | Listed securities (ISIN) |

(`opendata_armenia` is a civic CKAN portal with no register, and `data.gov.am` did not resolve — not modeled.)

## What Each Source Contributes

- **State Register** — the authoritative register: state registration number, TIN, legal name,
  legal form (ՍՊԸ/ԲԲԸ/ՓԲԸ), status, registration date, registered address, director and
  founders/participants. Radware bot-protected; planning-only; director/founders are personal
  data — redact.
- **SRC taxpayer search** — **TIN (ՀՎՀՀ)**, taxpayer name, taxpayer status (active/inactive),
  and VAT status, by per-TIN browser lookup. Covers individuals too (personal data); no
  bulk/API.
- **AMX** — listed-security **ISINs** (`AMxxxxxxxxxx`) and issuer names; JS SPA, browser-public,
  no clean public API found; listed only.

## Proposed Country Company Profile

A TIN-keyed object with sections: `registration` (tin_hvhh, state_registration_number),
`legal_identity` (name, legal form), `status` (register registration_status + SRC
taxpayer_status + vat_status + registration_date), `registered_location` (register),
`officers` + `owners` (register, redacted), and `listing` (AMX ISIN), each with
`source_provenance`. The example is anchored on the TIN-keyed model with register-gated fields
null and personal data `[REDACTED-PII]` (no real company captured — all sources gated).

## Join And Precedence Rules

- **Primary key**: TIN (ՀՎՀՀ), shared by register and SRC. **Join** register ↔ SRC on the TIN;
  **AMX** joins by **name** (no TIN on the page).
- **Precedence**: State Register authoritative for identity/legal form/status/address/officers
  (bot-protected); SRC for tax identity (browser); AMX for listing.
- **Keep two statuses distinct**: State Register `registration_status` vs SRC `taxpayer_status`
  (tax).
- **Language** Armenian (+ English); **currency** AMD; dates Gregorian.

## Missing Or Restricted Data

- **State Register is Radware bot-protected** → registration number, legal form, registration
  status/date, address, officers, owners are **planning-only**; nothing captured.
- **SRC** is **per-TIN** (no bulk/API) and mixes **individuals** (personal data — redact).
- **AMX** populated listings are not cleanly available (JS SPA).
- **data.gov.am** did not resolve; the civic Open Data Armenia has no register.
- `activity_code`, `financials`, `dissolution_date` are not available from these sources.

## Common Mapper Notes

`company_id` / `tax_id` → TIN (ՀՎՀՀ, browser via SRC); `registration_number`, `legal_form`,
`incorporation_date`, `registered_address`, `officers`, `owners` → bot-protected State
Register (planning-only). `vat_id` is a status; `activity_code`/`financials` are
`not_available_in_open_sources`. State Register is `blocked_authentication`; SRC and AMX are
`insufficient_transport_info`.
