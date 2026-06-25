# Armenia Company Profile — Mapping

Armenia's company data hinges on the **8-digit TIN (ՀՎՀՀ / HVHH)** — shared by the State
Register and the SRC — but the sources are gated. The authoritative **State Register**
(`e-register.am`) is **Radware bot-protected** (planning-only); the **SRC taxpayer search**
(`src.am`) is **browser-public per-TIN** (name/status/VAT); the **AMX** lists securities by
**ISIN** (JS SPA). `data.gov.am` did not resolve and the civic Open Data Armenia carries no
register. No registry per-company values were captured.

## Identifiers

- **TIN / ՀՎՀՀ (HVHH)** — 8-digit; tax id and universal join key (register ↔ SRC).
- **State registration number** — State Register key (bot-protected).
- **ISIN** — `AMxxxxxxxxxx`; AMX listed securities (listed only).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.tin_hvhh | src_taxpayer_search | tin_hvhh | yes | SRC/register | browser-public (SRC) |
| registration.state_registration_number | state_register_eregister | state_registration_number | yes | register | bot-protected (planning-only) |
| legal_identity.legal_name | src_taxpayer_search | taxpayer_name | no | register > SRC | register authoritative; SRC browser |
| legal_identity.legal_form | state_register_eregister | legal_form | no | register | ՍՊԸ/ԲԲԸ/ՓԲԸ (planning-only) |
| status.registration_status | state_register_eregister | status | no | register | active/liquidated (planning-only) |
| status.taxpayer_status | src_taxpayer_search | taxpayer_status | no | SRC | TAX status, not registration |
| status.vat_status | src_taxpayer_search | vat_status | no | SRC | VAT registration indicator |
| status.registration_date | state_register_eregister | registration_date | no | register | planning-only |
| registered_location.registered_address | state_register_eregister | registered_address | no | register | bot-protected |
| officers[] | state_register_eregister | director_and_founders | no | register | **PERSONAL DATA — REDACT** |
| owners[] | state_register_eregister | director_and_founders | no | register | **PERSONAL DATA — REDACT** |
| listing.isin | amx_listed | isin | no | AMX | listed only; SPA |
| listing.instrument_name | amx_listed | instrument_name | no | AMX | listed only |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Identity / registration / legal form / status / address / officers / owners**: the
  **State Register** is authoritative (bot-protected, planning-only). **Tax identity (TIN,
  taxpayer/VAT status, name)**: the **SRC** taxpayer search (browser-public). **Listing**:
  **AMX** (by ISIN).
- **Join**: register ↔ SRC on the **TIN (ՀՎՀՀ)** (both use it); AMX joins by **name** (no TIN
  published on the page).
- **Distinguish statuses**: State Register `registration_status` vs SRC `taxpayer_status`
  (tax) — do not conflate.
- **Language** Armenian (+ English); **currency** AMD.

## Missing / restricted

- **State Register is Radware bot-protected** → registration number, legal form, registration
  status/date, address, officers, owners are **planning-only**; nothing captured.
- **SRC** is **per-TIN** (no bulk/API); covers individuals (personal data — redact).
- **AMX** populated listings are not cleanly available (JS SPA; no public API found).
- **data.gov.am** did not resolve; the civic Open Data Armenia has no register.
- The **TIN** and **state registration number** are public; director/founders/individual
  taxpayers are personal data — redact.
