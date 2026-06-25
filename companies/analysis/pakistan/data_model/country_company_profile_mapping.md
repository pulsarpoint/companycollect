# Pakistan Company Profile — Mapping

Pakistan has **three identifiers** and a split between an **open listed source** and a
**firewalled/gated** registrar + tax register. The **PSX data portal** (open JSON) covers
listed companies; the **SECP** registrar (keyed on **CUIN**) was **WAF-blocked/firewalled**
from this environment (planning-only); the **FBR ATL** (keyed on **NTN**) is **per-NTN
verification** (planning-only). Because PSX publishes no CUIN/NTN, the practical open join is
by **company name**.

## Identifiers

- **CUIN (Company Universal Identification Number)** — SECP registrar key (firewalled here).
- **NTN (National Tax Number)** — FBR tax key (per-NTN verification).
- **PSX symbol** — listed-company ticker (open).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.cuin | secp_eservices | cuin | yes | SECP | firewalled (planning-only) |
| registration.ntn | fbr_atl | ntn | yes | FBR | per-NTN verification |
| registration.psx_symbol | psx_dataportal | symbol | yes | PSX | open; listed only |
| legal_identity.legal_name | psx_dataportal | name | yes | SECP > PSX | SECP authoritative; PSX open |
| legal_identity.company_kind | secp_eservices | company_kind | no | SECP | planning-only |
| status.registration_status | secp_eservices | status | no | SECP | active/dormant/dissolved |
| status.atl_status | fbr_atl | atl_status | no | FBR | TAX status, not registration |
| status.incorporation_date | secp_eservices | incorporation_date | no | SECP | planning-only |
| activity.sector | psx_dataportal | sectorName | no | PSX | listed only |
| registered_location.registered_address | psx_dataportal | company_page.registered_address | no | SECP > PSX | PSX page or SECP office |
| officers[] | secp_eservices | directors | no | SECP | **PERSONAL DATA — REDACT** |
| listing.* | psx_dataportal | symbol / sectorName | no | PSX | listed only |
| financial_statements[] | psx_dataportal | company_page.free_float | no | PSX | PKR; listed only |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Open listed identity (name, sector, symbol, address, free float)**: from the **PSX data
  portal** (free). **Authoritative identity + CUIN + kind + registration status + officers**:
  from **SECP** (firewalled here, planning-only). **Tax status (NTN, ATL)**: from **FBR**
  (per-NTN verification, planning-only).
- **Join**: PSX ⟷ SECP by **company name** (PSX has no CUIN); FBR's **Registration No.** can
  bridge NTN ⟷ SECP CUIN/registration. The **CUIN** is canonical once obtained.
- **Distinguish statuses**: SECP `registration_status` (company registration) vs FBR
  `atl_status` (tax-filing) — do not conflate.
- **Language** English; **currency** PKR.

## Missing / restricted

- **SECP is firewalled/WAF-blocked** here → CUIN, kind, registration status, incorporation
  date, officers are **planning-only** (use SECP from an unblocked network).
- **FBR ATL** has **no open bulk file located** (per-NTN verification); it mixes companies and
  individuals — **individuals are personal data**; redact.
- **Directors** (SECP) are personal data — redact.
- Open coverage is **listed companies only** (PSX); the full company population needs SECP.
