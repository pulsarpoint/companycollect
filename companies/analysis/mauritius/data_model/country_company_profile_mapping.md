# Mauritius Company Profile — Mapping

Mauritius splits between an **authoritative-but-gated** register and a **genuinely open but
sectoral** directory. The **CBRD/CBRIS** register (keyed on the **BRN**) is **Cloudflare
Turnstile-gated** with **paid** documents (planning-only). The one open dataset is the
**ICT-companies directory** (data.govmu.org, CC-BY-SA-4.0) — name + address + sector, **no
identifier**. **SEM** covers listed companies. Because the open directory has no BRN, the
profile's join is by **name**.

## Identifiers

- **BRN (Business Registration Number)** — CBRD/CBRIS registry key + MRA tax basis
  (Turnstile-gated; planning-only).
- (Open directory) — **name only**, no identifier.

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.brn | cbrd_cbris_search | brn | yes | CBRD | Turnstile-gated |
| legal_identity.legal_name | datagovmu_ict_companies | Title | yes | CBRD > directory | name is the open key |
| legal_identity.company_type | cbrd_cbris_search | company_type | no | CBRD | planning-only |
| status.status_text | cbrd_cbris_search | company_status | no | CBRD | planning-only |
| status.incorporation_date | cbrd_cbris_search | incorporation_date | no | CBRD | planning-only |
| activity.sectors | datagovmu_ict_companies | Sectors | no | directory | free-text, newline-split |
| registered_location.address | datagovmu_ict_companies | Address | no | directory / CBRD | + District |
| registered_location.district | datagovmu_ict_companies | District | no | directory | |
| officers[] | cbrd_cbris_search | directors | no | CBRD | **PERSONAL DATA — REDACT** |
| owners[] | cbrd_cbris_search | shareholders | no | CBRD | **PERSONAL DATA — REDACT** |
| listing.market_segment | sem_listed | market_segment | no | SEM | Official Market / DEM |
| financial_statements[] | sem_listed | published_accounts | no | SEM | PDF; MUR |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Open identity (name, address, sector)**: from the **ICT directory** (free, sectoral).
  **Authoritative identity + identifiers + status + officers/owners**: from **CBRD/CBRIS**
  (Turnstile-gated/paid, planning-only). **Listing + financials**: **SEM**.
- **Join**: the open directory ⟷ CBRD by **company name** (the directory has no BRN); SEM ⟷
  register also by **name**. The BRN is the canonical key once obtained from CBRD.
- **Language** English. **Currency** MUR (SEM financials).

## Missing / restricted

- The open directory has **no identifier, status, or dates** and is **ICT-only** (partial
  coverage). Identifiers/status/officers/owners require the **gated CBRD register**.
- **Directors / shareholders** are personal data under the **Data Protection Act 2017** —
  redact.
- **CBRD documents** (constitution, annual return, financials) are **paid**.
- **SEM** has no clean list/API (navigable HTML).
