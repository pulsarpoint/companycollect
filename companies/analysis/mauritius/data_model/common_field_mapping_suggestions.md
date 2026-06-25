# Common Field Mapping Suggestions — Mauritius

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Mauritius profile, which is the source of truth.

Only the open ICT directory is directly implementable (`ready`); CBRD fields are
**planning-only** (Turnstile-gated / paid) and SEM is browser-public.

| Common field | Mauritius mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.brn | cbrd_cbris_search | BRN (gated); open directory has no id |
| registration_number | registration.brn | cbrd_cbris_search | BRN |
| tax_id | registration.brn | cbrd_cbris_search | BRN is the MRA tax basis |
| vat_id | not_available_in_open_sources | — | MRA VAT registration not openly published |
| legal_name | legal_identity.legal_name | datagovmu_ict_companies | open (Title); CBRD authoritative |
| status | status.status_text | cbrd_cbris_search | planning-only (gated) |
| legal_form | legal_identity.company_type | cbrd_cbris_search | planning-only |
| incorporation_date | status.incorporation_date | cbrd_cbris_search | planning-only |
| dissolution_date | not_available_in_open_sources | cbrd_cbris_search | status only via CBRD (gated) |
| registered_address | registered_location.address | datagovmu_ict_companies | open (ICT); CBRD office gated |
| activity_code | activity.sectors | datagovmu_ict_companies | ICT free-text sectors (no code list) |
| financials | financial_statements | sem_listed | SEM published accounts (PDF; MUR); listed only |
| officers | officers | cbrd_cbris_search | **REDACT — personal data** (gated/paid) |
| owners | owners | cbrd_cbris_search | **REDACT — personal data** (paid) |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Mauritius:

- The only **open** company data is **name + address + sector** (ICT directory), with **no
  identifier**. `company_id` / `registration_number` / `tax_id` (BRN) require the
  **Turnstile-gated CBRD register**.
- `vat_id`, `dissolution_date`, full `financials`, `officers`, `owners` are
  `not_available_in_open_sources` (CBRD gated/paid, SEM listed-only, or not published).
- Coverage from open data is **ICT-sector only** — not the full company population.
