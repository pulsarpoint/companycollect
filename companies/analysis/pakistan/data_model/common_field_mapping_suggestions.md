# Common Field Mapping Suggestions — Pakistan

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Pakistan profile, which is the source of truth.

Only the PSX data portal is directly implementable (`ready`); SECP is firewalled
(planning-only) and FBR ATL is per-NTN verification (planning-only).

| Common field | Pakistan mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.cuin | secp_eservices | CUIN (firewalled); PSX symbol for listed |
| registration_number | registration.cuin | secp_eservices | CUIN |
| tax_id | registration.ntn | fbr_atl | NTN (per-NTN verification) |
| vat_id | not_available_in_open_sources | — | Pakistan uses sales-tax registration (STRN); not openly published |
| legal_name | legal_identity.legal_name | psx_dataportal | open (PSX); SECP authoritative |
| status | status.registration_status | secp_eservices | SECP status; distinct from FBR atl_status |
| legal_form | legal_identity.company_kind | secp_eservices | private/public ltd, SMC, LLP (planning-only) |
| incorporation_date | status.incorporation_date | secp_eservices | planning-only (firewalled) |
| dissolution_date | not_available_in_open_sources | secp_eservices | status only via SECP (firewalled) |
| registered_address | registered_location.registered_address | psx_dataportal | PSX page (listed) / SECP office |
| activity_code | activity.sector | psx_dataportal | PSX sector (listed only) |
| financials | financial_statements | psx_dataportal | PSX free float/shares (PKR); listed only |
| officers | officers | secp_eservices | **REDACT — personal data** (firewalled) |
| owners | not_available_in_open_sources | — | shareholding not openly published (SECP filings/login) |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Pakistan:

- Open data covers **listed companies only** (PSX). `company_id` (CUIN), `status`,
  `legal_form`, `incorporation_date`, `officers` require the **firewalled SECP** registrar.
- `tax_id` (NTN) is via **per-NTN FBR verification**; `vat_id`/STRN and `owners` are
  `not_available_in_open_sources`.
- Keep **SECP registration status** and **FBR ATL (tax-filing) status** as separate fields.
