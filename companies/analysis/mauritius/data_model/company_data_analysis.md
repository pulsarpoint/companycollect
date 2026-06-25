# Company Data Analysis For Mauritius

## Summary

Mauritius offers **one genuinely open but sectoral** company dataset and an **authoritative
but gated** full register. The open layer is **data.govmu.org's "List of ICT Companies in
Mauritius"** (CSV, **CC-BY-SA-4.0**, 1,060 rows: company name, address, district, sectors) —
fully reusable but **ICT-only and with no identifier**. The authoritative register is the
**CBRD/CBRIS** online search (`onlinesearch.mns.global`), keyed on the **BRN (Business
Registration Number)** — but its search is **Cloudflare Turnstile-gated** and full documents
are **paid** (planning-only). **SEM** (Official Market + DEM) covers listed companies
browser-public. A sectoral seed list (with sector + location) can be built openly; the BRN,
status, directors, and ownership require the gated CBRD register. Nothing was bypassed and no
identifiers were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| datagovmu_ict_companies | data.govmu.org ICT Companies | ready | open CSV | CC-BY-SA-4.0 | Open sectoral directory: name, address, sector |
| cbrd_cbris_search | CBRD CBRIS Online Search | blocked_authentication | Turnstile-gated; docs paid | restricted | Authoritative register: BRN, type, status, directors, shareholders |
| sem_listed | Stock Exchange of Mauritius | insufficient_transport_info | browser-public | public disclosure | Listed companies: segment, published accounts |

(`data_govmu_portal` is the CKAN catalog — the access path to the ICT dataset — not modeled as a separate data source.)

## What Each Source Contributes

- **data.govmu.org ICT directory** — the open, free layer: company name (`Title`), address,
  district, and ICT sector(s). CC-BY-SA-4.0; cp1252 encoding; no identifier, status, or
  dates; ICT-sector coverage only. No personal data.
- **CBRD CBRIS** — the authoritative register: **BRN**, company/business name, entity type,
  status (live/removed/defunct), incorporation/registration date, registered office,
  directors, shareholders. Turnstile-gated; documents paid; personal data — redact.
  Planning-only.
- **SEM** — listed-company market segment (Official Market / DEM), published accounts (PDF,
  MUR), and announcements; browser-public; navigable HTML, no clean list/API.

## Proposed Country Company Profile

A name-anchored object (BRN once obtained from CBRD) with sections: `registration` (brn),
`legal_identity` (name, company_type), `status` (CBRD), `activity` (ICT sectors from the open
directory), `registered_location` (address/district), `officers` + `owners` (CBRD, redacted),
`listing` (SEM segment), and `financial_statements` (SEM, PDF/MUR), each with
`source_provenance`. The example is anchored on a **real open-directory company** (A CHAMROO
LTD, Plaine Wilhems, software development) with BRN/status/officers null/redacted (those need
CBRD).

## Join And Precedence Rules

- **Identifier**: the **BRN** is the canonical key but is **gated** (CBRD). The open directory
  has **no identifier** → the practical open join is by **company name**.
- **Precedence**: CBRD authoritative for identity/status/officers/owners (gated); open
  directory for name/address/sector (free); SEM for listing/financials.
- **Language** English; **currency** MUR (SEM).

## Missing Or Restricted Data

- The open directory is **ICT-only** and carries **no identifier, status, or dates**.
- **BRN, status, directors, shareholders, registered office** require the **Turnstile-gated /
  paid CBRD register**.
- **Directors / shareholders** are personal data under the **Data Protection Act 2017** —
  redact.
- **VAT registration** and full **financials** for private companies are not openly published
  (SEM financials are listed-only).

## Common Mapper Notes

`legal_name` / `registered_address` / `activity_code` map to the **open ICT directory**;
`company_id` / `registration_number` / `tax_id` (BRN), `status`, `officers`, `owners` map to
the **gated CBRD register**; `financials` to **SEM** (listed). Only the ICT directory is
`ready`; CBRD is `blocked_authentication`; SEM is `insufficient_transport_info`. Open
coverage is sectoral (ICT) only.
