# Company Data Analysis For Estonia

## Summary

Estonia is **best-in-class fully-open**. A single authoritative source — the **e-Business Register open data**
(Äriregister, run by **RIK**) — supplies, free under **CC-BY 4.0** and keyed on the **registrikood** (8-digit):
company identity, **structured financial statements** (line items, not PDF), **registered shareholders**,
**beneficial owners** and **officers**. KMKR (VAT, `EE` + 9 digits) doubles as the tax id. Financials join via
**report_id**. This yields one of the richest practical company profiles of any country analyzed — identity,
ownership (two layers), governance and structured financials all from open data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ariregister_company_data | Company basic + general data | recommended | public | CC-BY 4.0 | **identity spine** |
| ariregister_annual_reports | Annual report financials | recommended | public | CC-BY 4.0 | **structured financials** |
| ariregister_beneficial_owners | Beneficial owners (kasusaajad) | recommended | public | CC-BY 4.0 | beneficial owners (open) |
| ariregister_shareholders | Shareholders (osanikud) | recommended | public | CC-BY 4.0 | registered owners |
| ariregister_persons_other | Persons on card + other | useful_secondary | public | CC-BY 4.0 | officers, pledges, rulings |
| ariregister_api | XML/REST API | useful_secondary | public | CC-BY 4.0 | real-time lookups |
| emta_vat | EMTA VAT / tax / VIES | useful_secondary | public | validation/open | VAT validity, tax debt |
| avaandmed_portal | National open data portal | useful_secondary | public | per dataset | discovery |

## What Each Source Contributes

- **ariregister_company_data** — the identity spine (verified: 373,025 companies): registrikood, nimi, legal
  form, KMKR (VAT), status, first registration date, normalized address + EHAK. CSV/JSON/XML/Parquet, daily.
- **ariregister_annual_reports** — **structured financial statements** in three joined layers: report metadata
  (year, audited, consolidated, auditor), per-year balance-sheet/income-statement **line items** (XBRL-like
  element names + EUR values), and revenue by activity/geography. Join report_id → registrikood. Monthly,
  2019–2025. Verified by downloading the 2024 elements + report metadata + EMTAK revenue.
- **ariregister_beneficial_owners** — beneficial owners as **open** bulk (unusual post-CJEU). Reachable (27 MB).
- **ariregister_shareholders** — registered shareholders/members (osanikud) as open bulk. Reachable (33 MB).
- **ariregister_persons_other** — officers (persons on the registry card) + registry cards, commercial pledges,
  court rulings.
- **ariregister_api** — ~16 real-time XML services for single-company refresh.
- **emta_vat** — VAT validity (VIES) + EMTA tax-debt datasets (risk/enrichment).
- **avaandmed_portal** — national CKAN catalog for discovery.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.registrikood` and groups: `tax_identifiers`
(KMKR + VIES validity), `legal_identity`, `status`, `activity` (EMTAK), `incorporation`, `registered_location`
(EHAK), `officers[]`, **`shareholders[]`** (registered owners), **`beneficial_owners[]`** (open BO), and
**`financial_statements[]`** — each report carrying year/audited/consolidated/auditor plus an `elements[]` array
of pivoted line items (table + XBRL-like element + EUR value). Every section carries `source_provenance`. The
example record uses real basic-data values (007 Autohaus osaühing) and the real financial element shape;
person identities are redacted (GDPR).

## Join And Precedence Rules

- **Single join key:** registrikood (8-digit). **Financial bridge:** report_id (elements + EMTAK join on it;
  report metadata carries registrikood).
- **Single authoritative source** — no aggregator reconciliation; only daily (company/owners) vs monthly
  (financials) cadence.
- **Three person/ownership layers kept distinct:** officers / registered shareholders / beneficial owners.

## Missing Or Restricted Data

- **Dissolution date** not a basic-data column — derive from status.
- **Exact employee count** not in the open company data (financial figures/revenue are).
- Deeper `yldandmed` JSON fields (capital, contacts) not fully cataloged — `raw_extension` until parsed.
- **GDPR**: officers, shareholders, beneficial owners are personal data — lawful basis + retention; no direct
  marketing. CC-BY governs IP reuse only, not data protection.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← registrikood, tax_id/vat_id ← KMKR, legal_name/
status/legal_form/incorporation_date/registered_address ← basic data, activity_code ← EMTAK, officers ←
kaardile_kantud_isikud. Estonia is a **model for structured open financials** (`financials` ← annual-report
elements, EUR, no OCR) and uniquely lets `owners` carry **both** registered shareholders and beneficial owners.
Mark dissolution_date and employee count as `not_available_in_open_sources`.
