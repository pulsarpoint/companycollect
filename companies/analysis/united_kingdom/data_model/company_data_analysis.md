# Company Data Analysis For United Kingdom

## Summary

The UK is **best-in-class fully-open** — the only major economy where the
register, bulk data, API, **and** financial accounts are all free, from
**Companies House** under the **Open Government Licence**, joined on the
**company number** (8 characters):

- **Free Company Data Product** (CSV, monthly) — the full register of ~5.9M live
  companies (55 columns): name, address, legal form, status, dates, **SIC
  activity**, accounts metadata, charges counts, previous names.
- **Accounts Bulk Data** (iXBRL, daily + monthly) — **structured financial
  statements** tagged to the FRC/UK GAAP taxonomy (turnover, profit, net assets,
  equity, …), keyed on company number.
- **PSC snapshot** — **beneficial ownership** (persons with significant control),
  free bulk.
- **REST API** (free key, 600 req/5 min) — **officers**, filing history, charges,
  and the document API.

All four were exercised on real data this run: register part1 = 849,999 rows;
one accounts daily zip = 9,717 iXBRL filings; real financials parsed for company
00009604 (turnover £1,615,243; net assets £5,782,684; GBP). The result is a very
rich profile — identity + financials + beneficial owners + officers — all
free/open. Notable limits: **no tax id/VAT** in Companies House (VAT is HMRC),
accounts cover **electronically-filed only** (~60–75%), and officers/PSC are
**personal data** (UK GDPR).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ch_basic_company_data | Free Company Data Product | ready | public | OGL | Register: identity, status, SIC, address |
| ch_accounts_bulk | Accounts Bulk Data (iXBRL) | ready | public | OGL | Structured financials |
| ch_psc_snapshot | PSC snapshot | ready | public | OGL | Beneficial owners |
| ch_rest_api | REST API | blocked_authentication | public (free key) | OGL | Officers, filing history, documents |

## What Each Source Contributes

- **ch_basic_company_data** — company number, name, legal form, status, dates,
  full address, SIC 2007 codes, accounts/CS metadata, charge counts, previous
  names. The register backbone.
- **ch_accounts_bulk** — iXBRL financial facts (FRC taxonomy), GBP, keyed on
  company number; e-filed accounts only.
- **ch_psc_snapshot** — PSC (beneficial owners): name, kind, natures_of_control,
  nationality, month/year of birth. Personal data.
- **ch_rest_api** — officers (the only source), filing history + document API,
  charges, real-time profile/PSC. Free key.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.company_number`** and
groups fields by real concepts: registration, legal_identity (+ previous names),
status, activity (SIC), incorporation, registered_location, accounts_meta,
financial_statements[] (iXBRL/FRC, GBP), officers[] (free-key, PII-flagged), and
owners[] (PSC, PII-flagged). The `example.json` uses **real** data for company
00009604 — real name, real registered address (from iXBRL entity facts), and real
2025 financials — with officers/owners **redacted** and register-only fields left
null (the register row was not joined in this example).

## Join And Precedence Rules

- **Company number** joins everything (zero-pad numeric to 8). Precedence:
  basic data (identity) > accounts (financials) > PSC (owners) > REST API
  (officers/documents). Prefer the register name over the iXBRL entity name. No
  VAT/tax id to derive.

## Missing Or Restricted Data

- **Tax id / VAT** — not in Companies House (HMRC holds VAT).
- **Financials coverage** — e-filed accounts only (~60–75%); iXBRL multi-context
  parsing required.
- **Officers** — only via the free-key REST API (no bulk product).
- **Personal data** — officers + PSC — redact.

## Common Mapper Notes

The UK is a **single-key (company number)** country with **open financials**
(iXBRL/FRC) and **open beneficial ownership** (PSC), but **no VAT/tax id** in the
register. Map `company_id`←company number, financials←iXBRL by FRC tag,
owners←PSC (redacted), officers←REST API (free key); mark `vat_id`/`tax_id`
`not_available`. See `common_field_mapping_suggestions.md`.
