# Company Data Analysis For Latvia

## Summary

Latvia is **best-in-class fully-open** and uniquely **CC0-1.0 (public domain)** — no attribution required,
commercial use allowed. A single authoritative source — the **Register of Enterprises (Uzņēmumu reģistrs / UR)**
on **data.gov.lv** — supplies, keyed on the **regcode** (11-digit registration number): company identity,
**structured financial statements** (balance sheet + income statement + cash flow line items, with **employee
counts**), **registered members**, **beneficial owners**, officers, share capital and lifecycle events. This is
one of the richest practical company profiles of any country analyzed — identity, ownership (two layers),
governance and structured financials, all open.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ur_register | UR Register of Enterprises | recommended | public | CC0-1.0 | **identity spine** |
| ur_financial_statements | Gada pārskatu finanšu dati | recommended | public | CC0-1.0 | **structured financials** |
| ur_beneficial_owners | Patiesie labuma guvēji | recommended | public | CC0-1.0 | beneficial owners (open) |
| ur_officers_members | Officers / members / equity / events | useful_secondary | public | CC0-1.0 | officers, members, share capital, events |
| vid_vat | VID / VIES (LV VAT) | useful_secondary | public | validation | VAT (= LV + regcode) |
| data_gov_lv | data.gov.lv | useful_secondary | public | CC0 (UR) | CKAN access/refresh |

## What Each Source Contributes

- **ur_register** — the identity spine (verified: **485,134 entities**): regcode, name (+ parsed parts), legal
  form (SIA/AS/IK), sub-register, registration/termination dates, address + postcode + ATVK, SEPA id. CC0, daily.
- **ur_financial_statements** — **structured financials** in four joined CSVs (verified: **1,970,094 reports**):
  report metadata (year, period, **employees**, currency) + balance sheets (total_assets, equity, …) + income
  statements (net_turnover, net_income, …) + cash flow. Join via statement_id/file_id then regcode. EUR (pre-2014
  LVL).
- **ur_beneficial_owners** — beneficial owners (name, birth date, nationality, control) as **open** CSV (unusual
  post-CJEU).
- **ur_officers_members** — officers (amatpersonas), registered members/shareholders (dalībnieki), share capital
  (equity-capitals), and lifecycle events (insolvency, liquidation, reorganization, historical names).
- **vid_vat** — VAT = `LV` + regcode (derivable); VIES validates.
- **data_gov_lv** — the CKAN portal/API that hosts and refreshes the UR datasets.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.regcode` and groups: `tax_identifiers` (VAT =
LV+regcode), `legal_identity`, `status`, `incorporation`, `registered_location` (postcode + ATVK),
`share_capital`, **`officers[]`**, **`members[]`** (registered), **`beneficial_owners[]`** (open BO), and
**`financial_statements[]`** — each report carrying year/employees/currency + total_assets/equity/net_turnover/
net_income (pivoted from the four CSVs). Every section carries `source_provenance`. The example uses real register
values (regcode 40103550818) + the real financial-report shape; person identities are redacted (GDPR).

## Join And Precedence Rules

- **Single join key:** regcode (11-digit). **Financial bridge:** statement_id/file_id (parts) + regcode (report
  → company).
- **Single authoritative source** (UR) — no aggregator reconciliation; only daily (register) vs annual
  (financials) cadence.
- **Three person/ownership layers kept distinct:** officers / registered members / beneficial owners.

## Missing Or Restricted Data

- **No NACE/activity code** in the register CSV (other UR/CSP datasets if needed); **no separate tax id** (VAT =
  LV+regcode).
- **GDPR**: officers, members and beneficial owners are personal data — lawful basis + retention; no direct
  marketing. CC0 governs IP reuse only.
- **Currency**: EUR; pre-2014 financials may be LVL; apply `rounded_to_nearest`.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← regcode, vat_id ← LV+regcode, legal_name/status/
legal_form/incorporation_date/registered_address ← register, officers ← amatpersonas, owners ← members
(dalībnieki) AND beneficial owners. Latvia is a **model for structured open financials** (`financials` ←
annual-report CSVs, with employees, EUR, no OCR) and uniquely lets `owners` carry both registered members and
beneficial owners. Mark activity_code and tax_id as `not_available_in_open_sources` in the register.
