# Company Data Analysis For Slovenia

## Summary

Slovenia offers **fully-open identity + tax/activity data** but **no open
structured financials**. Two free official datasets (both **CC-BY 4.0**) join on
the **matična številka** (registration number):

- **AJPES PRS — Poslovni register Slovenije** (via OPSI): name, legal form,
  registrar, full structured address. **293,222** entities. CSV (**UTF-16**),
  refreshed twice monthly.
- **FURS — Seznam davčnih zavezancev / legal entities** (via OPSI): **davčna
  številka** (tax number), **VAT status + date**, **SKD activity code**, name,
  address. **144,537** legal entities. CSV (UTF-8 semicolon), daily.

Both were downloaded and the join verified (ISTRA XLL d.o.o., MB 3282490000,
davčna 10001310 → VAT SI10001310, SKD 49.410). Together they give identity + tax
+ VAT + activity for free.

What is **not** open: **status**, **incorporation date**, **officers**, and
**ownership** (only via the **credentialed restPrsInfo** API or the court
register), and **financials** — AJPES **JOLP** shows annual reports
**view-only** per company, while the structured **Fi=Po** database and **S.BON**
ratings are **paid**. So Slovenia is a **partial-open** country: excellent free
identity/tax, but financials are view-only or paid.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ajpes_prs | AJPES PRS business register | ready | public | CC-BY 4.0 | Identity + address |
| furs_zavezanci_po | FURS tax payers (legal entities) | ready | public | CC-BY 4.0 | Tax number, VAT, SKD activity |
| ajpes_restprsinfo | AJPES restPrsInfo API | blocked_authentication | restricted | AJPES terms | Status, reg date, full SKD (planning-only) |
| ajpes_jolp | AJPES JOLP annual reports | planning_only | public (view-only) | view-only | Financials, view-only |
| ajpes_fipo | AJPES Fi=Po / S.BON | blocked_payment | paid | paid | Structured financials + ratings (planning-only) |

## What Each Source Contributes

- **ajpes_prs** — matična, full name, legal form (text), registrar, structured
  address (street/number/settlement/postal/post), HSEID. Identity backbone.
- **furs_zavezanci_po** — davčna številka, VAT liability + registration date,
  SKD activity code, name, address, tax office. The open route to tax/VAT/activity.
- **ajpes_restprsinfo** — credentialed: status, registration date, full SKD,
  change-list for sync. Planning-only.
- **ajpes_jolp** — free view-only annual reports (balance sheet + income
  statement, ~5 years); no bulk/API. Planning-only.
- **ajpes_fipo** — paid structured financials + indicators + S.BON ratings.
  Planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.maticna_stevilka`**
and groups fields by real concepts: registration, tax_identifiers (davčna, VAT id
= SI+davčna, VAT date), legal_identity, status (planning-only), activity (SKD),
incorporation (planning-only), registered_location (structured + HSEID),
registering_authority, financial_statements[] (planning-only, EUR), and officers[]
(planning-only, PII). The `example.json` is a **real** record — ISTRA XLL d.o.o.
(MB 3282490000): real identity, address, legal form, tax number, VAT id, SKD —
with status/incorporation/financials/officers left null (not openly available).

## Join And Precedence Rules

- **matična številka** joins PRS ↔ FURS (↔ restPrsInfo/JOLP/Fi=Po). `vat_id = "SI"
  + davčna` (davčna from FURS only). Precedence: PRS (identity) > FURS
  (tax/VAT/SKD) > restPrsInfo (status/date; planning-only) > JOLP/Fi=Po
  (financials; planning-only). Prefer PRS name over FURS name.

## Missing Or Restricted Data

- **Status, incorporation date, officers, ownership** — not open (credentialed
  restPrsInfo / court register).
- **Financials** — not openly downloadable (JOLP view-only, Fi=Po paid).
- **Encodings** — PRS UTF-16, FURS UTF-8 semicolon (trim trailing spaces).

## Common Mapper Notes

Slovenia is a **two-open-source, single-key** country (matična) where **tax/VAT
come only from FURS** (not the register) and **financials are not open**. Map
`company_id`←matična, `tax_id`←FURS davčna, `vat_id`=SI+davčna,
`activity_code`←FURS SKD; mark status/incorporation/officers/owners/financials
planning-only. See `common_field_mapping_suggestions.md`.
