# Company Data Analysis For Croatia

## Summary

Croatia supports a **rich, clean-key open** company profile — Belgium/Poland tier (free, behind a free
registration). A single key, the **OIB** (11-digit tax id = VAT root, `HR` + OIB), joins an **open API
register** to **open structured financials** with **no fuzzy matching**. The **Sudski registar** (Court
Register, Ministry of Justice) offers an **open REST API** (JSON) with the company spine — MBS, OIB, name,
legal form, status, seat/address, share capital, NKD activity, and **persons (members + management)** — under
the **Otvorena dozvola**. **FINA's RGFI** publishes annual accounts (balance sheet + income statement) as
**open machine-readable CSV**, also Otvorena dozvola. Both are **free** but require a **free
registration/account** (sudreg subscription key; FINA login). Beneficial ownership (RSV) is restricted, but
the register's **members/owners + management are open**.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| sudski_registar | Sudski registar API | ready | free + registration (key) | Otvorena dozvola | **Company spine** |
| fina_rgfi | FINA RGFI (javna objava) | ready | free + registration (login) | Otvorena dozvola | **Structured financials (CSV)** |
| data_gov_hr | data.gov.hr (CKAN) | ready | free | Otvorena dozvola | Discovery + license confirmation |
| rsv_beneficial_ownership | Registar stvarnih vlasnika | blocked_authentication | restricted | restricted | Beneficial ownership (planning-only) |

Also in `source_inventory.json`: Sudski registar web search (free lookups), DZS (aggregate), commercial
aggregators (Bisnode/Companywall — resell the open data).

## What Each Source Contributes

- **sudski_registar (spine).** OIB, MBS, name, legal form, status, competent court, seat + address, **share
  capital**, **NKD activities**, and **osobe** (members/partners + management board/directors). Open JSON API
  (Otvorena dozvola), free subscription key. Notably exposes **both owners and officers openly**.
- **fina_rgfi (financials).** Annual accounts as **open CSV**: **bilanca** (total assets, equity, liabilities)
  + **račun dobiti i gubitka** (revenue, operating result, net result) + employees, joined on **OIB** (clean
  join). micro/small file abbreviated forms; large-company full data may need the paid FINA product.
- **data_gov_hr.** CKAN catalog confirming both datasets = Otvorena dozvola; discovery only (resources point
  to the gated portals).
- **rsv_beneficial_ownership.** Beneficial ownership — restricted (legitimate interest); planning-only,
  sensitive PII. (The register's members are the open ownership signal.)

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`) models a Croatia-specific object: `registration`
(OIB + MBS + derived VAT), `legal_identity`, `status`, `activity` (NKD), `registered_location`, `capital`,
`officers_and_owners[]` (open/PII; type owner|officer), `beneficial_owners[]` (restricted/planning-only),
`financial_statements[]` (open FINA RGFI, size-category nullability, EUR/HRK boundary), and
`source_provenance[]`. Every section carries `x-source`; financial entries carry a `source` discriminator.

## Join And Precedence Rules

- **Single clean key**: the **OIB** (= VAT root) joins the Sudski registar, FINA RGFI, and RSV — no fuzzy
  matching (MBS also available for the court register).
- **Authority**: Sudski registar for identity/status/activity/capital/persons; FINA RGFI for financials; RSV
  (restricted) for beneficial ownership.
- **Build order**: Sudski registar API (spine) → FINA RGFI (join on OIB) → (RSV only with lawful access).
  Freshness: register continuous, financials annual.
- **Normalization**: Croatian; NKD where coded; **EUR since 2023** (HRK before); both core sources need free registration.

## Missing Or Restricted Data

- Very little is missing — identity, **financials**, activity, capital, **officers + owners** are all open.
- **Beneficial ownership (RSV)**: restricted (planning-only) — but members/owners + management are OPEN.
- **Access**: both core sources need a **free registration/account** (sudreg key; FINA login).
- **Financials**: micro/small abbreviated (revenue null); large-company full data may need the paid FINA product;
  EUR/HRK boundary at 2023.
- **PII**: osobe + beneficial owners — GDPR.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Croatia is a **clean-key, open** case: one OIB (= VAT root) joins
everything; **financials are open structured CSV** (FINA RGFI); NKD activity present; **officers AND owners
open** in the register; beneficial ownership restricted; currency EUR (since 2023); both core sources behind a
free registration.
