# Company Data Analysis For Belgium

## Summary

Belgium supports one of the **richest, cleanest open** company profiles analysed — top-tier with
Norway/France/Poland, and **best-in-class for structured financials**. A single clean key, the
**Ondernemingsnummer / EnterpriseNumber** (10 digits = VAT root), joins an **open company master**
(**KBO/BCE Open Data**, free bulk CSV) to **open structured XBRL financials** (**NBB Central Balance Sheet
Office**, free, full accounts back to 2007) — with **no fuzzy matching**. Activity codes are clean
(NACE-BEL); the gazette (Moniteur Belge) adds dated acts. The only real caveats: both core sources sit
behind a **free registration/account** (not payment), directors are **not** in the KBO open data (only in
gazette acts/paid aggregators), and beneficial ownership (**UBO**) is restricted.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| kbo_open_data | KBO/BCE Open Data | ready | free + registration | Licence-BCE-Open-Data | **Company master spine** |
| nbb_cbso_financials | NBB Central Balance Sheet Office | ready | free + account | free (Authentic Data) | **Structured financials (XBRL)** |
| kbo_public_search | KBO Public Search | blocked_payment | free web / paid API | free web; API paid | Single-company verification |
| moniteur_belge | Moniteur Belge / Belgisch Staatsblad | insufficient_transport_info | free public | free | Lifecycle acts/events |
| ubo_register | UBO register | blocked_authentication | restricted/fee | restricted | Beneficial ownership (planning-only) |

Also in `source_inventory.json`: free third-party CBE REST mirrors (API-key gated), data.gov.be (catalog),
commercial aggregators (Companyweb/BvD/Graydon — resell open data).

## What Each Source Contributes

- **kbo_open_data (spine).** Free bulk CSV: EnterpriseNumber, status, legal form, start date, names
  (multilingual), addresses, **NACE-BEL** activities (MAIN/SECO/ANCI), contacts (incl. website), and
  establishment units. Multi-file model joined on EntityNumber; codes via code.csv. ~1.9M+ enterprises.
- **nbb_cbso_financials (financials).** Free, structured **XBRL** annual accounts (≈99% XBRL) back to 2007:
  full balance sheet + income statement (totaal activa, eigen vermogen, omzet, winst/verlies, employees),
  joined on EnterpriseNumber. micro/abbreviated schemas omit revenue. The open-financials standout.
- **kbo_public_search.** Free web single-company lookup; the web-service API is paid → use kbo_open_data for bulk.
- **moniteur_belge.** Free gazette of company acts (incorporation/amendment/appointment/dissolution/merger),
  keyed on EnterpriseNumber — dated lifecycle events + the only open source of director appointments.
- **ubo_register.** Beneficial ownership — restricted (legitimate interest/fee); planning-only, sensitive PII.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`) models a Belgium-specific object: `registration`
(EnterpriseNumber + derived VAT), `legal_identity` (multilingual names + form), `status`, `activity`
(NACE-BEL), `registered_location` (+ region from zipcode), `contact`, `establishments[]`,
`beneficial_owners[]` (restricted/planning-only), `acts[]` (gazette), `financial_statements[]` (open NBB
XBRL, schema-aware nullability), and `source_provenance[]`. Every section carries `x-source`; financial
entries carry a `source` discriminator.

## Join And Precedence Rules

- **Single clean key**: EnterpriseNumber keys KBO, NBB, Moniteur, and UBO — the profile assembles with **no
  fuzzy matching**.
- **Authority**: KBO for identity/status/activity/establishments; NBB for financials; Moniteur for acts; UBO
  (restricted) for ownership.
- **Build order**: KBO bulk → NBB financials (join on EnterpriseNumber) → Moniteur acts → (UBO only with
  lawful access). Freshness: KBO daily, NBB annual, Moniteur daily.
- **Normalization**: KBO multi-file join + code.csv; NBB Belgian-GAAP XBRL schema variants + yearly
  taxonomy versions; pick one denomination language; region from zipcode.

## Missing Or Restricted Data

- Very little is missing — identity, **financials**, activity, establishments, contact, acts are **open**.
- **Directors/officers**: NOT in the KBO open data — only gazette acts (Moniteur) + paid aggregators.
- **Beneficial ownership (UBO)**: restricted (planning-only).
- **PII**: KBO natural persons (no direct-marketing reuse); UBO sensitive — GDPR.
- **Access**: both core sources need a **free registration/account** (not payment).
- **Financials nullability**: micro/abbreviated omit the income statement (no revenue).

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Belgium is a **top-tier open, clean-key** case: one
EnterpriseNumber (= VAT root) joins everything; **financials are open structured XBRL** (no paid tier);
NACE-BEL activity is open; officers are gazette-derived/sparse; owners restricted; currency EUR; both core
sources behind a free registration.
