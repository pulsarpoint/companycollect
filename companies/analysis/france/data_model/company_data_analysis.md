# Company Data Analysis For France

## Summary

France supports the **richest, cleanest open company profile** of the countries analysed so far — and,
unusually, **open financials**. Every source keys on a **single national identifier, `siren`** (9 digits),
so the profile assembles with **no fuzzy matching** (contrast Germany = no shared key, Spain = sparse
CIF). Identity/activity/status/address come from **INSEE Sirene** (Open Licence 2.0 bulk) and the no-auth **API
Recherche d'Entreprises**; legal data (capital, dirigeants, purpose, accounts refs) from **INPI RNE**;
lifecycle events from **BODACC**. **Financials are open**: headline **revenue + net income** via the
no-auth Recherche `finances` block, and **full balance sheet + income statement** via **INPI comptes
annuels** (free account). The only real limit is the legal **confidentiality option** (partial coverage
of small firms) and **beneficial ownership** being restricted.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| insee_sirene | Base Sirene (SIREN/SIRET) | recommended | public, no auth (bulk) | Open Licence 2.0 | **Spine** (identity/establishments/status) |
| ratios_inpi_bce | Ratios financiers BCE/INPI | recommended | public, no auth | Open Licence 2.0 | **Structured financial facts and ratios** |
| recherche_entreprises | API Recherche d'Entreprises | recommended | public, no auth | open | Aggregator + dirigeants + **open headline financials** |
| inpi_rne | RNE (Data INPI) | recommended | free account | INPI open | Legal: capital, dirigeants, purpose, accounts refs |
| inpi_comptes_annuels | INPI comptes annuels | recommended | free account | INPI open | **Full financial statements** |
| bodacc | BODACC | useful_secondary | public, no auth | Licence Ouverte 2.0 | Lifecycle events |

Excluded / not given their own catalog (in `source_inventory.json`): API Sirene (same data as the Sirene
bulk, via a keyed API — daily deltas), Documents et comptes (data.economie discovery catalog), API
Entreprise (restricted — DGFIP CA + Banque de France bilans, planning-only).

## What Each Source Contributes

- **insee_sirene (spine).** SIREN/SIRET, legal name, legal form, NAF activity, status (A/C), creation
  date, employee band, establishments + addresses. Open Licence 2.0 bulk (~25M units / ~36M establishments) +
  daily API. The backbone every other source attaches to.
- **recherche_entreprises (aggregator + open financials).** No-auth merge of Sirene + INPI: identity,
  activity (NAF Rev2 + NAF2025), HQ address + geo, dirigeants, size category — **and a `finances` block
  with `ca` (revenue) + `resultat_net` (net income) per year**. Verified live (La Poste 2024: CA €34.569B,
  net €1.722B). The fastest path to headline financials at scale, free.
- **inpi_rne (legal register).** Share capital, représentants/dirigeants with roles, objet social,
  acts/statutes, and references to annual accounts. Beneficial ownership exists but is **restricted**.
- **inpi_comptes_annuels (full financials).** Non-confidential balance sheet + income statement (per
  liasse-fiscale poste) + fixed assets/depreciation/provisions, since 2017, JSON since 2023. The full
  statement behind the Recherche API's two headline figures.
- **bodacc (events).** Daily gazette: créations, modifications, radiations, **procédures collectives**
  (insolvency → status signal), **dépôts de comptes** (→ trigger a financial refresh). Join via SIREN
  parsed from `registre`.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`, built from real La Poste open data) models a
France-specific object: `registration` (siren + siège siret + computed VAT), `legal_identity`, `status`
(+ insolvency-derived), `activity` (NAF Rev2 + NAF2025 — France has clean codes), `registered_location`
(+ geo), `capital`, `size`, `officers[]` (PII), `beneficial_owners[]` (planning-only/restricted),
`financial_statements[]` (multi-source, open, nullable under confidentiality), `establishments[]`,
`events[]`, `source_provenance[]`. Every section carries `x-source`; financial entries carry a `source`
discriminator (`recherche_entreprises` headline vs `inpi_comptes_annuels` full).

## Join And Precedence Rules

- **Single key `siren`** across all sources (SIRET for establishments; parse SIREN from BODACC
  `registre`). No fuzzy matching — France's structural advantage.
- **Authority**: Sirene for identity/activity/status; INPI RNE for capital/dirigeants/accounts; Recherche
  API is a convenient daily merge of both.
- **Financial precedence**: Ratios BCE/INPI (bulk facts and ratios, no auth) → Recherche `finances`
  (headline lookup) → INPI comptes annuels (authoritative full filing, free account) → API Entreprise
  only with habilitation. Dedupe on `siren + closing_date + accounts_type`.
- **Freshness**: Sirene monthly stock + daily API; INPI/Recherche/BODACC daily.

## Missing Or Restricted Data

- **Beneficial ownership (RBE)**: restricted since the 2022 CJEU ruling → planning-only, not open.
- **Financial coverage partial**: confidentiality option (small firms) + micro-entrepreneurs lacking
  DGFIP revenue. Treat missing financials as unknown, not zero.
- **Richer/authoritative financials** (DGFIP CA, Banque de France bilans): only via restricted API Entreprise.
- **PII**: dirigeants (and restricted beneficial owners) — GDPR; honor `statut_diffusion=P`.
- **License**: current Sirene publication is Open Licence 2.0; attribute all producers.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. France is the **clean-key, rich-data** case: one `siren` joins
everything; **activity code is available** (NAF, unlike DE/ES); **financials are open** (headline no-auth
+ full free-account) — a cross-country mapper should still tolerate null financials (confidentiality) and
carry a `source` discriminator. No separate company tax id (SIREN is id + registration + VAT root);
currency effectively always EUR.
