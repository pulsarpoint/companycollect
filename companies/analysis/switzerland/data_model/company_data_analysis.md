# Company Data Analysis For Switzerland

## Summary

Switzerland has **excellent open identity data** but a **structural financial-data
gap**. The federal commercial-register index **Zefix** is published as **LINDAS
Linked Data** and queryable via an **open SPARQL endpoint** (no auth) — the full
register of **788,989** legal entities with UID, legal name, legal form
(eCH-0097), registered address, municipality, website, and business purpose. The
Zefix **REST API** (free HTTP Basic credentials) adds **status**, **registered
capital**, and **SOGC gazette** links; SOGC provides register events and officers.

The defining limitation is **financials**: under Art. 958 of the Code of
Obligations, Swiss companies prepare annual accounts but private companies (AG,
GmbH) have **no public filing obligation** — so **no open financial source exists**
for the private universe. Financials are public **only** for **listed issuers**
(SIX Swiss Exchange) and regulated entities (FINMA). Everything keys on the **UID**
(CHE-xxx.xxx.xxx); `vat_id = UID + ' MWST'/'TVA'/'IVA'`.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| zefix_lindas | Zefix via LINDAS (SPARQL) | ready | public | OGD / Open use | Open identity backbone |
| zefix_rest_api | Zefix Public REST API | blocked_authentication | free credentialed | OGD / Open use | Status, capital, SOGC (documented) |
| sogc_shab | SOGC / SHAB gazette | blocked_authentication | free credentialed | OGD / Open use | Register events + officers (planning-only) |
| six_listed_financials | SIX listed-company reports | planning_only | public | issuer terms | Financials — listed only |
| handelsregister_extract | Cantonal register extracts | blocked_payment | paid | paid | Officers/capital/journal (planning-only) |

## What Each Source Contributes

- **zefix_lindas** — open, no-auth SPARQL: UID/CHID/EHRAID, legal name, legal form
  (eCH-0097), address, municipality, website, purpose. The identity backbone.
- **zefix_rest_api** — same open data per entity plus **status** and **registered
  capital** and SOGC links; gated by free Basic credentials (401 verified).
- **sogc_shab** — register events (incorporation/mutation/dissolution) and
  **officers**; free credentialed; officers are personal data.
- **six_listed_financials** — the only broadly-open financials, **listed issuers
  only**.
- **handelsregister_extract** — paid authoritative officers/capital/journal.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.uid`** and groups
fields by real concepts: registration (UID/CHID/EHRAID), tax_identifiers (VAT
derived), legal_identity (name, eCH-0097 form, purpose, website), status
(credentialed), registered_location, share_capital (credentialed), officers[]
(planning-only, PII), register_events[] (credentialed), and financial_statements[]
(listed-only). The `example.json` is a **real** record — Zazuko GmbH
(CHE-242.294.601, Biel/Bienne, website zazuko.com) — with status/capital/officers
left null (credentialed/paid) and financials null (Zazuko is private → no public
accounts).

## Join And Precedence Rules

- **UID** is the single universal key; `vat_id = UID + ' MWST'/'TVA'/'IVA'`,
  `tax_id = UID`. Precedence: Zefix LINDAS (identity) > Zefix REST (status/capital)
  > SOGC (events/officers) > SIX (listed financials) > paid extract. ISIN links
  listed financials to the UID by issuer name.

## Missing Or Restricted Data

- **Financials** — not available for private companies (no public filing); listed
  only (SIX). The defining gap.
- **Activity code** — no per-company NOGA in the open set.
- **Status / capital / register events / officers** — free-credentialed (REST/SOGC)
  or paid; officers are personal data.
- **Beneficial owners** — no public register.

## Common Mapper Notes

Switzerland is a **single-key (UID)** country with **rich open identity** but
**no open private-company financials**. Derive `vat_id`/`tax_id` from the UID; map
`financials` only for listed issuers; treat status/dates/officers/capital as
free-credentialed or paid; mark activity code and beneficial owners
`not_available`. See `common_field_mapping_suggestions.md`.
