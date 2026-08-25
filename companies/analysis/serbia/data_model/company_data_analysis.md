# Company Data Analysis For Serbia

## Summary

Serbia is **fully-open for company identity plus a one-year financial summary**.
APR (*Agencija za privredne registre* — Serbian Business Registers Agency)
publishes two JSON open-data APIs under the **Serbian Open Data License**, both keyed on the **matični
broj** (8-digit registration number) and refreshed **monthly**:

- **Companies** (`/api/opendata/companies`) — **133,634** companies (2026-07-31):
  name, legal form, status, incorporation date, municipality, KD2010 activity.
- **Financial statements** (`/api/opendata/companies/financial-statements`) —
  **122,863** records: the **latest** annual statement per company (assets,
  capital, total revenue, net profit/loss, employees), in **thousands of RSD**.

A third feed (`/api/opendata/ngo`, **40,547** associations/foundations) covers
non-commercial legal entities. All three were downloaded this run.

The open data is excellent for a free, SODL-licensed, country-wide identity +
headline-financials profile. Its gaps — **PIB/VAT**, **street address**,
**directors**, **beneficial owners**, **sole traders (preduzetnici)**, and
**multi-year financial history** — require paid APR products. Status data and
representatives are available through the status-register delivery/web service;
beneficial owners use the separate APR CEV register/service; financial-statement history
is not available through the status-register web service. OpenCorporates mirrors
APR and adds nothing authoritative.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| apr_companies | APR Companies open data | ready | public | SODL | Company master |
| apr_financial_statements | APR Financial statements open data | ready | public | SODL | Latest annual financials |
| apr_ngo | APR NGO open data | ready | public | SODL | Associations/foundations (separate stream) |
| apr_webservice | APR automated status web service | blocked_payment | restricted | contract | PIB, address, representatives, sole traders (planning-only) |
| apr_public_search | APR public company search | sample_only | manual/CAPTCHA | APR terms | Semantic check only; automated collection prohibited |
| apr_beneficial_owners | APR Central Register of Beneficial Owners | blocked_authentication | eID/contract | statutory/contract | Beneficial owners; separate restricted source |
| opencorporates | OpenCorporates register 224 | blocked_license | restricted | OC terms | Aggregator mirror (cross-check) |

(The RZS statistical office is aggregate-only — not company records — and is
excluded from the model.)

## What Each Source Contributes

- **apr_companies** — the company master keyed on matični broj: PoslovnoIme
  (Latin), NazivStatus / NazivPravneForme / NazivOpstine (Cyrillic),
  DatumOsnivanja, SifraDelatnosti (KD2010).
- **apr_financial_statements** — the latest annual RGFI summary per company
  (PoslovnaImovina, Kapital, UkupniPrihodi, NetoDobitak, NetoGubitak, Gubitak,
  ProsecanBrojZaposlenih), thousands of RSD; join on matični broj.
- **apr_ngo** — associations, foundations, endowments (same id space); kept as a
  separate entity stream.
- **apr_webservice** — planning-only: PIB, street address, representatives and
  sole traders. Paid/contract.
- **apr_public_search** — manually inspected for one company on 2026-08-25 to
  validate the representative shape. The legal-representative section exposed
  a name, function (`Директор` in the inspected record), masked JMBG reveal
  control and `Самостално заступа`. The real name and JMBG were not copied.
- **apr_beneficial_owners** — separate CEV source. The portal requires APR
  eID/SSO, so no live owner record was accessed. The target schema instead
  follows APR's current public documentation and the 2025 Act.
- **opencorporates** — planning-only cross-check; restricted bulk.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.maticni_broj`** and
groups fields by real concepts: registration, tax_identifiers (planning-only
PIB), legal_identity, status (raw Cyrillic + mapped enum), activity (KD2010),
incorporation, registered_location (municipality open; street planning-only),
financial_statements[] (thousands RSD, latest year), plus availability-wrapped
`officers.records[]` and `beneficial_owners.records[]`. The `example.json` is a **real** record — ENEKS MONT PLUS
DOO KRUŠEVAC (MB 21141666): real identity/status + real 2024 financials (revenue
123,852 thousand RSD, net profit 3,542, 4 employees), with PIB/officers/owners
left `not_acquired` with empty records (not open). This explicitly avoids
treating an unpurchased source as evidence that the company has no people.

## Join And Precedence Rules

- **matični broj** is the single universal join key (companies ↔ financials ↔
  NGOs ↔ paid web service). No PIB in open data → no VAT/tax joins from open
  sources.
- Precedence: apr_companies (identity/status) > apr_financial_statements
  (financials) > apr_webservice (representatives/planning-only) and
  apr_beneficial_owners (CEV/planning-only) > opencorporates (cross-check).
  Prefer apr_companies name over the financial-statement name.

## Missing Or Restricted Data

- **PIB / VAT**, **street address**, **representatives**, **beneficial owners**,
  **sole traders**, and **multi-year financials** are not in the open company
  feed. They require paid APR products; beneficial ownership and financial
  history are not part of the same status-register web service.
- **Company members are not CEV owners** — the public UI showed a `Чланови`
  section, but legal membership/shareholding must not be treated as proof of
  beneficial ownership.
- **Dissolution date** — not a field; only implied by status.
- **Cyrillic** status/legal-form/municipality — normalise.

## Common Mapper Notes

Serbia is a **single-key** country (matični broj) where **VAT/tax id cannot be
derived from open data**. Map `company_id`/`registration_number`←matični broj,
financials←APR statements (thousands RSD, single year), and mark
`tax_id`/`vat_id`/`officers`/`owners` planning-only. Use explicit availability
states, keep SP3/SP4 separate from CEV, never persist raw JMBG/passport/card
values, and handle Cyrillic normalization. See
`common_field_mapping_suggestions.md`.

For the complete 2026-07-31 company-snapshot profile, data-quality findings, and
the proposed ClickHouse manifest/history/current schema, see
`apr_companies_full_analysis_and_clickhouse.md`.
