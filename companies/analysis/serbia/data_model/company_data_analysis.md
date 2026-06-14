# Company Data Analysis For Serbia

## Summary

Serbia is **fully-open for company identity plus a one-year financial summary**.
APR (*Agencija za privredne registre* — Serbian Business Registers Agency)
publishes two **public-domain** JSON open-data APIs, both keyed on the **matični
broj** (8-digit registration number) and refreshed **monthly**:

- **Companies** (`/api/opendata/companies`) — **133,357** companies (2026-05-31):
  name, legal form, status, incorporation date, municipality, KD2010 activity.
- **Financial statements** (`/api/opendata/companies/financial-statements`) —
  **122,863** records: the **latest** annual statement per company (assets,
  capital, total revenue, net profit/loss, employees), in **thousands of RSD**.

A third feed (`/api/opendata/ngo`, **40,547** associations/foundations) covers
non-commercial legal entities. All three were downloaded this run.

The open data is excellent for a free, public-domain, country-wide identity +
headline-financials profile. Its gaps — **PIB/VAT**, **street address**,
**directors**, **beneficial owners**, **sole traders (preduzetnici)**, and
**multi-year financial history** — are available only via the **paid APR web
service** (free for state bodies). OpenCorporates mirrors APR and adds nothing
authoritative.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| apr_companies | APR Companies open data | ready | public | public_domain | Company master |
| apr_financial_statements | APR Financial statements open data | ready | public | public_domain | Latest annual financials |
| apr_ngo | APR NGO open data | ready | public | public_domain | Associations/foundations (separate stream) |
| apr_webservice | APR automated web service | blocked_payment | restricted | contract | PIB, address, directors, owners, sole traders, history (planning-only) |
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
- **apr_webservice** — planning-only: PIB, street address, directors, beneficial
  owners, sole traders, multi-year financials. Paid/contract.
- **opencorporates** — planning-only cross-check; restricted bulk.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.maticni_broj`** and
groups fields by real concepts: registration, tax_identifiers (planning-only
PIB), legal_identity, status (raw Cyrillic + mapped enum), activity (KD2010),
incorporation, registered_location (municipality open; street planning-only),
financial_statements[] (thousands RSD, latest year), and planning-only officers[]
/ beneficial_owners[]. The `example.json` is a **real** record — ENEKS MONT PLUS
DOO KRUŠEVAC (MB 21141666): real identity/status + real 2024 financials (revenue
123,852 thousand RSD, net profit 3,542, 4 employees), with PIB/officers/owners
left null/redacted (not open).

## Join And Precedence Rules

- **matični broj** is the single universal join key (companies ↔ financials ↔
  NGOs ↔ paid web service). No PIB in open data → no VAT/tax joins from open
  sources.
- Precedence: apr_companies (identity/status) > apr_financial_statements
  (financials) > apr_webservice (planning-only gaps) > opencorporates
  (cross-check). Prefer apr_companies name over the financial-statement name.

## Missing Or Restricted Data

- **PIB / VAT**, **street address**, **directors**, **beneficial owners**, **sole
  traders**, **multi-year financials** — all paid (APR web service), planning-only.
- **Dissolution date** — not a field; only implied by status.
- **Cyrillic** status/legal-form/municipality — normalise.

## Common Mapper Notes

Serbia is a **single-key** country (matični broj) where **VAT/tax id cannot be
derived from open data**. Map `company_id`/`registration_number`←matični broj,
financials←APR statements (thousands RSD, single year), and mark
`tax_id`/`vat_id`/`officers`/`owners` planning-only. Handle Cyrillic
normalisation. See `common_field_mapping_suggestions.md`.
