# Finland data sources — overview and state of processing

Last updated: 2026-07-19. One page per country would not scale, but Finland is our
deepest market, so this doc maps every Finnish source: what we download, how each
pipeline processes it, what is live in ClickHouse today, and what is planned. Deep
detail lives in each module's own README / design doc — this is the map, not the spec.

Discovery trail (searches, samples, licenses, rejected sources):
`companies/analysis/finland/` at the repo root.

## Source summary

| Module | Publisher / dataset | Acquisition | Cadence | ClickHouse tables (rows @ 2026-07-19) | License |
|---|---|---|---|---|---|
| `finland_ytj` | PRH — YTJ company register (open data API v3) | automated full snapshot | daily 04:45 | `fi_companies` 461k, `fi_names` 685k, `fi_industries` 461k, `fi_company_contacts` 119k, `fi_company_domains` 119k | CC-BY-4.0 |
| `finland_xbrl` | PRH — digital financial statements (XBRL API) | automated, partitioned by registration date | daily 06:00 | `fi_financial_statements` / `fi_financial_metrics` 46k, `fi_xbrl_facts_raw` 3.5M, `fi_company_financials_latest` 21k | CC-BY-4.0 |
| `finland_verotax` | Verohallinto — public corporate income tax CSVs | automated bulk (URL discovery from vero.fi page) | yearly, Nov 12 | `fi_tax_records` 1.79M (tax years 2020–2024, 385k entities/yr) | CC-BY-4.0 |
| `finland_hilma` | Hilma (hankintailmoitukset.fi) — public procurement notices | **manual portal export → S3** | manual, per upload | `fi_hilma_notices` 12.5k, `fi_hilma_notice_winners` 11.3k | free incl. commercial (portal ToS) |
| `ted_procurement` | TED (EU) — EU-threshold award notices, **country-agnostic** (FIN configured) | automated, monthly publication-date partitions from 2024-01 | monthly 3rd 05:35 | `ted_notices` 12.3k, `ted_notice_winners` 44.8k (2024-01→2026-06 backfilled; 100% winner ids, 6,990 distinct companies join `fi_companies`) | EU open data |

`ted_procurement` is a shared cross-country source, not a Finland-only pipeline —
Finland is one configured country. See **`docs/ted-procurement.md`** for its
architecture, how to add another country, and full coverage; it is summarised here
only because Finland is one of its consumers.

All four share the standard shape (`docs/data-source-guidelines.md`): download →
per-source DuckDB staging → set-based SQL transform → migration-owned ClickHouse
tables with atomic replace, `*_amount_original` + `*_amount_usd` pairs, and refuse-
to-replace-on-empty guards. Entity key everywhere: `business_id` (Y-tunnus,
`NNNNNNN-N`).

## 1. finland_ytj — company register (the spine)

- `GET /all_companies` streamed to a temp file (JSON or zip), parsed with `ijson`,
  loaded via dlt into `data/finland_ytj.duckdb`, resolved to normalized tables with
  **dbt** (the one Finland module that earns dbt), exported to ClickHouse.
- Provides: companies, name history, TOL2008→NACE industries, VAT/employer/
  prepayment registration flags, websites → canonical contacts/domains pair.
- Notable: YTJ open data **excludes** sole traders, associations, foundations,
  municipalities; ~819k register entries → 461k in scope. Finnish **VAT ids are
  derived**, not registered (`FI` + digits, valid while VAT-registered) — the
  backoffice derives them for display; `fi_companies.vat_id` is empty by design.
- **Why always the full snapshot (no delta, no raw S3 layer)**: the API offers
  no changed-since filter — `GET /companies` filters are search-shaped (name,
  location, registration date; a registration-date filter catches only *new*
  companies, not modifications), and unknown params are silently ignored
  (probed live 2026-07-20). Each record carries a `lastModified` timestamp,
  but that only enables *client-side* diffing after downloading everything —
  so the fetch cost (one bulk `/all_companies` request/day) is irreducible,
  and full-snapshot-replace is the simplest correct mode: deletions and every
  modification handled for free, no delta-bookkeeping drift. There is also no
  raw S3 archive layer: the response is a live API's current state, not an
  immutable artifact (contrast the Sweden bulk ZIPs) — the dlt raw table in
  `finland_ytj.duckdb` is the raw layer, and YTJ's payload carries its own
  history (name versions, address `registered_on`). Revisit only if the daily
  job's cost grows or PRH ships a real delta endpoint (then: `lastModified`
  client-side diff, dossier §12–13 in
  `companies/analysis/finland/prh_ytj/dossier.md`).
- **Known gap (planned)**: the API payload carries `companySituations` (bankruptcy /
  liquidation / restructuring), `registeredEntries`, tax-registration and legal-form
  history that we do not load yet — `fi_company_situations`, `fi_registered_entries`,
  `fi_tax_registrations`, `fi_legal_forms` exist but are empty. Highest-value next
  step for this module. (`fi_company_addresses` is in flight as of this writing.)

## 2. finland_xbrl — financial statements (digital filers)

- Pulls PRH statement listings by registration date (history from 2023-07-01,
  monthly partitions; daily incremental from 2026-06-01), downloads statement XML
  to S3 (`source-finland-prh-xbrl`), parses every context/unit/fact with lxml into
  partition DuckDBs, publishes the full model + curated metrics with EUR→USD.
- Metric fill within covered statements is good (assets 98%, profit 92%, revenue
  80%); `employees` is not in the OYTP taxonomy facts we map.
- **Coverage is the story**: only companies that file digitally appear — **~6.5% of
  active limited companies, 0 of 298 listed companies** (they file ESEF to the
  Nasdaq OAM instead). Statement volume grows every year (FY2023 11.5k → FY2025
  16.3k), and **mandatory iXBRL filing to PRH lands 2027 (audited companies) /
  2028 (most companies)** — this module converges to near-full coverage on its own.

## 3. finland_verotax — public corporate income tax (the breadth layer)

- Five yearly CSVs (2020–2024) from vero.fi; the year→URL map is **resolved at
  runtime from the open-data page** (filenames rotate; Finnish inflection means the
  match is on the stem `tuloverotu`), with a static fallback map. Latin-1,
  `;`-delimited, decimal comma; 2020–2022 files carry a 9th prepayments column
  that 2023+ dropped (header sniff picks the shape); ~150 company names contain
  unquoted `"` (read with quoting disabled).
- One row per (business_id, tax_year): taxable income, taxes assessed,
  prepayments, refund, residual tax — EUR + USD.
- **Why it exists**: universal-coverage profitability/size signal (274k of our
  461k companies, 18× the XBRL-covered set) as a bridge until the 2027/28 mandate.
  **`taxable_income` is a tax base, never map it onto `profit_loss`** (loss
  carryforwards, group contributions). New file appears each early November →
  yearly schedule; extend `EXPECTED_YEARS` by one line.
- Surfaced in the backoffice as the "Tax records" section (with CC-BY attribution).

## 4. finland_hilma — public procurement (manual export)

The Hilma portal has **no keyless machine interface**: the AVP read API requires an
account-bound `Ocp-Apim-Subscription-Key` (free self-registration — see
`companies/analysis/finland/search_attempts.md` attempts 9–10). Until a key is
provisioned, ingestion is a manual export:

**Operator procedure (the manual part):**
1. Log in to hankintailmoitukset.fi and export search results as CSV **with the
   FULL column set** — the pipeline validates the exact 58-column header and
   refuses partial exports loudly.
2. `uv run python scripts/upload_hilma_export.py <file.csv>` — validates the header
   locally, uploads to `s3://source-finland-hilma/exports/` with a sha256 metadata
   sidecar.
3. Launch `finland_hilma_job` from the Dagster UI. **No schedule** — runs follow
   uploads.

**Pipeline (the automated part):** the S3 object is a non-materializable
**external asset** (`finland_hilma_export_s3`) whose description repeats the
procedure above. `finland_hilma_notices_duckdb` reads *every* uploaded export
(cp1252 → UTF-8, `strict_mode=false` because the all-quoted multiline format
trips DuckDB's strict reader), **dedups by (notice_number, lot_id)** keeping the
newest published row — so overlapping re-exports simply supersede older data —
cleans `%u2013` mojibake, types values, and **normalizes winners**: the portal's
`Name (1234567-8)//Name2 (…)` strings become one row per winner with the business
id extracted. USD conversion per amount (each of the four money columns carries
its own currency; 99.9% EUR), then export.

**Current data**: 12,544 notices 2018→now (incl. national below-EU-threshold
notices TED never carries), 11,265 winner rows, 10,339 with business ids, 9,717
joined to `fi_companies` → 2,912 distinct companies with a public-contract record.
`fi_hilma_notice_winners` is ordered by `winner_business_id` — it is the
company-join surface.

**Upgrade path**: if an AVP key is ever provisioned, replace the external asset
with an automated download asset and add a schedule; everything downstream is
unchanged (design doc §8).

## Planned / not pursued

- **ESEF listed-company financials** (fixes the 0/298 listed gap): designed in
  `docs/esef-filings-research.md` — filings.xbrl.org, keyless, xBRL-JSON facts,
  LEI→business id via `gleif`, consolidated-vs-standalone scope flag. Not built.
- **YTJ company situations / registered entries**: see §1 gap — next in line.
- **Not freely available** (documented, not chased): officers/board (paid Virre
  only; Finnish XBRL filings carry no signature facts), beneficial owners
  (access restricted), employee headcount (no open per-company source).

## Backoffice surfaces (services/backoffice)

Finland company detail renders: register record (with derived VAT id and
trade-register status decoration, `fi-registry.tsx`), industries, financials
(`fi_financial_metrics`), **tax records** (`fi-tax-records.tsx`, CC-BY
attribution), contacts/addresses/domains. **Public contracts** is a
generic cross-country section (`public-contracts-section.tsx` + canonical
`PublicContractRow`): each country's `publicContractsQuery` unions its portals —
Finland unions Hilma + TED, deduping EU-threshold notices that appear in both
via the Hilma `ted_number` reference.
