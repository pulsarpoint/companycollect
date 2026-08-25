# backoffice

Internal explorer for CompanyCollect data. React Router v8 (SSR) + shadcn/ui,
reading source observations and resolved projections from ClickHouse.

## Setup

```bash
pnpm install
cp .env.example .env   # fill CLICKHOUSE_PASSWORD from corpscout/.env
pnpm dev               # http://localhost:5183
```

## Commands

- `pnpm dev` — dev server (port 5183)
- `pnpm build` / `pnpm start` — production build / serve (port 3000)
- `pnpm typecheck` — react-router typegen + tsc
- `pnpm test` — vitest (integration tests hit the real ClickHouse from .env)
- `pnpm temporal:people-worker` — run the durable Swedish Draft 1, Draft 2,
  and person-profile LLM worker; it must share the backoffice data directory
  and LLM environment

## Structure

- `app/lib/countries.ts` — static registry: one entry per country, maps URL
  code → ClickHouse table/columns/features. Add new countries here.
- `app/lib/clickhouse.server.ts` — server-only read and correction-write
  ClickHouse clients. Writer credentials never fall back to the read account.
- `app/lib/queries.server.ts` — per-country stats, company search, and the
  company detail query. Still the engine for `/company/{country_code}/{id}`
  and the live-schema test sweeps (see below).
- `app/lib/unified.server.ts` — `/companies` search + facets, backed by the
  `companies_all` table (see below), not a per-country UNION merge.
- `app/lib/facets.server.ts` — per-country facet options, also backed by
  `companies_all`.
- `app/routes.ts` — `/` redirects to `/companies` (unified list); the
  company detail page is `/company/{country_code}/{id}`.

## Running the Sweden people Temporal worker

The backoffice only submits workflows. A separate long-lived Temporal worker
executes Draft 1 rebuilds, Draft 2 rebuilds, and bulk person-profile LLM
enhancement.

Configure `.env` first. The worker uses the same ClickHouse, DuckDB, SQLite,
LLM provider, and API-key environment as the backoffice. At minimum, verify the
Temporal connection:

```dotenv
TEMPORAL_ADDRESS=companycollect:7233
TEMPORAL_NAMESPACE=corpscout
TEMPORAL_PEOPLE_TASK_QUEUE=backoffice-sweden-people
TEMPORAL_PEOPLE_WORKER_ACTIVITY_SLOTS=4
```

Run the worker in a separate terminal from the backoffice server:

```bash
cd corpscout/services/backoffice
pnpm install
pnpm temporal:people-worker
```

Keep this process running while people-processing workflows are active. A
successfully started worker logs `state: 'RUNNING'` and the task queue
`backoffice-sweden-people`. If the worker is stopped, Temporal retains queued
and running workflow state; processing resumes when a compatible worker starts
again.

The worker and backoffice must use the same `data/sweden` directory. Draft data
is stored in `people-draft.duckdb`, while UI-facing job progress and saved LLM
responses are stored in `people-curation.sqlite`. Stop the local worker with
Ctrl-C; Temporal performs a graceful drain before exit.

## Person identities and corrections

Person URLs use the stable country-scoped `person_id`, not a name. Names remain
search/display fields because two people can share a name and one person can
have multiple observed names. A profile shows both the combined identity and
the immutable source observations that produced it.

Reassign and merge forms search only active target people in the same ISO2
partition, while the submitted value is the selected person's UUID. Decisions
are appended to `country_person_correction`; raw observations are not edited or
deleted. There is no generated review queue: corrections are initiated from the
person and source evidence being inspected. This backoffice currently has no
user-authentication layer, so new correction rows use `backoffice` as their
`decided_by` source. Before enabling writes:

1. Apply ClickHouse migration 241.
2. Correction-ledger writes use the same `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`
   as reads (the Dagster pipelines' account); no separate writer user.

## Domain-suggestion reviews

`corpscout.company_domains` is the Sweden-only product projection for every
company/domain association proposed by Wikidata, ESEF filings, or Common Crawl
identity matching. Each row keeps aligned source, confidence, source-record,
URL, and confidence-basis arrays. Automated confidence and human review state
are separate fields.

The company Domains tab writes `confirmed_primary`, `confirmed_related`,
`rejected`, or `unreviewed` decisions back to the same ClickHouse row. Set
`COMPANY_DOMAIN_REVIEWER` to record a reviewer identifier. The Technology tab
can inspect every proposed domain and keeps the selected `domain` in its URL.

## Geocode analysis agent

`/admin/se/company-info/geocoding` triggers a country-parametrised analysis
agent (`app/agents/geocode-analysis.server.ts`, OpenAI Codex SDK) that clusters
the unmatched address pool, tests each hypothesis against matched exemplars,
and returns Dagster augmentation suggestions with examples and counts. Sweden
is the first wired country; the next one is added to the `GEOCODE_AGENT_COUNTRIES`
table in that module (a TypeScript constant, not an environment variable).

Guardrails, in the order they bind, and honest about which one carries weight:

- **ClickHouse writes: the server refuses them.** Every agent statement is sent
  with `readonly=1` on a connection separate from the backoffice's own. This is
  the barrier; nothing else is trusted to hold it.
- **ClickHouse's read-only escape hatches: judged from its own parse.** `url()`,
  `file()`, `s3()`, `remote()`, `executable()` are reads, so `readonly=1` runs
  them happily. Before a statement executes, `EXPLAIN AST` is fetched over the
  same connection and every table function ClickHouse reports must be on an
  allowlist of inert local ones (`merge`, `view`, `numbers`, ...). Reading the
  server's parse is what makes heredoc literals (`$x$...$x$`) and
  backtick-quoted names non-issues; the string checks in
  `app/agents/read-only-sql.ts` are fast feedback and defense in depth, not a
  boundary.
- **PostgreSQL: the agent never reaches it.** The report, suggestions and memory
  come back as structured output, are validated by
  `app/agents/geocode-analysis-contract.ts`, and are written by the app inside
  one transaction that first claims the run row.
- **It never writes the geocode store, deploys, or triggers Dagster.** An
  accepted suggestion is a work item for a golden-gated policy bump; marking one
  implemented records the policy version that shipped it.
- **The model process is narrowed, not jailed.** It starts with
  `sandboxMode: "read-only"`, no network, no approvals, `mcp_servers={}`, an
  empty working directory, an allowlisted environment (PATH, proxies -- none of
  the backoffice's credentials), and a private HOME/CODEX_HOME created under the
  OS temp directory. Note what read-only does NOT mean: it restricts writes, not
  reads, so a command in that sandbox can still read any file the process's uid
  can read. Narrowing HOME keeps `~/.aws`, `~/.ssh` and the operator's Codex
  config out of easy reach; real isolation is a separate uid or container and is
  a deployment decision. `GEOCODE_AGENT_CODEX_HOME` may point at a provisioned
  directory instead -- it must already exist, or the Codex CLI hard-errors.

Runs take minutes: the action inserts a queued row and returns, and the panel
polls `/admin/se/company-info/geocoding/agent` until the run is terminal. Page
loads and polls never write; a run abandoned by a dead process is reaped at the
next trigger, measured against that run's own stored budget.

Its three tables live in the review-queue PostgreSQL and are created by the
first migration in `migrations`:

```bash
make migrate-up        # needs BACKOFFICE_POSTGRES_MIGRATE_URL in .env
```

## companies_all

`corpscout.companies_all` is a ClickHouse table with one uniform row per
company across all 10 countries (115,605,146 rows as of the last verified
count): `country_code`, `company_id`, `name`/`name_normalized`, `is_active`,
`status`, `legal_form`, `place`, `size`, `industry_code`/`industry_label`,
`revenue_usd`, `fiscal_year`, `employees`, `has_financials`, `resolved_at`.
It's what `/companies` (`unified.server.ts`) and per-country facet options
(`facets.server.ts`) query directly — a single flat table, no more
per-country UNION branching.

**Build**: `dagster_v3/src/dagster_v3/defs/companies_all/` — one per-country
`INSERT INTO <stage> SELECT ...` leg (`sql.py`), a count-parity guard per
leg against the source `*_companies` table, then an atomic
`EXCHANGE TABLES` swap (`assets.py`). Scheduled **daily at 07:15
Europe/Oslo** (after the 06:30 `company_financials_latest` run). The asset
also declares `automation_condition=eager()`, so it will additionally
rebuild on every upstream change for free if the default automation-
condition sensor is ever turned on in the Dagster UI — until then the cron
schedule is the actual trigger.

### The duplicated-spec parity contract

The build's per-country SQL (`sql.py`) DUPLICATES this repo's
`app/lib/countries.ts` registry expressions by design — the dagster
(Python) side can't import the backoffice's TypeScript registry. The same
per-country logic (status/legal_form/place/size exprs, industry joins,
financials joins) is therefore maintained in two places, and nothing
mechanically stops them from drifting apart on a future edit to either
side.

**`tests/companies-all-parity.test.ts` is the permanent guard against that
drift.** It runs against the live ClickHouse for all 10 countries and
derives every comparison FROM the registry — never a hand-listed
expression — so drift on either side of the duplication fails a test:
row-count parity per country against its `companiesTable`; sampled
status/legal_form/place/size values against the registry's own column
exprs for keys a country defines, and `''` for keys it doesn't; financials
parity for every country with a `financialsLatest` table (no/fi/se/ee/lv/gb/br/sk)
against its own `<code>_company_financials_latest`; and industry-label
parity for Estonia against the registry's `industryQuery`. A failure here
means the two specs have drifted apart — treat it as a real bug, not
something to retry away.

### Benign failure mode: source refreshed after today's build

Several source registers rebuild on a schedule that can land AFTER the
companies_all 07:15 Europe/Oslo cron — sk's Monday 07:00 swap landing
later, se's Monday 06:15 overrunning, fr's 6th 07:15, gb's 7th 07:30, cz's
17th 07:45. When that happens, today's `companies_all` leg for that
country was built from _yesterday's_ source snapshot, so the source table
has since grown/changed while `companies_all` hasn't caught up yet —
exact-count and field parity then fail with **zero spec drift**, purely
from the calendar race between the two schedules.

The parity sweep runs a **per-country freshness preflight** before its
count/field checks: it compares `companies_all`'s per-country build
timestamp (`max(resolved_at)` for that `country_code` — set to `now64(3)`
at INSERT time in `sql.py`) against the source `companiesTable`'s own
`max(resolved_at)`. If the source is newer, the preflight **skips** that
country's count+field parity with a loud `console.warn` naming both
timestamps, instead of failing. Only `no_companies`/`fi_companies`/
`br_companies` carry a `resolved_at` column today — for the other seven
countries (including all five named above) there's no per-table freshness
signal available, so the preflight logs which table lacks it and runs
parity normally (unprotected) for those.

**For no/fi/br (the countries with a freshness signal): a parity failure
WITHOUT the freshness-skip warning is real drift** — treat it as a bug. A
skip with the warning is benign and self-resolving: re-run the sweep after
the next 07:15 `companies_all` build. **For the seven no-signal countries a
benign calendar failure produces NO skip warning** — before treating their
failure as drift, check whether that source's register schedule fired since
the last 07:15 build (sk Mon 07:00, se Mon 06:15, fr 6th, gb 7th, cz 17th).
Adding `resolved_at` to those seven company exports auto-upgrades the
preflight per country (it detects the column at runtime) and closes this
gap for good.

### Intentional semantic changes from the switch

Moving `/companies` and per-country facets onto `companies_all` changed a
few behaviors on purpose:

- **The old 400-page cap is gone.** The prior per-branch UNION merge
  bounded pagination depth (`MAX_UNIFIED_PAGE = 400`); a single flat table
  has no such limit.
- **`has_financials` facet counts are real.** It's a live
  `countIf(has_financials = 1)` over `companies_all`, not the zero-count
  stub the old per-branch merge returned for a key with no fixed facet
  column.
- **Per-country facet capability gaps return empty, not a thrown error.**
  The old `facets.server.ts` threw `unknown facet: <key>` when a country's
  registry had no column for that facet key (e.g. Brazil has no
  `legal_form`). `companies_all` always carries all four filter columns,
  defaulting the ones a country doesn't define to `''`, so the same
  request now just returns zero options for that country/key combo instead
  of throwing.
- **Latvia's industry columns will auto-populate.** `lv`'s `industry_code`/
  `industry_label` are `''` today because `lv_companies_nace` is
  unpopulated (its NACE classifier hasn't run yet) — once it lands, the
  next daily `companies_all` build picks it up with no registry or code
  change, the same auto-upgrade pattern as the financials-aggregates
  NACE breakdowns described further below.
- **The industry filter now matches the displayed primary-industry pick.**
  `companies_all`'s per-country `industry_subquery` picks one row per
  company (`LIMIT 1 BY`, preferring the primary row but falling back to a
  non-primary one), so filtering by industry now selects exactly the
  companies whose _displayed_ `industry_code`/`industry_label` matches —
  instead of the old per-country `industryFilterExpr` semi-join, which
  matched on ANY primary industry row for the company, independent of what
  the list actually showed. Measured impact: single-digit companies per
  country.
- **Swedish identities normalized (2026-07-18).** `se_companies` 16-prefixed
  organization-number duplicates were collapsed at the dagster layer
  (~728k phantom duplicates removed: 4,135,692 → 3,407,809 rows); SE counts
  across `companies_all` and the per-country registry dropped accordingly,
  and the `financialsAggregates.nace` entry for se no longer needs the old
  substring/prefix workaround (see below).

`app/lib/queries.server.ts` remains the engine for the company detail page
(`getCompanyDetail`, used by `/company/{country_code}/{id}`) and the
live-schema test sweeps in `tests/queries.server.test.ts` — it isn't a
legacy leftover, just scoped to detail-page/per-row lookups rather than
list search, which now lives in `unified.server.ts`.

### SK duplicate ICO quirk

Slovakia has ~53k `ico` values shared by two source registers, so a single
`ico` can legitimately correspond to 2 rows in both `sk_companies` and
`companies_all` (the build does no per-id dedup — every source row becomes
exactly one `companies_all` row). The parity test tolerates this for sk
specifically, comparing the SORTED MULTISET of values per sampled id rather
than a strict 1:1 row zip; every other country's id groups are always
singletons, so the tolerance doesn't weaken the check for them.

## Companies table (Legacy: per-country layer)

The per-country layer (`app/lib/queries.server.ts`) powers the company
detail page (`getCompanyDetail`) and the live-schema test sweeps
(`searchCompanies`/`getCountryStats`, exercised from
`tests/queries.server.test.ts`). There is no longer a routed
`/{country}/companies` list page — list search moved to `/companies`
(`unified.server.ts`, backed by `companies_all`); the description below of
`?q=`/`?sort=`/`?page=` URL-driven state describes `searchCompanies`'s own
contract, still validated by the test sweep even though nothing routes to
it directly: `?q=` name search, `?sort=` column key + `?dir=asc|desc`
(whitelisted against `countries.ts` column config; unknown values fall back to
name asc), `?page=`, `?pageSize=25|50|100`. The industry column is populated
by a second per-page lookup (`industryQuery` in `countries.ts`) and is not
sortable by design — sorting happens on base-table columns only so the 30–70M-row
countries stay fast. Add columns per country in `countries.ts` (`columns`),
never by editing SQL in the route.

### Filters

The Filters sheet offers one searchable multi-select per categorical column
(`filterable: true` in `countries.ts`) plus Industry (canonical NACE English
labels via `nace_categories`; `industryFacetQuery`/`industryFilterExpr` per
country — Latvia has none because `lv_companies_nace` is unpopulated).
Selected values live in the URL as repeated `f_<key>=` params and are applied
server-side by the loader. Option lists are cached in-process for 24h
(`facets.server.ts`) — typeahead searches the cache (diacritic-insensitive,
prefix-first), never ClickHouse per keystroke.

### Company detail

`/company/{country_code}/{id}` — identity header, overview (all list columns +
industry), and per-country sections declared in `countries.ts` (`detail`):
financials (no, fi, ee, lv, gb, br — canonical yearly metrics, USD chart via
recharts), contacts and domains (no, fi, ee, lv, cz, br). se/sk have no
financial metrics materialized yet (pipeline gap) and fr has no detail data —
those pages show identity/overview only. All section queries bind the id as
`{id:String}` and live in the registry, never in routes.

The Contact & location card shows contacts, addresses (per-country
`addressQuery` in `countries.ts` — Norway's `no_company_addresses` is wired
but awaits its first dagster materialization; Finland has no address data
yet), and a leaflet mini map. Coordinates come from Latvia's stored
lat/long where present; otherwise the address is geocoded server-side via
Nominatim (1 req/s, results — including misses — cached permanently in
`.cache/geocode.sqlite` via node:sqlite).

Fidelity rule: the detail page shows every column of the company row
("Company record" card; lineage fields collapsed under "Source & lineage")
and country-specific sections render full source shapes — Norway shows the
complete Brønnøysund statement (all P&L/balance/filing fields; any column a
future migration adds lands in "Other fields" automatically). Never trim a
country's data to fit a generic UI — add a country component instead
(`app/components/detail/countries/`, wired via COUNTRY_FINANCIALS in the
detail route).

NO/LV record cards join their `*_companies_translated` tables (LEFT JOIN —
never a table switch; the translated tables are missing base columns).
Industries render as a shared section (all rows, canonical NACE English).
Norway statement amounts carry their currency code; USD values shown with a
leading `≈` are derived in the UI as `original × fx_rate_to_usd` where the
pipeline left the stored USD NULL.

### Language toggle

The detail page supports a `?lang=original` URL parameter to switch between
English and original-language variants of multi-language fields (present for
NO, EE, LV, FI, SK today; GB and SE have no language pairs, so the toggle is
hidden there). Default is English; omitting `?lang` or setting `?lang=en`
both show English.
The toggle is a two-option segmented control ("English" / "Original") in the
detail header, hidden entirely when a record has zero language pairs
(`pairCount === 0`).

**Pair rule:** A base key is collapsed into a single selectable pair iff BOTH
`<base>_en` and `<base>_original` are present in the record (regardless of
whether their values are empty). Unpaired one-siders (e.g., a single
`status_en` with no `status_original`, or `share_capital_amount_original`
with no `_en`) always pass through under their own literal key name. Currency
fields (suffixed `_amount_original`) are therefore never treated as language
pairs — no `_amount_en` counterpart exists, so the both-sides rule excludes
them automatically (pinned by test with BR's `share_capital_amount_original`).

**Fallback markers:** When the selected language variant is empty, the detail
page falls back to the other one and appends a muted fallback marker:
`(original)` when showing English's fallback, `(english)` when showing
original's fallback. These markers appear in the key-facts strip (at the top),
in prose sections (articles purpose / activity text / any >240-char text), and
in the field grid. Translation provenance keys (`_language`, `_translated_at`,
`_translation_provider`, `_translation_model`) live in the collapsible
"Source & lineage" details block, which carries no markers.

**Rendering structure:**

1. Key-facts strip — identity facts (legal form, status, registered date,
   website), each with fallback markers if applicable.
2. Prose sections — articles purpose, activity text, and any field >240 chars,
   each with fallback markers.
3. Field grid — all other visible fields, collapsed pairs showing selected
   language with fallback markers as needed, one-siders and currency fields
   rendering at full fidelity.
4. Lineage details — translation provenance keys ("Source & lineage" section),
   rendered under the field grid.

## Financials section

Three routes power financial analytics:

- **`/financials`** — Global overview: total revenue by country (bar chart + table),
  top 15 NACE divisions (across all NACE-enabled countries), and top 25 companies.
- **`/financials/country/{code}`** — Country page (no/fi/ee/lv/gb/br/se/sk): total
  companies, revenue, and latest fiscal year; industry breakdown (NACE divisions
  for countries with mapping, or unmapped bucket); top companies for the country.
- **`/financials/industry/{division}`** — Division (2-digit NACE) page: revenue and
  company count by country, top companies across all countries in that industry.

### Sums vs. lists: Norwegian NUF exclusion

Norwegian foreign-branch companies (legal form NUF) file the foreign parent's full
accounts — real corporate data, but not Norway-earned. Revenue sums and company
counts **exclude** NUF rows via `financialsAggregates.sumExclusionExpr`, but **lists**
(top companies table) **keep** them badged with `excluded_from_sums: true`
so editors can see the data exists and understand why it's absent from aggregates.

### Unmapped bucket

Companies with financial data but no NACE mapping are explicitly counted as "Unmapped"
and included in country pages (for countries with NACE support). The unmapped count is
computed as total companies minus sum of all mapped divisions; revenue is the residual
(totals minus mapped). Unmapped is never dropped — if a company has financials, it
contributes to the country's total revenue, appearing either in a named division or
in the Unmapped bucket.

### NACE-breakdown countries vs. totals-only

**NACE-breakdown countries** (no/se/ee/gb/sk) have a registry `financialsAggregates.nace`
entry joining their industries table, yielding division (2-digit) breakdowns and feeding
the global industry view:

- **NO** (Norway): `no_industries` + `nace_normalized_code` + primary filter.
- **SE** (Sweden): `se_industries` + `nace_rev2_class_code` (REV2 to current via category fallback).
- **EE** (Estonia): `ee_industries` + `nace_normalized_code` + primary filter.
- **GB** (United Kingdom): `gb_industries` + `nace_normalized_code` + primary filter.
- **SK** (Slovakia): `sk_industries` + `nace_normalized_code` + primary filter.

**Totals-only countries** (fi/lv/br) have `financialsLatest` tables but no nace config:

- **FI** (Finland): TOL2008 source codes exist in `fi_industries`; mapping to
  canonical NACE not yet built. Once added, add a `financialsAggregates.nace` entry
  to unlock industry breakdown (both country page and global industry routes).
- **LV** (Latvia): NACE classifier not yet run; `lv_companies_nace` remains unpopulated.
  Once classifier lands, add `financialsAggregates.nace` to enable breakdowns.
- **BR** (Brazil): CNAE→NACE mapping stub exists (`br_cnae_to_nace` table) for industry
  labels, but no aggregates config. Once aggregates logic is added, instantiate
  `financialsAggregates.nace` to activate divisions (estimated ~100 NACE classes;
  layout fits existing charts).

Each country auto-upgrades when its mapping arrives — simply add the registry entry
in `countries.ts` and the division view activates without UI changes.

### Methodology & caveats

- **Latest filed year per company**: Financial aggregates use each company's most
  recent filing (`max(fiscal_year)` in `financialsLatest` tables), not a snapshot
  year. A company filing FY 2024 in June 2026 contributes at the 2024 rate; another
  filing FY 2023 contributes at its own 2023 rate. `latest_fiscal_year` shown in UI
  is the max across all companies in scope (country/division/global).
- **USD at period-end rates**: Revenue converted via period-end FX rates stored during
  pipeline materialization. Aggregates sum the already-converted USD values; no
  secondary normalization occurs. Rates vary by company filing date — sums are not
  anchored to a single rate.
- **Standalone vs. group accounts**: Some filings include standalone company results
  (and group results separately). Aggregates sum whichever metric is recorded in the
  `revenue_amount_usd` column; no deduplication across group/standalone occurs at this
  layer. Group-level double-counting is possible if a parent and subsidiary both file.

## Rules

- Read-only: `SELECT` only, no writes to ClickHouse.
- User input goes through ClickHouse query params (`{name:String}`), never
  string interpolation. Identifiers may only come from `countries.ts`.
