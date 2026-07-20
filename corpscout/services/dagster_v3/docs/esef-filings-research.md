# ESEF filings ingestion — research / pre-implementation design

Status: **implemented** (2026-07-20). This research doc is now historical context; the
as-built design (source, resource/client, assets, tables, jobs/schedule, correctness
decisions, deferred list) lives in
`defs/esef_filings/docs/esef_filings-design.md`. Migration `000149` is written but
**not yet applied** (ledger at 148) and no live materialization has run — server
backfill + validation is Task 8 of the implementation plan (pending).

## Why

Listed companies file ESEF (inline XBRL, IFRS consolidated) annual reports to national
OAMs, not to the company registers our per-country sources read. Concretely: 0 of 298
Finnish public limited companies have financials in `fi_financial_statements` even though
`finland_xbrl` works — listed issuers simply never appear in PRH's digital filing API.
The same blind spot exists for every country source. One ESEF pipeline fixes it for all
EU/EEA listed companies at once (~30 countries, low-thousands of filings/year).

## Source

**filings.xbrl.org** (XBRL International's public repository of ESEF filings):

- Index API: `https://filings.xbrl.org/api/filings` — JSON:API, filterable by `country`
  (e.g. `FI` → 1,168 filings at 2026-07-19), paginated, no auth.
- Per filing: `package_url` (the official iXBRL zip), `period_end`, checksums, validation
  counts, and — critically — **pre-extracted facts as JSON** (`json_url`, the Arelle-
  generated xBRL-JSON of the filing) for most filings.
- Entities keyed by **LEI**; join to national registry ids via the existing `gleif`
  module (`leis` → `business_id`).
- Alternative (rejected): per-country OAMs (e.g. Nasdaq Helsinki for FI) have no
  machine-readable bulk interface. filings.xbrl.org aggregates exactly this.
- License/terms: index is openly accessible; underlying filings are regulated public
  information. **Confirm redistribution terms before exposing parsed content downstream.**

## Ingest hierarchy decision (§5b)

Level 1 applies: **the repository already publishes extracted facts (xBRL-JSON)** — so
the default path needs *no XBRL parser at all*:

1. Crawl the index API (filter per country or all countries) → filing metadata table.
2. Download `json_url` (xBRL-JSON facts) per filing; fall back to the iXBRL `package_url`
   + an Arelle parse stage only for filings without a JSON rendition.
3. Land facts in a generic flat fact table `(entity LEI, period, concept_qname, context,
   unit, value, dimensions)` — same shape `finland_xbrl` uses, per §5b "extraction is
   swappable".

## Mapping (data, not code)

- Concepts are **IFRS taxonomy** qnames (`ifrs-full:Revenue`, `ifrs-full:ProfitLoss`,
  `ifrs-full:Assets`, `ifrs-full:Equity`, …) — one mapping table covers every country,
  which is the §5b payoff. Issuer taxonomy extensions that anchor to IFRS concepts map
  through their anchors; unanchored extensions stay raw.
- Canonical metrics mirror the existing metric set (revenue, operating result,
  profit/loss, total assets, equity, liabilities, …) with `_original` + `_usd` pairs
  per §7 (multi-currency: GBP/SEK/NOK/CHF filings exist; FX keyed on `period_end`).

## Scope flag — the one hard modeling rule

ESEF figures are **consolidated group** IFRS numbers. National-register statements
(PRH, Brreg, …) are usually **standalone statutory** numbers. The same `business_id`
can have both for the same fiscal year and they will legitimately disagree. Every
ESEF-derived row must carry `scope = 'consolidated_ifrs'`, and
`company_financials_latest` must prefer-or-segregate explicitly — never silently mix.

## Pipeline shape

Partitioned incremental (§4): partition by **filing period / index window**
(`MonthlyPartitionsDefinition` over `date_added`), `BackfillPolicy.multi_run(1)`,
own module `defs/esef_filings/`, own DuckDB + pool, raw zips/JSON cached in S3
(`source-esef-filings` bucket) — parse once, never re-fetch.

```
index crawl (API, monthly windows)
  -> filing metadata (DuckDB -> corpscout.esef_filings)
  -> fact JSON download to S3 (fallback: package zip + Arelle stage)
  -> flat facts (DuckDB -> corpscout.esef_facts)
  -> metric mapping table (IFRS -> canonical, versioned data)
  -> corpscout.esef_financial_metrics (USD step separate)
  -> LEI -> registry-id join view via gleif
```

## Open questions before implementation

- Redistribution terms of filings.xbrl.org content (index vs derived facts).
- Coverage check: what fraction of filings ship a usable `json_url` (drives whether the
  Arelle fallback is needed in v1 at all).
- Dual-listed / multi-LEI groups: dedup rule when one group files in two countries.
- Whether `company_financials_latest` consumes ESEF rows in v1 or the table stays
  standalone until the scope-preference rule is agreed.
