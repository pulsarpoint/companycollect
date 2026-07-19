# TED procurement source — design (pre-implementation)

Status: **design agreed 2026-07-19, implementation next.** When the module is built this
becomes `defs/ted_procurement/docs/ted_procurement-design.md` per §10 of
`docs/data-source-guidelines.md`.

## Why

EU-threshold public contracts for **every EU member state** from one keyless source.
Complements `finland_hilma` (national below-threshold layer) from the other side, and
`fi_hilma_notices.ted_number` gives a direct cross-validation join. First scope is
Finland; the module is country-parameterized from day one.

## Source (verified live 2026-07-19)

- **Search API**: `POST https://api.ted.europa.eu/v3/notices/search` — no auth.
  - Query language: `place-of-performance IN (FIN) AND notice-type IN (can-standard, ...)`
    plus `publication-date` ranges for partitioning.
  - **Max `limit` is 250/page** (verified: 300 → `SEARCH_EXCEEDS_MAX_LIMIT`); `page`
    parameter for pagination. FIN award volume ≈ hundreds/month → monthly windows
    never approach pagination limits.
  - Useful fields verified: `publication-number`, `publication-date`, `notice-title`,
    `buyer-name`, `winner-name`, `total-value(-cur)`, `links.xml.MUL`.
  - **Structured org identifiers are NOT in the search response** — names only.
- **Per-notice eForms XML**: `https://ted.europa.eu/en/notice/{publication-number}/xml`
  (~17 KB each). Verified content: `efac:Organizations/efac:Company` blocks with
  `cbc:RegistrationName` and **`cbc:CompanyID schemeID="002"` holding the national
  registration number** — for Finland the Y-tunnus (`3006157-6`), occasionally the
  VAT form (`FI09880841`, normalizable). Winner linkage chain inside the XML:
  `efac:LotResult` (per lot, has result + awarded value) → `efac:LotTender` →
  `efac:TenderingParty` → organization technical id (`efac:Company/cac:PartyIdentification`).
- Sample saved: `companies/data/finland/raw/api/ted_notice_496211_2026.xml`.
- **License**: EU open data, free reuse. 37,946 FIN `can-standard` notices at check time.
- Older (pre-eForms, ≤2023) notices use the legacy TED XML schema — **out of scope for
  v1**; partition start 2024-01 keeps the corpus eForms-only (eForms became mandatory
  late 2023). Hilma covers the FIN historical layer back to 2018.

## Ingest mode (§2/§4) — partitioned API incremental

No per-country bulk file exists (daily packages are all-EU, all-types bundles — the
scale-up path if we ever want every country at once). Therefore:

- `MonthlyPartitionsDefinition(start_date="2024-01-01", end_offset=1)` on
  **publication-date**, `BackfillPolicy.multi_run(max_partitions_per_run=1)`, per-source
  pool. Reference impl: `finland_xbrl`.
- Per partition: search API paged crawl (250/page) of configured countries + notice
  types → listing rows; then per-notice XML download to S3, skip-if-exists, with a
  partition `manifest.jsonl` + `_SUCCESS.json` marker (same contract as
  `finland_xbrl` XML snapshots).
- Notice types v1: award-carrying forms only (`can-standard`, `can-social`,
  `can-desg`, `can-modif` candidates — final list fixed during implementation from the
  type facet). Contract notices (pre-award) are Hilma/TED noise for our purpose.
- Countries v1: `("FIN",)` — a module-level tuple; adding a country = extending it
  (each country's rows land in the same tables keyed by `place_country`).

## Pipeline shape

```
search index (monthly partition, per country)
  -> listing DuckDB + S3 manifest
  -> per-notice eForms XML -> s3://source-ted-procurement/xml/...
  -> lxml parse: notices + organizations + lot_results (award values) + winner links
  -> DuckDB partition tables
  -> corpscout.ted_notices + corpscout.ted_notice_winners (atomic replace from all partitions)
  -> USD step (per-amount currency, publication-date FX key)
```

- **Parser output is generic** (§5b spirit): organizations with
  `(registration_name, company_id, company_id_scheme, country)` and lot results with
  awarded values; the winner join resolves TenderingParty → Company. National-id
  normalization per country (FIN: strip `FI` VAT prefix / hyphen check → Y-tunnus)
  happens in SQL, mapping-table-driven, not hardcoded in the parser.
- `ted_notice_winners` mirrors the `fi_hilma_notice_winners` shape
  (ORDER BY winner id first) so consumers query both the same way; plus
  `place_country` and `winner_country`.

## ClickHouse (migration `0001XX`, next free number at implementation time)

- `ted_notices` — 1 row per (publication_number, lot_id): buyer name/id, titles,
  CPV, NUTS, type, publication/deadline dates, awarded + estimated values
  (`*_amount_original/_usd/_currency`), fx trio, provenance.
- `ted_notice_winners` — 1 row per (publication_number, lot_id, winner_ordinal):
  winner_name, winner_national_id (normalized), winner_country, buyer id,
  published_at. ORDER BY (winner_national_id, publication_number, lot_id,
  winner_ordinal).

## Cross-cutting

- **Currency (§7)**: mostly EUR; per-amount conversion keyed on publication date
  (same approach as `finland_hilma`).
- **Translation (§8)**: none — titles come in source language + often English;
  proper nouns otherwise.
- **Contacts/industry (§8b/8c)**: none in source; CPV kept verbatim (same decision
  as Hilma). No canonical contacts pair (supplement source).
- **Schedule (§9)**: monthly, after month close (e.g. 3rd, 05:35 staggered), current
  month refreshable via `end_offset=1`. Backfill 2024-01→now from the UI.

## Risks / open items for implementation

- eForms winner-linkage (LotResult→LotTender→TenderingParty→Company) must be built
  test-first against several real notices incl. multi-lot, multi-winner, and
  consortium cases — the Hilma `ted_number` overlap gives a ready-made validation set.
- Rate limits are undocumented — throttle politely (the dlt requests client +
  modest per-partition volumes should be safe).
- `can-modif` (modification) notices reference earlier awards — decide during
  implementation whether to ingest as rows or skip for v1.
- Legacy pre-2024 XML intentionally excluded; revisit only if a consumer needs
  EU-threshold history beyond Hilma's coverage.
