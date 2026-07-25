# TED procurement source — design

Status: **implemented 2026-07-19** (`defs/ted_procurement/`). Countries configured:
FIN/FI and SWE/SE. First verified Finland partition: 2026-06 — 578 notices,
1,873 winner rows (100% with national ids, 91% joining `fi_companies`), published
to `corpscout.ted_notices` + `corpscout.ted_notice_winners` (migration 000148).
Sweden was enabled on 2026-07-23 for the shared government-contract signal.

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
- Countries: `FIN/FI` and `SWE/SE` — a module-level tuple; adding a country =
  extending it. A notice returned for more than one country scope is stored once
  per `(country_iso2, publication_number)` while its XML is downloaded and parsed
  only once.

## Pipeline shape

```
search index (monthly partition, per country)
  -> listing DuckDB + S3 manifest
  -> per-notice eForms XML -> s3://source-ted-procurement/xml/...
  -> lxml parse: notices + organizations + lot_results (award values) + winner links
  -> DuckDB partition tables
  -> corpscout.ted_notices + corpscout.ted_notice_winners (atomic replace from all partitions)
  -> exact national-id match to country company tables
  -> company_government_contract_evidence + company_government_contract_summary
  -> USD step (per-amount currency, publication-date FX key)
```

- **Parser output is generic** (§5b spirit): organizations with
  `(registration_name, company_id, company_id_scheme, country)` and lot results with
  awarded values; the winner join resolves TenderingParty → Company. National-id
  normalization happens after parsing: Finland strips the `FI` VAT prefix and
  validates Y-tunnus shape; Sweden strips separators and only the legal-entity
  `16` century prefix. Swedish 12-digit `19`/`20` personal identities are retained
  as non-company identifiers and never truncated into false company matches.
- `ted_notice_winners` mirrors the `fi_hilma_notice_winners` shape
  so consumers query both the same way; plus `place_country` and
  `winner_country`. Its physical key includes `country_iso2`, because the same TED
  notice can legitimately occur in several country search scopes.

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
- **Schedule (§9)**: monthly, after month close (3rd, 05:35), current month
  refreshable via `end_offset=1`. Backfill 2024-01→now from the UI. The partition
  job only snapshots/parses; the stopped-by-default
  `ted_publish_after_monthly_parse` run-status sensor launches the unpartitioned
  all-partition ClickHouse publish after a successful partition run.

## Issues found during implementation

- **ted.europa.eu rate-limits the XML endpoint** sporadically (429 on ~13/578 fetches
  even throttled), and the dlt session *raises* the 429 as HTTPError after its own
  internal retries — the client catches both the returned-response and raised forms,
  honours `Retry-After`, and backs off up to 6 attempts (plus a 0.2s politeness
  throttle between downloads).
- **A LotResult can reference several winning tenders** (multi-supplier framework
  awards, e.g. the Swedish grocery framework fixture) — all referenced tenders are
  winners; do not assume one winner per lot.
- **ContractingParty carries two org refs** when a procurement platform files on the
  buyer's behalf: the buyer under `cac:Party` directly and the platform under
  `ServiceProviderParty/cac:Party` — take only the direct child, or the platform
  becomes the buyer.
- DuckDB has no `generate_subscripts`; list explosion uses
  `generate_series(1, len(parts))`.

## Risks / open items

- eForms winner-linkage (LotResult→LotTender→TenderingParty→Company) must be built
  test-first against several real notices incl. multi-lot, multi-winner, and
  consortium cases — the Hilma `ted_number` overlap gives a ready-made validation set.
- Rate limits are undocumented — throttle politely (the dlt requests client +
  modest per-partition volumes should be safe).
- `can-modif` (modification) notices reference earlier awards — decide during
  implementation whether to ingest as rows or skip for v1.
- Legacy pre-2024 XML intentionally excluded; revisit only if a consumer needs
  EU-threshold history beyond Hilma's coverage.
