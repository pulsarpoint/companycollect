# Wikidata company pipeline design

Last verified against the implementation: 2026-09-03.

## 1. Purpose and boundary

The Wikidata source discovers company QIDs, downloads structured company facts, keeps
the raw weekly responses in object storage, normalizes one table at a time in DuckDB,
and atomically publishes a coherent set of ClickHouse tables.

This is a selected company snapshot, not a general Wikidata mirror. A company enters
the snapshot through at least one of these discovery paths:

1. it has a current stock-exchange statement (`P414`) on an exchange discovered by the
   pipeline; or
2. it has one of the configured national company/registry-number properties.

The current pipeline stores Wikidata's short English `companyDescription` value in
`corpscout.wikidata_companies.company_description`. That value is not the lead text of
a Wikipedia article. As of this document's verification date, neither Wikipedia
sitelinks nor Wikipedia article content are stored by this pipeline.

The planned Wikipedia extension is specified in
[`docs/superpowers/plans/2026-09-03-wikidata-wikipedia-links.md`](../../../../../docs/superpowers/plans/2026-09-03-wikidata-wikipedia-links.md).
It will preserve batched Wikipedia responses in S3/RustFS and bulk-publish the current
multilingual article text directly to ClickHouse. It deliberately does not add a
DuckDB layer.

## 2. Upstream interfaces

| interface | endpoint | current use |
| --- | --- | --- |
| Wikidata Query Service (WDQS) | `https://query.wikidata.org/sparql` | exchange discovery, company discovery, and structured company augmentations |
| Wikidata Action API | `https://www.wikidata.org/w/api.php` | not used today; planned for exact Wikipedia sitelinks via `wbgetentities` |
| Per-edition MediaWiki REST API | `https://<language>.wikipedia.org/w/rest.php/v1/` | not used today; planned for page, revision, license, and HTML content |

WDQS is appropriate for discovering items from statements and properties. Once QIDs
are known, the Wikidata documentation recommends `wbgetentities` for entity data such
as sitelinks. The relevant upstream documentation is:

- [Wikidata data access](https://www.wikidata.org/wiki/Wikidata:Data_access)
- [Presenting Wikidata knowledge with `wbgetentities`](https://www.mediawiki.org/wiki/API:Presenting_Wikidata_knowledge)
- [Wikidata sitelinks](https://www.wikidata.org/wiki/Help:Sitelinks)
- [MediaWiki REST API reference](https://www.mediawiki.org/wiki/API:REST_API/Reference)
- [Wikimedia User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)

Every request uses a descriptive User-Agent. The client applies bounded retry and
backoff for connection errors, timeouts, HTTP 429, and transient 5xx responses.

## 3. Company discovery

### 3.1 Current exchange listings

`wikidata_exchanges_raw` discovers exchanges from current `P414` listing statements.
A listing is current when it has no `P582` end-time qualifier. Companies carrying a
`P576` dissolution date are excluded. Each discovered exchange becomes a dynamic
`company_source` partition, and its company pages are downloaded with stable ordering
and offset pagination.

Exchange discovery is global. It is not limited to a configured list of exchanges
unless a run explicitly sets `exchange_ids_csv`.

### 3.2 National registry-number seeds

Country modules declare one `WikidataRegistrySeedSpec` beside their own table
constants. `defs/wikidata/registry_seed.py` aggregates those declarations. For each
property, company discovery anchors directly on:

```sparql
?company wdt:<property_id> ?registryValue .
```

These sources are represented internally as pseudo-exchanges such as
`registry_P6460`, allowing the existing page, batch, checkpoint, and manifest layout
to be reused.

| country | property | registry spine asset |
| --- | --- | --- |
| Sweden | `P6460` | `sweden_company_companies_clickhouse` |
| Norway | `P2333` | `norway_brreg_entities_snapshot_clickhouse` |
| Denmark | `P1059` | `denmark_cvr_companies_duckdb` |
| Finland | `P12980` | `finland_ytj_resolved_clickhouse` |
| United Kingdom | `P2622` | `uk_companies_house_clickhouse_companies` |
| France | `P1616` | `france_sirene_clickhouse_companies` |
| Czechia | `P4156` | `czech_ares_clickhouse_companies` |
| Latvia | `P8053` | `latvia_ur_clickhouse_companies` |
| Brazil | `P6204` | `brazil_comp_rfb_clickhouse_companies` |

The spine asset is an ordering/lineage `deps=` edge. Dagster does not pass the
country table into `wikidata_company_source_units`, and the discovery query does not
read ClickHouse. Its purpose is to make the country-to-Wikidata relationship visible
and validated in the asset graph.

### 3.3 Sweden's exact matching direction

Swedish candidates are discovered from Wikidata property `P6460`; the local
`corpscout.se_companies` table is not used to generate the WDQS request. After publish,
`se_company_info_wikidata_clickhouse` intersects the Wikidata results with the local
Swedish registry universe:

```text
Wikidata item with P6460
  -> corpscout.wikidata_company_identifiers (identifier_type = 'se_orgnr')
  -> remove non-digits from the organisation number
  -> corpscout.se_companies.company_id
  -> corpscout.wikidata_companies via wikidata_id
  -> corpscout.se_company_info_wikidata
```

A second path matches a Wikidata LEI to the current Swedish LEI in
`corpscout.company_identifier`. Both paths remain bounded by
`corpscout.se_companies`.

For example, Wikidata item `Q1421630` carries `P6460 = 502007-7862`. The normalized
identifier `5020077862` is matched to `corpscout.se_companies.company_id`.

## 4. Raw snapshot and orchestration

The raw bucket is `source-wikidata-weekly`. Raw objects are immutable checkpoints
under the weekly partition date and company-source identity:

```text
partition_date=YYYY-MM-DD/active_exchanges.json
partition_date=YYYY-MM-DD/seed_units.json
partition_date=YYYY-MM-DD/exchange_id=<QID-or-registry_PID>/page=000001.json
partition_date=YYYY-MM-DD/exchange_id=<id>/augmentation_kind=<kind>/page=000001_batch=000001.json
partition_date=YYYY-MM-DD/exchange_id=<id>/data_kind=<kind>/manifest.json
partition_date=YYYY-MM-DD/exchange_id=<id>/manifest.json
partition_date=YYYY-MM-DD/snapshot_manifest.json
```

The weekly schedule starts exchange discovery at `03:30` every Monday in
`Europe/Belgrade`. Sensors then advance the snapshot through these stages:

```text
wikidata_exchanges_raw
  -> wikidata_company_source_units
  -> wikidata_company_pages_raw (one dynamic company_source x week partition)
       -> wikidata_company_profiles_raw
       -> wikidata_company_identifiers_raw
       -> wikidata_company_relationships_raw
       -> wikidata_company_people_raw -> wikidata_persons_raw
  -> wikidata_company_source_snapshot
  -> wikidata_raw_snapshot
  -> one DuckDB asset per output table
  -> one ClickHouse asset per output table
  -> wikidata_snapshot_complete
  -> wikidata_clickhouse_canonical_contacts
```

The network assets share the `wikidata_sparql` pool. Each dynamic raw asset uses a
multi-run backfill policy with one partition per run. Requests checkpoint each page or
augmentation batch in object storage; retrying a failed partition reuses completed
objects.

`wikidata_raw_snapshot` is the completeness gate. It publishes only when every seed
unit has all required domain manifests. The publish sensor then gives every DuckDB
asset the same `partition_date`. `wikidata_snapshot_complete` verifies that all
ClickHouse tables contain exactly one and the same `source_run_id` before downstream
canonical contact tables are rebuilt.

## 5. Loading and ClickHouse tables

Each normalized table owns a separate DuckDB file:

```text
data/wikidata/<table_name>.duckdb
```

The file contains a raw staging schema (`wikidata_stage`) and a normalized schema
(`wikidata`). Rows are loaded in typed Arrow batches, normalized with set-based DuckDB
SQL, and then full-replaced in the migration-owned ClickHouse table through a staging
table swap. A source payload hash and weekly source-run id preserve snapshot
provenance.

| ClickHouse table | grain | purpose |
| --- | --- | --- |
| `wikidata_companies` | one row per company QID | canonical company label, short description, identity/profile facts and listing summary |
| `wikidata_exchanges` | one row per exchange QID and MIC | exchange identity, country and active-company count |
| `wikidata_company_listings` | one row per listing statement | exchange, ticker, ISIN and current flag |
| `wikidata_company_identifiers` | one row per QID, identifier type and value | LEI, CIK, national registry numbers and other external identifiers |
| `wikidata_company_websites` | one row per normalized website | official website and normalized domain evidence |
| `wikidata_company_relationships` | one row per subject, relationship type and object | parent, subsidiary and ownership edges |
| `wikidata_company_people` | one row per company, role and person QID | CEO, founder, chairperson, board member and person-valued owner links |
| `wikidata_persons` | one row per person QID | person label, short description, birth year and image |
| `wikidata_seed_extraction_runs` | one row per source run and query mode | extraction counts and query provenance |

`wikidata_clickhouse_canonical_contacts` derives
`corpscout.wikidata_company_contacts` and
`corpscout.wikidata_company_domains` from `wikidata_company_websites`; those two
derived tables are not members of the source-snapshot table tuple.

The planned Wikipedia article pipeline is a separate downstream branch:

```text
wikidata_snapshot_complete
  -> wikidata_company_wikipedia_articles_s3
  -> wikidata_company_wikipedia_articles
```

The first asset writes immutable, compressed response batches and a completion
manifest to S3/RustFS. The second streams those objects, normalizes article text, and
bulk-inserts blocks into a ClickHouse staging table before an atomic publish. It does
not create a persistent DuckDB file. Keeping this branch outside
`wikidata_snapshot_complete` allows Wikipedia ingestion to lag or retry without
blocking publication of the structured Wikidata snapshot.

## 6. Wikipedia relationship and current limitation

A Wikidata sitelink connects one QID to a page title on a particular Wikimedia site.
It is the authoritative bridge between the structured entity and language-specific
Wikipedia pages. A QID may have no Wikipedia article, one article, or articles in many
languages, and language editions can contain different text.

For `Q1421630`, two of the sitelinks returned by a live all-language
`wbgetentities` check on 2026-09-03 are:

| site | title | URL |
| --- | --- | --- |
| `enwiki` | Handelsbanken | `https://en.wikipedia.org/wiki/Handelsbanken` |
| `svwiki` | Svenska Handelsbanken | `https://sv.wikipedia.org/wiki/Svenska_Handelsbanken` |

The planned change stores the current article associated with every returned
Wikipedia sitelink, using one row per `(wikidata_id, site_id)` in
`corpscout.wikidata_company_wikipedia_articles`. Each row carries the Wikimedia
`site_id`, a consumer-facing `language_code`, page and revision identity, title, exact
URL, lead text, normalized full text, and license metadata. English and
Swedish above are examples for this QID, not specially treated columns or
country-specific behavior. Consumers select their preferred language at query time
and can apply their own fallback order.

Raw API responses are retained in S3/RustFS so a failed ClickHouse publication can be
retried without downloading the articles again. Objects contain batches rather than
one object per article. ClickHouse remains the query layer and stores the current
revision; the raw snapshot supplies replay and provenance. Historical serving tables,
embeddings, media binaries, and rendered HTML in ClickHouse remain out of scope.

## 7. Configuration and verification

`WikidataRawPullConfig` controls page size, augmentation batch size, request timeout,
delay, User-Agent, optional exchange filters, and optional registry-property filters.
`WikidataSnapshotConfig` selects the completed weekly partition used to rebuild the
DuckDB and ClickHouse snapshot.

Primary implementation checks:

```bash
uv run pytest tests/test_wikidata_assets.py tests/test_se_company_wikidata.py -q
uv run dg check defs
```

Operational verification should also confirm that every published table reports the
same `source_run_id`, listing rows reference known exchanges, and a known country
identifier such as `Q1421630 / P6460` still reaches its country artifact.
