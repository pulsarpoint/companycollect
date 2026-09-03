# Wikidata company Wikipedia articles — design and implementation plan

Date: 2026-09-03. Status: proposed and design-approved; not yet implemented.
Scope: the shared Wikidata company source for every country and Wikipedia language.

## 1. Goal

Download the current Wikipedia articles connected to company QIDs in the completed
Wikidata snapshot and make their lead and full text queryable in ClickHouse.

The implementation is language-neutral:

- discover pages only through Wikidata sitelinks, never company-name search;
- retain every Wikipedia edition returned for a QID;
- store one current row per `(wikidata_id, site_id)` in
  `corpscout.wikidata_company_wikipedia_articles`;
- preserve the Wikipedia page and revision identity, license, and
  retrieval provenance;
- let consumers apply their own language preference and fallback order.

For the known acceptance fixture, `Q1421630` must include at least:

```text
enwiki -> Handelsbanken          -> https://en.wikipedia.org/wiki/Handelsbanken
svwiki -> Svenska Handelsbanken -> https://sv.wikipedia.org/wiki/Svenska_Handelsbanken
```

Both rows must contain the downloaded article lead and normalized full text, not only
the title and URL.

## 2. Final architecture

```text
wikidata_snapshot_complete
  -> wikidata_company_wikipedia_articles_s3
  -> wikidata_company_wikipedia_articles
       -> corpscout.wikidata_company_wikipedia_articles
```

This adds exactly two Dagster assets and one ClickHouse table:

| asset | persistent output | responsibility |
| --- | --- | --- |
| `wikidata_company_wikipedia_articles_s3` | compressed S3/RustFS response batches plus manifest | resolve sitelinks, download current Wikipedia page responses, checkpoint and retain source data |
| `wikidata_company_wikipedia_articles` | `corpscout.wikidata_company_wikipedia_articles` | stream the completed S3 snapshot, normalize article text, bulk-load ClickHouse, and publish atomically |

There is no DuckDB asset or file. The ClickHouse asset reads S3 itself, so its Dagster
dependency is a `deps=` lineage and ordering edge rather than an in-memory Python
value passed through an IO manager.

The Wikipedia branch remains downstream of, but outside,
`wikidata_snapshot_complete`. A slow or retrying Wikipedia download must not block the
structured Wikidata tables from being published.

## 3. Design decisions

| question | decision |
| --- | --- |
| QID-to-page relationship | Wikidata sitelinks keyed by QID |
| Sitelink API | Wikidata Action API `wbgetentities` with `props=sitelinks/urls` |
| Article API | the REST API on the returned Wikipedia host, requesting the page with HTML and metadata |
| Languages | every returned Wikipedia edition; no fixed allowlist |
| S3 representation | immutable, compressed response batches and one completion manifest |
| S3 object sizing | batch many articles into each object; never create one object per article |
| ClickHouse grain | one current row per `(wikidata_id, site_id)` |
| ClickHouse loading | stream S3 objects and insert blocks into a staging table, then atomically publish |
| Article representation | lead text and full normalized plain text; raw HTML remains in S3 |
| Deletions | full-snapshot replacement removes sitelinks no longer present upstream |
| History | S3 retains source snapshots; ClickHouse serves only the current revision |
| Language selection | consumer policy, not a source-table flag |

One column per language is not the model. Adding a Wikipedia edition must require no
ClickHouse migration and no country-pipeline change.

`uv run dg list components --json` confirms that the project has S3 and ClickHouse
components but no Wikipedia integration. Implement the external Wikipedia HTTP
boundary as a small project component which owns these two assets and its automation.
Do not add a generic API interface or service layer: there is one concrete Wikidata
client and one concrete MediaWiki client.

Relevant upstream references:

- [Wikidata API comparison](https://www.wikidata.org/wiki/Wikidata:REST_API/Comparison)
- [`wbgetentities` sitelinks and URLs](https://www.mediawiki.org/wiki/API:Presenting_Wikidata_knowledge)
- [Wikidata sitelinks](https://www.wikidata.org/wiki/Help:Sitelinks)
- [MediaWiki REST API reference](https://www.mediawiki.org/wiki/API:REST_API/Reference)
- [Wikimedia User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)

## 4. ClickHouse schema

Add one migration-owned table:

```sql
CREATE TABLE corpscout.wikidata_company_wikipedia_articles
(
    wikidata_id String,
    site_id LowCardinality(String),
    language_code LowCardinality(String),

    wikipedia_page_id UInt64,
    wikipedia_revision_id UInt64,
    wikipedia_revision_at DateTime64(3, 'UTC'),

    article_title String,
    article_url String,
    article_revision_url String,
    article_lead_text String CODEC(ZSTD(3)),
    article_text String CODEC(ZSTD(3)),

    content_format LowCardinality(String),
    license_name String,
    license_url String,

    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (wikidata_id, site_id);
```

Column semantics:

- `wikidata_id`, `site_id`, `language_code`, `article_title`, and `article_url` come
  from the validated Wikidata sitelink;
- `wikipedia_page_id`, `wikipedia_revision_id`, and `wikipedia_revision_at` identify
  the exact Wikipedia content response;
- `article_revision_url` is a permanent URL for the stored revision;
- `article_lead_text` is the normalized lead section used as the richer company
  description;
- `article_text` is the complete normalized plain text, retaining meaningful section
  headings and paragraph boundaries;
- `content_format` is `text/plain` for the initial implementation;
- `license_name` and `license_url` are copied from Wikipedia page metadata;
- `source_system` is `wikipedia`;
- `source_record_id` is the stable `<wikidata_id>:<site_id>` natural record identity;
- `retrieved_at` records the source request time and `resolved_at` records the
  normalized row time.

Invariants:

- `wikidata_id` matches `^Q[1-9][0-9]*$`;
- `site_id` is the exact Wikidata site key, such as `enwiki` or `svwiki`;
- the sitelink URL uses HTTPS and a `wikipedia.org` host;
- page and revision IDs are positive;
- title, URL, full text, and license URL are non-empty;
- `(wikidata_id, site_id)` is unique within a published source run;
- every row has the same completed Wikidata `source_run_id`.

Do not add `wikipedia_en_url`, `wikipedia_sv_url`, or any other language-specific
columns to `wikidata_companies` or country tables. Do not store raw HTML in
ClickHouse; S3 is the replayable raw boundary.

Raw response and object hashes belong to the S3 manifest, not the ClickHouse table.
The repository excludes per-row payload hashes from serving tables because unique
hash strings compress poorly and are not part of normal queries. Wikipedia revision
identity already provides the source-level change marker.

## 5. S3/RustFS snapshot asset

### Task 1: add the API clients and content parser

Add two concrete HTTP clients:

1. A Wikidata entity client calls:

   ```text
   GET https://www.wikidata.org/w/api.php
     ?action=wbgetentities
     &ids=<QID|QID|...>
     &props=sitelinks/urls
     &format=json
     &formatversion=2
   ```

2. A MediaWiki page client calls the REST API on the validated host returned by the
   sitelink, using the `page/{title}/with_html` representation. That response contains
   page identity, latest revision metadata, license information, and rendered HTML.

The clients share the existing bounded retry behavior for connection errors,
timeouts, HTTP 429, and transient 5xx responses. Every request carries a descriptive
User-Agent. Hosts must be derived from validated Wikipedia sitelinks; never accept an
arbitrary URL as an API target.

The HTML normalizer removes navigation, references, edit controls, style/script
content, and other presentation-only nodes while preserving useful headings,
paragraphs, lists, and table text. It emits a separate lead section and full plain
text. Parsing behavior is covered with stored fixtures and does not require live
network access in unit tests.

Operator configuration owns defaults for request timeout, delay, Wikidata QID batch
size, article concurrency, and target S3 object size. Runtime request objects receive
explicit values without duplicated defaults.

### Task 2: materialize `wikidata_company_wikipedia_articles_s3`

The asset depends on `wikidata_snapshot_complete`, reads the distinct company QIDs
from the published Wikidata company table, and carries its `source_run_id` into the
Wikipedia snapshot.

Processing order:

1. sort and deduplicate QIDs;
2. resolve all Wikipedia sitelinks in Wikidata batches of at most 50 QIDs;
3. retain only validated `*.wikipedia.org` pages;
4. group requests by Wikipedia host and apply bounded per-host concurrency;
5. fetch the current page response, following documented title normalization and
   redirects;
6. append the sitelink context and complete Wikipedia response to a compressed batch;
7. flush each batch when its configured target byte size is reached;
8. publish the manifest only after all objects are durable and verified.

Use a dedicated bucket so large Wikipedia bodies can have retention and lifecycle
rules independent of the structured Wikidata source:

```text
bucket: source-wikipedia-articles-weekly

partition_date=YYYY-MM-DD/source_run_id=<wikidata-source-run-id>/
  part=000001.jsonl.gz
  part=000002.jsonl.gz
  ...
  manifest.json
```

Each JSON Lines record contains the QID and sitelink context, retrieval timestamp, and
the complete REST page response. An object contains many records. Its manifest entry
contains the object key, compressed byte size, row count, and SHA-256 checksum.

The completed manifest contains:

- schema version;
- Wikidata partition date and `source_run_id`;
- ordered object entries;
- requested and returned QID counts;
- total article count and counts by language/site;
- terminal missing-page counts;
- request, retry, and reused-checkpoint counts;
- total compressed bytes and aggregate checksum.

A retry reuses verified batch objects and continues from the first incomplete batch.
It must not adopt a manifest from a different source run or schema version. Raw
objects are immutable and are not deleted by a retry.

The initial implementation performs a complete weekly snapshot. Conditional requests
or cross-snapshot reuse of unchanged revision IDs can be added after measuring volume;
that optimization must preserve a self-contained, replayable manifest.

## 6. Direct S3-to-ClickHouse publication

### Task 3: materialize `wikidata_company_wikipedia_articles`

The ClickHouse asset has `deps=["wikidata_company_wikipedia_articles_s3"]`. It locates
and validates the completed manifest itself rather than receiving the dataset as a
Python return value.

Publication order:

1. verify every manifest object exists and matches its expected checksum;
2. stream and decompress the objects in manifest order;
3. normalize the HTML into lead and full plain text;
4. create typed rows matching the migration-owned ClickHouse schema;
5. accumulate rows by byte size and insert each block into a staging table;
6. reject duplicate `(wikidata_id, site_id)` rows or conflicting revisions;
7. compare inserted counts, hashes, and language totals with the manifest;
8. atomically replace the canonical table only after all validation passes.

Insertion must use ClickHouse block inserts. It must never execute one insert per
article. Size blocks by bytes rather than only row count because article lengths vary
substantially; an initial target of 32–64 MiB per block is reasonable and remains
operator-configurable.

The final table is allowed to be empty for filtered development runs, but its table
must exist and its manifest must still be complete. Python must not create the
canonical table: migrations own persistent ClickHouse DDL.

No DuckDB constant, file, asset, table, transformation, or exporter entry is added for
Wikipedia articles.

### Task 4: add the ClickHouse migration

Create the next free forward/down migration pair for
`corpscout.wikidata_company_wikipedia_articles` and register it in the migration
contract tests. At implementation time, resolve the migration number again rather
than relying on the number that was free when this plan was written.

The up migration creates the schema from section 4. The down migration drops only
that table. Extend schema contract tests so the migration DDL and Python insertion
column order cannot drift.

## 7. Automation and failure boundaries

Trigger the S3 asset after a new `wikidata_snapshot_complete` materialization. Trigger
the ClickHouse asset only after the matching S3 manifest is complete.

The two materializations share the Wikidata partition date and source-run identity,
but Wikipedia failures do not invalidate or roll back the structured Wikidata
snapshot. A failed ClickHouse run reuses S3 and does not call Wikipedia again.

Treat outcomes as follows:

- timeout, connection failure, 429, and 5xx: retry with bounded backoff;
- normalized redirect: follow and retain the resolved page/revision identity;
- missing page after a valid sitelink: record a terminal missing-page outcome and
  metric, but do not emit a ClickHouse article row;
- malformed response, invalid host, checksum mismatch, or incomplete manifest: fail
  the materialization;
- ClickHouse validation failure: leave the canonical table unchanged.

## 8. Consumer contract

Every country reaches the table through its existing company QID. Country artifacts
do not copy Wikipedia columns or rows.

```sql
SELECT
    language_code,
    article_title,
    article_url,
    wikipedia_revision_id,
    article_lead_text,
    article_text
FROM corpscout.wikidata_company_wikipedia_articles FINAL
WHERE wikidata_id = 'Q1421630'
ORDER BY
    indexOf(['sv', 'en'], language_code) = 0,
    indexOf(['sv', 'en'], language_code),
    language_code;
```

The example preference is consumer-specific. Swedish and English receive no special
treatment in the source pipeline or schema.

## 9. Verification and rollout

Automated gates:

```bash
uv run pytest tests/test_wikidata_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```

Tests must cover:

- all-language sitelink parsing and rejection of non-Wikipedia Wikimedia projects;
- the `Q1421630` English and Swedish fixtures;
- page/revision/license parsing and redirect behavior;
- HTML-to-lead/full-text normalization;
- S3 multi-record batching, checksums, manifest-last publication, and retry reuse;
- missing and malformed page responses;
- bulk ClickHouse inserts containing multiple rows and respecting the byte threshold;
- uniqueness, count validation, staging cleanup, and atomic replacement;
- migration/schema/insertion-column parity;
- asset dependencies and automation handoff;
- successful `dg check defs` loading.

Live rollout order:

1. Apply the forward ClickHouse migration.
2. Deploy the Wikipedia component and its two assets.
3. Materialize the S3 asset for the latest completed Wikidata snapshot.
4. Inspect its manifest counts, object sizes, failures, and language distribution.
5. Materialize the ClickHouse asset and confirm it used block inserts.
6. Verify `Q1421630` has the expected `enwiki` and `svwiki` rows with non-empty text,
   positive revision IDs, and license metadata.

Suggested live check:

```sql
SELECT
    wikidata_id,
    site_id,
    language_code,
    article_title,
    wikipedia_revision_id,
    lengthUTF8(article_lead_text) AS lead_characters,
    lengthUTF8(article_text) AS article_characters,
    source_run_id
FROM corpscout.wikidata_company_wikipedia_articles FINAL
WHERE wikidata_id = 'Q1421630'
ORDER BY site_id;
```

Materialization metadata should report QID count, article count, languages, compressed
S3 bytes, raw and normalized byte counts, retries, terminal missing pages, ClickHouse
block count, and final row count.

## 10. Out of scope

This change does not add embeddings, chunks, vector indexes, Wikipedia media files,
language-specific columns, country-specific copies, a rendered-HTML ClickHouse
column, or a ClickHouse revision-history table. Those are separate consumer or search
concerns and should be designed only when required.
