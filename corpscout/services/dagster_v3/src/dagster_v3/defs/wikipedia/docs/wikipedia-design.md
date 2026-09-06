# Company Wikipedia articles

## 1. Source and scope

This language-neutral enrichment starts from **all** QIDs in the completed
`corpscout.wikidata_companies` snapshot, including exchange and registry-number
seeds. Wikidata's `wbgetentities` sitelinks supply page identities; company-name
search is not used. MediaWiki's `/w/rest.php/v1/page/{title}/with_html` supplies
HTML, page ID, current revision ID/timestamp and license metadata without credentials.

The inline `WikipediaArticlesComponent` registers exactly two assets in the
`wikidata` group and one stopped-by-default sensor:

```text
wikidata_snapshot_complete
  -> wikidata_company_wikipedia_articles_s3
  -> wikidata_company_wikipedia_articles
```

The output is `corpscout.wikidata_company_wikipedia_articles`, owned by migration
`000380_corpscout_wikidata_company_wikipedia_articles`.

## 2. Ingest mode and snapshot identity

Both assets are non-partitioned, full-snapshot assets. Required config
`source_run_id: YYYY-MM-DD` identifies the completed **Wikidata** source date, not
the date Wikipedia last edited each page. Historical Wikipedia revisions are not
reconstructed: every new snapshot fetches the then-current article.

The initial run checks the `wikidata_snapshot_complete` event and freezes all company
QIDs from one ClickHouse SELECT. That SELECT must contain exactly the requested
`source_run_id`. The immutable company inventory lets retries continue even if the
structured Wikidata table has since advanced. A previously uncaptured old snapshot
cannot be reconstructed from the current serving table and fails explicitly.

No country/language filters or production `max_companies` option are exposed: a
limited smoke test must never be mistaken for a complete serving snapshot.

## 3. Raw loading and checkpoints

Raw HTML is valuable for replay, so the agreed design deliberately bypasses the
usual dlt-to-DuckDB stage. The existing `ObjectStoreResource` writes compressed raw
batches to RustFS/S3. The concrete Wikimedia client uses dlt's retrying HTTP helper;
no new dependency or generic client interface is introduced.

Bucket: `source-wikipedia-articles-weekly`. Snapshot prefix:
`partition_date=<date>/source_run_id=<date>/`.

- `companies.json`: immutable sorted QID inventory.
- `discovery/batch=NNNNNN.json.gz`: raw `wbgetentities` response for up to 50 QIDs.
- `part=NNNNNN-NNN.jsonl.gz`: raw article responses and terminal missing-page outcomes.
- `checkpoints/batch=NNNNNN.json`: target digest and completed object descriptors.
- `manifest.json`: completion marker, exact counts, language distribution and checksums.

Article work batches hold at most 25 targets; objects split around 16 MiB of
uncompressed JSONL. A single unusually large article may exceed that target; API
responses over 32 MiB fail. This is not a strict HTTP transfer-memory limit because
the retrying client reads each response completely. There is no object per article
by design (a final remainder or unusually large article can occupy one object).

Completed discovery and article checkpoints are reused without HTTP requests.
An interrupted, uncheckpointed batch can be fetched again; manifest-last publication
keeps it invisible to ClickHouse. Object SHA-256 values stay in S3, not in ClickHouse.

Requests are serial with a minimum 0.2-second delay, a default 60-second timeout,
a descriptive User-Agent, five HTTP attempts and Retry-After support. dlt retries
429/5xx/network failures including a failed non-streaming response read. Wikidata
maxlag/rate-limit error bodies are retried too. The S3 asset has three step retries.
Only a confirmed MediaWiki missing-title response is a terminal missing page;
generic 404s, other errors and malformed success responses fail the snapshot.

## 4. Text transformation

lxml normalizes the saved HTML into separate lead and full-text fields. The lead
contains introductory paragraphs, excluding infobox tables. Full text retains
headings, paragraphs, lists and table text while removing scripts, styles,
navigation, edit controls, reference markers/lists and media figures. It is a
plain-text rendition, not lossless rendered layout. Raw HTML remains available in S3.

## 5. ClickHouse schema and publication

Grain and sort key: `(wikidata_id, site_id)`. Engine:
`ReplacingMergeTree(resolved_at)`. All language editions use the same columns;
`language_code` comes from the Wikipedia hostname, preserving edition codes such
as `simple` and `be-tarask` rather than assuming every code is ISO 639-1.

The row includes title/URL, page/revision identity, revision timestamp, lead/full
text, `text/plain` format, license name/URL and retrieval/resolution provenance.
`source_record_id` is `<QID>:<site_id>`; `source_system` is `wikipedia`.
Consumers can attribute text through the article and revision URLs plus license.
No raw HTML or payload-hash columns are served.

Publication verifies inventory and object hashes, exact discovered-target coverage,
terminal outcomes, counts and uniqueness. Rows are inserted in blocks of up to
1,000 rows or roughly 16 MiB of full text into a unique staging table. An independent
ClickHouse count/uniqueness check precedes `EXCHANGE TABLES`; the old staging table
is then dropped. Failure before exchange leaves the serving table unchanged.
Older source dates cannot replace a newer nonempty table.

Both assets use pool `wikidata_wikipedia`, which **must remain limited to one**
(the instance default is one). This serializes same-snapshot checkpoint writes and
publication. Do not increase this pool or bypass it for production publications.
A zero-row snapshot is valid only if a nonempty complete company inventory produced
no Wikipedia sitelinks; an all-missing download is rejected.

## 6. Languages, contacts and currency

Article languages remain original; no translation jobs, embeddings, classification,
or per-language columns are added. This is the explicitly agreed two-asset scope.
Wikipedia article URLs are encyclopedia evidence, not official-company domains.
Existing Wikidata contact assets remain authoritative for that pipeline. Currency
conversion does not apply to unstructured encyclopedia text.

## 7. Automation and launch configuration

`wikidata_wikipedia_articles_sensor` watches `wikidata_snapshot_complete` and sends
its source date to both steps of `wikidata_wikipedia_articles_job`. It defaults to
STOPPED until the first full run is validated. This branch is deliberately not a
dependency of structured Wikidata completion.

To run manually, launch the job with the same completed date in both steps:

```yaml
ops:
  wikidata_company_wikipedia_articles_s3:
    config:
      source_run_id: '2026-07-20'
  wikidata_company_wikipedia_articles:
    config:
      source_run_id: '2026-07-20'
```

The date is an example: use the `source_run_id` shown on the completed Wikidata
materialization. To retry publication only, select the ClickHouse asset and pass
that same date; it reads S3 and makes no Wikipedia requests.

## 8. Operational limitations

The first run may download many language editions for tens of thousands of QIDs.
No revision-aware incremental cache is implemented yet. Frozen QIDs and sitelinks
are held in memory; article content is bounded by checkpoint/block sizes. Completed
snapshots retain raw objects without an automatic deletion policy.

The source snapshot pins coverage, not a single instant in Wikipedia's editing
timeline. Page revision IDs/timestamps describe the individually downloaded versions.

## 9. Issues found during implementation

A missing page uses `errorKey=rest-nonexistent-title`; treating every HTTP 404 as a
missing article would hide broken endpoints. Tests also load the project's `.env`
because unrelated Webtech component definitions require their environment values.
Deployment should preserve server source/dependency differences outside this feature.

## 10. Verification

```bash
uv run --env-file .env pytest tests/test_wikipedia_articles.py tests/test_wikidata_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```

Live checks: migration version and table shape, both registered assets, pool limit,
then a bounded `Q1421630` API/S3/ClickHouse smoke in an isolated test table/bucket.
Do not publish a one-company fixture to the production snapshot table. After a full
job succeeds, verify language counts and known enwiki/svwiki rows before starting
the sensor.
