# dagster_v3

Company open-data pipelines (ingest per country → DuckDB → ClickHouse). See `CLAUDE.md` for the
authoring standard and day-to-day gotchas.

The current host-mode development deployment is managed by
[`ansible/`](ansible/README.md). It rsyncs the local source tree to `dagster` while
preserving remote runtime state and supervises `dg dev` with systemd.

Source-specific architecture is documented beside each source package. For the
Wikidata company pipeline, including exchange and national-registry discovery,
snapshot orchestration, ClickHouse tables, and the current Wikipedia boundary, see
[`src/dagster_v3/defs/wikidata/docs/wikidata-design.md`](src/dagster_v3/defs/wikidata/docs/wikidata-design.md).

---

# Translation

Free-text company fields (Norwegian articles-of-purpose, activity text, Latvian activity
descriptions, ...) are translated to English by the **standalone Go translator service**
(`corpscout/translator` — see its README for the full API contract), completely decoupled from the
Dagster ingestion graph. A translation failure can never block ingestion: a Dagster **loader asset**
scans for untranslated text, POSTs it to the service, and returns; the service fills the shared
ClickHouse cache asynchronously.

```
loader asset (this repo)  →  Go translator service (corpscout/translator)  →  corpscout.text_translations
```

Two resolution paths per field:
- **LLM** — free-text fields (descriptions, purpose, activity) are enqueued to the service, which
  drives an OpenAI-compatible model through a single Temporal workflow (`translator/process`).
- **Static dict** — closed enumerated fields (e.g. legal-form descriptions) are resolved from an
  authoritative in-loader mapping and inserted **directly** into `text_translations` with
  `provider='static'`; they never touch the LLM or the queue.

Both land in the same cache and are exposed through the same per-source `<source>_translated` join
view. The cache is a `ReplacingMergeTree(version)` keyed by
`(source_table, source_column, source_text_hash)`, so it **survives** the wipe-and-replace that
every ClickHouse export does — a re-export only creates work for genuinely new or changed text.

## The loader pattern

The explicit translator HTTP resource lives in
`src/dagster_v3/defs/translator_load/resource.py`; ClickHouse scan/static-insert
helpers live in `translator_load/loader.py`. Each source defines its own translation asset in
`defs/<source>/translation.py`
(`norway_brreg_translation_load`, `latvia_ur_translation_load`), each downstream of that source's
ClickHouse publish asset. The asset body visibly performs the scan, API enqueue, and optional
static insert. The contract is:

1. **Anti-join scan** — `SELECT DISTINCT` untranslated texts for one `(table, column)` by
   LEFT ANTI JOIN against `corpscout.text_translations`. **Loaders own dedup**: the service's queue
   upsert only prevents duplicates *within* the queue; the anti-join is what stops re-enqueueing
   already-translated text.
2. **Hash in SQL** — `cityHash64(column)` is computed in ClickHouse, never in Python, so hashes
   always agree with past runs and with the view's join key.
3. **Chunked POST** — at most 10,000 items per request to `POST /v1/queue/items`, with the language
   pair declared once per request and `source_text_hash` sent as a **decimal string** (uint64
   exceeds JSON's safe integer range). The first successful enqueue signal-with-starts the
   service's Temporal workflow; no separate trigger is needed.
4. **Completion gate** — after enqueueing, the asset polls `/v1/queue/stats` until the queue is
   fully drained and flushed. Any failed item, workflow-start warning, or completion timeout fails
   the Dagster step, so a green materialization means translation completed rather than merely
   being accepted by the API.
5. **Static maps direct-insert** — statically mapped columns (e.g. Norway legal forms) skip the
   queue: the loader joins the key column against an in-loader code→EN dict and inserts the rows
   straight into `text_translations` with `provider='static'`.

Table and column names are interpolated into the scan SQL without escaping — loader configs are
trusted, developer-authored code and must never be built from untrusted input.

## Adding a source

1. Ensure the base table carries `<field>_original` columns and a `<source>_translated` join view
   exists (mirror `corpscout.no_companies_translated`; migrations own the schema).
2. Add the source language constants, translated fields, and optional static map to
   `defs/<source>/translation.py`. The asset must accept `translator: TranslatorResource`, call
   `translator.enqueue_translation_rows(...)`, wait with
   `translator.wait_for_queue_completion()`, and be downstream of the source's ClickHouse publish
   asset.
3. Wire the loader into the source's full-refresh job selection if it should run per refresh
   (see `norway_brreg_entities_full_snapshot_job`).

The service needs no per-source configuration — it only ever sees
`(table, column, text, hash, lang pair)` tuples.

## Operations

- **Run the service**: `cd corpscout/translator && make run` (needs `CLICKHOUSE_*`,
  `TEMPORAL_ADDRESS`, and the `TRANSLATION_PROVIDER_LOCAL_*` variables; config in
  `config/translator.json`).
- **Watch progress**: `curl -s http://localhost:8080/v1/queue/stats` →
  `{"input": N, "pending": N, "output": N, "failed": N}`.
- **Manual kick**: `curl -s -X POST http://localhost:8080/v1/queue/process` (normally unnecessary —
  enqueue and boot-resume start the workflow automatically).
- **Environment**: the shared Dagster resource reads `TRANSLATOR_API_URL` (default
  `http://localhost:8080`).

## Monitoring the cache

```sql
-- coverage per column (incl. provider: model / static)
SELECT source_column, source_lang, target_lang, provider, count() AS terms
FROM corpscout.text_translations
WHERE source_table = 'corpscout.no_companies'
GROUP BY source_column, source_lang, target_lang, provider;

-- look up one source string
SELECT source_column, translated_text, provider, model
FROM corpscout.text_translations
WHERE source_table = 'corpscout.no_companies'
  AND source_text_hash = cityHash64('Holdingselskap')
  AND source_lang = 'no'
  AND target_lang = 'en';
```

Schema is owned by golang-migrate migrations in `corpscout/clickhouse/migrations/`:
`000056` (`text_translations` table), `000059`–`000062` (the `no_companies` `*_original` columns +
the `no_companies_translated` view), and `000069` (re-keyed `text_translations` to
physical table/column identity), plus `000252` (the complete multilingual key:
`(source_table, source_column, source_text_hash, source_lang, target_lang)`).

## Tests

```bash
uv run pytest tests/test_translator_load.py -q        # loader contract (scan SQL, chunking, static insert)
uv run pytest tests/test_text_translations_schema.py -q
```
