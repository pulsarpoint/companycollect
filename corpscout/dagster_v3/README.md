# dagster_v3

Company open-data pipelines (ingest per country → DuckDB → ClickHouse). See `CLAUDE.md` for the
authoring standard and day-to-day gotchas.

---

# Translation

Free-text company fields (Norwegian articles-of-purpose, activity text, company descriptions, …) are
translated to English by a **standalone Temporal worker** (`translator/`), completely decoupled from the
Dagster ingestion graph. A translation failure can never block ingestion: Dagster just fires the workflow
and forgets it; the worker fills English columns in ClickHouse asynchronously.

Two resolution paths per field:
- **LLM** — free-text fields (descriptions, purpose, activity) go to an OpenAI-compatible model.
- **Static dict** — closed enumerated fields (e.g. legal-form descriptions) are resolved from an
  authoritative in-code mapping, never sent to the LLM.

Both land in the same cache and are exposed through the same view.

## Architecture at a glance

```
Dagster (ingestion)                          translator worker (this package)
────────────────────                         ─────────────────────────────────
ingest source                                Temporal worker, task queue "translation-local-llm"
   │
norway_resolved → corpscout.no_companies      TranslateSourceWorkflow(source_slug):
   (resolved table, *_original text)            1. scan ClickHouse for untranslated terms (per field)
   │                                            2a. static fields → resolve from dict, flush directly
fire Temporal workflow ────────────────────►   2b. dynamic fields → seed per-source DuckDB queue
  (fire-and-forget, USE_EXISTING)               3.  loop: claim batch → LLM translate → save
   │                                            4.  flush results → corpscout.text_translations
   ▼                                                  │
asset completes immediately                          │
queries read the view  ◄──────────────────────────────┘
corpscout.no_companies_translated = no_companies LEFT JOIN text_translations (cityHash64 join)
```

The English values live in a separate `corpscout.text_translations` cache table, so they **survive** the
wipe-and-replace that every ClickHouse export does (`no_companies` is rebuilt by dbt + `EXCHANGE TABLES`
each run). A re-export only creates work for genuinely new or changed source text.

## 1. Running the worker

A long-running process registered on the Temporal task queue `translation-local-llm`.

```bash
# from corpscout/dagster_v3/
uv run translator-worker
```

It **auto-loads a `.env` file** from the current directory (so plain `uv run translator-worker` from
`corpscout/dagster_v3/` just works), then connects to Temporal at `$TEMPORAL_ADDRESS`
(default `companycollect:7233`), registers `TranslateSourceWorkflow` plus its activities, and waits for work.
Stop it with Ctrl-C.

- `--env-file PATH` points at a different env file. Real environment variables always take precedence over
  the `.env` file (so `docker -e` / shell exports win).
- In a container, mount/copy a `.env` next to the worker (auto-loaded) or pass `--env-file` / `-e` to
  `docker run`. The worker is self-contained — no orchestrator injects its environment.

### Required environment

| Variable | Purpose | Default |
|----------|---------|---------|
| `TEMPORAL_ADDRESS` | Temporal frontend | `companycollect:7233` |
| `CLICKHOUSE_HOST` | ClickHouse host | `companycollect` |
| `CLICKHOUSE_HTTP_PORT` | ClickHouse **HTTP** port (clickhouse-connect uses HTTP, not the native 9002) | `8123` |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DATABASE` | ClickHouse creds | `default` / `change-me` / `corpscout` |
| `CLICKHOUSE_SECURE` | TLS to ClickHouse | `false` |
| `TRANSLATION_PROVIDER_LOCAL_BASE_URL` | OpenAI-compatible LLM endpoint | — (required) |
| `TRANSLATION_PROVIDER_LOCAL_MODEL` | model name | — (required) |
| `TRANSLATION_PROVIDER_LOCAL_API_KEY` | API key (local servers ignore it) | `not-needed` |

### How a run is started

Dagster triggers it. After `norway_resolved_clickhouse` lands `corpscout.no_companies`, the Dagster asset
`norway_brreg_translation_trigger` calls `client.start_workflow(...)` with a **fixed workflow id**
(`translate-norway_brreg`) and `WorkflowIDConflictPolicy.USE_EXISTING`, then returns immediately
(fire-and-forget — never awaits the result). The fixed id means at most one translation run per source is in
flight; a second trigger while one is running attaches to the existing run instead of starting a duplicate.

To start a run by hand, materialize the trigger asset in Dagster, or run `norway_brreg_refresh_job` (its
`.upstream()` selection drives entities → resolved → `no_companies` export → trigger).

## 2. Specifying sources to translate

Which columns get translated is a **static, in-code registry** — `translator/registry.py`. It is the source
of truth for *which* columns are translatable (the base tables no longer carry the free-text `_en` columns,
so the list can't be inferred from the schema). To add a source, add a `SourceConfig`:

```python
REGISTRY: dict[str, SourceConfig] = {
    "norway_brreg": SourceConfig(
        source_slug="norway_brreg",          # identifier; also the workflow id suffix
        source_lang="no",                    # source language passed to the LLM / stored in the cache
        ch_table="corpscout.no_companies",   # ClickHouse table holding the *_original columns
        fields=(
            # dynamic (LLM) free-text fields:
            FieldConfig(original_col="articles_purpose_original"),
            FieldConfig(original_col="activity_text_original"),
            FieldConfig(original_col="company_description_original"),
            # static (dict) reference field — resolved from a code→EN map, no LLM:
            FieldConfig(
                original_col="legal_form_description_original",
                static_map=LEGAL_FORM_DESCRIPTION_EN_BY_CODE,   # from translator/static_maps.py
                static_key_col="legal_form_code",              # the column whose value keys the dict
            ),
        ),
    ),
    # add another source here ...
}
```

- `original_col` — the column holding the native-language text; the cache row stores this as `source_column` and keys on `cityHash64` of the value. The cache row is self-describing via `source_table`/`source_column`.
- `static_map` / `static_key_col` — set both to make a field **static**: instead of the LLM, the value is
  `static_map[row[static_key_col]]`. The cache row is still keyed by `cityHash64(original_col)`, so the same
  view serves it. Unknown keys resolve to nothing (left untranslated, never re-sent). Static maps live in
  `translator/static_maps.py` (copied from the source's authoritative mapping — keep the translator
  self-contained, don't import from `dagster_v3.defs.*`).

Adding a new source also needs: a join **view** for that source (mirror `corpscout.no_companies_translated`),
a Dagster trigger asset wired downstream of that source's ClickHouse export, the `*_original` columns present
on the base table, and `text_translations` (shared, already exists). See the `norway_brreg` migrations and
trigger asset as the template.

## 3. How the queue is built (dynamic fields) / resolved (static fields)

When the workflow starts, the `scan_and_seed` activity scans ClickHouse for untranslated terms per field
(`translator/clickhouse.py:build_scan_sql`):

```sql
-- dynamic field:
SELECT DISTINCT c.<original_col> AS source_text
FROM corpscout.no_companies AS c
LEFT JOIN ( SELECT source_text_hash FROM corpscout.text_translations
            WHERE source_table = {table:String} AND source_column = {column:String}
            GROUP BY source_text_hash ) AS t
       ON t.source_text_hash = cityHash64(c.<original_col>)
WHERE c.<original_col> <> '' AND t.source_text_hash IS NULL
-- static field additionally selects the key column:  c.<static_key_col> AS static_key
```

`SELECT DISTINCT` + the `cityHash64` anti-join returns only **distinct, not-yet-translated** strings, so a
string shared by thousands of companies is handled once.

- **Static fields** are resolved immediately (`static_map[static_key]`) and flushed straight to
  `text_translations` with `provider='static'` — they never touch the LLM queue.
- **Dynamic fields** seed a per-source DuckDB queue at `data/translator/<source_slug>.duckdb`
  (`translator/queue.py`). Term-level and deduplicated (item id = `sha256(source_text | target_language)`),
  so re-seeding is idempotent. Tables: `translation_items` / `translation_locations` / `translation_results`
  / `translation_batch_attempts`. The queue persists between runs, so a new run resumes `failed_retryable`
  items.

## 4. How dynamic terms are sent to the LLM

The workflow loops, calling `process_translation_batch` until the queue drains:

- **Claim** a batch of pending (or `failed_retryable`) items — default `batch_size = 50`.
- Send them to the **OpenAI-compatible** provider (`translator/smoke.py`) via `chat.completions.create`
  against `TRANSLATION_PROVIDER_LOCAL_BASE_URL` / `_MODEL`. JSON-in / JSON-out, `temperature = 0`,
  `max_tokens = 8192` (sized so a 50-item batch's combined output can't truncate), and a configurable
  `extra_body` (e.g. `{"chat_template_kwargs": {"enable_thinking": false}}`).

Failure handling tolerates an unreliable LLM: a failed batch marks its items `failed_retryable` and the
workflow keeps going; it **completes** (rather than hard-failing) after up to `max_batch_failures` failed
batches (default `20`), leftovers retried on the next trigger. No completion sensor, no held DuckDB pool slot
— a bad LLM can never wedge ingestion.

## 5. How results are stored back in ClickHouse

Completed dynamic results (and static results) are written to `corpscout.text_translations`
(`translator/flush.py`). **The Python side never computes the hash** — it stages the raw
`(source_column, source_text, translated_text)` rows in a `Memory` table, then lets ClickHouse compute the hash:

```sql
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang,
     translated_text, provider, model, version)
SELECT {table:String}, source_column, cityHash64(source_text), {lang:String}, 'en',
       translated_text, {provider:String}, {model:String}, {version:UInt64}
FROM <staging>;
```

ClickHouse computes `cityHash64(source_text)` on **both** write and read (the view), so there is one
definition of the join key — no cross-language hash drift. Empty translations are skipped. `provider` records
the origin: the LLM model name, `'static'` (dict), or `'legacy-import'` (see §7).

`text_translations` is a `ReplacingMergeTree(version)` keyed by `(source_table, source_column, source_text_hash)` —
re-translating the same term just replaces the row (latest `version` wins):

```sql
corpscout.text_translations
    source_table      LowCardinality(String)   -- 'corpscout.no_companies'
    source_column     LowCardinality(String)   -- 'company_description_original'
    source_text_hash  UInt64                    -- cityHash64(original text)
    source_lang       LowCardinality(String)   -- 'no'
    target_lang       LowCardinality(String)   -- 'en'
    translated_text   String
    provider          LowCardinality(String)   -- model name | 'static' | 'legacy-import'
    model             LowCardinality(String)
    version           UInt64                    -- ReplacingMergeTree version
```

## 6. Seeing translations in ClickHouse

Read the **join view** `corpscout.no_companies_translated` — it stitches `no_companies` to the cache by
hashing the original text, exposing the resolved columns plus `articles_purpose_en`, `activity_text_en`,
`company_description_en`, and `legal_form_description_en` (empty where not yet translated):

```sql
SELECT org_number, company_description_original, company_description_en, legal_form_description_en
FROM corpscout.no_companies_translated
WHERE company_description_en <> ''
LIMIT 10;
```

Inspect / monitor the cache directly:

```sql
-- coverage per column (incl. provider: model / static / legacy-import)
SELECT source_column, provider, count() AS terms
FROM corpscout.text_translations
WHERE source_table = 'corpscout.no_companies'
GROUP BY source_column, provider;

-- look up one source string
SELECT source_column, translated_text, provider, model
FROM corpscout.text_translations
WHERE source_table = 'corpscout.no_companies' AND source_text_hash = cityHash64('Holdingselskap');
```

Schema is owned by golang-migrate migrations in `corpscout/clickhouse/migrations/`:
`000056` (`text_translations` table), `000059`–`000062` (the `no_companies` `*_original` columns + the
`no_companies_translated` view; `000061` dropped the legacy raw `corpscout.companies`/`financial_statements`),
and `000069` (re-keyed `text_translations` to `(source_table, source_column, source_text_hash)`).

## 7. Reusing already-translated terms (legacy import)

A previous translation run may have left completed translations in an old DuckDB queue. To avoid
re-translating them, import them into `text_translations` once (run on the host that has the file +
ClickHouse access):

```bash
uv run translator-import-legacy-queue \
  --duckdb /path/to/norway_brreg_translation_queue.duckdb \
  --source norway_brreg          # --dry-run to preview counts without writing
```

It reads the old queue's completed results and flushes them to `text_translations` with
`provider='legacy-import'`, **filtered to the source's dynamic (LLM) fields** — static fields (resolved from
the dict) and unknown fields are skipped and reported. After import, the scan's anti-join skips those terms.

## 8. Operational runbook — translating an existing table

1. **Deploy + migrate:** `make clickhouse-migrate-up` (applies `000056`–`000062` and `000069`).
2. **Rebuild the resolved table** so `*_original` is populated — materialize `norway_resolved` (the dbt
   models + `norway_resolved_clickhouse`). The `*_original` columns are new; existing rows are empty until a
   rebuild. *(Skip this and the scan finds nothing to translate.)*
3. *(Optional)* **Import legacy translations** (§7) to reuse prior work.
4. **Start the worker** (`uv run translator-worker`) — it must be up and able to reach the LLM endpoint.
5. **Fire the trigger** — materialize `norway_brreg_translation_trigger`, or run `norway_brreg_refresh_job`.
   The first run resolves static fields instantly and translates whatever the import didn't cover; subsequent
   runs only handle new/changed text.
6. **Read** `corpscout.no_companies_translated` for companies-with-`_en`.

## Tests

```bash
uv run pytest tests/ -q -k translator     # registry, scan, flush, static map, workflow, worker, import
uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q
```
