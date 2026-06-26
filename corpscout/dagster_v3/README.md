# dagster_v3

Company open-data pipelines (ingest per country → DuckDB → ClickHouse). See `CLAUDE.md` for the
authoring standard and day-to-day gotchas.

---

# Translation

Free-text company fields (Norwegian articles-of-purpose, activity text, company descriptions, …) are
translated to English by a **standalone Temporal worker** (`translator/`), completely decoupled from the
Dagster ingestion graph. A translation failure can never block ingestion: Dagster just fires the workflow
and forgets it; the worker fills English columns in ClickHouse asynchronously.

## Architecture at a glance

```
Dagster (ingestion)                         translator worker (this package)
────────────────────                        ─────────────────────────────────
ingest source                               Temporal worker, task queue "translation-local-llm"
   │
export corpscout.companies   (no _en)
   │
fire Temporal workflow ────────────────────►  TranslateSourceWorkflow(source_slug)
  (fire-and-forget, USE_EXISTING)                1. scan ClickHouse for untranslated terms
   │                                             2. seed a per-source DuckDB queue (term-level dedup)
   ▼                                             3. loop: claim batch → LLM translate → save results
asset completes immediately                      4. flush results → corpscout.text_translations
                                                       │
queries read the view  ◄───────────────────────────────┘
corpscout.norway_companies_translated  =  companies LEFT JOIN text_translations (hash join)
```

The English values live in a separate `corpscout.text_translations` cache table, so they **survive** the
wipe-and-replace that every ClickHouse export does. A re-export only creates work for genuinely new or
changed source text.

## 1. Running the worker

The worker is a long-running process registered on the Temporal task queue `translation-local-llm`.

```bash
# from corpscout/dagster_v3/
uv run translator-worker
```

It **auto-loads a `.env` file** from the current directory (so plain `uv run translator-worker`
from `corpscout/dagster_v3/` just works), then connects to Temporal at `$TEMPORAL_ADDRESS`
(default `companycollect:7233`), registers `TranslateSourceWorkflow` plus its activities, and
waits for work. Stop it with Ctrl-C.

- `--env-file PATH` points at a different env file.
- Real environment variables always take precedence over the `.env` file (so `docker -e` / shell
  exports win).

In a container, provide the env the same way — mount/copy a `.env` next to the worker (it is
auto-loaded), or pass `--env-file` / `-e` to `docker run`. The worker is self-contained; it does
not depend on any external orchestrator to inject its environment.

### Required environment

| Variable | Purpose | Default |
|----------|---------|---------|
| `TEMPORAL_ADDRESS` | Temporal frontend | `companycollect:7233` |
| `CLICKHOUSE_HOST` | ClickHouse host | `companycollect` (compose) |
| `CLICKHOUSE_HTTP_PORT` | ClickHouse **HTTP** port (clickhouse-connect uses HTTP, not the native 9002) | `8123` |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DATABASE` | ClickHouse creds | `default` / `change-me` / `corpscout` (compose) |
| `CLICKHOUSE_SECURE` | TLS to ClickHouse | `false` |
| `TRANSLATION_PROVIDER_LOCAL_BASE_URL` | OpenAI-compatible LLM endpoint | — (required) |
| `TRANSLATION_PROVIDER_LOCAL_MODEL` | model name | — (required) |
| `TRANSLATION_PROVIDER_LOCAL_API_KEY` | API key (local servers ignore it) | `not-needed` |

### How a run is started

Dagster triggers it. After a source's companies export lands in ClickHouse, the Dagster asset
`norway_brreg_translation_trigger` calls `client.start_workflow(...)` with a **fixed workflow id**
(`translate-norway_brreg`) and `WorkflowIDConflictPolicy.USE_EXISTING`, then returns immediately
(fire-and-forget — it never awaits the result). The fixed id means there is at most one translation run
per source in flight; a second trigger while one is running attaches to the existing run instead of
starting a duplicate.

To start a run by hand (e.g. to backfill), materialize the trigger asset in Dagster, or start the workflow
directly with a Temporal client against task queue `translation-local-llm` passing a
`TranslateSourceWorkflowInput` (see `translator/workflow.py`).

## 2. Specifying sources to translate

Which columns get translated is a **static, in-code registry** — `translator/registry.py`. It is the source
of truth for *which* columns are translatable (the ClickHouse base tables no longer carry the free-text
`_en` columns, so the list cannot be inferred from the schema). To add a source, add a `SourceConfig`:

```python
REGISTRY: dict[str, SourceConfig] = {
    "norway_brreg": SourceConfig(
        source_slug="norway_brreg",          # identifier; also the workflow id suffix
        source_lang="no",                    # source language passed to the LLM
        ch_table="corpscout.companies",      # ClickHouse table holding the *_original columns
        fields=(
            FieldConfig(field="articles_purpose",    original_col="articles_purpose_original"),
            FieldConfig(field="activity_text",       original_col="activity_text_original"),
            FieldConfig(field="company_description",  original_col="company_description_original"),
        ),
    ),
    # add another source here ...
}
```

- `field` is the logical name stored in `text_translations.field` and joined by the view.
- `original_col` is the column on `ch_table` holding the native-language text to translate.

Adding a new source also needs: a `corpscout.text_translations` entry is automatic (same table for all
sources), a join **view** for that source (mirror `corpscout.norway_companies_translated`), a Dagster
trigger asset, and dropping that source's free-text `_en` columns from its base table. See the
`norway_brreg` migrations `000056`–`000058` and the trigger asset as the template.

## 3. How the translation queue is built

When the workflow starts, the `scan_and_seed` activity:

1. **Scans ClickHouse** for untranslated terms. For each registered field it runs (see
   `translator/clickhouse.py:build_scan_sql`):

   ```sql
   SELECT DISTINCT c.<original_col> AS source_text
   FROM <ch_table> AS c
   LEFT JOIN (
       SELECT source_text_hash
       FROM corpscout.text_translations
       WHERE source_slug = {slug:String} AND field = {field:String}
       GROUP BY source_text_hash
   ) AS t ON t.source_text_hash = cityHash64(c.<original_col>)
   WHERE c.<original_col> <> '' AND t.source_text_hash IS NULL
   ```

   `SELECT DISTINCT` + the `cityHash64` anti-join means only **distinct, not-yet-translated** strings come
   back — identical text shared by thousands of companies is translated once.

2. **Seeds a per-source DuckDB queue** at `data/translator/<source_slug>.duckdb` (`translator/queue.py`).
   The queue is term-level and deduplicated: each item's id is `sha256(source_text | target_language)`, so
   re-seeding is idempotent. Tables: `translation_items` (status: pending / leased / completed /
   failed_retryable), `translation_locations` (which field a term belongs to), `translation_results`, and
   `translation_batch_attempts`. The queue file persists between runs, so a new run resumes leftover
   `failed_retryable` items.

## 4. How translation tasks are sent to the LLM

The workflow loops, calling the `process_translation_batch` activity until the queue drains:

- **Claim** a batch of pending (or `failed_retryable`) items — default `batch_size = 50`.
- Send them to the **OpenAI-compatible** provider (`translator/smoke.py:LocalOpenAICompatibleTranslationProvider`)
  via `chat.completions.create` against `TRANSLATION_PROVIDER_LOCAL_BASE_URL` / `_MODEL`. The request is
  JSON-in / JSON-out (a list of `{item_id, source_text}` → `{item_id, translated_text}`), `temperature = 0`,
  with a configurable `max_tokens` and `extra_body` (e.g. `{"chat_template_kwargs": {"enable_thinking": false}}`).

Failure handling is built for an unreliable LLM: a failed batch marks its items `failed_retryable` and the
workflow keeps going; it **completes** (rather than hard-failing) after tolerating up to `max_batch_failures`
failed batches (production default `20`), and the leftovers are retried on the next trigger. No completion
sensor, no held DuckDB pool slot — so a bad LLM can never wedge ingestion.

## 5. How responses are processed

For each batch the provider returns, `complete_batch` (in `translator/queue.py`):

- maps the provider's per-item ids back to queue item ids,
- writes the translated text into `translation_results`,
- flips the items to `completed` and records a successful `translation_batch_attempts` row.

A batch that raises instead calls `fail_batch`, incrementing `attempt_count` and setting status
`failed_retryable` with the error category/message.

## 6. How results are stored back in ClickHouse

After the queue drains, the `flush` activity writes completed results to `corpscout.text_translations`
(`translator/flush.py`). Crucially, **the Python side never computes the hash** — it inserts the raw
`(field, source_text, translated_text)` rows into a uniquely-named `Memory` staging table, then:

```sql
INSERT INTO corpscout.text_translations
    (source_slug, field, source_text_hash, source_lang, target_lang,
     translated_text, provider, model, version)
SELECT
    {slug:String}, field, cityHash64(source_text), {lang:String}, 'en',
    translated_text, {provider:String}, {model:String}, {version:UInt64}
FROM <staging>;
```

ClickHouse computes `cityHash64(source_text)` on **both** write (here) and read (the view), so there is
exactly one definition of the join key and no cross-language hash drift. The staging table is dropped in a
`finally`. Empty translations are skipped.

`text_translations` is a `ReplacingMergeTree(version)` keyed by `(source_slug, field, source_text_hash)` —
re-translating the same term just replaces the row (latest `version` wins).

```sql
corpscout.text_translations
    source_slug       LowCardinality(String)   -- 'norway_brreg'
    field             LowCardinality(String)   -- 'company_description'
    source_text_hash  UInt64                    -- cityHash64(original text)
    source_lang       LowCardinality(String)   -- 'no'
    target_lang       LowCardinality(String)   -- 'en'
    translated_text   String
    provider          LowCardinality(String)
    model             LowCardinality(String)
    version           UInt64                    -- ReplacingMergeTree version
```

## 7. Seeing translations in ClickHouse

Read the **join view**, which stitches each source's base table to the cache by hashing the original text.
For Norway: `corpscout.norway_companies_translated` exposes the `companies` columns plus the translated
`articles_purpose_en`, `activity_text_en`, `company_description_en` (empty string where not yet translated):

```sql
-- a company with its English description
SELECT org_number, company_description_original, company_description_en
FROM corpscout.norway_companies_translated
WHERE company_description_en <> ''
LIMIT 10;
```

Inspect the cache directly:

```sql
-- how much is translated, per field
SELECT field, count() AS translated_terms
FROM corpscout.text_translations
WHERE source_slug = 'norway_brreg'
GROUP BY field;

-- look up the translation of one specific source string
SELECT field, translated_text, provider, model
FROM corpscout.text_translations
WHERE source_slug = 'norway_brreg'
  AND source_text_hash = cityHash64('Holdingselskap');
```

Schema is owned by the golang-migrate migrations in `corpscout/clickhouse/migrations/`:
`000056` (the `text_translations` table), `000057` (the Norway view), `000058` (drops the free-text `_en`
columns from `companies` so the view supplies them).

## Tests

```bash
uv run pytest tests/ -q -k translator   # registry, scan, flush, workflow, worker
uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q
```
