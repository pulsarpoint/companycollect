# text_translations table+column keying — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-key `corpscout.text_translations` on the physical `(source_table, source_column)` instead of the abstract `(source_slug, field)`, making the cache self-describing with no in-code mapping.

**Architecture:** Keep the cache+view shape (translation stored once; `_en` computed on read). Replace the cache's two key columns and rewire the translator's scan/flush/registry/workflow/import and the view to the physical table+column. Spec: `corpscout/docs/superpowers/specs/2026-06-27-text-translations-table-column-keying-design.md`.

**Tech Stack:** Python 3.14 (`uv`), ClickHouse (golang-migrate, clickhouse-connect HTTP), Temporal, DuckDB, pytest.

## Global Constraints

- `source_table` is the **fully-qualified** table, e.g. `corpscout.no_companies` (= `SourceConfig.ch_table`).
- `source_column` is the **exact literal column name** — no suffix logic (`articles_purpose_original`, etc.).
- `source_slug` survives only as the registry/Temporal-workflow key — **never** a column in `text_translations`.
- Keep the view; do **not** materialize `_en` onto `no_companies`; do **not** rename `no_companies` base columns.
- `cityHash64(<text>)` is computed **only in ClickHouse**, identically on write (flush) and read (view).
- Every migration `up` starts with `CREATE DATABASE IF NOT EXISTS corpscout;`.
- `ORDER BY` columns must be non-nullable (`source_table`/`source_column` are `LowCardinality(String)`).
- All commands run with `uv run` from `corpscout/dagster_v3/`. Validate with `uv run dg check defs`.
- Commit by **explicit `git add` paths** — NEVER `git add -A` (the tree carries a large unrelated scheduler/ui/compose removal).
- The 2.1M legacy import already ran on the OLD schema; migration `000063` drops+recreates the table, so it is re-imported afterward (rollout, not part of these tasks).

## File Structure

- `corpscout/clickhouse/migrations/000063_corpscout_text_translations_table_column.{up,down}.sql` — **create**: reshape table + re-point view.
- `corpscout/dagster_v3/translator/registry.py` — drop `FieldConfig.field`.
- `corpscout/dagster_v3/translator/clickhouse.py` — `ScannedTerm.source_column`; scan SQL/params on `source_table`/`source_column`.
- `corpscout/dagster_v3/translator/flush.py` — write `source_table`/`source_column`.
- `corpscout/dagster_v3/translator/queue.py` — `FlushTranslationRow.source_column`; `completed_results_for_flush`.
- `corpscout/dagster_v3/translator/workflow.py` — `field_by_col`; static flush + queue-item via `source_column`.
- `corpscout/dagster_v3/translator/import_legacy.py` — classify/import by `source_column`.
- Tests: `test_translator_registry.py`, `test_translator_scan.py`, `test_translator_flush.py`, `test_translator_workflow.py`, `test_translator_import_legacy.py`, `test_text_translations_schema.py`, `test_clickhouse_migrations.py`.
- Docs: `corpscout/dagster_v3/README.md`, `corpscout/dagster_v3/docs/data-source-guidelines.md`, `corpscout/dagster_v3/docs/source-design-doc-template.md`.

Three tasks: **Task 1** = migration + SQL-contract tests (SQL only; Python untouched; suite green). **Task 2** = the Python re-key (atomic — `FieldConfig`/`ScannedTerm`/`FlushTranslationRow` are shared types, so all consumers + their unit tests change together; suite green). **Task 3** = docs.

---

### Task 1: Migration 000063 — reshape table + re-point view

**Files:**
- Create: `corpscout/clickhouse/migrations/000063_corpscout_text_translations_table_column.up.sql`
- Create: `corpscout/clickhouse/migrations/000063_corpscout_text_translations_table_column.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_text_translations_schema.py` (append a 000063 contract block)
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`)

**Interfaces:**
- Produces: `corpscout.text_translations(source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)` and a `no_companies_translated` view filtering `source_table='corpscout.no_companies' AND source_column='<col>_original'`. Task 2's flush/scan/view-contract align to these names.

- [ ] **Step 1: Write the failing contract tests** — append to `corpscout/dagster_v3/tests/test_text_translations_schema.py`:

```python
TABLE_COLUMN_UP = (
    MIGRATIONS_DIR / "000063_corpscout_text_translations_table_column.up.sql"
)
TABLE_COLUMN_DOWN = (
    MIGRATIONS_DIR / "000063_corpscout_text_translations_table_column.down.sql"
)

TABLE_COLUMN_FIELDS = (
    ("articles_purpose_original", "articles_purpose_en"),
    ("activity_text_original", "activity_text_en"),
    ("company_description_original", "company_description_en"),
    ("legal_form_description_original", "legal_form_description_en"),
)


def test_000063_up_recreates_table_keyed_on_table_and_column():
    sql = TABLE_COLUMN_UP.read_text(encoding="utf-8")
    assert "CREATE DATABASE IF NOT EXISTS corpscout;" in sql
    assert "DROP TABLE IF EXISTS corpscout.text_translations" in sql
    assert "source_table      LowCardinality(String)" in sql
    assert "source_column     LowCardinality(String)" in sql
    # Old key columns are gone from the new table definition.
    assert "source_slug" not in sql
    assert "ReplacingMergeTree(version)" in sql
    assert "ORDER BY (source_table, source_column, source_text_hash)" in sql


def test_000063_up_repoints_view_to_table_and_column():
    sql = TABLE_COLUMN_UP.read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW corpscout.no_companies_translated" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "EXCEPT (" not in sql
    for original, en_col in TABLE_COLUMN_FIELDS:
        assert f"source_table = 'corpscout.no_companies' AND source_column = '{original}'" in sql
        assert f"cityHash64(c.{original})" in sql
        assert en_col in sql
    assert "argMax(translated_text, version)" in sql
    # The old key column must not appear in the new view's WHERE clauses.
    assert "field = '" not in sql


def test_000063_down_restores_slug_field_schema_and_view():
    sql = TABLE_COLUMN_DOWN.read_text(encoding="utf-8")
    assert "DROP TABLE IF EXISTS corpscout.text_translations" in sql
    assert "ORDER BY (source_slug, field, source_text_hash)" in sql
    assert "CREATE OR REPLACE VIEW corpscout.no_companies_translated" in sql
    for field in ("articles_purpose", "activity_text", "company_description", "legal_form_description"):
        assert f"field = '{field}'" in sql
```

- [ ] **Step 2: Run them to verify they fail** — `uv run pytest tests/test_text_translations_schema.py -k 000063 -v` → FAIL (files missing).

- [ ] **Step 3: Write the up migration** — `000063_corpscout_text_translations_table_column.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.text_translations;

CREATE TABLE corpscout.text_translations
(
    source_table      LowCardinality(String),
    source_column     LowCardinality(String),
    source_text_hash  UInt64,
    source_lang       LowCardinality(String),
    target_lang       LowCardinality(String),
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash);

CREATE OR REPLACE VIEW corpscout.no_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en,
    ifNull(lf.translated_text, '') AS legal_form_description_en
FROM corpscout.no_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'articles_purpose_original'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'company_description_original'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'legal_form_description_original'
    GROUP BY source_text_hash
) AS lf ON lf.source_text_hash = cityHash64(c.legal_form_description_original);
```

- [ ] **Step 4: Write the down migration** — `000063_corpscout_text_translations_table_column.down.sql` (restores the `(source_slug, field)` table + the 000062-form view):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.text_translations;

CREATE TABLE corpscout.text_translations
(
    source_slug       LowCardinality(String),
    field             LowCardinality(String),
    source_text_hash  UInt64,
    source_lang       LowCardinality(String),
    target_lang       LowCardinality(String),
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_slug, field, source_text_hash);

CREATE OR REPLACE VIEW corpscout.no_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en,
    ifNull(lf.translated_text, '') AS legal_form_description_en
FROM corpscout.no_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'articles_purpose'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'activity_text'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'company_description'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'legal_form_description'
    GROUP BY source_text_hash
) AS lf ON lf.source_text_hash = cityHash64(c.legal_form_description_original);
```

- [ ] **Step 5: Add the migration to `EXPECTED_MIGRATIONS`** — in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`, append after `"000062_corpscout_no_companies_legal_form_via_cache",`:

```python
    "000063_corpscout_text_translations_table_column",
```

- [ ] **Step 6: Run the migration tests** — `uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q` → PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000063_corpscout_text_translations_table_column.up.sql \
        corpscout/clickhouse/migrations/000063_corpscout_text_translations_table_column.down.sql \
        corpscout/dagster_v3/tests/test_text_translations_schema.py \
        corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): re-key text_translations on source_table+source_column (000063)"
```

---

### Task 2: Python re-key — registry / scan / flush / queue / workflow / import

This is one atomic change: `FieldConfig.field`, `ScannedTerm.field`, and `FlushTranslationRow.field` are shared types consumed across modules, so all producers/consumers and their unit tests move together to keep the suite green.

**Files:**
- Modify: `corpscout/dagster_v3/translator/registry.py`
- Modify: `corpscout/dagster_v3/translator/clickhouse.py`
- Modify: `corpscout/dagster_v3/translator/flush.py`
- Modify: `corpscout/dagster_v3/translator/queue.py`
- Modify: `corpscout/dagster_v3/translator/workflow.py`
- Modify: `corpscout/dagster_v3/translator/import_legacy.py`
- Test: `test_translator_registry.py`, `test_translator_scan.py`, `test_translator_flush.py`, `test_translator_workflow.py`, `test_translator_import_legacy.py`

**Interfaces:**
- Consumes: the 000063 table/view names from Task 1.
- Produces: `ScannedTerm(source_column, source_text, static_key)`, `FlushTranslationRow(source_column, source_text, translated_text)`, `FieldConfig(original_col, static_map=None, static_key_col=None)`. Flush writes `(source_table, source_column, …)`.

- [ ] **Step 1: Update the unit tests to the new shape (they will fail first).**

`test_translator_registry.py` — replace `test_norway_brreg_config_has_four_fields_three_dynamic_one_static` body assertions:
```python
def test_norway_brreg_config_has_four_fields_three_dynamic_one_static():
    config = get_source_config("norway_brreg")
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.no_companies"
    assert len(config.fields) == 4

    dynamic_fields = config.fields[:3]
    assert dynamic_fields[0] == FieldConfig(original_col="articles_purpose_original")
    assert dynamic_fields[1] == FieldConfig(original_col="activity_text_original")
    assert dynamic_fields[2] == FieldConfig(original_col="company_description_original")
    for f in dynamic_fields:
        assert f.static_map is None
        assert f.static_key_col is None

    lf = config.fields[3]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None
    assert len(lf.static_map) == len(LEGAL_FORM_DESCRIPTION_EN_BY_CODE)
    assert lf.static_map_dict() == LEGAL_FORM_DESCRIPTION_EN_BY_CODE
```

`test_translator_scan.py` — change the SQL assertions and `ScannedTerm` construction. Replace the slug/field assertions:
```python
def test_build_scan_sql_selects_distinct_untranslated_terms():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[2])  # company_description (dynamic)
    assert "SELECT DISTINCT c.company_description_original AS source_text" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "corpscout.text_translations" in sql
    assert "source_table = {table:String}" in sql
    assert "source_column = {column:String}" in sql
    assert "cityHash64(c.company_description_original)" in sql
    assert "c.company_description_original <> ''" in sql
    assert "t.source_text_hash IS NULL" in sql
    assert "static_key" not in sql
```
and in `test_scanned_term_is_frozen_dataclass` change keyword `field=` → `source_column=`:
```python
def test_scanned_term_is_frozen_dataclass():
    t = ScannedTerm(source_column="legal_form_description_original", source_text="Aksjeselskap", static_key="AS")
    assert t.source_column == "legal_form_description_original"
    assert t.source_text == "Aksjeselskap"
    assert t.static_key == "AS"
    t_dynamic = ScannedTerm(source_column="company_description_original", source_text="Holding", static_key=None)
    assert t_dynamic.static_key is None
```
(`test_build_scan_sql_per_field_uses_its_original_column`, `test_build_scan_sql_for_legal_form_description_includes_key_column`, and the two `static_map_resolution` tests need no change — they already reference `original_col`/`static_map_dict`.)

`test_translator_flush.py`:
```python
def test_build_flush_select_sql_computes_hash_in_clickhouse():
    sql = build_flush_select_sql("corpscout.stage_abc")
    assert "INSERT INTO corpscout.text_translations" in sql
    assert "(source_table, source_column, source_text_hash" in sql
    assert "cityHash64(source_text)" in sql
    assert "FROM corpscout.stage_abc" in sql
    assert "{table:String}" in sql and "{lang:String}" in sql and "{version:UInt64}" in sql


def test_flush_skips_empty_and_writes_rows():
    client = _FakeClient()
    config = get_source_config("norway_brreg")
    rows = [
        FlushTranslationRow("company_description_original", "Holdingselskap", "Holding company"),
        FlushTranslationRow("activity_text_original", "Tomt", ""),  # empty -> skipped
    ]
    written = flush_translations(
        client, config, rows, provider="prov", model="model", version=123, run_id="run-1"
    )
    assert written == 1
    assert any("CREATE TABLE" in c and "ENGINE = Memory" in c for c in client.commands)
    assert client.inserts and client.inserts[0][1] == [["company_description_original", "Holdingselskap", "Holding company"]]
    assert any("INSERT INTO corpscout.text_translations" in c for c in client.commands)
    assert any(c.startswith("DROP TABLE") for c in client.commands)
```

`test_translator_workflow.py` — change every `ScannedTerm(...)` first positional arg to the `*_original` column name:
- `ScannedTerm("company_description", ...)` → `ScannedTerm("company_description_original", ...)`
- `ScannedTerm("activity_text", ...)` → `ScannedTerm("activity_text_original", ...)`
- `ScannedTerm("legal_form_description", "Aksjeselskap", "AS")` → `ScannedTerm("legal_form_description_original", "Aksjeselskap", "AS")`
(There are 3 such constructions: two in the first test, two in the failure test, one in the static test. Assertions on counts/translations stay the same.)

`test_translator_import_legacy.py` — the synthetic queue already uses `*_original` `source_field` values; update the imported-field assertions to the column names and the `.source_column` attribute:
```python
    imported_fields = {r.source_column for r in call["rows"]}
    assert imported_fields == {"company_description_original", "articles_purpose_original"}
    assert "legal_form_description_original" not in imported_fields, "static field must be skipped"
    assert "bogus_field" not in imported_fields, "unknown field must be skipped"
```

- [ ] **Step 2: Run the updated tests to verify they fail** — `uv run pytest tests/test_translator_registry.py tests/test_translator_scan.py tests/test_translator_flush.py tests/test_translator_workflow.py tests/test_translator_import_legacy.py -q` → FAIL (code still uses `field`).

- [ ] **Step 3: `registry.py` — drop `field` from `FieldConfig`.** Replace the `FieldConfig` class and `REGISTRY`:

```python
@dataclass(frozen=True)
class FieldConfig:
    original_col: str
    # Frozen tuple-of-pairs keeps FieldConfig hashable; convert to dict at use via static_map_dict().
    static_map: tuple[tuple[str, str], ...] | None = None
    static_key_col: str | None = None

    def static_map_dict(self) -> dict[str, str] | None:
        if self.static_map is None:
            return None
        return dict(self.static_map)


@dataclass(frozen=True)
class SourceConfig:
    source_slug: str
    source_lang: str
    ch_table: str
    fields: tuple[FieldConfig, ...]


REGISTRY: dict[str, SourceConfig] = {
    "norway_brreg": SourceConfig(
        source_slug="norway_brreg",
        source_lang="no",
        ch_table="corpscout.no_companies",
        fields=(
            FieldConfig(original_col="articles_purpose_original"),
            FieldConfig(original_col="activity_text_original"),
            FieldConfig(original_col="company_description_original"),
            FieldConfig(
                original_col="legal_form_description_original",
                static_map=tuple(LEGAL_FORM_DESCRIPTION_EN_BY_CODE.items()),
                static_key_col="legal_form_code",
            ),
        ),
    ),
}
```

- [ ] **Step 4: `clickhouse.py` — `ScannedTerm.source_column` + scan on `source_table`/`source_column`.** Replace `ScannedTerm`, `build_scan_sql`, and `scan_untranslated_terms`:

```python
@dataclass(frozen=True)
class ScannedTerm:
    """A single untranslated term discovered during a scan.

    ``static_key`` is None for dynamic fields; for static fields it holds the
    companion key-column value (e.g. ``legal_form_code``).
    """

    source_column: str
    source_text: str
    static_key: str | None


def build_scan_sql(source_config: SourceConfig, field: FieldConfig) -> str:
    original = field.original_col
    if field.static_key_col:
        select_cols = f"c.{original} AS source_text, c.{field.static_key_col} AS static_key"
    else:
        select_cols = f"c.{original} AS source_text"
    return (
        f"SELECT DISTINCT {select_cols}\n"
        f"FROM {source_config.ch_table} AS c\n"
        f"LEFT JOIN (\n"
        f"    SELECT source_text_hash\n"
        f"    FROM corpscout.text_translations\n"
        f"    WHERE source_table = {{table:String}} AND source_column = {{column:String}}\n"
        f"    GROUP BY source_text_hash\n"
        f") AS t ON t.source_text_hash = cityHash64(c.{original})\n"
        f"WHERE c.{original} <> '' AND t.source_text_hash IS NULL"
    )


def scan_untranslated_terms(client: Any, source_config: SourceConfig) -> list[ScannedTerm]:
    terms: list[ScannedTerm] = []
    for field in source_config.fields:
        result = client.query(
            build_scan_sql(source_config, field),
            parameters={"table": source_config.ch_table, "column": field.original_col},
        )
        for row in result.result_rows:
            source_text = row[0]
            static_key = row[1] if field.static_key_col else None
            terms.append(
                ScannedTerm(source_column=field.original_col, source_text=source_text, static_key=static_key)
            )
    return terms
```

- [ ] **Step 5: `queue.py` — `FlushTranslationRow.source_column`.** Replace the `FlushTranslationRow` dataclass:

```python
@dataclass(frozen=True)
class FlushTranslationRow:
    source_column: str
    source_text: str
    translated_text: str
```
and in `completed_results_for_flush` change the return comprehension:
```python
        return [
            FlushTranslationRow(source_column=row[0], source_text=row[1], translated_text=row[2])
            for row in rows
        ]
```
(The SQL there already selects `l.source_field` as `row[0]`; the DuckDB queue stores the column name in `source_field`, so it maps straight to `source_column`. Leave the `source_field` DuckDB column and the rest of `queue.py` unchanged.)

- [ ] **Step 6: `flush.py` — write `source_table`/`source_column`.** Replace `build_flush_select_sql` and `flush_translations`:

```python
def build_flush_select_sql(staging_table: str) -> str:
    return (
        "INSERT INTO corpscout.text_translations\n"
        "    (source_table, source_column, source_text_hash, source_lang, target_lang,\n"
        "     translated_text, provider, model, version)\n"
        "SELECT\n"
        "    {table:String}, source_column, cityHash64(source_text), {lang:String}, 'en',\n"
        "    translated_text, {provider:String}, {model:String}, {version:UInt64}\n"
        f"FROM {staging_table}"
    )


def flush_translations(
    client: Any,
    source_config: SourceConfig,
    rows: list[FlushTranslationRow],
    *,
    provider: str,
    model: str,
    version: int,
    run_id: str,
) -> int:
    data = [
        [row.source_column, row.source_text, row.translated_text]
        for row in rows
        if row.translated_text != ""
    ]
    if not data:
        return 0

    staging = _staging_table_name(run_id)
    client.command(
        f"CREATE TABLE IF NOT EXISTS {staging} "
        "(source_column String, source_text String, translated_text String) ENGINE = Memory"
    )
    try:
        client.insert(staging, data, column_names=["source_column", "source_text", "translated_text"])
        client.command(
            build_flush_select_sql(staging),
            parameters={
                "table": source_config.ch_table,
                "lang": source_config.source_lang,
                "provider": provider,
                "model": model,
                "version": version,
            },
        )
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")
    return len(data)
```

- [ ] **Step 7: `workflow.py` — `field_by_col` + `source_column`.** In `scan_and_seed_once` replace `field_by_name` and the loops:

```python
    source_config = get_source_config(params.source_slug)
    field_by_col = {f.original_col: f for f in source_config.fields}

    client = clickhouse_client_from_env()
    static_flushed = 0
    try:
        terms = scan_untranslated_terms(client, source_config)

        static_terms = []
        dynamic_terms = []
        for term in terms:
            fc = field_by_col.get(term.source_column)
            if fc is not None and fc.static_map is not None:
                static_terms.append(term)
            else:
                dynamic_terms.append(term)

        if static_terms:
            static_rows: list[FlushTranslationRow] = []
            for term in static_terms:
                fc = field_by_col[term.source_column]
                translation = (fc.static_map_dict() or {}).get(term.static_key or "", "")
                if translation:
                    static_rows.append(
                        FlushTranslationRow(
                            source_column=term.source_column,
                            source_text=term.source_text,
                            translated_text=translation,
                        )
                    )
            if static_rows:
                static_flushed = flush_translations(
                    client,
                    source_config,
                    static_rows,
                    provider="static",
                    model="static",
                    version=int(time.time()),
                    run_id=params.queue_duckdb_path,
                )
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
```
and in the queue-seeding list, change `source_field=term.field` → `source_field=term.source_column`:
```python
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table=source_config.ch_table,
            source_pk="",
            source_field=term.source_column,
            source_text=term.source_text,
            target_language="en",
        )
        for term in dynamic_terms
    ]
```

- [ ] **Step 8: `import_legacy.py` — classify/import by column.** Replace the classification block (the `dynamic_fields`/`dynamic_key_to_field`/`static_keys` setup and the `for row in all_rows` loop) with:

```python
    # The DuckDB queue records each location's source_field as the *_original column name, which is
    # exactly the cache's source_column. Classify by the registry's columns; no remap needed.
    dynamic_columns = {f.original_col for f in config.fields if f.static_map is None}
    static_columns = {f.original_col for f in config.fields if f.static_map is not None}

    queue = TranslationQueue(duckdb_path)
    queue.initialize()
    all_rows = queue.completed_results_for_flush()
    total_in_queue = len(all_rows)

    import_rows: list[FlushTranslationRow] = []
    imported_per_field: dict[str, int] = {}
    skipped_static: dict[str, int] = {}
    skipped_unknown: dict[str, int] = {}

    for row in all_rows:
        col = row.source_column
        if col in dynamic_columns:
            if row.translated_text:  # drop empty translations (flush does this too, but count accurately)
                import_rows.append(row)
                imported_per_field[col] = imported_per_field.get(col, 0) + 1
        elif col in static_columns:
            skipped_static[col] = skipped_static.get(col, 0) + 1
        else:
            skipped_unknown[col] = skipped_unknown.get(col, 0) + 1
```
and change the summary line that printed `Dynamic fields` to use the columns:
```python
    print(f"  Dynamic columns               : {sorted(dynamic_columns)}")
```
(Keep the rest of `import_legacy.py` — argparse, batched flush, summary prints — unchanged.)

- [ ] **Step 9: Run the translator unit tests** — `uv run pytest tests/test_translator_registry.py tests/test_translator_scan.py tests/test_translator_flush.py tests/test_translator_workflow.py tests/test_translator_import_legacy.py -q` → PASS.

- [ ] **Step 10: Guard against residual references** — `rg -n "source_slug|\bfield\b" corpscout/dagster_v3/translator/clickhouse.py corpscout/dagster_v3/translator/flush.py` returns no `text_translations` column usage of `source_slug`/`field` (the only remaining `source_slug` is `SourceConfig.source_slug` in registry/workflow as the processing key). Then full suite + dg check: `uv run pytest -q` (all pass) and `uv run dg check defs` (passes).

- [ ] **Step 11: Commit**

```bash
git add corpscout/dagster_v3/translator/registry.py \
        corpscout/dagster_v3/translator/clickhouse.py \
        corpscout/dagster_v3/translator/flush.py \
        corpscout/dagster_v3/translator/queue.py \
        corpscout/dagster_v3/translator/workflow.py \
        corpscout/dagster_v3/translator/import_legacy.py \
        corpscout/dagster_v3/tests/test_translator_registry.py \
        corpscout/dagster_v3/tests/test_translator_scan.py \
        corpscout/dagster_v3/tests/test_translator_flush.py \
        corpscout/dagster_v3/tests/test_translator_workflow.py \
        corpscout/dagster_v3/tests/test_translator_import_legacy.py
git commit -m "feat(translator): re-key cache writes on source_table+source_column"
```

---

### Task 3: Docs — README + guidelines + template

**Files:**
- Modify: `corpscout/dagster_v3/README.md`
- Modify: `corpscout/dagster_v3/docs/data-source-guidelines.md`
- Modify: `corpscout/dagster_v3/docs/source-design-doc-template.md`

- [ ] **Step 1: README `# Translation`.** Update the `text_translations` schema block, the flush SQL block, the view description, and the monitoring queries from `source_slug`/`field` to `source_table`/`source_column`. Specifically:
  - The schema table: replace the `source_slug` / `field` rows with `source_table LowCardinality(String) -- 'corpscout.no_companies'` and `source_column LowCardinality(String) -- 'company_description_original'`.
  - §5 flush SQL: `(source_table, source_column, source_text_hash, …) SELECT {table:String}, source_column, cityHash64(source_text), …`.
  - §6 monitoring queries: `WHERE source_table='corpscout.no_companies'` and `GROUP BY source_column` (replace the `source_slug`/`field` filters).
  - The registry snippet/notes in §2: `FieldConfig(original_col=…)` (no `field`); note the cache row is self-describing (`source_table`/`source_column`).
  - Migration list in §6: add `000063`.

- [ ] **Step 2: `data-source-guidelines.md` §8.** Update the cache description: "English values live in the shared `corpscout.text_translations` cache, **keyed by `(source_table, source_column, source_text_hash)`** — a cache row names its exact table and column." Update the registry bullet to `FieldConfig(original_col=…)` (no `field`).

- [ ] **Step 3: `source-design-doc-template.md` §6.** In the translation table, change the column header note to reflect that the cache stores `source_table`/`source_column`; the `field` column reference becomes `source_column` (the original column name).

- [ ] **Step 4: Verify no stale references** — `rg -n "source_slug|text_translations.field|field = '\\w" corpscout/dagster_v3/README.md corpscout/dagster_v3/docs/data-source-guidelines.md corpscout/dagster_v3/docs/source-design-doc-template.md` shows no remaining `text_translations` `source_slug`/`field` usage (registry/workflow `source_slug` mentions as the processing key are fine).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/README.md \
        corpscout/dagster_v3/docs/data-source-guidelines.md \
        corpscout/dagster_v3/docs/source-design-doc-template.md
git commit -m "docs(translator): text_translations keyed by source_table+source_column"
```

---

## Rollout (after all tasks merge)

1. Deploy `main` + `make clickhouse-migrate-up` on the server (applies `000063`; the table is empty/recreated — safe).
2. Re-run the legacy import (now writing `source_table`/`source_column`):
   `uv run translator-import-legacy-queue --duckdb data/norway_brreg_translation_queue.duckdb --source norway_brreg`
3. Start the worker / fire the trigger; verify `corpscout.no_companies_translated` returns `_en` values.

## Self-Review

- **Spec coverage:** schema (Task 1), view re-point (Task 1), registry/scan/flush/queue/workflow/import (Task 2), rename procedure + docs (Task 3), rollout/import (rollout section). The DISTINCT-before-flush idea is intentionally **out of scope** (per decision: the one-shot import already ran; harmless duplication collapsed by ReplacingMergeTree).
- **Placeholders:** none — every code/SQL step is complete.
- **Type consistency:** `source_column` used uniformly across `ScannedTerm`, `FlushTranslationRow`, scan params (`{column:String}`), flush staging column, and the view filter; `source_table` = `ch_table` everywhere; `FieldConfig` has no `field` in any task.
