# Translator Service — Plan 01: ClickHouse Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `corpscout.text_translations` cache table and the `corpscout.norway_companies_translated` join view to ClickHouse so later plans can write/read term-level translations without touching the wiped-and-replaced `companies` table.

**Architecture:** Two forward-only golang-migrate migrations under `corpscout/clickhouse/migrations`. The table is a `ReplacingMergeTree(version)` keyed by `(source_slug, field, source_text_hash)`. The view exposes the 3 free-text `_en` columns by `LEFT JOIN`ing `companies` to `text_translations` on `cityHash64(<original_col>)`, while passing every other base column (including the 4 reference `_en` columns) through unchanged. No application code in this plan — it is verified by migration-contract tests that grep the migration SQL.

**Tech Stack:** ClickHouse SQL (golang-migrate), Python `pytest` (the dagster_v3 test suite).

## Global Constraints

- Migrations live in `corpscout/clickhouse/migrations/` as `NNNNNN_name.up.sql` + `.down.sql` (golang-migrate). Highest existing = `000055_corpscout_br_rfb_contact_domains`; new ones are `000056` and `000057`.
- Every migration name MUST be appended to `EXPECTED_MIGRATIONS` in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`.
- Only the `corpscout` database; never the old `reference` DB.
- `ORDER BY` cannot contain `Nullable` columns (`allow_nullable_key` is off) — sort keys stay non-nullable.
- The ledger is **forward-only**: never rewind/renumber an existing migration.
- v1 hashing = **identity (raw text)**: `cityHash64(<original_col>)` — no normalization function (matches the queue's exact-text dedup; avoids three-way hash drift).
- The 3 **free-text** fields this service owns: `articles_purpose` → `articles_purpose_original`/`articles_purpose_en`; `activity_text` → `activity_text_original`/`activity_text_en`; `company_description` → `company_description_original`/`company_description_en`. The 4 reference `_en` columns (`legal_form_description_en`, `nace1_description_en`, `nace2_description_en`, `nace3_description_en`) are **out of scope** and pass through the view untouched.
- Run tests with `uv run pytest` from `corpscout/dagster_v3/`.
- Commit by explicit path (the working tree carries unrelated WIP; never `git add -A`).

---

## File Structure

| File | Responsibility |
|------|----------------|
| `corpscout/clickhouse/migrations/000056_corpscout_text_translations.up.sql` (create) | Create `corpscout.text_translations` |
| `corpscout/clickhouse/migrations/000056_corpscout_text_translations.down.sql` (create) | Drop it |
| `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.up.sql` (create) | Create the join view |
| `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.down.sql` (create) | Drop the view |
| `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (modify) | Append both names to `EXPECTED_MIGRATIONS` |
| `corpscout/dagster_v3/tests/test_text_translations_schema.py` (create) | Contract tests that grep the two migration files for required structure |

---

## Task 1: `text_translations` cache table (migration 000056)

**Files:**
- Create: `corpscout/clickhouse/migrations/000056_corpscout_text_translations.up.sql`
- Create: `corpscout/clickhouse/migrations/000056_corpscout_text_translations.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (append to `EXPECTED_MIGRATIONS`)
- Test: `corpscout/dagster_v3/tests/test_text_translations_schema.py`

**Interfaces:**
- Produces: a ClickHouse table `corpscout.text_translations` with columns `source_slug, field, source_text_hash (UInt64), source_lang, target_lang, translated_text (String), provider, model, version (UInt64)`, engine `ReplacingMergeTree(version)`, `ORDER BY (source_slug, field, source_text_hash)`. Plan 02's flush inserts into this table; the view in Task 2 reads it.

- [ ] **Step 1: Write the failing contract test**

Create `corpscout/dagster_v3/tests/test_text_translations_schema.py`:

```python
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"

TEXT_TRANSLATIONS_UP = MIGRATIONS_DIR / "000056_corpscout_text_translations.up.sql"
TEXT_TRANSLATIONS_DOWN = MIGRATIONS_DIR / "000056_corpscout_text_translations.down.sql"

REQUIRED_COLUMNS = (
    "source_slug",
    "field",
    "source_text_hash",
    "source_lang",
    "target_lang",
    "translated_text",
    "provider",
    "model",
    "version",
)


def test_text_translations_up_defines_required_columns():
    sql = TEXT_TRANSLATIONS_UP.read_text(encoding="utf-8")
    for column in REQUIRED_COLUMNS:
        assert column in sql, f"missing column {column!r} in text_translations migration"


def test_text_translations_up_uses_replacing_merge_tree_keyed_on_hash():
    sql = TEXT_TRANSLATIONS_UP.read_text(encoding="utf-8")
    assert "corpscout.text_translations" in sql
    assert "ReplacingMergeTree(version)" in sql
    assert "ORDER BY (source_slug, field, source_text_hash)" in sql
    assert "source_text_hash  UInt64" in sql or "source_text_hash UInt64" in sql


def test_text_translations_down_drops_table():
    sql = TEXT_TRANSLATIONS_DOWN.read_text(encoding="utf-8")
    assert "DROP TABLE IF EXISTS corpscout.text_translations" in sql
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_text_translations_schema.py -q`
Expected: FAIL — `FileNotFoundError` (the `000056_*.up.sql` file does not exist yet).

- [ ] **Step 3: Create the up migration**

Create `corpscout/clickhouse/migrations/000056_corpscout_text_translations.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS corpscout.text_translations
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
```

- [ ] **Step 4: Create the down migration**

Create `corpscout/clickhouse/migrations/000056_corpscout_text_translations.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.text_translations;
```

- [ ] **Step 5: Append the migration name to `EXPECTED_MIGRATIONS`**

In `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`, find the last entry in the `EXPECTED_MIGRATIONS` tuple (`"000055_corpscout_br_rfb_contact_domains",`) and add the new line immediately after it:

```python
    "000055_corpscout_br_rfb_contact_domains",
    "000056_corpscout_text_translations",
```

- [ ] **Step 6: Run both tests to verify they pass**

Run: `uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q`
Expected: PASS (the new schema tests pass; the existing migrations-vs-`EXPECTED_MIGRATIONS` test still passes because the `000056` files now exist on disk and the name is listed).

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000056_corpscout_text_translations.up.sql \
        corpscout/clickhouse/migrations/000056_corpscout_text_translations.down.sql \
        corpscout/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/dagster_v3/tests/test_text_translations_schema.py
git commit -m "feat(clickhouse): add corpscout.text_translations cache table (000056)"
```

---

## Task 2: `norway_companies_translated` join view (migration 000057)

**Files:**
- Create: `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.up.sql`
- Create: `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (append to `EXPECTED_MIGRATIONS`)
- Test: `corpscout/dagster_v3/tests/test_text_translations_schema.py` (add view assertions)

**Interfaces:**
- Consumes: `corpscout.companies` (existing, still has all 7 `_en` columns at this point) and `corpscout.text_translations` (Task 1).
- Produces: view `corpscout.norway_companies_translated` exposing the 3 free-text `_en` columns from `text_translations` (via `cityHash64(<original_col>)` join, `ifNull(..., '')`), with all other base columns — including the 4 reference `_en` columns — passed through. Plan 02 tests and downstream consumers read this view.

- [ ] **Step 1: Add the failing view contract test**

Append to `corpscout/dagster_v3/tests/test_text_translations_schema.py`:

```python
VIEW_UP = MIGRATIONS_DIR / "000057_corpscout_norway_companies_translated_view.up.sql"
VIEW_DOWN = MIGRATIONS_DIR / "000057_corpscout_norway_companies_translated_view.down.sql"

FREE_TEXT_FIELDS = (
    ("articles_purpose", "articles_purpose_original", "articles_purpose_en"),
    ("activity_text", "activity_text_original", "activity_text_en"),
    ("company_description", "company_description_original", "company_description_en"),
)


def test_view_up_excludes_free_text_en_and_rejoins_them():
    sql = VIEW_UP.read_text(encoding="utf-8")
    assert "corpscout.norway_companies_translated" in sql
    assert "corpscout.companies" in sql
    assert "corpscout.text_translations" in sql
    # The 3 free-text base _en columns are excluded then re-supplied from the cache.
    for _field, _original, en_col in FREE_TEXT_FIELDS:
        assert en_col in sql, f"view must reference {en_col}"
    assert "EXCEPT (articles_purpose_en, activity_text_en, company_description_en)" in sql
    # Each field joins on the raw-text cityHash64 and selects argMax over version.
    for field, original, _en in FREE_TEXT_FIELDS:
        assert f"field = '{field}'" in sql, f"view must filter field {field!r}"
        assert f"cityHash64(c.{original})" in sql, f"view must join on cityHash64(c.{original})"
    assert "argMax(translated_text, version)" in sql


def test_view_up_passes_through_reference_en_columns():
    sql = VIEW_UP.read_text(encoding="utf-8")
    # Reference _en columns must NOT be in the EXCEPT list (they pass through from base).
    except_clause = sql.split("EXCEPT (", 1)[1].split(")", 1)[0]
    for reference_en in (
        "legal_form_description_en",
        "nace1_description_en",
        "nace2_description_en",
        "nace3_description_en",
    ):
        assert reference_en not in except_clause, (
            f"{reference_en} is reference data and must pass through, not be excluded"
        )


def test_view_down_drops_view():
    sql = VIEW_DOWN.read_text(encoding="utf-8")
    assert "DROP VIEW IF EXISTS corpscout.norway_companies_translated" in sql
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_text_translations_schema.py -q`
Expected: FAIL — `FileNotFoundError` for `000057_*.up.sql`.

- [ ] **Step 3: Create the view up migration**

Create `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.up.sql`:

```sql
CREATE OR REPLACE VIEW corpscout.norway_companies_translated AS
SELECT
    c.* EXCEPT (articles_purpose_en, activity_text_en, company_description_en),
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en
FROM corpscout.companies AS c
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
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original);
```

- [ ] **Step 4: Create the view down migration**

Create `corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.down.sql`:

```sql
DROP VIEW IF EXISTS corpscout.norway_companies_translated;
```

- [ ] **Step 5: Append the migration name to `EXPECTED_MIGRATIONS`**

In `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`, add the new line immediately after the `000056` entry:

```python
    "000056_corpscout_text_translations",
    "000057_corpscout_norway_companies_translated_view",
```

- [ ] **Step 6: Run both tests to verify they pass**

Run: `uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.up.sql \
        corpscout/clickhouse/migrations/000057_corpscout_norway_companies_translated_view.down.sql \
        corpscout/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/dagster_v3/tests/test_text_translations_schema.py
git commit -m "feat(clickhouse): add norway_companies_translated join view (000057)"
```

---

## Optional Task 3: Live migration smoke (only if a local ClickHouse is available)

This is a manual verification, not a committed test (the suite must not require a live ClickHouse).

- [ ] Apply the two migrations against the local ClickHouse (golang-migrate or `\i` the up files via `clickhouse-client`), then run:

```sql
-- table exists and is empty
SELECT count(*) FROM corpscout.text_translations;   -- expect 0

-- seed one term and confirm the view surfaces it
INSERT INTO corpscout.text_translations
    (source_slug, field, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)
SELECT 'norway_brreg', 'company_description',
       cityHash64((SELECT company_description_original FROM corpscout.companies
                   WHERE company_description_original <> '' LIMIT 1)),
       'no', 'en', 'TEST TRANSLATION', 'manual', 'manual', 1;

SELECT company_description_original, company_description_en
FROM corpscout.norway_companies_translated
WHERE company_description_en = 'TEST TRANSLATION'
LIMIT 1;   -- expect exactly the seeded row
```

- [ ] Roll back with the `.down.sql` files and confirm the view + table are gone. (Leave the migrations applied if this is the real environment.)

---

## Self-Review

**Spec coverage (this plan's slice):** `text_translations` table (Decision 6) ✓ Task 1; join view + free-text-only `_en` (Decisions 6, 9 + scoping note) ✓ Task 2; raw-text `cityHash64` (normalize-identity note) ✓ Task 2 join; reference `_en` pass-through (scoping note) ✓ Task 2 test. Dropping the base `_en` columns and the export-column change are **Plan 03** (deliberately deferred — the `* EXCEPT` in the view lets this plan ship while the base columns still exist).

**Placeholder scan:** none — every step has full SQL/Python and exact run commands.

**Type consistency:** column names (`source_text_hash`, `version`, `translated_text`) and field strings (`articles_purpose`, `activity_text`, `company_description`) are identical across Task 1, Task 2, and the tests, and match `tables.py` (`*_original`/`*_en`) verbatim.
