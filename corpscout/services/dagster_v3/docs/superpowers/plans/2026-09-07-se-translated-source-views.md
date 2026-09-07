# SE Translated Source Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Key the Swedish activity-description translations on the register tables (`se_bolagsverket_companies`, `se_ratsit_company`), expose them through plain `_translated` views, and make the basic-info Bolagsverket and Ratsit extractors read those views instead of joining `text_translations` themselves.

**Architecture:** One additive ClickHouse migration copies the spine-keyed translation rows under the two register keys (preserving `version`) and creates `se_bolagsverket_companies_translated` and `se_ratsit_company_translated` (the Latvia view shape plus a `<column>_translated_at` stamp). The Sweden translation scan re-points to the Bolagsverket register and a new Ratsit scan joins the coverage job. The two extractors select `<column>_en` from the views and fold the stamp into `observed_at`, so a translation that lands later still re-selects the company. The spine key, `se_companies_serving` and `se_company/scb.py` are untouched until basic-info slice 5.

**Tech Stack:** ClickHouse (golang-migrate ledger, `x-multi-statement`), Python 3.14 / Dagster / pytest (`uv run pytest`), clickhouse-local (or docker) for the integration tests.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-07-se-translated-source-views-design.md`

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands run from `corpscout/services/dagster_v3` with `uv run`. Migration files live in `corpscout/clickhouse/migrations/`.
- **Branch:** work on `se-translated-source-views` off `main` (`git checkout -b se-translated-source-views` from a clean tree; the tree was committed on 2026-09-07 except two stray root PNGs, which stay untracked).
- **Definitions-loading tests** (`tests/test_translation_coverage.py`, `tests/test_sweden_financial_concepts.py`) need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment.
- **Integration tests** (`tests/test_se_company_basic_info_extractors_clickhouse_local.py`) need `clickhouse-local`, `clickhouse`, or a running docker. They self-skip otherwise; a task whose integration test skipped is not done until it has run green somewhere with docker.
- **Migration number:** `000390`. Prod `schema_migrations` head is 389 and 000388/000389 are on main as of commit `c23430d49`. Before merging, re-check both: `ls corpscout/clickhouse/migrations | tail -2` and `SELECT max(version) FROM corpscout.schema_migrations` on prod must still be 389.
- **Keys (verbatim, spec section "Translation keys"):** `('corpscout.se_bolagsverket_companies', 'activity_description')` and `('corpscout.se_ratsit_company', 'business_description')`, both `source_lang = 'sv'`, `target_lang = 'en'`. The spine key `('corpscout.se_companies', 'activity_description')` is never deleted, altered or renamed by this work.
- **View columns (verbatim):** `activity_description_en`, `activity_description_translated_at` on the Bolagsverket view; `business_description_en`, `business_description_translated_at` on the Ratsit view. `<column>_en` is `ifNull(translated_text, '')` (never NULL); readers test `<column>_en != ''`, never the stamp, to know a translation exists.
- **Extractor versions:** `bolagsverket-v3`, `ratsit-v2`.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```
- **Not in scope:** an SCB view, Ratsit `legal_form` mapping, deleting spine-key rows, `se_companies_serving`, `se_company/scb.py`, the backoffice.

---

## File map

| File | Responsibility |
|---|---|
| `corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.up.sql` (create) | Copy spine-key rows under the two register keys; create the two views. |
| `corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.down.sql` (create) | Drop the two views; rows stay. |
| `tests/test_clickhouse_migrations.py` (modify) | Register 000390 in the ledger list; pin its statements. |
| `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` (modify) | Extractor reads `se_bolagsverket_companies_translated`. |
| `src/dagster_v3/defs/se_company/basic_info/ratsit.py` (modify) | Extractor reads `se_ratsit_company_translated`. |
| `tests/test_se_company_basic_info_extractors_sql.py` (modify) | Text pins of both extractors. |
| `tests/test_se_company_basic_info_extractors_clickhouse_local.py` (modify) | Harness replays the views; behaviour tests on clickhouse-local. |
| `src/dagster_v3/defs/sweden_company/translation.py` (modify) | Scan keyed on `se_bolagsverket_companies`, dep on its export. |
| `tests/test_sweden_company_translation.py` (create) | Pins the Sweden field, scan SQL, dep and enqueue key. |
| `tests/test_sweden_financial_concepts.py` (modify) | Parent-key assertion follows the dep change. |
| `src/dagster_v3/defs/sweden_ratsit/translation.py` (create) | Ratsit scan asset, queue-health check, coverage check. |
| `src/dagster_v3/defs/sweden_ratsit/assets.py` (modify) | Registers the new asset and health check in the module's `Definitions`. |
| `src/dagster_v3/defs/czech_legal_forms/assets.py` (modify) | `TRANSLATION_LOAD_ASSETS` gains the Ratsit loader. |
| `tests/test_sweden_ratsit_translation.py` (create) | Pins the Ratsit field, scan/coverage scope, dep, enqueue key. |

---

### Task 1: Migration 000390 (copy keys, create views)

**Files:**
- Create: `corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.up.sql`
- Create: `corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (the `MIGRATIONS` tuple ends at line 404 with `"000389_corpscout_technology_proposals",`; append the new test at the end of the file)

**Interfaces:**
- Consumes: nothing.
- Produces: views `corpscout.se_bolagsverket_companies_translated` (columns of `se_bolagsverket_companies` plus `activity_description_en String`, `activity_description_translated_at DateTime64(3, 'UTC')`) and `corpscout.se_ratsit_company_translated` (columns of `se_ratsit_company` plus `business_description_en String`, `business_description_translated_at DateTime64(3, 'UTC')`). Tasks 2 and 3 read them; the local harness replays their `CREATE OR REPLACE VIEW` statements from this file.

- [ ] **Step 1: Write the failing migration test**

Append to `tests/test_clickhouse_migrations.py`:

```python
def test_se_source_translated_views_re_key_translations_and_define_views() -> None:
    """Spec 2026-09-07-se-translated-source-views: translations keyed on the register
    tables, exposed as plain views. Additive: the spine key stays until slice 5."""
    up = _normalize_sql("\n".join(_statement_lines(_migration_sql("000390_corpscout_se_source_translated_views.up.sql"))))
    down = _normalize_sql("\n".join(_statement_lines(_migration_sql("000390_corpscout_se_source_translated_views.down.sql"))))

    assert up.count("INSERT INTO corpscout.text_translations") == 2
    # Bolagsverket: every spine-key row under the register's name, version preserved so
    # the translation stamps (and the extractor's change scan) do not move.
    assert (
        "SELECT 'corpscout.se_bolagsverket_companies', source_column, source_text_hash, source_lang, target_lang, "
        "translated_text, provider, model, version FROM corpscout.text_translations "
        "WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'"
    ) in up
    # Ratsit: seeded only where a Ratsit text is the same text, so the LLM sees the rest only.
    assert (
        "SELECT 'corpscout.se_ratsit_company', 'business_description', source_text_hash, source_lang, target_lang, "
        "translated_text, provider, model, version FROM corpscout.text_translations "
        "WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description' "
        "AND source_text_hash IN ( SELECT cityHash64(ifNull(business_description, '')) FROM corpscout.se_ratsit_company "
        "WHERE ifNull(business_description, '') != '' )"
    ) in up
    for view, base, column in (
        ("se_bolagsverket_companies_translated", "se_bolagsverket_companies", "activity_description"),
        ("se_ratsit_company_translated", "se_ratsit_company", "business_description"),
    ):
        assert (
            f"CREATE OR REPLACE VIEW corpscout.{view} AS SELECT c.*, ifNull(act.translated_text, '') AS {column}_en, "
            f"act.translated_at AS {column}_translated_at FROM corpscout.{base} AS c LEFT JOIN ( "
            "SELECT source_text_hash, argMax(translated_text, version) AS translated_text, "
            "toDateTime64(max(version), 3, 'UTC') AS translated_at FROM corpscout.text_translations "
            f"WHERE source_table = 'corpscout.{base}' AND source_column = '{column}' "
            "AND source_lang = 'sv' AND target_lang = 'en' AND source_text_hash != cityHash64('') "
            f"GROUP BY source_text_hash ) AS act ON act.source_text_hash = cityHash64(ifNull(c.{column}, ''))"
        ) in up, view
    for verb in ("DELETE", "ALTER", "DROP", "RENAME"):
        assert verb not in up.upper()
    assert "DROP VIEW IF EXISTS corpscout.se_ratsit_company_translated" in down
    assert "DROP VIEW IF EXISTS corpscout.se_bolagsverket_companies_translated" in down
    assert "DELETE" not in down.upper() and "INSERT" not in down.upper()
```

Also add `"000390_corpscout_se_source_translated_views",` as the last entry of the `MIGRATIONS` tuple, after `"000389_corpscout_technology_proposals",`.

- [ ] **Step 2: Run the test to verify it fails**

Run (from `corpscout/services/dagster_v3`): `uv run pytest tests/test_clickhouse_migrations.py -q -k "se_source_translated_views or ledger or every_migration"`
Expected: FAIL with `FileNotFoundError` for the 000390 up file (and any ledger-completeness test that lists files on disk fails until the files exist).

- [ ] **Step 3: Write the up migration**

`corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Translations keyed on the register tables (spec 2026-09-07-se-translated-source-views).
-- Additive: the spine key ('corpscout.se_companies', 'activity_description') stays until
-- basic-info slice 5 retires corpscout.se_companies; se_companies_serving (000347) and the
-- old se_company_info_scb artifact still read it.
--
-- 1. Bolagsverket: every spine-key row copied under the register table's name. `version`
--    is the translation's own stamp (the loader writes int(time.time())) and is preserved,
--    so <column>_translated_at below does not move and the basic-info change scan does
--    not revisit every company. Register and spine texts are byte-identical (0 of
--    2,855,016 differ on 2026-09-07), so the cityHash64 keys carry over unchanged.
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)
SELECT
    'corpscout.se_bolagsverket_companies', source_column, source_text_hash, source_lang, target_lang,
    translated_text, provider, model, version
FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';

-- 2. Ratsit: seeded from the same rows wherever a Ratsit business description is the same
--    text (65,080 of 66,910 distinct texts on 2026-09-07), so only the texts Ratsit alone
--    has ever reach the LLM.
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version)
SELECT
    'corpscout.se_ratsit_company', 'business_description', source_text_hash, source_lang, target_lang,
    translated_text, provider, model, version
FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'
  AND source_text_hash IN (
      SELECT cityHash64(ifNull(business_description, ''))
      FROM corpscout.se_ratsit_company
      WHERE ifNull(business_description, '') != ''
  );

-- 3. The views: lv_companies_translated's shape plus the translation stamp, which the
--    basic-info extractors fold into observed_at so a translation that lands after an
--    extraction re-selects the company. Under join_use_nulls = 0 an untranslated row's
--    stamp is the zero DateTime64; readers test <column>_en != '' instead. The empty-text
--    hash is excluded so a row without text can never borrow another row's translation.
CREATE OR REPLACE VIEW corpscout.se_bolagsverket_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_description_en,
    act.translated_at AS activity_description_translated_at
FROM corpscout.se_bolagsverket_companies AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, version) AS translated_text,
        toDateTime64(max(version), 3, 'UTC') AS translated_at
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_bolagsverket_companies'
      AND source_column = 'activity_description'
      AND source_lang = 'sv' AND target_lang = 'en'
      AND source_text_hash != cityHash64('')
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''));

CREATE OR REPLACE VIEW corpscout.se_ratsit_company_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS business_description_en,
    act.translated_at AS business_description_translated_at
FROM corpscout.se_ratsit_company AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(translated_text, version) AS translated_text,
        toDateTime64(max(version), 3, 'UTC') AS translated_at
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_ratsit_company'
      AND source_column = 'business_description'
      AND source_lang = 'sv' AND target_lang = 'en'
      AND source_text_hash != cityHash64('')
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.business_description, ''));
```

- [ ] **Step 4: Write the down migration**

`corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.down.sql`:

```sql
-- Drops the views only. The rows copied under the register keys stay: a rollback never
-- removes translations (000252's convention), and re-running the up migration re-inserts
-- identical rows that collapse on merge.
DROP VIEW IF EXISTS corpscout.se_ratsit_company_translated;
DROP VIEW IF EXISTS corpscout.se_bolagsverket_companies_translated;
```

- [ ] **Step 5: Run the migration tests**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS (the whole file, so the ledger-completeness tests see both new files).

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.up.sql corpscout/clickhouse/migrations/000390_corpscout_se_source_translated_views.down.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): 000390 register-keyed SE translations and _translated views

Copies the se_companies-keyed activity translations under
se_bolagsverket_companies (all) and se_ratsit_company (identical texts),
version preserved, and creates the two plain _translated views with a
<column>_translated_at stamp. Spine key kept until basic-info slice 5."
```
(append the commit footer from Global Constraints)

---

### Task 2: Bolagsverket extractor reads the view

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` (whole file, 110 lines)
- Modify: `tests/test_se_company_basic_info_extractors_sql.py` (`test_bolagsverket_select_matches_the_contract`, lines 36-83)
- Modify: `tests/test_se_company_basic_info_extractors_clickhouse_local.py` (`MIGRATIONS` line 21-25, `_schema` line 29-40, `TRANSLATION_ROW` line 75-78, `late_translation` line 126-130)

**Interfaces:**
- Consumes: `corpscout.se_bolagsverket_companies_translated` from Task 1; `define_suggestion_asset`, `SUGGESTION_SELECT_COLUMNS` from `basic_info/extract.py` (unchanged); `bolagsverket_legal_form_sql` from `basic_info/legal_form.py` (unchanged).
- Produces: `bolagsverket_current_sql() -> str`, `bolagsverket_select_sql() -> str`, `BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-v3"`, `TRANSLATED_TABLE = "corpscout.se_bolagsverket_companies_translated"`, asset `se_basic_info_suggestions_bolagsverket` (same key, same deps).

- [ ] **Step 1: Rewrite the SQL pin test**

Replace `test_bolagsverket_select_matches_the_contract` in `tests/test_se_company_basic_info_extractors_sql.py` with:

```python
def test_bolagsverket_select_matches_the_contract() -> None:
    sql = bolagsverket.bolagsverket_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    # The register is read through its _translated view (migration 000390): the English
    # text and its stamp are columns, and no extractor joins text_translations itself.
    assert "FROM corpscout.se_bolagsverket_companies_translated AS register FINAL" in sql
    assert "text_translations" not in sql and "cityHash64" not in sql
    assert "WHERE has_company = 1 AND company_id IN %(company_ids)s" in sql
    assert "'bolagsverket' AS source" in sql
    assert REGISTER_UID in sql and "'sweden_bolagsverket'" in sql
    assert "if(register.deregistration_date IS NULL, 'active', 'inactive') AS status" in sql
    # The organisationsform token becomes SCB's juridisk form code, so the entity has one
    # legal-form vocabulary whichever source wins; an unknown token passes through.
    assert (
        "nullIf(transform(trim(ifNull(register.legal_form_code, '')), ['AB-ORGFO', " in sql
        and "trim(ifNull(register.legal_form_code, ''))), '') AS legal_form_code" in sql
    )
    assert bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION == "bolagsverket-v3"
    swedish = "nullIf(trim(ifNull(register.activity_description, '')), '')"
    assert f"if(register.activity_description_en != '', register.activity_description_en, {swedish}) AS description" in sql
    assert f"if(register.activity_description_en != '', 'en', if({swedish} IS NULL, NULL, 'sv')) AS description_language" in sql
    assert f"{swedish} AS description_sv" in sql
    # observed_at is the later of the register stamp and the translation stamp, so a text
    # translated after the last extraction re-selects the company. The text guards the
    # stamp: under join_use_nulls = 1 an untranslated row's stamp is NULL and a bare
    # greatest would be NULL.
    observed_at = (
        "greatest(register.observed_at, if(register.activity_description_en != '', "
        "ifNull(register.activity_description_translated_at, register.observed_at), register.observed_at))"
    )
    assert f"    {observed_at} AS observed_at,\n" in sql
    # current_sql computes the same observed_at, unscoped, so the change scan converges.
    assert bolagsverket.bolagsverket_current_sql() == (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {observed_at} AS observed_at\n"
        "FROM corpscout.se_bolagsverket_companies_translated AS register FINAL\n"
        "WHERE has_company = 1"
    )
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_sql.py -q -k bolagsverket`
Expected: FAIL on `"FROM corpscout.se_bolagsverket_companies_translated AS register FINAL" in sql`.

- [ ] **Step 3: Rewrite the extractor**

Replace the whole of `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` with:

```python
"""Bolagsverket register record -> basic-info suggestion: legal name, the organisationsform
token mapped to SCB's juridisk form code (legal_form.py), status, registration date, the
Swedish activity description and its English translation.

The register is read through corpscout.se_bolagsverket_companies_translated (migration
000390): the translation pipeline keys text_translations on the register table itself and
the view joins it back as activity_description_en plus its stamp, so this extractor
selects columns and never joins text_translations."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.se_company.basic_info.legal_form import bolagsverket_legal_form_sql

# v2 (2026-09-04): legal_form_code is the SCB code, not the raw -ORGFO token.
# v3 (2026-09-07): reads the _translated view; translations keyed on the register table.
BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-v3"
TRANSLATED_TABLE = "corpscout.se_bolagsverket_companies_translated"

BOLAGSVERKET_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_bolagsverket', "
    "'\\nregistry_company\\n', register.source_record_id, '\\n', lowerUTF8(register.source_payload_hash)))))"
)

_ACTIVITY_SV = "nullIf(trim(ifNull(register.activity_description, '')), '')"
# The register row is not the only input: the translation pipeline fills text_translations
# asynchronously. If observed_at were the register's alone, a company whose Swedish text
# was translated after its last extraction would keep description_language = 'sv' and the
# Swedish text on an English-facing field until its register record next changed.
# observed_at is therefore the later of the two stamps, and the same expression appears
# in current_sql, so the change scan re-selects a company when only its translation is
# new. The text guards the stamp: under join_use_nulls = 1 an untranslated row's stamp is
# NULL and a bare greatest would be NULL.
_TRANSLATED_AT_SQL = (
    "if(register.activity_description_en != '', "
    "ifNull(register.activity_description_translated_at, register.observed_at), register.observed_at)"
)
_OBSERVED_AT_SQL = f"greatest(register.observed_at, {_TRANSLATED_AT_SQL})"


def bolagsverket_current_sql() -> str:
    return (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at\n"
        f"FROM {TRANSLATED_TABLE} AS register FINAL\n"
        "WHERE has_company = 1"
    )


def bolagsverket_select_sql() -> str:
    return (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        "    'bolagsverket' AS source,\n"
        f"    {BOLAGSVERKET_RECORD_UID_SQL} AS source_record_uid,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at,\n"
        "    nullIf(trim(ifNull(register.legal_name, '')), '') AS legal_name,\n"
        f"    {bolagsverket_legal_form_sql('register.legal_form_code')} AS legal_form_code,\n"
        "    if(register.deregistration_date IS NULL, 'active', 'inactive') AS status,\n"
        "    register.registration_date AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        f"    if(register.activity_description_en != '', register.activity_description_en, {_ACTIVITY_SV}) AS description,\n"
        f"    if(register.activity_description_en != '', 'en', if({_ACTIVITY_SV} IS NULL, NULL, 'sv')) AS description_language,\n"
        f"    {_ACTIVITY_SV} AS description_sv\n"
        f"FROM {TRANSLATED_TABLE} AS register FINAL\n"
        "WHERE has_company = 1 AND company_id IN %(company_ids)s"
    )


se_basic_info_suggestions_bolagsverket = define_suggestion_asset(
    source="bolagsverket",
    extractor_version=BOLAGSVERKET_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    description=(
        "One bolagsverket suggestion row per company from se_bolagsverket_companies_translated: "
        "legal name, the organisationsform token mapped to SCB's juridisk form code (an unknown "
        "token passes through), active/inactive from the deregistration date, registration "
        "date, the Swedish activity description and its English translation when the view "
        "carries one. observed_at is the later of the register row's stamp and the "
        "translation's, so a newly translated text re-selects the company. execute=false previews."
    ),
)
```

- [ ] **Step 4: Run the SQL pins**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_sql.py -q`
Expected: PASS (all five extractors; the wikidata test still finds `FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 1` in its own links CTE).

- [ ] **Step 5: Teach the local harness to replay the views and move the seeded key**

In `tests/test_se_company_basic_info_extractors_clickhouse_local.py`:

Replace the `MIGRATIONS` tuple with:

```python
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
    "000376_corpscout_se_company_basic_info_suggestion.up.sql",
    "000390_corpscout_se_source_translated_views.up.sql",
)
```

Replace `_schema` with:

```python
def _schema() -> list[str]:
    tables: list[str] = []
    views: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                tables.append(statement)
            elif statement.upper().startswith("CREATE OR REPLACE VIEW"):
                views.append(statement)
    fixture = [s.strip() for s in FIXTURE.read_text(encoding="utf-8").split(";") if s.strip()]
    # Views last: 000390's read se_ratsit_company and text_translations, which the fixture
    # creates. Its INSERT ... SELECT statements are data moves and are not replayed.
    return tables + fixture + views
```

Replace `TRANSLATION_ROW` with:

```python
TRANSLATION_ROW = (
    "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
    "('corpscout.se_bolagsverket_companies', 'activity_description', cityHash64('Handel med kaffe'), 'sv', 'en', 'Coffee trading', 'p', 'm', 1)"
)
```

In `test_a_later_translation_re_selects_bolagsverket_and_flips_the_language`, change the `late_translation` literal's first value from `'corpscout.se_companies'` to `'corpscout.se_bolagsverket_companies'`.

- [ ] **Step 6: Run the local tests**

Run: `docker info >/dev/null 2>&1 && echo docker-ok; uv run pytest tests/test_se_company_basic_info_extractors_clickhouse_local.py -q`
Expected: PASS for every test (13 including parametrizations). If the run reports SKIPPED with "no clickhouse-local binary and no docker", start docker and re-run; do not proceed on a skip.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/se_company/basic_info/bolagsverket.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_company_basic_info_extractors_clickhouse_local.py
git commit -m "feat(dagster): bolagsverket basic-info extractor reads se_bolagsverket_companies_translated

bolagsverket-v3 selects activity_description_en and folds the view's
translation stamp into observed_at; no text_translations join in the
extractor. The clickhouse-local harness replays 000390's views."
```
(append the commit footer)

---

### Task 3: Ratsit extractor reads the view

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/ratsit.py` (whole file, 60 lines)
- Modify: `tests/test_se_company_basic_info_extractors_sql.py` (`test_ratsit_select_takes_the_newest_report_and_maps_status_text`, lines 131-150)
- Modify: `tests/test_se_company_basic_info_extractors_clickhouse_local.py` (add two tests after `test_ratsit_takes_the_newest_report_and_maps_status`)

**Interfaces:**
- Consumes: `corpscout.se_ratsit_company_translated` from Task 1; `RATSIT_NORMALIZER_VERSION` from `sweden_ratsit/normalization.py` (unchanged, value `ratsit-normalizer-v2`).
- Produces: `ratsit_current_sql() -> str`, `ratsit_select_sql() -> str`, `RATSIT_EXTRACTOR_VERSION = "ratsit-v2"`, `TRANSLATED_TABLE = "corpscout.se_ratsit_company_translated"`, asset `se_basic_info_suggestions_ratsit` (same key, same deps, same `select_params`).

- [ ] **Step 1: Rewrite the SQL pin test**

Replace `test_ratsit_select_takes_the_newest_report_and_maps_status_text` with:

```python
def test_ratsit_select_takes_the_newest_report_and_maps_status_text() -> None:
    sql = ratsit.ratsit_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    # Read through the _translated view (migration 000390); no text_translations join here.
    assert "FROM corpscout.se_ratsit_company_translated FINAL" in sql
    assert "text_translations" not in sql and "cityHash64" not in sql
    assert "normalizer_version = %(normalizer_version)s" in sql and "company_id IN %(company_ids)s" in sql
    assert "concat('ratsit:', toString(result_sha256)) AS source_record_uid" in sql
    stamp = "toDateTime64(normalized_at, 3, 'UTC')"
    observed_at = (
        f"greatest({stamp}, if(business_description_en != '', "
        f"ifNull(business_description_translated_at, {stamp}), {stamp}))"
    )
    assert f"    {observed_at} AS observed_at,\n" in sql
    assert "nullIf(trim(name), '') AS legal_name" in sql
    assert "multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status" in sql
    swedish = "nullIf(trim(ifNull(business_description, '')), '')"
    assert f"if(business_description_en != '', business_description_en, {swedish}) AS description" in sql
    assert f"if(business_description_en != '', 'en', if({swedish} IS NULL, NULL, 'sv')) AS description_language" in sql
    assert f"{swedish} AS description_sv" in sql
    assert "CAST(NULL AS Nullable(String)) AS legal_form_code" in sql
    assert sql.rstrip().endswith("ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id")
    assert ratsit.RATSIT_EXTRACTOR_VERSION == "ratsit-v2"
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    # current_sql takes the newest report per company and stamps it exactly as the SELECT
    # does, so the change scan converges even when an older report's text is translated
    # later than the newest report.
    assert ratsit.ratsit_current_sql() == (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        company_id AS company_id,\n"
        f"        {observed_at} AS observed_at\n"
        "    FROM corpscout.se_ratsit_company_translated FINAL\n"
        "    WHERE normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "    LIMIT 1 BY company_id\n"
        ")"
    )
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_sql.py -q -k ratsit`
Expected: FAIL on `"FROM corpscout.se_ratsit_company_translated FINAL" in sql`.

- [ ] **Step 3: Rewrite the extractor**

Replace the whole of `src/dagster_v3/defs/se_company/basic_info/ratsit.py` with:

```python
"""Ratsit's newest normalized report -> basic-info suggestion: name, status text mapped
to active/inactive, the Swedish business description and its English translation. Both
texts are columns of corpscout.se_ratsit_company_translated (migration 000390); the
extractor never joins text_translations itself. Ratsit's legal_form is free text of
another vocabulary and has no precedence, so it is not supplied."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

# v2 (2026-09-07): description is the English translation when the view carries one.
RATSIT_EXTRACTOR_VERSION = "ratsit-v2"
RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}
TRANSLATED_TABLE = "corpscout.se_ratsit_company_translated"

_DESCRIPTION = "nullIf(trim(ifNull(business_description, '')), '')"
_NORMALIZED_AT = "toDateTime64(normalized_at, 3, 'UTC')"
# The translation is a second input to observed_at (see bolagsverket.py for the why); the
# text guards the stamp because under join_use_nulls = 1 an untranslated row's stamp is
# NULL and a bare greatest would be NULL.
_OBSERVED_AT = (
    f"greatest({_NORMALIZED_AT}, if(business_description_en != '', "
    f"ifNull(business_description_translated_at, {_NORMALIZED_AT}), {_NORMALIZED_AT}))"
)
_NEWEST_REPORT = "ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id"


def ratsit_current_sql() -> str:
    # The newest report per company, stamped exactly as ratsit_select_sql stamps it. A
    # max() over every report would keep re-selecting a company whose OLDER report's text
    # was translated after its newest report, because the SELECT never writes that stamp.
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        company_id AS company_id,\n"
        f"        {_OBSERVED_AT} AS observed_at\n"
        f"    FROM {TRANSLATED_TABLE} FINAL\n"
        "    WHERE normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "    LIMIT 1 BY company_id\n"
        ")"
    )


def ratsit_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'ratsit' AS source,\n"
        "    concat('ratsit:', toString(result_sha256)) AS source_record_uid,\n"
        f"    {_OBSERVED_AT} AS observed_at,\n"
        "    nullIf(trim(name), '') AS legal_name,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_form_code,\n"
        "    multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status,\n"
        "    CAST(NULL AS Nullable(Date32)) AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        f"    if(business_description_en != '', business_description_en, {_DESCRIPTION}) AS description,\n"
        f"    if(business_description_en != '', 'en', if({_DESCRIPTION} IS NULL, NULL, 'sv')) AS description_language,\n"
        f"    {_DESCRIPTION} AS description_sv\n"
        f"FROM {TRANSLATED_TABLE} FINAL\n"
        "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s\n"
        f"{_NEWEST_REPORT}"
    )


se_basic_info_suggestions_ratsit = define_suggestion_asset(
    source="ratsit",
    extractor_version=RATSIT_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_SELECT_PARAMS,
    deps=[dg.AssetKey("se_ratsit_normalized")],
    description=(
        "One ratsit suggestion row per company from the newest normalized Ratsit report "
        "(se_ratsit_company_translated): name, active/inactive from the status text, the "
        "English business description when the view carries a translation else the Swedish "
        "one, and the Swedish text as description_sv. observed_at is the later of the "
        "report's stamp and the translation's. execute=false previews."
    ),
)
```

- [ ] **Step 4: Run the SQL pins**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_sql.py -q`
Expected: PASS.

- [ ] **Step 5: Add the local behaviour tests**

Append after `test_ratsit_takes_the_newest_report_and_maps_status` in `tests/test_se_company_basic_info_extractors_clickhouse_local.py` (the existing test keeps passing: without a translation row the Swedish text stays with language `sv`):

```python
RATSIT_ROWS = (
    "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, legal_form, status, business_description, normalized_at) VALUES "
    f"('5560000000', repeat('a', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'Old Name AB', '556000-0000', 'Aktiebolag', 'Aktiv', 'Gammal text', toDateTime64('2026-08-01 00:00:00', 6, 'UTC')), "
    f"('5560000000', repeat('b', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'New Name AB', '556000-0000', 'Aktiebolag', 'Aktiv', 'Ny text', toDateTime64('2026-09-01 00:00:00', 6, 'UTC'))"
)
RATSIT_TRANSLATION_ROW = (
    "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
    "('corpscout.se_ratsit_company', 'business_description', cityHash64('Ny text'), 'sv', 'en', 'New text', 'p', 'm', 1)"
)


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_ratsit_with_a_translation_writes_the_english_text(join_use_nulls: int) -> None:
    script = _schema() + [
        RATSIT_ROWS,
        RATSIT_TRANSLATION_ROW,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        f"SELECT legal_name, description, description_language, description_sv, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL",
    ]
    # version 1 is 1970: the report's own stamp is the later one and stays.
    assert _run(script, join_use_nulls=join_use_nulls) == ["New Name AB\tNew text\ten\tNy text\t2026-09-01 00:00:00.000"]


def test_a_later_translation_re_selects_ratsit_and_flips_the_language() -> None:
    late_translation = (
        "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
        "('corpscout.se_ratsit_company', 'business_description', cityHash64('Ny text'), 'sv', 'en', 'New text', 'p', 'm', "
        "toUnixTimestamp(toDateTime('2026-09-10 00:00:00', 'UTC')))"
    )
    scope = _bind(changed_scope_sql(current_sql=ratsit.ratsit_current_sql()), source="ratsit", normalizer_version=RATSIT_NORMALIZER_VERSION)
    script = _schema() + [
        RATSIT_ROWS,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        _labelled(scope, "converged"),
        late_translation,
        _labelled(scope, "translated"),
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        # Two rows now differ only in observed_at; read the newer one without FINAL, whose
        # ReplacingMergeTree version (suggested_at) can tie inside one clickhouse-local run.
        f"SELECT description, description_language, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} ORDER BY observed_at DESC LIMIT 1",
    ]
    assert _run(script, join_use_nulls=0) == ["translated\t5560000000", "New text\ten\t2026-09-10 00:00:00.000"]


def test_a_translation_of_an_older_ratsit_report_does_not_keep_re_selecting() -> None:
    """current_sql stamps the newest report only. Translating the OLD report's text later
    than the newest report must not re-select forever, because the SELECT never writes
    that stamp."""
    old_text_translation = (
        "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
        "('corpscout.se_ratsit_company', 'business_description', cityHash64('Gammal text'), 'sv', 'en', 'Old text', 'p', 'm', "
        "toUnixTimestamp(toDateTime('2026-09-10 00:00:00', 'UTC')))"
    )
    scope = _bind(changed_scope_sql(current_sql=ratsit.ratsit_current_sql()), source="ratsit", normalizer_version=RATSIT_NORMALIZER_VERSION)
    script = _schema() + [
        RATSIT_ROWS,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        old_text_translation,
        _labelled(scope, "stale-text"),
    ]
    assert _run(script, join_use_nulls=0) == []
```

- [ ] **Step 6: Run the local tests**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_clickhouse_local.py -q`
Expected: PASS, none skipped (docker or clickhouse-local present, as in Task 2).

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/se_company/basic_info/ratsit.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_company_basic_info_extractors_clickhouse_local.py
git commit -m "feat(dagster): ratsit basic-info extractor reads se_ratsit_company_translated

ratsit-v2 writes the English business description when the view carries a
translation, folds the translation stamp into observed_at, and stamps the
newest report only so the change scan converges."
```
(append the commit footer)

---

### Task 4: Sweden translation scan keyed on the Bolagsverket register

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/translation.py` (docstring lines 1-12, `ACTIVITY_DESCRIPTION_FIELD` lines 46-51, the asset lines 275-330)
- Create: `tests/test_sweden_company_translation.py`
- Modify: `tests/test_sweden_financial_concepts.py` (lines 90-94)

**Interfaces:**
- Consumes: `TranslationField`, `build_scan_sql` from `translator_load/loader.py` (unchanged); `TranslatorResource.enqueue_translation_rows(source_table=, source_column=, source_lang=, target_lang=, source_language_name=, target_language_name=, rows=)` (unchanged).
- Produces: `ACTIVITY_DESCRIPTION_FIELD = TranslationField("corpscout.se_bolagsverket_companies", "activity_description", "sv", "en", extra_where="has_company = 1")`; asset `sweden_company_translation_load` depending on `sweden_company_bolagsverket_companies_clickhouse`; its two checks unchanged in name.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sweden_company_translation.py`:

```python
"""The Sweden company translation loader is keyed on the Bolagsverket register table
(spec 2026-09-07-se-translated-source-views), not on the retiring se_companies spine."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.sweden_company import translation
from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.loader import TranslationField, build_coverage_sql, build_scan_sql
from dagster_v3.defs.translator_load.resource import TranslatorResource
from tests.test_translator_load import _FakeSession


def test_activity_description_field_is_keyed_on_the_bolagsverket_register() -> None:
    assert translation.ACTIVITY_DESCRIPTION_FIELD == TranslationField(
        "corpscout.se_bolagsverket_companies", "activity_description", "sv", "en", extra_where="has_company = 1"
    )


def test_scan_and_coverage_read_register_rows_with_a_company() -> None:
    field = translation.ACTIVITY_DESCRIPTION_FIELD
    kwargs = dict(source_lang=field.source_lang, target_lang=field.target_lang, extra_where=field.extra_where)
    for sql in (build_scan_sql(field.table, field.column, **kwargs), build_coverage_sql(field.table, field.column, **kwargs)):
        assert "FROM corpscout.se_bolagsverket_companies AS c" in sql
        assert "source_table = 'corpscout.se_bolagsverket_companies' AND source_column = 'activity_description'" in sql
        assert "(has_company = 1)" in sql
        assert "se_companies'" not in sql


def test_load_asset_depends_on_the_register_export() -> None:
    asset = translation.sweden_company_translation_load
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {
        dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")
    }
    assert asset.op.required_resource_keys == {"clickhouse", "translator"}


class _ScanClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, sql, params=None, **kwargs):
        self.queries.append(sql)
        return [("Handel med kaffe", 123)] if "text_translations" in sql else []


class _ScanResource:
    def __init__(self) -> None:
        self.client = _ScanClient()

    @contextmanager
    def get_connection(self):
        yield self.client


def test_load_asset_enqueues_under_the_field_key(monkeypatch) -> None:
    """The scan's anti-join and the enqueue must name the same key, or the loader
    re-enqueues every text on every run."""
    session = _FakeSession(stats={"input": 1, "pending": 0, "output": 1, "failed": 0})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    clickhouse = _ScanResource()

    result = translation.sweden_company_translation_load.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), clickhouse, TranslatorResource(base_url="http://translator:8080")
    )

    received = result.metadata["enqueued_received"]
    assert getattr(received, "value", received) == 1
    scan = clickhouse.client.queries[0]
    assert "FROM corpscout.se_bolagsverket_companies AS c" in scan and "(has_company = 1)" in scan
    (url, payload), = session.posts
    assert url.endswith("/v1/queue/items")
    assert payload["source_lang"] == "sv" and payload["target_lang"] == "en"
    assert payload["items"] == [
        {
            "source_table": "corpscout.se_bolagsverket_companies",
            "source_column": "activity_description",
            "source_text": "Handel med kaffe",
            "source_text_hash": "123",
        }
    ]
```

In `tests/test_sweden_financial_concepts.py`, change the block at lines 90-94 to:

```python
    company_load_node = graph.get(dg.AssetKey("sweden_company_translation_load"))
    assert company_load_node.group_name == "sweden_company"
    assert company_load_node.parent_keys == {
        dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_sweden_company_translation.py tests/test_sweden_financial_concepts.py -q`
Expected: the four new tests FAIL (field still names `corpscout.se_companies`); the financial-concepts graph test FAILS on `parent_keys`.

- [ ] **Step 3: Re-point the field, the dep and the enqueue**

In `src/dagster_v3/defs/sweden_company/translation.py`:

Replace the docstring's first two paragraphs (lines 1-12, up to and including the sentence ending `se_company_info_scb``).`) with:

```python
"""Sweden company translation loaders: free text via the translator service,
codes via a curated dictionary table.

``activity_description`` is Swedish business-purpose free text (~2.17M
distinct) on the Bolagsverket register table ``corpscout.se_bolagsverket_companies``
-> scanned and enqueued to the Go translator service (the sole writer of
``text_translations``), keyed on that table (migration 000390 moved the key off
the retiring ``se_companies`` spine) and read back through
``se_bolagsverket_companies_translated``, exactly the Norway/Latvia pattern.
Legal-form and status-reason values are Bolagsverket/SCB CODES, not prose --
an LLM would guess -- so they get labels from the curated in-repo
dictionaries below, seeded into ``corpscout.se_code_labels`` (migration 000150
owns the schema, 000305 adds ``label_sv``).
```

Replace the field definition (lines 46-51) with:

```python
# has_company = 1 mirrors the extractors' scope; the export may one day carry rows for
# organisations that are not companies, and their text is nobody's description.
ACTIVITY_DESCRIPTION_FIELD = TranslationField(
    "corpscout.se_bolagsverket_companies",
    "activity_description",
    SOURCE_LANG,
    TARGET_LANG,
    extra_where="has_company = 1",
)
```

In the asset, change the decorator's `deps` and description, the scan call and the enqueue call:

```python
@dg.asset(
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.se_bolagsverket_companies.activity_description for untranslated "
        "texts (anti-join vs text_translations under the register's key), enqueue them "
        "to the translator service, and wait for queue completion."
    ),
)
def sweden_company_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql(
                ACTIVITY_DESCRIPTION_FIELD.table,
                ACTIVITY_DESCRIPTION_FIELD.column,
                source_lang=ACTIVITY_DESCRIPTION_FIELD.source_lang,
                target_lang=ACTIVITY_DESCRIPTION_FIELD.target_lang,
                extra_where=ACTIVITY_DESCRIPTION_FIELD.extra_where,
            )
        )
    context.log.info(
        "scanned %d untranslated activity descriptions", len(untranslated_rows)
    )
    enqueue_result = translator.enqueue_translation_rows(
        source_table=ACTIVITY_DESCRIPTION_FIELD.table,
        source_column=ACTIVITY_DESCRIPTION_FIELD.column,
        source_lang=SOURCE_LANG,
        target_lang=TARGET_LANG,
        source_language_name=SOURCE_LANGUAGE_NAME,
        target_language_name=TARGET_LANGUAGE_NAME,
        rows=untranslated_rows,
    )
```

The rest of the function body (warnings, wait, `MaterializeResult`) is unchanged.

- [ ] **Step 4: Run the tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_sweden_company_translation.py tests/test_sweden_financial_concepts.py tests/test_translator_load.py tests/test_translation_coverage.py tests/test_se_company_scb.py -q`
Expected: PASS. (`test_se_company_scb.py` still expects the old artifact to depend on `sweden_company_translation_load`; that dep is untouched.)

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/sweden_company/translation.py tests/test_sweden_company_translation.py tests/test_sweden_financial_concepts.py
git commit -m "feat(dagster): key the Sweden activity translation scan on se_bolagsverket_companies

The scan and enqueue both take the key from ACTIVITY_DESCRIPTION_FIELD
(has_company = 1), and the load depends on the Bolagsverket export rather
than the retiring se_companies builder."
```
(append the commit footer)

---

### Task 5: Ratsit translation scan asset

**Files:**
- Create: `src/dagster_v3/defs/sweden_ratsit/translation.py`
- Modify: `src/dagster_v3/defs/sweden_ratsit/assets.py` (imports near line 20-37; `defs = dg.Definitions(...)` at line 1547-1558)
- Modify: `src/dagster_v3/defs/czech_legal_forms/assets.py` (`TRANSLATION_LOAD_ASSETS`, lines 319-329)
- Create: `tests/test_sweden_ratsit_translation.py`

**Interfaces:**
- Consumes: `RATSIT_NORMALIZER_VERSION` (`ratsit-normalizer-v2`), `translation_coverage_result`, `TranslationField`, `build_scan_sql`, `TranslatorResource`, `translator_queue_health_check` (all unchanged).
- Produces: `BUSINESS_DESCRIPTION_FIELD`, asset `sweden_ratsit_translation_load` (group `sweden_ratsit`, dep `se_ratsit_normalized`), checks `sweden_ratsit_translator_queue_health_check` (`translator_queue_healthy`) and `sweden_ratsit_translation_coverage` (`translations_present`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sweden_ratsit_translation.py`:

```python
"""Ratsit business descriptions join the translator pipeline under their own table's key
(spec 2026-09-07-se-translated-source-views)."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.czech_legal_forms.assets import TRANSLATION_LOAD_ASSETS
from dagster_v3.defs.sweden_ratsit import translation
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.loader import TranslationField, build_coverage_sql, build_scan_sql
from dagster_v3.defs.translator_load.resource import TranslatorResource
from tests.test_translator_load import _FakeSession


def test_business_description_field_is_keyed_on_the_ratsit_table_and_scoped_to_the_normalizer() -> None:
    assert translation.BUSINESS_DESCRIPTION_FIELD == TranslationField(
        "corpscout.se_ratsit_company",
        "business_description",
        "sv",
        "en",
        extra_where=f"normalizer_version = '{RATSIT_NORMALIZER_VERSION}'",
    )


def test_scan_and_coverage_share_the_normalizer_scope() -> None:
    field = translation.BUSINESS_DESCRIPTION_FIELD
    kwargs = dict(source_lang=field.source_lang, target_lang=field.target_lang, extra_where=field.extra_where)
    for sql in (build_scan_sql(field.table, field.column, **kwargs), build_coverage_sql(field.table, field.column, **kwargs)):
        assert "FROM corpscout.se_ratsit_company AS c" in sql
        assert "source_table = 'corpscout.se_ratsit_company' AND source_column = 'business_description'" in sql
        assert f"(normalizer_version = '{RATSIT_NORMALIZER_VERSION}')" in sql


def test_load_asset_follows_the_normalizer_and_is_covered_by_the_ten_minute_job() -> None:
    asset = translation.sweden_ratsit_translation_load
    assert asset.key == dg.AssetKey("sweden_ratsit_translation_load")
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {dg.AssetKey("se_ratsit_normalized")}
    assert asset.specs_by_key[asset.key].group_name == "sweden_ratsit"
    assert asset.op.required_resource_keys == {"clickhouse", "translator"}
    assert "sweden_ratsit_translation_load" in TRANSLATION_LOAD_ASSETS


class _ScanClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, sql, params=None, **kwargs):
        self.queries.append(sql)
        return [("Ny text", 456)] if "text_translations" in sql else []


class _ScanResource:
    def __init__(self) -> None:
        self.client = _ScanClient()

    @contextmanager
    def get_connection(self):
        yield self.client


def test_load_asset_enqueues_under_the_field_key(monkeypatch) -> None:
    session = _FakeSession(stats={"input": 1, "pending": 0, "output": 1, "failed": 0})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    clickhouse = _ScanResource()

    result = translation.sweden_ratsit_translation_load.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), clickhouse, TranslatorResource(base_url="http://translator:8080")
    )

    received = result.metadata["enqueued_received"]
    assert getattr(received, "value", received) == 1
    assert f"(normalizer_version = '{RATSIT_NORMALIZER_VERSION}')" in clickhouse.client.queries[0]
    (url, payload), = session.posts
    assert url.endswith("/v1/queue/items")
    assert payload["source_language_name"] == "Swedish" and payload["target_language_name"] == "English"
    assert payload["items"] == [
        {
            "source_table": "corpscout.se_ratsit_company",
            "source_column": "business_description",
            "source_text": "Ny text",
            "source_text_hash": "456",
        }
    ]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_sweden_ratsit_translation.py -q`
Expected: FAIL at import (`ModuleNotFoundError: dagster_v3.defs.sweden_ratsit.translation`).

- [ ] **Step 3: Create the scan module**

Create `src/dagster_v3/defs/sweden_ratsit/translation.py`:

```python
"""Ratsit business descriptions -> the translator service (sv -> en).

``business_description`` is the Swedish verksamhetsbeskrivning Ratsit republishes. Most of
it is the same text Bolagsverket holds (65,080 of 66,910 distinct texts on 2026-09-07)
and migration 000390 seeded those under this table's key, so the scan enqueues only what
Ratsit alone has. Keyed on ``corpscout.se_ratsit_company`` and read back through
``se_ratsit_company_translated`` by the basic-info Ratsit extractor -- the same shape as
``sweden_company/translation.py`` and the other country loaders.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

GROUP_NAME = "sweden_ratsit"
SOURCE_LANG = "sv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Swedish"
TARGET_LANGUAGE_NAME = "English"
# Scoped to the current normalizer: rows of a retired normalizer version are read by
# nothing and must not be enqueued (94 v1 rows remained on 2026-09-07).
BUSINESS_DESCRIPTION_FIELD = TranslationField(
    "corpscout.se_ratsit_company",
    "business_description",
    SOURCE_LANG,
    TARGET_LANG,
    extra_where=f"normalizer_version = '{RATSIT_NORMALIZER_VERSION}'",
)


@dg.asset(
    deps=[dg.AssetKey("se_ratsit_normalized")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.se_ratsit_company.business_description (current normalizer) for "
        "untranslated texts (anti-join vs text_translations under this table's key), "
        "enqueue them to the translator service, and wait for queue completion."
    ),
)
def sweden_ratsit_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql(
                BUSINESS_DESCRIPTION_FIELD.table,
                BUSINESS_DESCRIPTION_FIELD.column,
                source_lang=BUSINESS_DESCRIPTION_FIELD.source_lang,
                target_lang=BUSINESS_DESCRIPTION_FIELD.target_lang,
                extra_where=BUSINESS_DESCRIPTION_FIELD.extra_where,
            )
        )
    context.log.info("scanned %d untranslated business descriptions", len(untranslated_rows))
    enqueue_result = translator.enqueue_translation_rows(
        source_table=BUSINESS_DESCRIPTION_FIELD.table,
        source_column=BUSINESS_DESCRIPTION_FIELD.column,
        source_lang=SOURCE_LANG,
        target_lang=TARGET_LANG,
        source_language_name=SOURCE_LANGUAGE_NAME,
        target_language_name=TARGET_LANGUAGE_NAME,
        rows=untranslated_rows,
    )
    for warning in enqueue_result.workflow_start_warnings:
        context.log.warning("translator workflow start warning: %s", warning)
    if enqueue_result.workflow_start_warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(enqueue_result.workflow_start_warnings),
                "warnings": dg.MetadataValue.json(enqueue_result.workflow_start_warnings),
            },
        )
    if enqueue_result.received > 0:
        completion_stats = translator.wait_for_queue_completion(baseline_failed=baseline_failed)
        context.log.info(
            "translator queue completed: input=%d pending=%d output=%d failed=%d",
            completion_stats.input,
            completion_stats.pending,
            completion_stats.output,
            completion_stats.failed,
        )
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": enqueue_result.received,
            "enqueued_inserted": enqueue_result.inserted,
        }
    )


@dg.asset_check(asset=sweden_ratsit_translation_load, name="translator_queue_healthy")
def sweden_ratsit_translator_queue_health_check(translator: TranslatorResource) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=sweden_ratsit_translation_load, name="translations_present")
def sweden_ratsit_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many current-normalizer Ratsit descriptions exist, and how many are translated."""
    return translation_coverage_result(clickhouse, (BUSINESS_DESCRIPTION_FIELD,))
```

- [ ] **Step 4: Register the asset and add it to the coverage job list**

In `src/dagster_v3/defs/sweden_ratsit/assets.py`, add after the existing `from dagster_v3.defs.sweden_ratsit.resources import (...)` import block:

```python
from dagster_v3.defs.sweden_ratsit.translation import (
    sweden_ratsit_translation_load,
    sweden_ratsit_translator_queue_health_check,
)
```

and change the module's `defs` to:

```python
defs = dg.Definitions(
    assets=[se_ratsit_scan_dispatch, se_ratsit_normalized, sweden_ratsit_translation_load],
    asset_checks=[sweden_ratsit_translator_queue_health_check],
    jobs=[se_ratsit_scan_dispatch_job, se_ratsit_normalize_job],
    resources={
        "sweden_ratsit_browser": SwedenRatsitBrowserResource(
            crawl_proxy1=dg.EnvVar("crawl_proxy1"),
            crawl_proxy2=dg.EnvVar("crawl_proxy2"),
            crawl_proxy3=dg.EnvVar("crawl_proxy3"),
        ),
        "sweden_ratsit_object_store": ObjectStoreResource(bucket=RATSIT_S3_BUCKET),
    },
)
```

(The `translations_present` check is collected from the module by `load_from_defs_folder`, the way `sweden_company_translation_coverage` is today.)

In `src/dagster_v3/defs/czech_legal_forms/assets.py`, change `TRANSLATION_LOAD_ASSETS` to:

```python
TRANSLATION_LOAD_ASSETS = (
    "brazil_comp_cnae_translation_load",
    "brazil_pncp_translation_load",
    "company_entity_types_translation_load",
    "czech_legal_forms_translation_load",
    "france_legal_forms_translation_load",
    "latvia_ur_translation_load",
    "norway_brreg_translation_load",
    "sweden_company_translation_load",
    "sweden_financial_taxonomy_translation_load",
    "sweden_ratsit_translation_load",
)
```

- [ ] **Step 5: Run the tests, including the whole-repository loader check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_sweden_ratsit_translation.py tests/test_translation_coverage.py tests/test_sweden_ratsit_pilot.py tests/test_schedule_cron_contracts.py -q`
Expected: PASS. `test_translation_coverage_job_covers_every_loader` loads every Definitions and proves the new `translations_present` check is in the ten-minute job.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/sweden_ratsit/translation.py src/dagster_v3/defs/sweden_ratsit/assets.py src/dagster_v3/defs/czech_legal_forms/assets.py tests/test_sweden_ratsit_translation.py
git commit -m "feat(dagster): sweden_ratsit_translation_load scans Ratsit business descriptions

Keyed on corpscout.se_ratsit_company, scoped to the current normalizer,
with the queue-health and translations_present checks; listed in the
ten-minute coverage job."
```
(append the commit footer)

---

### Task 6: Whole-suite verification and merge

**Files:** none new.

- [ ] **Step 1: Run the full Dagster unit suite**

Run (from `corpscout/services/dagster_v3`): `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest -q -x -m "not integration"`
Expected: PASS.

- [ ] **Step 2: Run the integration tests this work touches**

Run: `uv run pytest tests/test_se_company_basic_info_extractors_clickhouse_local.py tests/test_se_company_basic_info_clickhouse_local.py -q`
Expected: PASS, none skipped.

- [ ] **Step 3: Migration-number collision check**

Run: `git fetch origin main && git ls-tree --name-only origin/main corpscout/clickhouse/migrations/ | tail -2`, and on prod:

```sql
SELECT max(version) FROM corpscout.schema_migrations
```

Expected: no `000390` on `origin/main`, prod head `389`. If either differs, renumber the migration (file names, the `MIGRATIONS` entries in both test files, the test name) before merging.

- [ ] **Step 4: Merge**

```bash
git checkout main && git merge --no-ff se-translated-source-views -m "Merge branch 'se-translated-source-views'"
```

---

### Task 7: Rollout (owner-run, in this order)

**Files:** none. These are production steps; each has a check.

- [ ] **Step 1: Apply 000390 on prod** with the usual golang-migrate command for the ClickHouse ledger, then verify:

```sql
SELECT source_table, count() AS n
FROM corpscout.text_translations
WHERE source_column IN ('activity_description', 'business_description')
  AND source_table IN ('corpscout.se_companies', 'corpscout.se_bolagsverket_companies', 'corpscout.se_ratsit_company')
GROUP BY source_table ORDER BY source_table;
SELECT name FROM system.tables WHERE database = 'corpscout' AND name LIKE 'se_%_translated';
```

Expected: the `se_bolagsverket_companies` count equals the `se_companies` count (2,176,663 on 2026-09-07, higher if the translator ran since); `se_ratsit_company` about 65,000; both views listed. **Do not deploy Dagster until this holds**: deployed first, both scans anti-join an empty key and enqueue every text to the LLM again.

- [ ] **Step 2: Deploy dagster_v3** (the pristine-worktree recipe if the tree is dirty; dbt-state refresh as usual).

- [ ] **Step 3: Materialize, in order, and read the metadata**

1. `sweden_company_translation_load`: expected `enqueued_received` 0 (every register text already translated).
2. `sweden_ratsit_translation_load`: expected about 1,830 enqueued; wait for the queue.
3. `se_basic_info_suggestions_bolagsverket` with `execute: true`: expected about 226,104 companies (those whose translation landed 2026-09-05/06) plus any register change since 2026-09-04.
4. `se_basic_info_suggestions_ratsit` with `execute: true`: expected every company whose newest report text now has a translation.
5. The basic-info fold.

Verify afterwards:

```sql
SELECT description_language, count()
FROM (SELECT company_id, argMax(description_language, suggested_at) AS description_language
      FROM corpscout.se_company_basic_info_suggestion WHERE source = 'bolagsverket' GROUP BY company_id)
GROUP BY description_language;
```

Expected: `sv` near zero (only texts the translator has not returned yet), `en` for the rest.

- [ ] **Step 4: Confirm the Info tab** for company 5020077862 at `http://localhost:5183/admin/se/company/5020077862/info?field=description` shows the Bolagsverket suggestion in English (it already did) and pick one company from the former 226,104 to confirm it flipped.
