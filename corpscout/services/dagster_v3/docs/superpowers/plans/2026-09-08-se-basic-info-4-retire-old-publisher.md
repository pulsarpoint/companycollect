# SE Basic Info Slice 4: Retire the Old Publisher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every live reader of `se_company_info` to `se_company_basic_info`, delete the old publisher and its artifacts, and retire five tables, keeping the LLM observation cache.

**Architecture:** The serving view `se_companies_serving` is re-based on the basic-info main row plus the Bolagsverket register table through a new staged-swap migration (000391) that keeps the view's column list and cadence. The backoffice company header re-points one query; the old pipeline page and dead modules go. Dagster keeps only the LLM helpers of `info.py`, drops the three artifact assets and the publisher, and the migration files of the dropped tables are emptied per the ledger policy. Prod: stop the old instigators, deploy, apply 000391, drop the tables by an owner-run gated script.

**Tech Stack:** ClickHouse (golang-migrate ledger, refreshable materialized view), Python 3.14 / Dagster / pytest (`uv run pytest`), clickhouse-local or docker for integration tests, TypeScript / React Router / vitest (`npx vitest run`, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-08-se-basic-info-4-retire-old-publisher-design.md`

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice`. Migrations in `corpscout/clickhouse/migrations/`.
- **Branch:** `se-basic-info-4-retire-publisher` (exists, holds the spec commit `89cb98aad`). Commit by explicit path only; the tree carries other people's WIP.
- **Definitions-loading tests** need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`.
- **Integration tests** (`tests/test_se_companies_serving_sql.py`, the `*_clickhouse_local.py` files) need docker or clickhouse-local; a skip is not a pass.
- **Keep:** `se_company_info_enrichment_observation` and everything that reads or writes it (`SE_COMPANY_INFO_OBSERVATION`, `OBSERVATION_COLUMNS`, `OBSERVATION_FLUSH_ROWS`, `common.py`'s observation helpers, migration 000298's grant on it).
- **Migration number:** 000391. Prod ledger head is 390. `corpscout.se_companies_serving_retired` does not exist on prod (checked 2026-09-08), so the 000347 rename pattern can reuse that name.
- **Cadence (verbatim):** `REFRESH EVERY 1 HOUR OFFSET 45 MINUTE` (migration 000366), not 000347's 15 minutes.
- **Serving view columns:** unchanged list, unchanged order; the backoffice reads them by name.
- **Prod actions are owner-named:** the classifier refuses prod migrations, deploys and instigator changes unless the user names them. Task 6 lists each one; ask before running.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```
- **Out of scope:** renaming the observation table; `SHELL_REGISTER_SQL`, the public country detail page and every other `se_companies` reader (slice 5); the `economically_active` field.

---

## File map

| File | Responsibility |
|---|---|
| `src/dagster_v3/defs/se_company/common.py` (modify) | Gains `bolagsverket_record_uid_sql(alias)`, shared by the extractor and the serving builder. |
| `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` (modify) | Uses the shared uid helper. |
| `src/dagster_v3/defs/sweden_company/companies_current.py` (modify) | Serving SELECT re-based on `se_company_basic_info` + `se_bolagsverket_companies` + `se_code_labels`. |
| `corpscout/clickhouse/migrations/000391_corpscout_se_companies_serving_basic_info.{up,down}.sql` (create) | Staged swap of the re-based view. |
| `tests/test_se_companies_serving_mv.py` (modify) | Pins 000391. |
| `tests/test_se_companies_serving_sql.py` (modify) | Seeds the new base tables. |
| `tests/test_clickhouse_migrations.py` (modify) | Registers 000391; `EMPTIED_MIGRATIONS` gains seven files; two migration-text tests go. |
| backoffice `app/lib/se-company-shell.server.ts` (modify) | `SHELL_INFO_SQL` reads the main table. |
| backoffice deletions | `routes/admin-se-companies-pipeline.ts` + test, `components/admin/se-company-info-pipeline.tsx`, `lib/se-company-info-pipeline.ts` + test, `lib/se-company-info-pipeline.server.ts` + test, `lib/se-company-info.server.ts`, `lib/se-info-field-values.ts`. |
| backoffice `app/lib/dagster.server.ts`, `app/lib/dagster.server.test.ts`, `app/lib/clickhouse.server.ts`, `app/routes.ts`, `app/lib/se-company-info-filters.ts`, `app/lib/se-company-geocoding-list.server.ts` (modify) | Constants, helper, route entry and comments. |
| `src/dagster_v3/defs/se_company/info.py` (rewrite) | LLM helpers only. |
| `src/dagster_v3/defs/se_company/scb.py` (modify) | Address artifact only. |
| `src/dagster_v3/defs/se_company/esef.py`, `wikidata.py` (delete) | Old artifacts. |
| `src/dagster_v3/defs/se_company/info_rules.py` (rewrite) | `ArtifactRow`, `evidence_set_hash_for`, `_text`. |
| `src/dagster_v3/defs/common/clickhouse_checks.py` (modify) | Four leaves removed. |
| `tests/test_se_company_llm_support.py` (create) | The kept helpers' tests. |
| `tests/test_se_company_info.py`, `test_se_company_info_clickhouse_local.py`, `test_se_company_esef.py`, `test_se_company_wikidata.py`, `test_se_company_scb.py` (delete); `tests/test_se_company_info_rules.py`, `tests/test_se_company_address_scb.py`, `tests/se_company_ddl.py` (modify) | Old publisher tests. |
| `corpscout/clickhouse/migrations/000297, 000299, 000300, 000301, 000304, 000306, 000365, 000371` (modify) | DDL of the dropped tables leaves the files. |
| `corpscout/clickhouse/operations/se_company_info_retire.md` (create) | Owner-run gated drop. |

---

### Task 1: Serving view re-based on the basic-info main row

**Files:**
- Modify: `src/dagster_v3/defs/se_company/common.py` (append after `ledger_sensor` or at the end)
- Modify: `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` (lines 19-22)
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py` (docstring lines 5 and 20, constants lines 62-63, the join constants lines 197-212, the inner SELECT lines 317-357)
- Create: `corpscout/clickhouse/migrations/000391_corpscout_se_companies_serving_basic_info.up.sql`, `.down.sql`
- Modify: `tests/test_se_companies_serving_mv.py`, `tests/test_se_companies_serving_sql.py`, `tests/test_se_company_basic_info_extractors_sql.py` (unchanged assertions must still pass), `tests/test_clickhouse_migrations.py` (the `EXPECTED_MIGRATIONS` tuple)

**Interfaces:**
- Consumes: `se_company_basic_info` (000377), `se_bolagsverket_companies` (000374), `se_code_labels` (000150/000305).
- Produces: `bolagsverket_record_uid_sql(alias: str) -> str` in `common.py`; `build_se_companies_serving_sql()` with the same column list; migration 000391.

- [x] **Step 1: Write the failing pin test (000391) and the failing serving SQL test**

In `tests/test_se_companies_serving_mv.py` replace the module docstring, `MIGRATION`, and the affected assertions:

```python
"""Migration 000391: the serving view re-based on the basic-info main row, pinned to its builder.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads. Since 000391 its spine is `se_company_basic_info` (slice 4 of the basic-info
design): legal name, status, legal form and the two descriptions come from the folded row,
the register fields from `se_bolagsverket_companies`, the legal-form labels from
`se_code_labels`. `se_company_info`, `se_companies` and `text_translations` are gone from
the view. Same staged swap as 000347: build under _next, SYSTEM WAIT, one atomic RENAME.

The drift pin couples the migration's embedded SELECT to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""
```

```python
MIGRATION = "000391_corpscout_se_companies_serving_basic_info"
```

In `test_the_pin_is_not_vacuous`, replace the three lines

```python
    assert "text_translations" in embedded
    assert "activity_description_en" in embedded
    assert "se_code_labels" in embedded
```

with

```python
    assert "corpscout.se_company_basic_info AS i FINAL" in embedded
    assert "corpscout.se_bolagsverket_companies" in embedded
    assert "text_translations" not in embedded
    assert "corpscout.se_company_info" not in embedded
    assert "corpscout.se_companies AS" not in embedded
    assert "activity_description_en" in embedded
    assert "se_code_labels" in embedded
    assert "code_type = 'legal_form'" in embedded
```

and replace `assert "INNER JOIN corpscout.se_company_info" not in embedded` with `assert "se_company_info" not in embedded`.

In `test_the_up_migration_is_a_staged_swap_waited_on_before_the_rename` replace `assert "REFRESH EVERY 15 MINUTE" in create` with `assert "REFRESH EVERY 1 HOUR OFFSET 45 MINUTE" in create`.

Rename `test_the_down_migration_swaps_back_restarts_and_discards_the_eodhd_render` to `test_the_down_migration_swaps_back_restarts_and_discards_the_basic_info_render` and change its last assertion to `assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_basic_info_discard" in down`.

In `tests/test_se_companies_serving_sql.py`:

Replace `_info_row` and `INFO_COLUMNS` (lines 74-91) with:

```python
BASIC_INFO_COLUMNS = (
    "company_id, legal_name, legal_name_source, legal_form_code, legal_form_code_source, "
    "status, status_source, incorporation_date, incorporation_date_source, lei, lei_source, "
    "wikidata_id, wikidata_id_source, description, description_source, description_language, "
    "description_sv, description_sv_source, folded_at, fold_version, source_run_id"
)


def _basic_info_row(
    company_id: str,
    legal_name: str,
    *,
    legal_form_code: str = "NULL",
    description: str = "NULL",
    description_language: str = "NULL",
    description_sv: str = "NULL",
) -> str:
    source = "'bolagsverket'" if description != "NULL" else "''"
    return (
        f"('{company_id}', '{legal_name}', 'scb', {legal_form_code}, 'scb', 'active', 'bolagsverket', "
        f"NULL, '', NULL, '', NULL, '', {description}, {source}, {description_language}, "
        f"{description_sv}, {source}, {_literal(NOW)}, 'fold-v1', 'run')"
    )


BOLAGSVERKET_COLUMNS = (
    "company_id, company_id_raw, legal_name, deregistration_reason, source_run_id, "
    "source_record_id, source_payload_hash, observed_at"
)


def _bolagsverket_row(company_id: str, *, reason: str = "NULL") -> str:
    return (
        f"('{company_id}', '{company_id}$X', 'Register AB', {reason}, 'run', "
        f"'rec-{company_id}', 'HASH-{company_id}', {_literal(NOW)})"
    )


def _record_uid(company_id: str) -> str:
    """What the view must render for bolagsverket_source_record_uid: the extractor's
    company-source-record hash over the register row's id and (lower-cased) payload hash."""
    import hashlib

    text = (
        "company-source-record-v1\nstructured\nsweden_bolagsverket\nregistry_company\n"
        f"rec-{company_id}\nhash-{company_id}"
    )
    return hashlib.sha256(text.encode()).hexdigest()
```

In `_script`, replace the block from `table_block("se_company_info"),` through the `se_code_labels` stub line (lines 200-210) with:

```python
        table_block("se_company_basic_info"),
        table_block("se_bolagsverket_companies"),
        table_block("se_company_address"),
        _served_table_ddl() + ";",
        # Stubs for the presence-set reads: only the columns the serving SELECT's
        # IN-subqueries touch. Seeds prove each arm independently.
        "CREATE TABLE corpscout.se_code_labels (code_type String, code String, label_en String, label_sv String, version UInt32) ENGINE = MergeTree ORDER BY code;",
```

Replace the two spine seed lines (the `INSERT INTO corpscout.se_companies ...` and `INSERT INTO corpscout.text_translations ...` lines) and the `se_code_labels` seed with:

```python
        # Register rows: COARSE is deregistered with a labeled reason; PRECISE has no reason;
        # NOSERVED has no register row at all (every register-derived field folds to '').
        f"INSERT INTO corpscout.se_bolagsverket_companies ({BOLAGSVERKET_COLUMNS}) VALUES "
        + ", ".join((_bolagsverket_row(COARSE, reason="'konkurs avslutad'"), _bolagsverket_row(PRECISE)))
        + ";",
        "INSERT INTO corpscout.se_code_labels VALUES ('status_reason', 'konkurs avslutad', 'Bankruptcy concluded', '', 1), ('legal_form', '49', 'Limited company (aktiebolag)', 'Aktiebolag', 1);",
```

Replace the `INSERT INTO corpscout.se_company_info ({INFO_COLUMNS}) VALUES` block with:

```python
        f"INSERT INTO corpscout.se_company_basic_info ({BASIC_INFO_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                # COARSE: translated activity text -- English in description, Swedish beside it.
                _basic_info_row(COARSE, "Coarse AB", legal_form_code="'49'",
                                description="'Building trade with timber'", description_language="'en'",
                                description_sv="'Bygghandel med trävaror'"),
                # PRECISE: untranslated -- the Swedish text is the description, language sv.
                _basic_info_row(PRECISE, "Precise AB", description="'Handel med maskiner'",
                                description_language="'sv'", description_sv="'Handel med maskiner'"),
                _basic_info_row(NOSERVED, "Noserved AB"),
                _basic_info_row(POSTAL_BOX, "Postal Box AB"),
                _basic_info_row(NOADDRESS, "Addressless AB"),
            )
        )
        + ";",
```

Replace `test_translations_are_absorbed_from_the_spine_join` with:

```python
def test_descriptions_come_from_the_main_row_and_register_fields_from_bolagsverket(rows: dict[str, dict]) -> None:
    # COARSE: the folded row carries English + Swedish; the register row is deregistered
    # with a labeled reason; the record uid is the extractor's hash over the register row.
    assert rows[COARSE]["activity_description"] == "Bygghandel med trävaror"
    assert rows[COARSE]["activity_description_en"] == "Building trade with timber"
    assert rows[COARSE]["status_reason"] == "konkurs avslutad"
    assert rows[COARSE]["status_reason_label_en"] == "Bankruptcy concluded"
    assert rows[COARSE]["bolagsverket_source_record_uid"] == _record_uid(COARSE)
    assert rows[COARSE]["legal_form_code"] == "49"
    assert rows[COARSE]["legal_form_label_en"] == "Limited company (aktiebolag)"
    assert rows[COARSE]["legal_form_label_sv"] == "Aktiebolag"
    # PRECISE: untranslated -> the English column stays '' (never the Swedish text).
    assert rows[PRECISE]["activity_description"] == "Handel med maskiner"
    assert rows[PRECISE]["activity_description_en"] == ""
    assert rows[PRECISE]["status_reason"] == ""
    assert rows[PRECISE]["bolagsverket_source_record_uid"] == _record_uid(PRECISE)
    # NOSERVED: no register row -> every register-derived field folds to ''.
    assert rows[NOSERVED]["activity_description"] == ""
    assert rows[NOSERVED]["activity_description_en"] == ""
    assert rows[NOSERVED]["bolagsverket_source_record_uid"] == ""
    assert rows[NOSERVED]["legal_form_label_en"] == ""
```

Rename `test_legal_name_comes_from_company_info` to `test_legal_name_comes_from_the_main_row` (body unchanged). In `test_presence_flags_come_from_the_child_tables` replace the final loop with:

```python
    for company in (COARSE, PRECISE, NOSERVED, POSTAL_BOX):
        assert rows[company]["has_address"] == 1
    # has_description is the folded description, whatever its language.
    assert rows[COARSE]["has_description"] == 1
    assert rows[PRECISE]["has_description"] == 1
    for company in (NOSERVED, POSTAL_BOX, NOADDRESS):
        assert rows[company]["has_description"] == 0
```

In `tests/test_clickhouse_migrations.py` append `"000391_corpscout_se_companies_serving_basic_info",` after the `000390` entry of the migrations tuple.

- [x] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_se_companies_serving_mv.py tests/test_se_companies_serving_sql.py -q`
Expected: the mv tests FAIL with `FileNotFoundError` for 000391; the SQL test FAILS (the builder still selects from `se_company_info`, which the script no longer creates).

- [x] **Step 3: The shared uid helper**

Append to `src/dagster_v3/defs/se_company/common.py`:

```python
def bolagsverket_record_uid_sql(alias: str) -> str:
    """The Bolagsverket register row's company-source-record uid, as SQL over `alias`.

    The one expression the basic-info Bolagsverket extractor writes as source_record_uid
    and the serving view renders as bolagsverket_source_record_uid, so the two can never
    drift: sha256 of the fixed envelope, the register's source_record_id and its
    lower-cased payload hash.
    """
    return (
        "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_bolagsverket', "
        f"'\\nregistry_company\\n', {alias}.source_record_id, '\\n', lowerUTF8({alias}.source_payload_hash)))))"
    )
```

In `src/dagster_v3/defs/se_company/basic_info/bolagsverket.py` replace the `BOLAGSVERKET_RECORD_UID_SQL = (...)` literal (lines 19-22) with:

```python
from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql

BOLAGSVERKET_RECORD_UID_SQL = bolagsverket_record_uid_sql("register")
```

(keep the import with the other imports at the top of the file).

- [x] **Step 4: Re-base the builder**

In `src/dagster_v3/defs/sweden_company/companies_current.py`:

Docstring line 5: `-- without paying the FINAL merges on `se_company_address`/`se_company_basic_info`, the served-view`. Docstring lines 20-21 become: `- `legal_name`, `status`, the legal form and both descriptions come from `se_company_basic_info` FINAL, the folded basic-info row (slice 4, 2026-09-08); the register fields from `se_bolagsverket_companies`.`

Replace the constants block (lines 62-64) with:

```python
COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address"
BASIC_INFO_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_basic_info"
BOLAGSVERKET_TABLE = f"{CLICKHOUSE_DATABASE}.se_bolagsverket_companies"
CODE_LABELS_TABLE = f"{CLICKHOUSE_DATABASE}.se_code_labels"
SERVED_GEOCODES_TABLE = f"{CLICKHOUSE_DATABASE}.se_address_geocodes_served"
```

Add the import `from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql` below the geocode_store import.

Replace the block from the comment `# The registered-activity translation and the status-reason label, absorbed VERBATIM from the` through `STATUS_REASON_LABEL_JOIN = f"""..."""` (lines 193-212) with:

```python
# The register row behind the main row: deregistration reason (the status-reason code),
# the record identity the extractor hashes, and the register's own stamp. FINAL and the
# has_company scope inside the subquery, so the outer LEFT JOIN sees one row per company.
BOLAGSVERKET_JOIN = f"""LEFT JOIN (
    SELECT company_id, deregistration_reason, source_record_id, source_payload_hash, observed_at
    FROM {BOLAGSVERKET_TABLE} FINAL
    WHERE has_company = 1
  ) AS b ON b.company_id = i.company_id"""
# The curated dictionaries (se_code_labels): what the legal-form code and the
# deregistration reason are called. argMax(version) so a re-seeded label wins.
LEGAL_FORM_LABEL_JOIN = f"""LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
    FROM {CODE_LABELS_TABLE}
    WHERE code_type = 'legal_form'
    GROUP BY code
  ) AS lf ON lf.code = ifNull(i.legal_form_code, '')"""
STATUS_REASON_LABEL_JOIN = f"""LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM {CODE_LABELS_TABLE}
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(b.deregistration_reason, '')"""
```

In `build_se_companies_serving_sql`, replace the inner SELECT's first twelve projections (from `i.company_id AS company_id,` through `ifNull(c.updated_from_raw_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,`) with:

```python
    i.company_id AS company_id,
    i.legal_name AS legal_name,
    toString(i.status) AS status,
    ifNull(i.legal_form_code, '') AS legal_form_code,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    ifNull(i.description_sv, '') AS activity_description,
    if(ifNull(i.description_language, '') = 'en', ifNull(i.description, ''), '') AS activity_description_en,
    ifNull(b.deregistration_reason, '') AS status_reason,
    ifNull(sr.label_en, '') AS status_reason_label_en,
    if(ifNull(b.company_id, '') = '', '', ifNull({bolagsverket_record_uid_sql('b')}, '')) AS bolagsverket_source_record_uid,
    ifNull(b.observed_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,
```

Replace the four description/identity arms:

```python
    toUInt8(i.description_source = 'esef') AS desc_esef,
    toUInt8(i.lei IS NOT NULL) AS has_lei,
    toUInt8(i.wikidata_id IS NOT NULL) AS has_wikidata,
    toUInt8(i.description_source = 'wikidata') AS desc_wikidata,
```

Replace the FROM block:

```python
  FROM {BASIC_INFO_TABLE} AS i FINAL
  {BOLAGSVERKET_JOIN}
  {LEGAL_FORM_LABEL_JOIN}
  {STATUS_REASON_LABEL_JOIN}
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
```

`has_description` stays `toUInt8(i.description IS NOT NULL)`. Nothing else in the SELECT changes.

- [x] **Step 5: Write migration 000391 from the builder**

Run from `corpscout/services/dagster_v3` (writes both files):

```bash
uv run python - <<'EOF'
from pathlib import Path
from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql
mig = Path.cwd().parents[1] / "clickhouse" / "migrations"  # cwd is corpscout/services/dagster_v3
up = f"""CREATE DATABASE IF NOT EXISTS corpscout;

-- Basic-info slice 4 (spec 2026-09-08-se-basic-info-4-retire-old-publisher): the serving
-- view's spine moves from the retired publisher's se_company_info to the folded
-- se_company_basic_info row. Legal name, status, legal form and both descriptions come from
-- the main row, the register fields (deregistration reason, record identity, stamp) from
-- se_bolagsverket_companies, the legal-form labels from se_code_labels. se_company_info,
-- se_companies and the text_translations join leave the view. Same column list, same
-- 000366 cadence, same staged swap as 000347 with a SYSTEM STOP VIEW guard.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py.

SYSTEM STOP VIEW corpscout.se_companies_serving;

CREATE MATERIALIZED VIEW corpscout.se_companies_serving_next
REFRESH EVERY 1 HOUR OFFSET 45 MINUTE
ENGINE = MergeTree
ORDER BY company_id
AS {build_se_companies_serving_sql()};

SYSTEM WAIT VIEW corpscout.se_companies_serving_next;

RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_retired,
    corpscout.se_companies_serving_next TO corpscout.se_companies_serving;
"""
down = """CREATE DATABASE IF NOT EXISTS corpscout;

-- Swap the pre-slice-4 render back under the serving name, restart its refresh, and
-- discard the basic-info render.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_basic_info_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_basic_info_discard;
"""
(mig / "000391_corpscout_se_companies_serving_basic_info.up.sql").write_text(up, encoding="utf-8")
(mig / "000391_corpscout_se_companies_serving_basic_info.down.sql").write_text(down, encoding="utf-8")
print("written")
EOF
```

Then confirm no comment line in either file contains a semicolon: `rg -n "^\s*--.*;" ../../clickhouse/migrations/000391_*` must print nothing.

- [x] **Step 6: Run the tests**

Run: `uv run pytest tests/test_se_companies_serving_mv.py tests/test_se_companies_serving_sql.py tests/test_se_companies_current_asset.py tests/test_se_company_basic_info_extractors_sql.py tests/test_clickhouse_migrations.py -q`
Expected: PASS, none skipped.

- [x] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/common.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/bolagsverket.py corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py corpscout/clickhouse/migrations/000391_corpscout_se_companies_serving_basic_info.up.sql corpscout/clickhouse/migrations/000391_corpscout_se_companies_serving_basic_info.down.sql corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py corpscout/services/dagster_v3/tests/test_se_companies_serving_sql.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse,dagster): 000391 re-bases se_companies_serving on se_company_basic_info

The serving view's spine is the folded basic-info row; register fields
come from se_bolagsverket_companies and labels from se_code_labels.
se_company_info, se_companies and the text_translations join leave the
view. Same columns, 000366 cadence, 000347 staged swap."
```
(append the commit footer)

---

### Task 2: Backoffice: header re-point, old pipeline page and dead modules removed

**Files:**
- Modify: `app/lib/se-company-shell.server.ts` (doc comments lines 16, 24, 56-57; `SHELL_INFO_SQL` line 67)
- Delete: `app/routes/admin-se-companies-pipeline.ts`, `app/routes/admin-se-companies-pipeline.test.ts`, `app/components/admin/se-company-info-pipeline.tsx`, `app/lib/se-company-info-pipeline.ts`, `app/lib/se-company-info-pipeline.test.ts`, `app/lib/se-company-info-pipeline.server.ts`, `app/lib/se-company-info-pipeline.server.test.ts`, `app/lib/se-company-info.server.ts`, `app/lib/se-info-field-values.ts`
- Modify: `app/routes.ts` (lines 189-192), `app/lib/dagster.server.ts` (lines 48-50, 61-64 and the comment at 71), `app/lib/dagster.server.test.ts` (lines 16-17 and 428), `app/lib/clickhouse.server.ts` (lines 140-150), `app/lib/se-company-info-filters.ts` (lines 56-57, 128), `app/lib/se-company-geocoding-list.server.ts` (lines 11, 19)

**Interfaces:**
- Consumes: `se_company_basic_info` columns `company_id, legal_name, legal_form_code, status, incorporation_date`.
- Produces: nothing new; every remaining reader of the deleted modules is deleted with them (verified: no other importers).

- [x] **Step 1: Delete the old pipeline page and dead modules**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice
git rm -q app/routes/admin-se-companies-pipeline.ts app/routes/admin-se-companies-pipeline.test.ts app/components/admin/se-company-info-pipeline.tsx app/lib/se-company-info-pipeline.ts app/lib/se-company-info-pipeline.test.ts app/lib/se-company-info-pipeline.server.ts app/lib/se-company-info-pipeline.server.test.ts app/lib/se-company-info.server.ts app/lib/se-info-field-values.ts
```

Check nothing else imported them: `rg -n "se-company-info-pipeline|se-company-info\.server|se-info-field-values" app` must print nothing but the comment in `app/lib/se-people-simple-sync.server.test.ts` line 5 (reword that comment to `mirrors se-people-simple-sync's own faked ClickHouse read`).

- [x] **Step 2: Route, constants, helper, comments**

`app/routes.ts`: delete the four lines

```ts
    // Not a page: the resource route behind the companies list's Pipeline
    // sheet. Its loader answers that sheet's fetcher and redirects anyone who
    // navigates to the URL back to the list.
    route("se/companies/pipeline", "routes/admin-se-companies-pipeline.ts"),
```

`app/lib/dagster.server.ts`: delete lines 48-50 (`SE_COMPANY_INFO_JOB`, `SE_COMPANY_INFO_REVIEW_JOB`, `SE_COMPANY_INFO_ASSET`) and lines 61-64 (the two-line comment plus `SE_COMPANY_INFO_SCHEDULE`, `SE_COMPANY_INFO_SENSOR`). In the people comment that follows, replace `unlike SE_COMPANY_INFO's pair above` with `unlike the address pipeline's schedule and sensor`.

`app/lib/dagster.server.test.ts`: remove `SE_COMPANY_INFO_SCHEDULE,` and `SE_COMPANY_INFO_SENSOR,` from the import list and replace line 428's `{ names: [SE_COMPANY_INFO_SCHEDULE, SE_COMPANY_INFO_SENSOR] }` with `{ names: ["se_company_info_weekly", "se_company_info_field_value_sensor"] }` (the test's roster is fake data; the names only have to match the roster).

`app/lib/clickhouse.server.ts`: delete `chInsertSeCompanyInfoFieldValues` and its doc comment (lines 140-150).

`app/lib/se-company-info-filters.ts` line 56-57 comment becomes `// SCB is the register base: the basic-info fold publishes nothing without a register legal name, so every listed company has this one.`; line 128 becomes ` * Description is NOT here: it is a column of se_company_basic_info itself, answered`.

`app/lib/se-company-geocoding-list.server.ts`: line 11 `se_company_address/se_company_info` becomes `se_company_address/se_company_basic_info`; line 19 `se_company_info -- so every query here filters` becomes `se_company_basic_info -- so every query here filters`.

- [x] **Step 3: Re-point the header**

`app/lib/se-company-shell.server.ts`: in `SHELL_INFO_SQL` replace `FROM corpscout.se_company_info AS i FINAL` with `FROM corpscout.se_company_basic_info AS i FINAL`. Comments: line 16 `(SHELL_REGISTER_SQL), and se_companies carries` unchanged; line 24 becomes ` * True when the shell came from \`se_company_basic_info\` -- the fold has published`; lines 56-57 become ` * one row type covers either source. \`FINAL\` on both: se_company_basic_info and` / ` * se_companies are ReplacingMergeTrees, and the newest version is the only`.

- [x] **Step 4: Typecheck and tests**

Run: `npm run typecheck && npx vitest run app/lib/dagster.server.test.ts app/lib/se-company-shell.server.test.ts app/lib/se-people-simple-sync.server.test.ts 2>&1 | tail -5`
Expected: typecheck clean; tests pass. If `se-company-shell.server.test.ts` does not exist, run `npx vitest run app/lib` instead.

- [x] **Step 5: Commit**

```bash
git add -A app/routes.ts app/lib/dagster.server.ts app/lib/dagster.server.test.ts app/lib/clickhouse.server.ts app/lib/se-company-info-filters.ts app/lib/se-company-geocoding-list.server.ts app/lib/se-company-shell.server.ts app/lib/se-people-simple-sync.server.test.ts
git commit -m "refactor(backoffice): company header reads se_company_basic_info; old info pipeline page and modules removed

The pipeline sheet launched the retired publisher's jobs; the info server
module and the field-values module had no importers left."
```
(append the commit footer; the `git rm` deletions from Step 1 are already staged)

---

### Task 3: Dagster: publisher, artifacts and their tests removed

**Files:**
- Rewrite: `src/dagster_v3/defs/se_company/info.py`
- Modify: `src/dagster_v3/defs/se_company/scb.py` (delete lines 41-188: from `TABLE = "se_company_info_scb"` through the end of `se_company_info_scb_clickhouse`; fix the docstring)
- Delete: `src/dagster_v3/defs/se_company/esef.py`, `src/dagster_v3/defs/se_company/wikidata.py`
- Rewrite: `src/dagster_v3/defs/se_company/info_rules.py`
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py` (lines 230-240)
- Delete: `tests/test_se_company_info.py`, `tests/test_se_company_info_clickhouse_local.py`, `tests/test_se_company_esef.py`, `tests/test_se_company_wikidata.py`, `tests/test_se_company_scb.py`
- Create: `tests/test_se_company_llm_support.py`
- Rewrite: `tests/test_se_company_info_rules.py`
- Modify: `tests/test_se_company_address_scb.py` (lines 66-75), `tests/se_company_ddl.py` (docstring lines 20-27)

**Interfaces:**
- Consumes: nothing new.
- Produces: `info.py` exports exactly `DESCRIPTION_PROMPT_VERSION`, `SE_COMPANY_INFO_OBSERVATION`, `OBSERVATION_FLUSH_ROWS`, `OBSERVATION_COLUMNS`, `LlmProfileConfig`, `DEFAULT_LLM_PROFILE`, `llm_api_key_variable`, `build_llm_client`, `DescriptionSuggestion`, `parse_description_suggestion`, `map_ordered`; `info_rules.py` exports `ArtifactRow`, `evidence_set_hash_for`, `_text`.

- [x] **Step 1: Write the kept-helper tests**

Create `tests/test_se_company_llm_support.py`:

```python
"""The LLM helpers se_company/info.py keeps for the basic-info LLM extractor (slice 4 of
the basic-info design retired the publisher around them)."""

import threading

import pytest

from dagster_v3.defs.se_company.info import (
    DESCRIPTION_PROMPT_VERSION,
    DescriptionSuggestion,
    LlmProfileConfig,
    build_llm_client,
    llm_api_key_variable,
    map_ordered,
    parse_description_suggestion,
)


def _profile(model: str, **overrides) -> LlmProfileConfig:
    return LlmProfileConfig(model=model, **overrides)


def test_parse_description_suggestion_validates_shape() -> None:
    suggestion = parse_description_suggestion(
        '{"description": "Alpha AB is a Swedish fintech company offering IT consulting.",'
        ' "description_sv": "Alpha AB aer ett svenskt fintechbolag.",'
        ' "language": "en", "rationale": "both"}'
    )
    assert isinstance(suggestion, DescriptionSuggestion) and suggestion.language == "en"
    assert suggestion.description_sv == "Alpha AB aer ett svenskt fintechbolag."
    for bad in (
        '{"description": "", "description_sv": "sv", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "", "language": "en", "rationale": ""}',
        # Both languages are required: a reply with only the English half would publish a
        # company whose Swedish column silently reverts to another source's text.
        '{"description": "ok", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "sv", "language": "EN"}',  # two letters, not a code
        '{"description": "ok", "description_sv": "sv", "language": "en", "extra": 1}',  # extra="forbid"
        "no json here",
        None,
    ):
        with pytest.raises(ValueError):
            parse_description_suggestion(bad)
    # v3: the prompt asks for both languages, so every v2 suggestion answers a request
    # this pipeline no longer makes (input_hash covers the prompt version).
    assert DESCRIPTION_PROMPT_VERSION == "se-company-info-description-v3"


def test_the_api_key_is_read_from_the_host_by_provider_name(monkeypatch) -> None:
    """The one thing the run config never carries. A provider whose key this host does
    not have fails with the variable's name, before any write and before any call."""
    assert llm_api_key_variable("deepseek") == "DEEPSEEK_API_KEY"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "   ")
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        build_llm_client(_profile("deepseek-v4-flash"), timeout_seconds=10)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    built = build_llm_client(_profile("deepseek-v4-flash", base_url="https://api.deepseek.com/"),
                             timeout_seconds=30)
    assert str(built.base_url).rstrip("/") == "https://api.deepseek.com"
    monkeypatch.delenv("OTHER_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OTHER_API_KEY"):
        build_llm_client(_profile("m", provider="other"), timeout_seconds=10)


def test_map_ordered_keeps_item_order_at_any_concurrency() -> None:
    """Results are consumed in item order however they finish; concurrency 1 runs inline."""
    barrier = threading.Barrier(2, timeout=10)

    def call(item: int) -> int:
        barrier.wait()  # both calls must be in flight together, or this deadlocks
        return item * 10

    assert list(map_ordered(call, [1, 2], concurrency=2)) == [10, 20]
    assert list(map_ordered(lambda item: item + 1, [3, 4, 5], concurrency=1)) == [4, 5, 6]
    assert list(map_ordered(lambda item: item, [], concurrency=4)) == []
```

Rewrite `tests/test_se_company_info_rules.py` to:

```python
from dagster_v3.defs.se_company.info_rules import evidence_set_hash_for


def test_evidence_set_hash_for_is_order_independent() -> None:
    # Must equal the address final's MATERIALIZED expression:
    # lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n')))).
    forward = evidence_set_hash_for(["a" * 64, "b" * 64])
    reverse = evidence_set_hash_for(["b" * 64, "a" * 64])
    assert forward == reverse
    assert len(forward) == 64 and forward == forward.lower()
```

In `tests/test_se_company_address_scb.py` replace `test_the_two_scb_assets_are_separate_and_write_separate_tables` with:

```python
def test_the_address_scb_asset_writes_its_own_table() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    address = graph.get(dg.AssetKey("se_company_address_scb_clickhouse"))
    assert address.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert address.group_name == "se_company_scb"
    assert address.metadata["table"] == "corpscout.se_company_address_scb"
    # Slice 4 retired the info artifact that used to share this module.
    assert dg.AssetKey("se_company_info_scb_clickhouse") not in graph.all_asset_keys
```

Delete the five test files:

```bash
git rm -q tests/test_se_company_info.py tests/test_se_company_info_clickhouse_local.py tests/test_se_company_esef.py tests/test_se_company_wikidata.py tests/test_se_company_scb.py
```

- [x] **Step 2: Run the new tests to verify the state (they pass against the untrimmed module; the address test fails)**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_se_company_llm_support.py tests/test_se_company_info_rules.py tests/test_se_company_address_scb.py -q`
Expected: the llm-support and rules tests PASS (the helpers already exist); `test_the_address_scb_asset_writes_its_own_table` FAILS on `graph.has(...)` because the info artifact still exists.

- [x] **Step 3: Trim `info.py` to the helpers**

Replace the whole of `src/dagster_v3/defs/se_company/info.py` with:

```python
"""LLM helpers shared by the basic-info LLM extractor (basic_info/llm.py).

This module used to be the Swedish company-information publisher; slice 4 of the basic-info
design (2026-09-08) retired that publisher, its jobs, its schedule and its field-value
sensor together with the se_company_info* tables. What stays is what the extractor imports:
the model profile config, the client factory keyed by provider, the description-answer
parser, the ordered concurrent map, and the names of the observation cache
(se_company_info_enrichment_observation) the extractor reads and writes through
common.py's observation helpers.
"""

import os
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import dagster as dg
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, ValidationError

DESCRIPTION_PROMPT_VERSION = "se-company-info-description-v3"
# The observation cache: one row per (company, input_hash) model answer, kept across the
# publisher's retirement because every row is a paid call the extractor can reuse.
SE_COMPANY_INFO_OBSERVATION = "se_company_info_enrichment_observation"
OBSERVATION_FLUSH_ROWS = 200
OBSERVATION_COLUMNS = ("suggestion_id", "company_id", "input_hash", "suggestion", "raw_response",
                       "model_provider", "model_name", "prompt_version", "prompt_tokens", "completion_tokens",
                       "source_run_id", "created_at")


class LlmProfileConfig(dg.Config):
    """The model this run may call, as run config -- never read from host env.

    The host contributes exactly one thing: the API key, looked up by provider name
    (``DEEPSEEK_API_KEY`` for provider ``deepseek``). Everything that decides what the
    call costs and what it says travels in the run config, so a run's own record shows
    which model wrote its descriptions and a caller can switch models without a
    deployment. ``prompt_version`` is part of the observation cache key, so changing it
    invalidates every stored suggestion rather than silently reusing answers to a
    different prompt.
    """

    provider: str = Field(default="deepseek", min_length=1, max_length=64)
    model: str = Field(default="deepseek-v4-flash", min_length=1, max_length=200)
    base_url: str = Field(default="https://api.deepseek.com", min_length=1, max_length=2_048)
    temperature: float = Field(default=0, ge=0, le=2)
    # deepseek-v4-flash is a reasoning model: reasoning_content counts against
    # max_tokens, and the answer carries two summaries.
    max_tokens: int = Field(default=6_000, ge=256, le=32_000)
    prompt_version: str = Field(default=DESCRIPTION_PROMPT_VERSION, min_length=1, max_length=120)
    # Model calls issued at once. 1 is the sequential path; the cap is deliberately low --
    # these are paid calls against one vendor account.
    concurrency: int = Field(default=1, ge=1, le=8)


# Today's production model, pinned here rather than left to the field defaults so that
# changing a default can never silently change what an automated run calls.
DEFAULT_LLM_PROFILE: dict[str, Any] = {
    "provider": "deepseek",
    "model": "deepseek-v4-flash",
    "base_url": "https://api.deepseek.com",
    "temperature": 0,
    "max_tokens": 6_000,
    "prompt_version": DESCRIPTION_PROMPT_VERSION,
    "concurrency": 1,
}


def llm_api_key_variable(provider: str) -> str:
    """The host environment variable holding this provider's key."""
    return f"{provider.upper()}_API_KEY"


def build_llm_client(profile: LlmProfileConfig, *, timeout_seconds: int) -> OpenAI:
    """The OpenAI-compatible client for ``profile``, or a clear failure.

    Called before any page is touched, so a run configured for a provider whose key this
    host does not carry fails without having written a row or spent a call.
    """
    variable = llm_api_key_variable(profile.provider)
    api_key = os.getenv(variable, "").strip()
    if not api_key:
        raise ValueError(
            f"No API key for LLM provider {profile.provider!r}: set {variable} on the "
            "Dagster host, or run with resolve_multi_source_with_llm: false")
    return OpenAI(base_url=profile.base_url.rstrip("/"), api_key=api_key,
                  timeout=float(timeout_seconds), max_retries=2)


class DescriptionSuggestion(BaseModel):
    """One model answer: the same company description in both published languages.

    Both are required. A reply carrying only the English half would publish a company
    whose Swedish column silently falls back to another source's text. ``language``
    describes ``description`` (always "en" in practice); ``description_sv`` is Swedish
    by definition, so it carries no language of its own.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    description: str = Field(min_length=1, max_length=2000)
    description_sv: str = Field(min_length=1, max_length=2000)
    language: str = Field(min_length=2, max_length=2, pattern="^[a-z]{2}$")
    rationale: str = Field(default="", max_length=2000)


def parse_description_suggestion(content: str | None) -> DescriptionSuggestion:
    if content is None:
        raise ValueError("Description request returned no content")
    start, end = content.find("{"), content.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"Description request did not return a JSON object: {content[:160]!r}")
    try:
        return DescriptionSuggestion.model_validate_json(content[start : end + 1])
    except ValidationError as exc:
        raise ValueError(f"Description response failed validation: {exc}") from exc


T = TypeVar("T")
R = TypeVar("R")


def map_ordered(call: Callable[[T], R], items: Sequence[T], *, concurrency: int) -> Iterator[R]:
    """``call`` over every item, at most ``concurrency`` at a time, results in item order.

    concurrency=1 runs inline with no thread pool at all, so the default path is
    literally the sequential one -- and the caller's own ordering (observation rows,
    the flush every OBSERVATION_FLUSH_ROWS, the published row order) holds unchanged at
    every concurrency, because results are consumed in item order however they finish.
    """
    if concurrency <= 1 or len(items) <= 1:
        for item in items:
            yield call(item)
        return
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="se_company_info_llm") as pool:
        yield from pool.map(call, items)
```

- [x] **Step 4: Split `scb.py`, delete the other two artifacts, trim `info_rules.py`, drop the leaves**

`src/dagster_v3/defs/se_company/scb.py`: delete everything from the line `TABLE = "se_company_info_scb"` (line 41) up to but not including `ADDRESS_TABLE = "se_company_address_scb"` (line 191). Keep `GROUP_NAME`, `DATABASE`, the imports. Replace the module docstring with:

```python
"""The SCB address artifact of the Swedish company register.

Input (source layer): corpscout.se_company_addresses_current, the register's address
snapshot. Writes corpscout.se_company_address_scb: the address SCB holds for each company
(visiting or postal -- the register does not distinguish), one source value apart from the
Bolagsverket artifact; a new version is written only when evidence_hash changes.

The info artifact (se_company_info_scb) that used to share this module was retired with
the old publisher in basic-info slice 4 (2026-09-08).
Downstream: address_legacy.py (field precedence bolagsverket > scb).
"""
```

Then check for now-unused imports: `rg -n "SE_COMPANY_ID_PATTERN|datetime|UTC" src/dagster_v3/defs/se_company/scb.py` and drop any import the remaining code does not use.

```bash
git rm -q src/dagster_v3/defs/se_company/esef.py src/dagster_v3/defs/se_company/wikidata.py
```

Replace the whole of `src/dagster_v3/defs/se_company/info_rules.py` with:

```python
"""Pure helpers the address rules share with the retired information merge.

`merge_company_info`, the field values and the rest of the old publisher's rules went with
basic-info slice 4 (2026-09-08); what stays is what address_rules.py and address_legacy.py
import: the artifact row shape, the evidence-set hash and the text normaliser.
"""

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ArtifactRow:
    source: str
    source_record_uid: str
    evidence_hash: str
    observed_at: datetime
    values: Mapping[str, Any]


def evidence_set_hash_for(evidence_hashes: Sequence[str]) -> str:
    """Sha256 hex of the sorted hashes joined by ``\\n``.

    Must equal the address final's MATERIALIZED ``evidence_set_hash`` column:
    ``lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x),
    evidence_hashes)), '\\n'))))``. ``sorted()`` on strings matches ClickHouse's
    default ascending ``arraySort``, and ``hexdigest()`` is already lowercase.
    """
    return hashlib.sha256("\n".join(sorted(evidence_hashes)).encode()).hexdigest()


def _text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None
```

`src/dagster_v3/defs/common/clickhouse_checks.py`: delete lines 230-240 (the `# se_company — the information pilot` comment and the four `ClickhouseLeaf("se_company_info_*...` entries).

`tests/se_company_ddl.py` docstring lines 21-23 become: `The se_company layer's tables live in several migrations (000297 declares the observation table since slice 4 retired the info tables, 000307 the address ones), so the helpers below locate the creating file`.

- [x] **Step 5: Check definitions and run the se_company tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs 2>&1 | tail -2 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_se_company_llm_support.py tests/test_se_company_info_rules.py tests/test_se_company_address_scb.py tests/test_se_company_address_rules.py tests/test_se_company_address.py tests/test_se_company_common.py tests/test_se_company_basic_info_llm.py tests/test_clickhouse_leaf_checks.py tests/test_se_company_layout.py tests/test_se_company_basic_info_tables.py -q`
Expected: "All definitions loaded successfully." and PASS. If a test still imports a deleted name, it is one of the files this task lists; fix it, do not restore the module.

Also: `rg -n "se_company\.(esef|wikidata)\b|merge_company_info|apply_field_values|FieldValueRow|se_company_info_clickhouse|se_company_info_job" src tests` must print nothing except the string literals inside `tests/test_se_company_common.py` (synthetic job names in the ledger-sensor tests, which stay).

- [x] **Step 6: Commit**

```bash
git add -A corpscout/services/dagster_v3/src/dagster_v3/defs/se_company corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py corpscout/services/dagster_v3/tests/test_se_company_llm_support.py corpscout/services/dagster_v3/tests/test_se_company_info_rules.py corpscout/services/dagster_v3/tests/test_se_company_address_scb.py corpscout/services/dagster_v3/tests/se_company_ddl.py
git commit -m "refactor(dagster): retire the se_company_info publisher, its artifacts and field values

info.py keeps only the LLM helpers the basic-info extractor imports; the
SCB module keeps its address artifact; esef.py, wikidata.py, the merge
rules, the jobs, the weekly schedule, the field-value sensor and the four
freshness leaves are gone. The observation cache table stays."
```
(append the commit footer; the `git rm` deletions are staged already)

---

### Task 4: Ledger: the dropped tables' DDL leaves the migration files

**Files:**
- Modify: `corpscout/clickhouse/migrations/000297_corpscout_se_company_info.{up,down}.sql`
- Modify (empty): `000299_corpscout_se_company_info_sole_traders`, `000300_corpscout_se_company_info_scb_english`, `000301_corpscout_se_company_info_description_sv`, `000304_corpscout_se_company_info_llm_enhanced`, `000306_corpscout_se_company_info_legal_form_label`, `000365_corpscout_se_company_info_esef_enrichment`, `000371_corpscout_se_company_info_field_value` (both up and down)
- Modify: `tests/test_clickhouse_migrations.py` (`EMPTIED_MIGRATIONS`; delete `test_se_company_info_esef_enrichment_migration_is_additive` and `test_se_company_info_field_value_replaces_the_correction_ledger`)

**Interfaces:** none; text only.

- [x] **Step 1: Extend `EMPTIED_MIGRATIONS` and delete the two tests (failing state)**

In `tests/test_clickhouse_migrations.py` replace the `EMPTIED_MIGRATIONS` set with:

```python
EMPTIED_MIGRATIONS = {
    "000052_corpscout_lei_wikidata_companies_view",
    "000111_corpscout_dns_axfr_observations",
    "000121_corpscout_commoncrawl_domain_hostname_axfr_sync",
    "000218_corpscout_no_contract_awards",
    # Basic-info slice 4 (2026-09-08): the se_company_info* tables were dropped by hand and
    # their DDL left these files. 000297 is not here: it still declares the observation
    # cache table the LLM extractor keeps.
    "000299_corpscout_se_company_info_sole_traders",
    "000300_corpscout_se_company_info_scb_english",
    "000301_corpscout_se_company_info_description_sv",
    "000304_corpscout_se_company_info_llm_enhanced",
    "000306_corpscout_se_company_info_legal_form_label",
    "000365_corpscout_se_company_info_esef_enrichment",
    "000371_corpscout_se_company_info_field_value",
    "000379_corpscout_se_company_basic_info_precedence",
}
```

Delete the functions `test_se_company_info_esef_enrichment_migration_is_additive` and `test_se_company_info_field_value_replaces_the_correction_ledger` entirely. Update the comment above the set (`Entries whose objects left the ledger on 2026-09-03.` becomes `Entries whose objects left the ledger by hand (2026-09-03 and 2026-09-08).`).

Run: `uv run pytest tests/test_clickhouse_migrations.py -q -k "create_databases_and_tables or have_down_files"`
Expected: FAIL for the seven files (they still hold DDL).

- [x] **Step 2: Empty the seven files**

For each of the seven names, both the up and the down file become this body (the tests require exactly one statement line, and the file must end on it, so the comments go above):

```sql
-- Basic-info slice 4 (2026-09-08): this migration's objects (the retired se_company_info*
-- tables or their columns) were dropped by hand on the server and their DDL left this
-- file per the dev-phase ledger policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
```

and the same text for each down file. A shell loop from `corpscout/clickhouse/migrations`:

```bash
for m in 000299_corpscout_se_company_info_sole_traders 000300_corpscout_se_company_info_scb_english 000301_corpscout_se_company_info_description_sv 000304_corpscout_se_company_info_llm_enhanced 000306_corpscout_se_company_info_legal_form_label 000365_corpscout_se_company_info_esef_enrichment 000371_corpscout_se_company_info_field_value; do
  for side in up down; do
    printf '%s\n' "-- Basic-info slice 4 (2026-09-08): this migration's objects (the retired se_company_info*" "-- tables or their columns) were dropped by hand on the server and their DDL left this" "-- file per the dev-phase ledger policy. The file stays for history." "CREATE DATABASE IF NOT EXISTS corpscout;" > "$m.$side.sql"
  done
done
```

- [x] **Step 3: Shrink 000297 to the observation table**

Rewrite `000297_corpscout_se_company_info.up.sql` as: the `CREATE DATABASE IF NOT EXISTS corpscout;` line, a comment `-- Sweden company information (2026-08). Basic-info slice 4 (2026-09-08) dropped the five` / `-- se_company_info* tables by hand and their DDL left this file; the observation cache` / `-- below stays: every row is a paid model answer the basic-info LLM extractor reuses.`, then the unchanged `CREATE TABLE IF NOT EXISTS corpscout.se_company_info_enrichment_observation (...) ENGINE = MergeTree ORDER BY (company_id, input_hash, created_at);` block copied verbatim from the current file (lines 156-173). Rewrite the down file as `CREATE DATABASE IF NOT EXISTS corpscout;` followed by `DROP TABLE IF EXISTS corpscout.se_company_info_enrichment_observation;`.

- [x] **Step 4: Run the ledger tests and the DDL-helper users**

Run: `uv run pytest tests/test_clickhouse_migrations.py tests/test_se_company_address.py tests/test_se_company_address_tables.py tests/test_se_company_address_layout.py tests/test_se_company_address_bolagsverket.py tests/test_se_company_basic_info_tables.py tests/test_sweden_company_source_tables.py tests/test_se_company_layout.py -q`
Expected: PASS. (`tests/test_se_company_common.py` line 207 reads the observation table by name; it still exists.)

- [x] **Step 5: Commit**

```bash
git add corpscout/clickhouse/migrations/000297_corpscout_se_company_info.up.sql corpscout/clickhouse/migrations/000297_corpscout_se_company_info.down.sql corpscout/clickhouse/migrations/000299_corpscout_se_company_info_sole_traders.*.sql corpscout/clickhouse/migrations/000300_corpscout_se_company_info_scb_english.*.sql corpscout/clickhouse/migrations/000301_corpscout_se_company_info_description_sv.*.sql corpscout/clickhouse/migrations/000304_corpscout_se_company_info_llm_enhanced.*.sql corpscout/clickhouse/migrations/000306_corpscout_se_company_info_legal_form_label.*.sql corpscout/clickhouse/migrations/000365_corpscout_se_company_info_esef_enrichment.*.sql corpscout/clickhouse/migrations/000371_corpscout_se_company_info_field_value.*.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "chore(clickhouse): the retired se_company_info tables' DDL leaves the ledger files

Per the dev-phase policy: dropped by hand, files kept for history, listed
in EMPTIED_MIGRATIONS. 000297 keeps the observation cache table."
```
(append the commit footer)

---

### Task 5: Drop script, whole-suite verification, merge

**Files:**
- Create: `corpscout/clickhouse/operations/se_company_info_retire.md`

- [x] **Step 1: Write the gated drop script**

```markdown
# Retire the se_company_info tables (basic-info slice 4)

Owner-run, on the companycollect ClickHouse, AFTER migration 000391 is applied and the
dagster_v3 deploy without the old publisher is live. Never drops
`se_company_info_enrichment_observation` (the LLM cache).

## Gates (every one must hold before the DROPs)

```sql
-- 1. The serving view no longer names the old table.
SELECT countIf(create_table_query LIKE '%se_company_info%') = 0 AS view_rebased
FROM system.tables WHERE database = 'corpscout' AND name = 'se_companies_serving';
-- 2. No view or materialized view references any of the five tables.
SELECT count() = 0 AS no_readers FROM system.tables
WHERE database = 'corpscout' AND engine IN ('View', 'MaterializedView')
  AND (create_table_query LIKE '%se_company_info_scb%' OR create_table_query LIKE '%se_company_info_esef%'
       OR create_table_query LIKE '%se_company_info_wikidata%' OR create_table_query LIKE '%se_company_info_field_value%'
       OR create_table_query LIKE '%corpscout.se_company_info %' OR create_table_query LIKE '%corpscout.se_company_info\n%');
-- 3. Row counts recorded before the drop (paste into the retirement note).
SELECT table, sum(rows) AS rows FROM system.parts WHERE database = 'corpscout' AND active
  AND table IN ('se_company_info', 'se_company_info_scb', 'se_company_info_esef', 'se_company_info_wikidata', 'se_company_info_field_value')
GROUP BY table ORDER BY table;
```

Dagster gates, from the webserver: `se_company_info_weekly` and
`se_company_info_field_value_sensor` are absent or STOPPED, and the asset
`se_company_info_clickhouse` is not in the code location.

## Drops

```sql
DROP TABLE IF EXISTS corpscout.se_company_info_field_value;
DROP TABLE IF EXISTS corpscout.se_company_info_wikidata;
DROP TABLE IF EXISTS corpscout.se_company_info_esef;
DROP TABLE IF EXISTS corpscout.se_company_info_scb;
DROP TABLE IF EXISTS corpscout.se_company_info;
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;
```

The last statement discards 000391's pre-swap render once the new view has served for a
full refresh; leave it if a rollback is still on the table.
```

- [x] **Step 2: Full unit suite and the integration files this branch touches**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest -q -m "not integration" --deselect tests/test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair -p no:cacheprovider 2>&1 | tail -8`
Expected: only the four failures already present on main (backfill-policy, DuckDB executemany, NACE staged-flow, Lantmäteriet credentials); nothing new.

Run: `uv run pytest tests/test_se_companies_serving_sql.py tests/test_se_company_address_clickhouse_local.py tests/test_se_company_basic_info_clickhouse_local.py tests/test_se_company_basic_info_extractors_clickhouse_local.py -q`
Expected: PASS, none skipped.

Backoffice: `cd corpscout/services/backoffice && npm run typecheck && npx vitest run 2>&1 | tail -4`
Expected: clean and green (live tests that need a server skip as they do on main).

- [x] **Step 3: Commit and merge**

```bash
git add corpscout/clickhouse/operations/se_company_info_retire.md
git commit -m "docs(clickhouse): gated owner-run drop of the retired se_company_info tables"
git checkout main && git merge --no-ff se-basic-info-4-retire-publisher -m "Merge branch 'se-basic-info-4-retire-publisher'"
```
(append the commit footer to both messages)

---

### Task 6: Prod rollout (each step owner-named)

- [x] **Step 1: Stop the old instigators** (ids read on 2026-09-08; re-read them if the code location was redeployed since):

```bash
curl -s -X POST http://dagster:3000/graphql -H 'Content-Type: application/json' --data '{"query":"mutation { stopRunningSchedule(id: \"bd4fbf83b45cfd7203191dc2fa631b95ad79d2b7::ea43949cc907c1b5b6e72d3051f5fd05d6ce799d\") { __typename ... on ScheduleStateResult { scheduleState { status } } ... on PythonError { message } } }"}'
curl -s -X POST http://dagster:3000/graphql -H 'Content-Type: application/json' --data '{"query":"mutation { stopSensor(id: \"cd793c1a633e84f7b4c90cead510eed25e5f088f::4049bb3a6f4dcc444b8eb9a6e4ec1e38a4e69c63\") { __typename ... on StopSensorMutationResult { instigationState { status } } ... on PythonError { message } } }"}'
```

Expected: both report `STOPPED`.

- [x] **Step 2: Deploy dagster_v3.** The local defs-state was refreshed on 2026-09-08 at commit `956949b7e`. First run, from the repo root, `git log --oneline 956949b7e..HEAD -- 'corpscout/services/dagster_v3/src/dagster_v3/defs/*/dbt' 'corpscout/services/dagster_v3/src/dagster_v3/defs/*/*/dbt'`; if it lists anything, run the two `dbt parse` commands and `uv run --frozen --no-sync dg utils refresh-defs-state` first. Then from `ansible/`: `ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml`, capture the exit code. Verify: the GraphQL `assetOrError` for `se_company_info_clickhouse` returns `AssetNotFoundError`, and for `se_company_basic_info_fold` returns the asset.

- [x] **Step 3: Apply 000391** from `corpscout/`: `make clickhouse-migrate-up-one` (one view rebuild, about three minutes). Verify:

```sql
SELECT (SELECT count() FROM corpscout.se_companies_serving) AS served, (SELECT count() FROM corpscout.se_company_basic_info FINAL) AS main;
SELECT countIf(create_table_query LIKE '%se_company_basic_info%') AS rebased FROM system.tables WHERE database = 'corpscout' AND name = 'se_companies_serving';
```

Expected: `served = main` and `rebased = 1`. Then open `http://localhost:5183/admin/se/companies`, the geocoding list, and `http://localhost:5183/admin/se/company/5020077862/info`: list rows render with names and status, the header shows Handelsbanken with its legal form.

- [x] **Step 4: Run the drop script** (`corpscout/clickhouse/operations/se_company_info_retire.md`), gates first, then the DROPs. Record the row counts in the memory note.

---

Rollout record (2026-09-08): instigators stopped 10:05 UTC, deploy 10:0x UTC, 000391 applied by the
owner; its `SYSTEM WAIT VIEW` outran the migrate driver's 300 s read timeout (the refresh took 27 min
right after the full re-fold), so the RENAME was run by hand and the ledger forced to 391. Drops ran
10:35 UTC: retired render, then se_company_info_field_value (2 rows), _wikidata (3,119), _esef (675),
_scb (3,749,662), se_company_info (3,779,090). se_company_info_enrichment_observation kept (2,296 rows).
