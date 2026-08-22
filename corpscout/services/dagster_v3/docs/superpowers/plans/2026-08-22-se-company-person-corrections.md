# Sweden Company-Person Corrections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a reviewer correct any Sweden company person (merge, split, reassign, override, approve/reject a model suggestion, set/remove a role) through an append-only ClickHouse ledger that the Dagster pipeline applies on every run, with model output recorded as suggestions and a sensor that re-runs only the touched companies.

**Architecture:** Two new ClickHouse tables (`se_company_person_correction`, `se_company_person_suggestion`) plus provenance columns on `se_company_person` / `se_company_person_role`. `normalization.py` loads the effective ledger per company, applies it after deterministic/LLM grouping with precedence *correction > suggestion > deterministic*, skips stale rows, and records applied ids; `roles.py` applies role corrections in SQL. A sensor scoped to touched `company_ids` runs a review job. The backoffice validates and appends ledger rows through the existing correction-writer role and shows one ClickHouse-backed review page.

**Tech Stack:** ClickHouse (golang-migrate files under `corpscout/clickhouse/migrations/`), Dagster 1.13 assets/sensors in `dagster_v3/defs/company_people/`, pytest via `uv run`, React Router 8 + vitest in `corpscout/services/backoffice`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-08-21-se-company-person-corrections-design.md`

## Global Constraints

- Ledger and suggestion tables are append-only `MergeTree`; rows are never updated or deleted. Published tables stay `ReplacingMergeTree(updated_at)`; a "removal" is a new version row (`merged_into_person_id`, `is_current = 0`), never a mutation.
- Every correction is company-scoped: `company_id` is a 10-digit Swedish orgnr (`normalized_company_ids` in `draft.py` validates this).
- Corrections reference `person_id` / `draft_id` only — never names.
- Precedence for a field: correction > approved or current suggestion > deterministic. Kind application order: `merge_persons`, `reassign_draft`, `split_person`, `approve_suggestion`, `reject_suggestion`, `override_field`, then `set_role`, `remove_role`. `undo` rows only supersede; they are never applied themselves.
- A stale correction (spec §4.3) is never applied, never deleted, always counted in asset metadata.
- `decided_by = 'backoffice'`; the backoffice writes only through `CLICKHOUSE_WRITE_USER` (role `corpscout_person_correction_writer`), which gets INSERT on the two new tables and nothing else.
- Multi-source companies still auto-publish model output when no correction exists (review is an override, not a gate).
- Hash conventions: `correction_set_hash` = `lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(id -> toString(id), ids)), '\n'))))` — sorted *strings*, so Python can reproduce it with `sorted(str(id) ...)`. `evidence_hash` is the published `se_company_person.draft_set_hash` read from ClickHouse; Python never recomputes `draft_set_hash`.
- Dagster: no `from __future__ import annotations` in defs modules; `uv run` for every command; `uv run dg check defs` green before each commit. Python 3.14.
- Commits by explicit path only (shared working tree carries unrelated uncommitted work). Conventional Commits. Never commit the backoffice admin WIP (`app/routes/admin-*`, `app/lib/sweden-*`, …) as part of this plan unless a task names the file.
- Backoffice: named ClickHouse params only; route components never import values from `.server` modules; `pnpm typecheck` and `npx vitest run` green before each commit.
- All migrations: `CREATE DATABASE IF NOT EXISTS corpscout;` first line, only the `corpscout` database, a `.down.sql` twin, no `;` inside `--` comments, and the file name appended to `EXPECTED_MIGRATIONS` / `EXPECTED_ACCESS_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.

---

### Task 1: Commit the pending role-mapping work this plan builds on

Migration `000294` and the `employee_board_representative` mapping are already registered in the test suite but uncommitted. Everything below appends after them.

**Files:**
- Commit (no edits): `corpscout/clickhouse/migrations/000294_corpscout_employee_board_representative_role.up.sql`, `.down.sql`, `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/roles.py`, `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`, `corpscout/services/dagster_v3/tests/test_se_company_person_roles.py`

- [ ] **Step 1: Run the two affected test modules**

Run (from `corpscout/services/dagster_v3`): `uv run pytest tests/test_clickhouse_migrations.py tests/test_se_company_person_roles.py -q`
Expected: PASS

- [ ] **Step 2: Commit by explicit path**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000294_corpscout_employee_board_representative_role.up.sql \
        corpscout/clickhouse/migrations/000294_corpscout_employee_board_representative_role.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/roles.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_roles.py
git commit -m "feat(company_people): add employee_board_representative role and exact Bolagsverket role mappings"
```

---

### Task 2: Migrations 000295/000296 and the `corrections.py` schema constants

**Files:**
- Create: `corpscout/clickhouse/migrations/000295_corpscout_se_company_person_corrections.up.sql`, `.down.sql`
- Create: `corpscout/clickhouse/migrations/000296_corpscout_se_company_person_correction_writer_grants.up.sql`, `.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py:306-315` (registries)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py`

**Interfaces:**
- Produces: `CORRECTION_TABLE`, `SUGGESTION_TABLE`, `CORRECTION_COLUMNS`, `SUGGESTION_COLUMNS`, `PERSON_CORRECTION_KINDS`, `ROLE_CORRECTION_KINDS`, `UNDO_KIND`, `CORRECTION_KINDS`, `ZERO_HASH` in `corrections.py`; migration adds `correction_ids`, `correction_set_hash`, `suggestion_id`, `merged_into_person_id` to `se_company_person` and `correction_ids` to `se_company_person_role`.

- [ ] **Step 1: Write the failing contract test**

```python
# tests/test_se_company_person_corrections.py
from pathlib import Path

from dagster_v3.defs.company_people.corrections import (
    CORRECTION_COLUMNS,
    CORRECTION_KINDS,
    PERSON_CORRECTION_KINDS,
    ROLE_CORRECTION_KINDS,
    SUGGESTION_COLUMNS,
    UNDO_KIND,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _sql(name: str) -> str:
    return (MIGRATIONS_DIR / name).read_text(encoding="utf-8")


def test_correction_and_suggestion_tables_match_insert_contracts() -> None:
    sql = _sql("000295_corpscout_se_company_person_corrections.up.sql")
    down = _sql("000295_corpscout_se_company_person_corrections.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion" in sql
    assert sql.count("ENGINE = MergeTree") == 2
    for column in CORRECTION_COLUMNS:
        assert f"    {column} " in sql
    for column in SUGGESTION_COLUMNS:
        assert f"    {column} " in sql
    assert "CONSTRAINT valid_payload CHECK isValidJSON(payload)" in sql
    assert "ALTER TABLE corpscout.se_company_person" in sql
    assert "correction_ids Array(UUID) DEFAULT []" in sql
    assert "correction_set_hash FixedString(64) MATERIALIZED" in sql
    assert "arraySort(arrayMap(id -> toString(id), correction_ids))" in sql
    assert "suggestion_id Nullable(UUID)" in sql
    assert "merged_into_person_id Nullable(UUID)" in sql
    assert "ALTER TABLE corpscout.se_company_person_role" in sql

    assert "DROP TABLE IF EXISTS corpscout.se_company_person_suggestion" in down
    assert "DROP TABLE IF EXISTS corpscout.se_company_person_correction" in down
    assert "DROP COLUMN IF EXISTS correction_ids" in down


def test_writer_grants_are_insert_only() -> None:
    sql = _sql("000296_corpscout_se_company_person_correction_writer_grants.up.sql")
    down = _sql("000296_corpscout_se_company_person_correction_writer_grants.down.sql")

    assert (
        "GRANT INSERT ON corpscout.se_company_person_correction\n"
        "TO corpscout_person_correction_writer"
    ) in sql
    assert (
        "GRANT INSERT ON corpscout.se_company_person_suggestion\n"
        "TO corpscout_person_correction_writer"
    ) in sql
    assert "GRANT SELECT" not in sql
    assert "GRANT ALL" not in sql
    assert "CREATE USER" not in sql
    assert "REVOKE INSERT ON corpscout.se_company_person_correction" in down


def test_correction_kinds_are_closed_and_ordered() -> None:
    assert PERSON_CORRECTION_KINDS == (
        "merge_persons",
        "reassign_draft",
        "split_person",
        "approve_suggestion",
        "reject_suggestion",
        "override_field",
    )
    assert ROLE_CORRECTION_KINDS == ("set_role", "remove_role")
    assert UNDO_KIND == "undo"
    assert CORRECTION_KINDS == frozenset(
        (*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, UNDO_KIND)
    )
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_se_company_person_corrections.py -q`
Expected: FAIL with `ModuleNotFoundError: ... corrections`

- [ ] **Step 3: Write migration 000295**

`corpscout/clickhouse/migrations/000295_corpscout_se_company_person_corrections.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only ledger of human decisions about Sweden company people. The
-- pipeline applies these as input on every run. Rows are never updated. A
-- later row names an earlier one in supersedes_correction_id to retire it.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    subject_person_id UUID,
    target_person_id Nullable(UUID),
    draft_ids Array(UUID),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, subject_person_id, created_at, correction_id);

-- One row per model call per person. The newest created_at for a
-- (person_id, input_hash) pair is the current suggestion unless a correction
-- approves an older one.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion
(
    suggestion_id UUID,
    company_id String,
    person_id UUID,
    input_hash FixedString(64),
    draft_ids Array(UUID),
    suggestion String,
    raw_response String,
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT valid_suggestion CHECK isValidJSON(suggestion)
)
ENGINE = MergeTree
ORDER BY (company_id, person_id, input_hash, created_at);

-- Provenance of applied corrections and the published suggestion. A merged
-- person keeps its evidence rows and points at the surviving person.
ALTER TABLE corpscout.se_company_person
    ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] AFTER draft_ids,
    ADD COLUMN IF NOT EXISTS correction_set_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(arrayStringConcat(
            arraySort(arrayMap(id -> toString(id), correction_ids)), '\n'
        )))) AFTER correction_ids,
    ADD COLUMN IF NOT EXISTS suggestion_id Nullable(UUID) AFTER correction_set_hash,
    ADD COLUMN IF NOT EXISTS merged_into_person_id Nullable(UUID) AFTER suggestion_id;

ALTER TABLE corpscout.se_company_person_role
    ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] AFTER person_draft_ids;
```

`.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_person_role
    DROP COLUMN IF EXISTS correction_ids;

ALTER TABLE corpscout.se_company_person
    DROP COLUMN IF EXISTS merged_into_person_id,
    DROP COLUMN IF EXISTS suggestion_id,
    DROP COLUMN IF EXISTS correction_set_hash,
    DROP COLUMN IF EXISTS correction_ids;

DROP TABLE IF EXISTS corpscout.se_company_person_suggestion;
DROP TABLE IF EXISTS corpscout.se_company_person_correction;
```

- [ ] **Step 4: Write migration 000296 (access)**

`000296_corpscout_se_company_person_correction_writer_grants.up.sql`:

```sql
GRANT INSERT ON corpscout.se_company_person_correction
TO corpscout_person_correction_writer;

GRANT INSERT ON corpscout.se_company_person_suggestion
TO corpscout_person_correction_writer;
```

`.down.sql`:

```sql
REVOKE INSERT ON corpscout.se_company_person_correction
FROM corpscout_person_correction_writer;

REVOKE INSERT ON corpscout.se_company_person_suggestion
FROM corpscout_person_correction_writer;
```

- [ ] **Step 5: Register both migrations in the test registries**

In `tests/test_clickhouse_migrations.py`, after line 310 (`"000294_corpscout_employee_board_representative_role",`) add:

```python
    "000295_corpscout_se_company_person_corrections",
```

and change line 315 to:

```python
EXPECTED_ACCESS_MIGRATIONS = (
    "000241_corpscout_person_correction_writer_role",
    "000296_corpscout_se_company_person_correction_writer_grants",
)
```

`test_person_correction_writer_role_is_least_privileged` indexes `EXPECTED_ACCESS_MIGRATIONS[0]`, so it still checks 000241 only.

- [ ] **Step 6: Create `corrections.py` with the schema constants**

```python
"""Human corrections and model suggestions for Sweden company people.

The ledger is append-only input to normalization and role materialization.
Nothing in this module edits published rows.
"""

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

DATABASE = "corpscout"
GROUP_NAME = "company_people"

CORRECTION_TABLE = "se_company_person_correction"
SUGGESTION_TABLE = "se_company_person_suggestion"
QUALIFIED_CORRECTION_TABLE = f"{DATABASE}.{CORRECTION_TABLE}"
QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"

CORRECTION_COLUMNS = (
    "correction_id",
    "company_id",
    "correction_kind",
    "subject_person_id",
    "target_person_id",
    "draft_ids",
    "payload",
    "evidence_hash",
    "reason",
    "decided_by",
    "supersedes_correction_id",
    "created_at",
)

SUGGESTION_COLUMNS = (
    "suggestion_id",
    "company_id",
    "person_id",
    "input_hash",
    "draft_ids",
    "suggestion",
    "raw_response",
    "model_provider",
    "model_name",
    "prompt_version",
    "prompt_tokens",
    "completion_tokens",
    "source_run_id",
    "created_at",
)

PERSON_CORRECTION_KINDS = (
    "merge_persons",
    "reassign_draft",
    "split_person",
    "approve_suggestion",
    "reject_suggestion",
    "override_field",
)
ROLE_CORRECTION_KINDS = ("set_role", "remove_role")
UNDO_KIND = "undo"
CORRECTION_KINDS = frozenset((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, UNDO_KIND))
KIND_ORDER = {
    kind: index
    for index, kind in enumerate((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS))
}
ZERO_HASH = "0" * 64
```

(The dataclasses and functions are added in Task 3; this file compiles as is.)

- [ ] **Step 7: Run the tests**

Run: `uv run pytest tests/test_se_company_person_corrections.py tests/test_clickhouse_migrations.py -q`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add corpscout/clickhouse/migrations/000295_corpscout_se_company_person_corrections.up.sql \
        corpscout/clickhouse/migrations/000295_corpscout_se_company_person_corrections.down.sql \
        corpscout/clickhouse/migrations/000296_corpscout_se_company_person_correction_writer_grants.up.sql \
        corpscout/clickhouse/migrations/000296_corpscout_se_company_person_correction_writer_grants.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py
git commit -m "feat(company_people): add se_company_person correction ledger and suggestion tables"
```

---

### Task 3: Ledger model, loaders and effective-set logic in `corrections.py`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py`

**Interfaces:**
- Produces:
  - `PersonCorrection` (frozen dataclass: `correction_id: uuid.UUID, company_id: str, kind: str, subject_person_id: uuid.UUID, target_person_id: uuid.UUID | None, draft_ids: tuple[uuid.UUID, ...], payload: Mapping[str, Any], evidence_hash: str, supersedes_correction_id: uuid.UUID | None, created_at: datetime`)
  - `StoredSuggestion` (frozen dataclass: `suggestion_id: uuid.UUID, company_id: str, person_id: uuid.UUID, input_hash: str, draft_ids: tuple[uuid.UUID, ...], name: str, description: str | None, existing_person_id: uuid.UUID | None, created_at: datetime`)
  - `build_company_corrections_sql() -> str`, `build_company_suggestions_sql() -> str` (both use `%(selected_company_ids)s`)
  - `correction_from_row(row) -> tuple[str, PersonCorrection]`, `suggestion_from_row(row) -> tuple[str, StoredSuggestion]`
  - `effective_corrections(corrections: Sequence[PersonCorrection]) -> tuple[PersonCorrection, ...]`
  - `correction_set_hash(correction_ids: Sequence[uuid.UUID]) -> str`
  - `effective_company_corrections_cte() -> str` (CTE text used by normalization's `is_unchanged`)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_person_corrections.py`:

```python
import json
import uuid
from datetime import UTC, datetime, timedelta

from dagster_v3.defs.company_people.corrections import (
    PersonCorrection,
    StoredSuggestion,
    build_company_corrections_sql,
    build_company_suggestions_sql,
    correction_from_row,
    correction_set_hash,
    effective_company_corrections_cte,
    effective_corrections,
    suggestion_from_row,
)

NOW = datetime(2026, 8, 22, 9, tzinfo=UTC)
COMPANY_ID = "5565200028"


def _correction(
    index: int,
    kind: str,
    *,
    supersedes: int | None = None,
    payload: dict[str, object] | None = None,
) -> PersonCorrection:
    return PersonCorrection(
        correction_id=uuid.UUID(int=index),
        company_id=COMPANY_ID,
        kind=kind,
        subject_person_id=uuid.UUID(int=1000),
        target_person_id=None,
        draft_ids=(),
        payload=payload or {},
        evidence_hash="0" * 64,
        supersedes_correction_id=None if supersedes is None else uuid.UUID(int=supersedes),
        created_at=NOW + timedelta(seconds=index),
    )


def test_effective_corrections_drop_superseded_and_undo_rows_and_order_by_kind() -> None:
    rows = (
        _correction(1, "override_field", payload={"name": "First"}),
        _correction(2, "merge_persons"),
        _correction(3, "undo", supersedes=1),
        _correction(4, "override_field", payload={"name": "Second"}),
        _correction(5, "set_role"),
        _correction(6, "not_a_kind"),
    )

    effective = effective_corrections(rows)

    assert [c.correction_id.int for c in effective] == [2, 4, 5]


def test_correction_set_hash_sorts_string_ids_like_clickhouse() -> None:
    ids = [uuid.UUID(int=2), uuid.UUID(int=1)]

    assert correction_set_hash(ids) == (
        "7a70c782c5d30f61a8f57b905eaa11c41ec8ffeafaa3a0e99c4fca60044a28d4"
    )
    assert correction_set_hash([]) == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_row_mappers_parse_payload_and_nullable_columns() -> None:
    company_id, correction = correction_from_row(
        (
            uuid.UUID(int=7),
            COMPANY_ID,
            "override_field",
            uuid.UUID(int=1000),
            None,
            [],
            json.dumps({"name": "Anna K. Svensson"}),
            "a" * 64,
            None,
            NOW,
        )
    )
    assert company_id == COMPANY_ID
    assert correction.kind == "override_field"
    assert correction.payload == {"name": "Anna K. Svensson"}
    assert correction.target_person_id is None

    company_id, suggestion = suggestion_from_row(
        (
            uuid.UUID(int=8),
            COMPANY_ID,
            uuid.UUID(int=1000),
            "b" * 64,
            [uuid.UUID(int=1)],
            json.dumps(
                {
                    "existing_person_id": None,
                    "name": "Anna Svensson",
                    "description": None,
                    "draft_ids": [str(uuid.UUID(int=1))],
                }
            ),
            NOW,
        )
    )
    assert company_id == COMPANY_ID
    assert suggestion.name == "Anna Svensson"
    assert suggestion.draft_ids == (uuid.UUID(int=1),)


def test_loader_sql_scopes_by_selected_companies() -> None:
    for sql in (build_company_corrections_sql(), build_company_suggestions_sql()):
        assert "WHERE company_id IN %(selected_company_ids)s" in sql
        assert "ORDER BY company_id" in sql
    assert "FROM corpscout.se_company_person_correction" in build_company_corrections_sql()
    assert "FROM corpscout.se_company_person_suggestion" in build_company_suggestions_sql()

    cte = effective_company_corrections_cte()
    assert "effective_company_corrections AS (" in cte
    assert "supersedes_correction_id IS NOT NULL" in cte
    assert "correction_kind IN ('merge_persons'" in cte
    assert "'undo'" not in cte
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_se_company_person_corrections.py -q`
Expected: FAIL with `ImportError: cannot import name 'PersonCorrection'`

- [ ] **Step 3: Implement the model and loaders**

Append to `corrections.py`:

```python
@dataclass(frozen=True)
class PersonCorrection:
    correction_id: uuid.UUID
    company_id: str
    kind: str
    subject_person_id: uuid.UUID
    target_person_id: uuid.UUID | None
    draft_ids: tuple[uuid.UUID, ...]
    payload: Mapping[str, Any]
    evidence_hash: str
    supersedes_correction_id: uuid.UUID | None
    created_at: datetime


@dataclass(frozen=True)
class StoredSuggestion:
    suggestion_id: uuid.UUID
    company_id: str
    person_id: uuid.UUID
    input_hash: str
    draft_ids: tuple[uuid.UUID, ...]
    name: str
    description: str | None
    existing_person_id: uuid.UUID | None
    created_at: datetime


def build_company_corrections_sql() -> str:
    return """SELECT
    correction_id,
    company_id,
    correction_kind,
    subject_person_id,
    target_person_id,
    draft_ids,
    payload,
    toString(evidence_hash),
    supersedes_correction_id,
    created_at
FROM corpscout.se_company_person_correction
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, created_at, correction_id"""


def build_company_suggestions_sql() -> str:
    return """SELECT
    suggestion_id,
    company_id,
    person_id,
    toString(input_hash),
    draft_ids,
    suggestion,
    created_at
FROM corpscout.se_company_person_suggestion
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, person_id, input_hash, created_at"""


def _person_kinds_sql() -> str:
    return ", ".join(f"'{kind}'" for kind in PERSON_CORRECTION_KINDS)


def effective_company_corrections_cte() -> str:
    """Per-company sorted ids of live person-level corrections.

    Used by normalization's company-status query so a new ledger row counts as
    changed evidence for exactly that company. Role kinds are excluded because
    they never change se_company_person rows.
    """
    return f"""effective_company_corrections AS (
    SELECT
        company_id,
        arraySort(groupArrayIf(
            toString(correction_id),
            correction_kind IN ({_person_kinds_sql()}) AND NOT superseded
        )) AS correction_ids
    FROM (
        SELECT
            ledger.company_id,
            ledger.correction_id,
            ledger.correction_kind,
            ledger.correction_id IN (
                SELECT supersedes_correction_id
                FROM corpscout.se_company_person_correction
                WHERE supersedes_correction_id IS NOT NULL
            ) AS superseded
        FROM corpscout.se_company_person_correction AS ledger
        WHERE (%(all_companies)s OR ledger.company_id IN %(company_ids)s)
    )
    GROUP BY company_id
)"""


def _nullable_uuid(value: object) -> uuid.UUID | None:
    if value is None:
        return None
    return uuid.UUID(str(value))


def _payload(value: object) -> Mapping[str, Any]:
    parsed = json.loads(str(value) or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("Correction payload must be a JSON object")
    return parsed


def correction_from_row(row: Sequence[Any]) -> tuple[str, PersonCorrection]:
    company_id = str(row[1])
    return company_id, PersonCorrection(
        correction_id=uuid.UUID(str(row[0])),
        company_id=company_id,
        kind=str(row[2]),
        subject_person_id=uuid.UUID(str(row[3])),
        target_person_id=_nullable_uuid(row[4]),
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[5])),
        payload=_payload(row[6]),
        evidence_hash=str(row[7]),
        supersedes_correction_id=_nullable_uuid(row[8]),
        created_at=row[9],
    )


def suggestion_from_row(row: Sequence[Any]) -> tuple[str, StoredSuggestion]:
    company_id = str(row[1])
    suggestion = _payload(row[5])
    description = suggestion.get("description")
    return company_id, StoredSuggestion(
        suggestion_id=uuid.UUID(str(row[0])),
        company_id=company_id,
        person_id=uuid.UUID(str(row[2])),
        input_hash=str(row[3]),
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[4])),
        name=str(suggestion.get("name", "")),
        description=None if description is None else str(description),
        existing_person_id=_nullable_uuid(suggestion.get("existing_person_id")),
        created_at=row[6],
    )


def effective_corrections(
    corrections: Sequence[PersonCorrection],
) -> tuple[PersonCorrection, ...]:
    """Drop superseded rows, undo rows and unknown kinds; order by kind then time."""
    superseded = {
        correction.supersedes_correction_id
        for correction in corrections
        if correction.supersedes_correction_id is not None
    }
    live = [
        correction
        for correction in corrections
        if correction.correction_id not in superseded
        and correction.kind in KIND_ORDER
    ]
    return tuple(
        sorted(
            live,
            key=lambda item: (
                KIND_ORDER[item.kind],
                item.created_at,
                str(item.correction_id),
            ),
        )
    )


def correction_set_hash(correction_ids: Sequence[uuid.UUID]) -> str:
    """Match the ClickHouse MATERIALIZED correction_set_hash (sorted strings)."""
    joined = "\n".join(sorted(str(value) for value in correction_ids))
    return hashlib.sha256(joined.encode()).hexdigest()
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_se_company_person_corrections.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py
git commit -m "feat(company_people): model the correction ledger and suggestion rows"
```

---

### Task 4: Record and reuse model suggestions in `normalization.py`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/normalization.py` (lines 119-160 dataclasses, 459-637 request/response, 813-941 multi-source, 944-991 `normalize_companies`, 994-1038 loader, 1117-1297 materialize)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_normalization.py`

**Interfaces:**
- Consumes: `StoredSuggestion`, `build_company_suggestions_sql`, `suggestion_from_row`, `SUGGESTION_COLUMNS`, `QUALIFIED_SUGGESTION_TABLE` from Task 3.
- Produces:
  - `CompanyPersonWork` gains `suggestions: tuple[StoredSuggestion, ...] = ()` and `corrections: tuple[PersonCorrection, ...] = ()` (corrections used in Task 5).
  - `LlmCompanyPeopleResult` gains `input_hash: str = ""`, `raw_response: str = ""`.
  - `request_input_hash(request: Mapping[str, Any]) -> str`.
  - `SuggestionWrite` (frozen dataclass: `suggestion_id, company_id, person_id, input_hash, draft_ids, suggestion_json: str, raw_response, model_provider, model_name, prompt_version, prompt_tokens, completion_tokens, created_at`).
  - `normalize_companies(...)` now returns `tuple[list[PersonProfileWrite], list[SuggestionWrite], dict[str, int]]` and takes `llm_model: str | None`.
  - `_insert_suggestion_writes(*, clickhouse, writes, source_run_id) -> int`.
  - `PersonProfileWrite` gains `suggestion_id: uuid.UUID | None = None` (persisted in Task 5).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_person_normalization.py` (and add `StoredSuggestion` plus `request_input_hash`, `SuggestionWrite` to the imports):

```python
from dagster_v3.defs.company_people.corrections import StoredSuggestion
from dagster_v3.defs.company_people.normalization import (
    SuggestionWrite,
    request_input_hash,
)


def test_request_input_hash_is_stable_and_ignores_repair_turns() -> None:
    observation = _observation("esef", name="David Mindus", role="chief_executive", index=1)
    batch = batch_company_observations([observation], maximum_observations_per_request=10)[0]
    request = build_company_people_request(
        company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
    )

    first = request_input_hash(request)
    request["messages"].append({"role": "assistant", "content": "{}"})

    assert len(first) == 64
    assert request_input_hash(request) != first  # repair turns change the payload
    assert request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
        )
    ) == first


def test_multi_source_company_records_one_suggestion_per_person() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    company = _company(*observations)

    def suggest(company_id, batch, previous_profiles):
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name="David Gustaf Mindus",
                    description="CEO.",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )

    writes, suggestions, metrics = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert len(suggestions) == 1
    assert suggestions[0].person_id == writes[0].person_id
    assert writes[0].suggestion_id == suggestions[0].suggestion_id
    assert json.loads(suggestions[0].suggestion_json)["name"] == "David Gustaf Mindus"
    assert len(suggestions[0].input_hash) == 64
    assert metrics["llm_request_count"] == 1


def test_stored_suggestion_with_current_input_hash_skips_the_llm() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    batch = batch_company_observations(observations, maximum_observations_per_request=10)[0]
    input_hash = request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
        )
    )
    stored = StoredSuggestion(
        suggestion_id=uuid.UUID(int=500),
        company_id=COMPANY_ID,
        person_id=uuid.UUID(int=1000),
        input_hash=input_hash,
        draft_ids=tuple(sorted(item.draft_id for item in observations)),
        name="David Gustaf Mindus",
        description="CEO.",
        existing_person_id=None,
        created_at=NOW,
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(stored,),
    )
    calls: list[str] = []

    def suggest(company_id, batch, previous_profiles):
        calls.append(company_id)
        raise AssertionError("LLM must not be called when a suggestion is current")

    writes, suggestions, metrics = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert calls == []
    assert suggestions == []
    assert len(writes) == 1
    assert writes[0].name == "David Gustaf Mindus"
    assert writes[0].suggestion_id == uuid.UUID(int=500)
    assert metrics["llm_reused_batch_count"] == 1
    assert metrics["llm_request_count"] == 0
```

Update the three existing `normalize_companies(...)` calls in this file to unpack three values (`writes, _suggestions, metrics = ...`) and pass `llm_model="deepseek-v4-flash"` (or `llm_model=None` for the single-source test).

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_se_company_person_normalization.py -q`
Expected: FAIL with `ImportError: cannot import name 'SuggestionWrite'`

- [ ] **Step 3: Extend the dataclasses and the request hash**

In `normalization.py`:

Add imports after line 38:

```python
from dagster_v3.defs.company_people.corrections import (
    QUALIFIED_SUGGESTION_TABLE,
    SUGGESTION_COLUMNS,
    PersonCorrection,
    StoredSuggestion,
    build_company_corrections_sql,
    build_company_suggestions_sql,
    correction_from_row,
    suggestion_from_row,
)
```

Replace `CompanyPersonWork` (lines 119-127) with:

```python
@dataclass(frozen=True)
class CompanyPersonWork:
    status: CompanyPersonStatus
    observations: tuple[DraftPersonObservation, ...]
    previous_profiles: tuple[ExistingPersonProfile, ...]
    suggestions: tuple[StoredSuggestion, ...] = ()
    corrections: tuple[PersonCorrection, ...] = ()

    @property
    def requires_llm(self) -> bool:
        return self.status.source_count > 1
```

Replace `LlmCompanyPeopleResult` (lines 138-146) with:

```python
@dataclass(frozen=True)
class LlmCompanyPeopleResult:
    response: "LlmCompanyPeopleResponse"
    model_provider: str
    model_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    contract_retry_count: int = 0
    input_hash: str = ""
    raw_response: str = ""
```

Add after `PersonProfileWrite` (line 159) the field `suggestion_id: uuid.UUID | None = None` as the last field of `PersonProfileWrite`, and add the new dataclass:

```python
@dataclass(frozen=True)
class SuggestionWrite:
    suggestion_id: uuid.UUID
    company_id: str
    person_id: uuid.UUID
    input_hash: str
    draft_ids: tuple[uuid.UUID, ...]
    suggestion_json: str
    raw_response: str
    model_provider: str
    model_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    created_at: datetime
```

Add `suggestion_id: uuid.UUID | None = None` to `_ProfileAccumulator` after `touched`.

Add after `build_company_people_request`:

```python
def request_input_hash(request: Mapping[str, Any]) -> str:
    """Hash the exact model, prompt version and messages of one request."""
    payload = json.dumps(
        {
            "model": request["model"],
            "prompt_version": PROMPT_VERSION,
            "messages": request["messages"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()
```

In `request_company_people`, after `request = build_company_people_request(...)` add `input_hash = request_input_hash(request)`, and in the returned `LlmCompanyPeopleResult` add `input_hash=input_hash, raw_response=content or ""`. Replace `model_provider="deepseek"` with `model_provider=model_provider` and add a keyword parameter `model_provider: str = "deepseek"` to `request_company_people`.

- [ ] **Step 4: Reuse stored suggestions and emit suggestion writes in the multi-source path**

Replace the body of `_normalize_multi_source_company` from `for batch in batches:` through the end of that loop (lines 844-898) with:

```python
    suggestion_writes: list[SuggestionWrite] = []
    reused_batch_count = 0
    request_count = 0
    stored_by_hash: dict[str, list[StoredSuggestion]] = defaultdict(list)
    for stored in company.suggestions:
        stored_by_hash[stored.input_hash].append(stored)

    for batch in batches:
        request_profiles = _profiles_for_request(profiles)
        if llm_model is None:
            raise RuntimeError("A multi-source company requires an LLM model name")
        input_hash = request_input_hash(
            build_company_people_request(
                company_id=company.status.company_id,
                batch=batch,
                previous_profiles=request_profiles,
                model=llm_model,
            )
        )
        result = _reuse_stored_suggestions(
            stored_by_hash.get(input_hash, ()),
            batch=batch,
            previous_profiles=request_profiles,
            model=llm_model,
            input_hash=input_hash,
        )
        suggestion_ids_by_person: dict[uuid.UUID, uuid.UUID] = {}
        if result is None:
            result = llm_suggester(company.status.company_id, batch, request_profiles)
            validate_company_people_response(
                result.response, batch=batch, previous_profiles=request_profiles
            )
            request_count += 1
        else:
            reused_batch_count += 1
            suggestion_ids_by_person = {
                stored.person_id: stored.suggestion_id
                for stored in sorted(
                    stored_by_hash[input_hash], key=lambda item: item.created_at
                )
            }
        prompt_tokens += result.prompt_tokens
        completion_tokens += result.completion_tokens
        contract_retry_count += result.contract_retry_count

        batch_draft_ids = {observation.draft_id for observation in batch.observations}
        for profile in profiles.values():
            removed_ids = profile.draft_ids & batch_draft_ids
            if removed_ids:
                profile.draft_ids -= removed_ids
                profile.touched = True

        profiles_by_name = {
            _name_match_key(profile.name): profile.person_id
            for profile in profiles.values()
        }
        for suggestion in result.response.people:
            person_id = suggestion.existing_person_id
            if person_id is None:
                person_id = profiles_by_name.get(_name_match_key(suggestion.name))
            if person_id is None:
                person_id = _person_id(company.status.company_id, suggestion.name)

            profile = profiles.get(person_id)
            if profile is None:
                profile = _ProfileAccumulator(
                    person_id=person_id,
                    name=suggestion.name,
                    description=suggestion.description,
                    draft_ids=set(),
                    created_at=created_at,
                    model_provider=result.model_provider,
                    model_name=result.model_name,
                    prompt_version=result.prompt_version,
                )
                profiles[person_id] = profile
            profile.name = suggestion.name
            if suggestion.description is not None:
                profile.description = suggestion.description
            profile.draft_ids.update(suggestion.draft_ids)
            profile.model_provider = result.model_provider
            profile.model_name = result.model_name
            profile.prompt_version = result.prompt_version
            profile.touched = True

            suggestion_id = suggestion_ids_by_person.get(person_id)
            if suggestion_id is None:
                suggestion_id = uuid.uuid4()
                suggestion_writes.append(
                    SuggestionWrite(
                        suggestion_id=suggestion_id,
                        company_id=company.status.company_id,
                        person_id=person_id,
                        input_hash=result.input_hash or input_hash,
                        draft_ids=tuple(sorted(suggestion.draft_ids)),
                        suggestion_json=suggestion.model_dump_json(),
                        raw_response=result.raw_response,
                        model_provider=result.model_provider,
                        model_name=result.model_name,
                        prompt_version=result.prompt_version,
                        prompt_tokens=result.prompt_tokens,
                        completion_tokens=result.completion_tokens,
                        created_at=created_at,
                    )
                )
            profile.suggestion_id = suggestion_id
```

Add the helper above `_normalize_multi_source_company`:

```python
def _reuse_stored_suggestions(
    stored: Sequence[StoredSuggestion],
    *,
    batch: CompanyObservationBatch,
    previous_profiles: Sequence[ExistingPersonProfile],
    model: str,
    input_hash: str,
) -> LlmCompanyPeopleResult | None:
    """Rebuild a validated response from stored rows, or None to call the model."""
    if not stored:
        return None
    newest_by_person: dict[uuid.UUID, StoredSuggestion] = {}
    for row in sorted(stored, key=lambda item: (item.created_at, str(item.suggestion_id))):
        newest_by_person[row.person_id] = row
    try:
        response = LlmCompanyPeopleResponse(
            people=[
                LlmCompanyPersonSuggestion(
                    existing_person_id=row.existing_person_id,
                    name=row.name,
                    description=row.description,
                    draft_ids=list(row.draft_ids),
                )
                for row in newest_by_person.values()
            ]
        )
        validate_company_people_response(
            response, batch=batch, previous_profiles=previous_profiles
        )
    except (ValidationError, ValueError):
        return None
    return LlmCompanyPeopleResult(
        response=response,
        model_provider="stored",
        model_name=model,
        prompt_version=PROMPT_VERSION,
        prompt_tokens=0,
        completion_tokens=0,
        input_hash=input_hash,
    )
```

Change `_normalize_multi_source_company`'s signature to add `llm_model: str | None,` after `llm_suggester`, return `writes, suggestion_writes, {...}` and replace the metrics dict with:

```python
    return writes, suggestion_writes, {
        "llm_request_count": request_count,
        "llm_reused_batch_count": reused_batch_count,
        "llm_role_batch_count": len(batches) if len(batches) > 1 else 0,
        "llm_observation_count": len(company.observations),
        "llm_prompt_tokens": prompt_tokens,
        "llm_completion_tokens": completion_tokens,
        "llm_contract_retry_count": contract_retry_count,
        "unchanged_profile_count": unchanged_profile_count,
    }
```

Where `PersonProfileWrite` objects are built in this function add `suggestion_id=profile.suggestion_id`. When a profile's provenance comes from a reused batch (`result.model_provider == "stored"`), keep the profile's previous `model_provider`/`model_name` instead of overwriting with `"stored"`: guard the three `profile.model_* = result.model_*` lines with `if result.model_provider != "stored":`.

- [ ] **Step 5: Thread the new return value through `normalize_companies` and `materialize_se_company_people`**

`normalize_companies` signature becomes:

```python
def normalize_companies(
    companies: Sequence[CompanyPersonWork],
    *,
    llm_suggester: CompanyLlmSuggester | None,
    llm_model: str | None,
    maximum_observations_per_request: int,
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], list[SuggestionWrite], dict[str, int]]:
```

Add `"llm_reused_batch_count": 0` to its `metrics` dict, collect `suggestion_writes` from the multi-source branch (`company_writes, company_suggestions, company_metrics = _normalize_multi_source_company(..., llm_model=llm_model, ...)`), and return `writes, suggestion_writes, metrics`.

In `_load_company_work`, load suggestions and corrections alongside observations:

```python
    suggestions_by_company: dict[str, list[StoredSuggestion]] = defaultdict(list)
    corrections_by_company: dict[str, list[PersonCorrection]] = defaultdict(list)
    ...
        for row in client.execute(build_company_suggestions_sql(), parameters):
            company_id, suggestion = suggestion_from_row(row)
            suggestions_by_company[company_id].append(suggestion)
        for row in client.execute(build_company_corrections_sql(), parameters):
            company_id, correction = correction_from_row(row)
            corrections_by_company[company_id].append(correction)
```

and pass `suggestions=tuple(suggestions_by_company[status.company_id]), corrections=tuple(corrections_by_company[status.company_id])` into `CompanyPersonWork`.

Add the suggestion insert next to `_insert_person_writes`:

```python
def _insert_suggestion_writes(
    *,
    clickhouse: ClickhouseResource,
    writes: Sequence[SuggestionWrite],
    source_run_id: str,
) -> int:
    if not writes:
        return 0
    insert_columns = _insert_columns(SUGGESTION_COLUMNS)
    rows = [
        (
            write.suggestion_id,
            write.company_id,
            write.person_id,
            write.input_hash,
            list(write.draft_ids),
            write.suggestion_json,
            write.raw_response,
            write.model_provider,
            write.model_name,
            write.prompt_version,
            write.prompt_tokens,
            write.completion_tokens,
            source_run_id,
            write.created_at,
        )
        for write in writes
    ]
    with clickhouse.get_connection() as client:
        client.execute(
            f"INSERT INTO {QUALIFIED_SUGGESTION_TABLE} ({insert_columns}) VALUES",
            rows,
        )
    return len(rows)
```

In `materialize_se_company_people`: add `"llm_reused_batch_count": 0` and `"suggestion_inserted_count": 0` to `normalization_metrics`; read settings once before the loop so the provider name is configurable:

```python
    model_provider = os.getenv("DEEPSEEK_PROVIDER", "deepseek").strip() or "deepseek"
```

(add `import os` at the top), pass `model_provider=model_provider` into `request_company_people` inside `suggest_company_people`, call `normalize_companies(..., llm_model=selected_model, ...)` unpacking three values, and **before** `_insert_person_writes` insert suggestions:

```python
        normalization_metrics["suggestion_inserted_count"] += _insert_suggestion_writes(
            clickhouse=clickhouse,
            writes=suggestion_writes,
            source_run_id=source_run_id,
        )
```

Add `assert_clickhouse_tables_exist(..., tables=(PERSON_DRAFT_TABLE, PERSON_TABLE, SUGGESTION_TABLE, CORRECTION_TABLE))` using the names imported from `corrections.py` (`SUGGESTION_TABLE`, `CORRECTION_TABLE`).

- [ ] **Step 6: Run the normalization tests**

Run: `uv run pytest tests/test_se_company_person_normalization.py -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/normalization.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_normalization.py
git commit -m "feat(company_people): persist and reuse model suggestions for multi-source companies"
```

---

### Task 5: Apply person corrections with precedence and staleness

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/normalization.py` (`PERSON_COLUMNS` line 47, `ExistingPersonProfile` line 102, `_company_status_ctes` line 231, `build_existing_profiles_sql` line 292, `_profile_from_row` line 332, single/multi-source functions, `_insert_person_writes`, `materialize_se_company_people`)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_normalization.py`

**Interfaces:**
- Consumes: `PersonCorrection`, `effective_corrections`, `effective_company_corrections_cte` (Task 3); `_ProfileAccumulator.suggestion_id` (Task 4).
- Produces:
  - `ExistingPersonProfile` gains `draft_set_hash: str = ""`, `merged_into_person_id: uuid.UUID | None = None`.
  - `_ProfileAccumulator` gains `merged_into_person_id: uuid.UUID | None = None`, `correction_ids: list[uuid.UUID]` (default empty via `field(default_factory=list)`).
  - `PersonProfileWrite` gains `correction_ids: tuple[uuid.UUID, ...] = ()`, `merged_into_person_id: uuid.UUID | None = None`.
  - `PERSON_COLUMNS` gains `"correction_ids", "suggestion_id", "merged_into_person_id"` after `"draft_ids"`.
  - `CorrectionOutcome` (frozen dataclass: `applied: tuple[PersonCorrection, ...]`, `stale: tuple[PersonCorrection, ...]`).
  - `apply_person_corrections(profiles: dict[uuid.UUID, _ProfileAccumulator], *, company: CompanyPersonWork, current_input_hashes: frozenset[str], created_at: datetime) -> CorrectionOutcome`.
  - `build_pending_companies_sql()` gains `AND company_id NOT IN %(processed_company_ids)s`.
  - Metrics: `applied_correction_count`, `stale_correction_count`, `settled_company_count`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_person_normalization.py` (add `PersonCorrection`, `apply_person_corrections`, `_person_id`-equivalent import `person_id_for` — see Step 3 — to imports):

```python
from dagster_v3.defs.company_people.corrections import PersonCorrection
from dagster_v3.defs.company_people.normalization import person_id_for


def _correction(
    index: int,
    kind: str,
    *,
    subject: uuid.UUID,
    target: uuid.UUID | None = None,
    draft_ids: tuple[uuid.UUID, ...] = (),
    payload: dict[str, object] | None = None,
    evidence_hash: str = "0" * 64,
) -> PersonCorrection:
    return PersonCorrection(
        correction_id=uuid.UUID(int=9000 + index),
        company_id=COMPANY_ID,
        kind=kind,
        subject_person_id=subject,
        target_person_id=target,
        draft_ids=draft_ids,
        payload=payload or {},
        evidence_hash=evidence_hash,
        supersedes_correction_id=None,
        created_at=NOW + timedelta(minutes=index),
    )


def _previous(name: str, draft_ids: tuple[uuid.UUID, ...], draft_set_hash: str) -> ExistingPersonProfile:
    return ExistingPersonProfile(
        person_id=person_id_for(COMPANY_ID, name),
        name=name,
        description=None,
        draft_ids=tuple(sorted(draft_ids)),
        created_at=NOW - timedelta(days=1),
        draft_set_hash=draft_set_hash,
    )


def test_override_field_wins_over_deterministic_name_and_records_provenance() -> None:
    observation = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    subject = person_id_for(COMPANY_ID, "Anna Svensson")
    previous = _previous("Anna Svensson", (observation.draft_id,), "a" * 64)
    company = CompanyPersonWork(
        status=_company(observation).status,
        observations=(observation,),
        previous_profiles=(previous,),
        corrections=(
            _correction(
                1,
                "override_field",
                subject=subject,
                payload={"name": "Anna K. Svensson"},
                evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics = normalize_companies(
        [company], llm_suggester=None, llm_model=None,
        maximum_observations_per_request=10, created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].name == "Anna K. Svensson"
    assert writes[0].correction_ids == (uuid.UUID(int=9001),)
    assert writes[0].model_provider == "deterministic"
    assert metrics["applied_correction_count"] == 1
    assert metrics["stale_correction_count"] == 0


def test_override_is_stale_when_evidence_hash_moved() -> None:
    observation = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    subject = person_id_for(COMPANY_ID, "Anna Svensson")
    previous = _previous("Anna Svensson", (observation.draft_id,), "b" * 64)
    company = CompanyPersonWork(
        status=_company(observation).status,
        observations=(observation,),
        previous_profiles=(previous,),
        corrections=(
            _correction(
                1, "override_field", subject=subject,
                payload={"name": "Anna K. Svensson"}, evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics = normalize_companies(
        [company], llm_suggester=None, llm_model=None,
        maximum_observations_per_request=10, created_at=NOW,
    )

    assert writes == []  # unchanged profile, nothing applied
    assert metrics["stale_correction_count"] == 1
    assert metrics["applied_correction_count"] == 0


def test_merge_persons_moves_evidence_and_tombstones_the_subject() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation("bolagsverket", name="Anna K Svensson", role="board_member", index=2)
    # "Anna K Svensson" keys as anna|svensson too, so force distinct ids via previous profiles
    subject = uuid.UUID(int=4242)
    target = person_id_for(COMPANY_ID, "Anna Svensson")
    previous_subject = ExistingPersonProfile(
        person_id=subject, name="Anna K Svensson", description=None,
        draft_ids=(second.draft_id,), created_at=NOW - timedelta(days=1), draft_set_hash="c" * 64,
    )
    previous_target = _previous("Anna Svensson", (first.draft_id,), "d" * 64)
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(previous_target, previous_subject),
        corrections=(
            _correction(1, "merge_persons", subject=subject, target=target, evidence_hash="c" * 64),
        ),
    )

    writes, _suggestions, metrics = normalize_companies(
        [company], llm_suggester=None, llm_model=None,
        maximum_observations_per_request=10, created_at=NOW,
    )

    by_id = {write.person_id: write for write in writes}
    assert set(by_id[target].draft_ids) == {first.draft_id, second.draft_id}
    assert by_id[target].correction_ids == (uuid.UUID(int=9001),)
    assert by_id[subject].merged_into_person_id == target
    assert by_id[subject].draft_ids == (second.draft_id,)
    assert metrics["applied_correction_count"] == 1


def test_reassign_draft_requires_the_draft_on_the_subject() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation("bolagsverket", name="Erik Eriksson", role="board_member", index=2)
    anna = person_id_for(COMPANY_ID, "Anna Svensson")
    erik = person_id_for(COMPANY_ID, "Erik Eriksson")
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(
            _previous("Anna Svensson", (first.draft_id,), "a" * 64),
            _previous("Erik Eriksson", (second.draft_id,), "b" * 64),
        ),
        corrections=(
            _correction(1, "reassign_draft", subject=anna, target=erik,
                        draft_ids=(uuid.UUID(int=77),), evidence_hash="a" * 64),
        ),
    )

    writes, _suggestions, metrics = normalize_companies(
        [company], llm_suggester=None, llm_model=None,
        maximum_observations_per_request=10, created_at=NOW,
    )

    assert writes == []
    assert metrics["stale_correction_count"] == 1


def test_split_person_creates_a_new_deterministic_person_from_payload_name() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation("bolagsverket", name="Anna Svensson", role="auditor", index=2)
    anna = person_id_for(COMPANY_ID, "Anna Svensson")
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(_previous("Anna Svensson", (first.draft_id, second.draft_id), "a" * 64),),
        corrections=(
            _correction(1, "split_person", subject=anna, draft_ids=(second.draft_id,),
                        payload={"name": "Anna Svensson (auditor)"}, evidence_hash="a" * 64),
        ),
    )

    writes, _suggestions, _metrics = normalize_companies(
        [company], llm_suggester=None, llm_model=None,
        maximum_observations_per_request=10, created_at=NOW,
    )

    by_id = {write.person_id: write for write in writes}
    new_id = person_id_for(COMPANY_ID, "Anna Svensson (auditor)")
    assert by_id[anna].draft_ids == (first.draft_id,)
    assert by_id[new_id].draft_ids == (second.draft_id,)
    assert by_id[new_id].name == "Anna Svensson (auditor)"


def test_company_status_sql_includes_effective_corrections_and_processed_guard() -> None:
    statistics_sql = build_company_statistics_sql()
    pending_sql = build_pending_companies_sql()

    for sql in (statistics_sql, pending_sql):
        assert "effective_company_corrections AS (" in sql
        assert "arrayMap(id -> toString(id), correction_ids)" in sql
        assert "published.correction_ids = corrections.correction_ids" in sql
    assert "AND company_id NOT IN %(processed_company_ids)s" in pending_sql


def test_main_table_migration_matches_insert_contract_after_corrections() -> None:
    sql = (MIGRATIONS_DIR / "000295_corpscout_se_company_person_corrections.up.sql").read_text(
        encoding="utf-8"
    )
    for column in ("correction_ids", "suggestion_id", "merged_into_person_id"):
        assert column in PERSON_COLUMNS
        assert f"ADD COLUMN IF NOT EXISTS {column} " in sql
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_se_company_person_normalization.py -q`
Expected: FAIL with `ImportError: cannot import name 'person_id_for'`

- [ ] **Step 3: Schema-facing changes**

In `normalization.py`:

- `PERSON_COLUMNS` becomes `("person_id", "company_id", "name", "description", "draft_ids", "correction_ids", "suggestion_id", "merged_into_person_id", "model_provider", "model_name", "prompt_version", "source_run_id", "created_at", "updated_at")`.
- `ExistingPersonProfile`: append `draft_set_hash: str = ""` and `merged_into_person_id: uuid.UUID | None = None`.
- `PersonProfileWrite`: append `correction_ids: tuple[uuid.UUID, ...] = ()` and `merged_into_person_id: uuid.UUID | None = None` (after `suggestion_id`).
- `_ProfileAccumulator`: append `merged_into_person_id: uuid.UUID | None = None` and `correction_ids: list[uuid.UUID] = field(default_factory=list)` (import `field` from `dataclasses`).
- Rename `_person_id` to `person_id_for` (public; keep `_person_id = person_id_for` alias so nothing else breaks).
- `build_existing_profiles_sql` selects `toString(draft_set_hash), merged_into_person_id` after `created_at`, and `_profile_from_row` maps `draft_set_hash=str(row[6]), merged_into_person_id=None if row[7] is None else uuid.UUID(str(row[7]))`.
- `_company_status_ctes` becomes:

```python
def _company_status_ctes() -> str:
    return f"""draft_companies AS (
    SELECT
        company_id,
        uniqExact(source) AS source_count,
        count() AS observation_count,
        arraySort(groupUniqArray(draft_id)) AS draft_ids
    FROM corpscout.se_company_person_draft FINAL
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
published_companies AS (
    SELECT
        company_id,
        arraySort(arrayDistinct(arrayFlatten(groupArray(draft_ids)))) AS draft_ids,
        arraySort(arrayDistinct(arrayFlatten(groupArray(
            arrayMap(id -> toString(id), correction_ids)
        )))) AS correction_ids
    FROM corpscout.se_company_person FINAL
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
{effective_company_corrections_cte()},
company_status AS (
    SELECT
        drafts.*,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids
            AND published.correction_ids = corrections.correction_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
    LEFT JOIN effective_company_corrections AS corrections USING (company_id)
)"""
```

- `build_pending_companies_sql` adds `AND company_id NOT IN %(processed_company_ids)s` after `WHERE NOT is_unchanged`.
- `_insert_person_writes` row tuple gains, after `list(write.draft_ids)`: `list(write.correction_ids), write.suggestion_id, write.merged_into_person_id`.

- [ ] **Step 4: Implement `apply_person_corrections`**

Add above `_normalize_single_source_company`:

```python
@dataclass(frozen=True)
class CorrectionOutcome:
    applied: tuple[PersonCorrection, ...]
    stale: tuple[PersonCorrection, ...]


def _evidence_is_current(
    correction: PersonCorrection,
    profile: _ProfileAccumulator | None,
    previous: ExistingPersonProfile | None,
) -> bool:
    """True when the reviewer's evidence still matches the published row and this run."""
    if correction.evidence_hash == ZERO_HASH:
        return profile is not None
    if profile is None or previous is None:
        return False
    return (
        previous.draft_set_hash == correction.evidence_hash
        and tuple(sorted(profile.draft_ids)) == previous.draft_ids
    )


def _deterministic_name(
    profile: _ProfileAccumulator,
    observations_by_id: Mapping[uuid.UUID, DraftPersonObservation],
) -> str:
    ordered = sorted(
        (observations_by_id[draft_id] for draft_id in profile.draft_ids if draft_id in observations_by_id),
        key=lambda item: (item.source_observed_at, item.fiscal_year or 0, str(item.draft_id)),
        reverse=True,
    )
    for observation in ordered:
        name = _source_name(observation)
        if name:
            return name
    return profile.name


def apply_person_corrections(
    profiles: dict[uuid.UUID, _ProfileAccumulator],
    *,
    company: CompanyPersonWork,
    current_input_hashes: frozenset[str],
    created_at: datetime,
) -> CorrectionOutcome:
    """Apply live person corrections in kind order; never apply a stale one."""
    previous_by_id = {item.person_id: item for item in company.previous_profiles}
    observations_by_id = {item.draft_id: item for item in company.observations}
    suggestions_by_id = {item.suggestion_id: item for item in company.suggestions}
    applied: list[PersonCorrection] = []
    stale: list[PersonCorrection] = []

    def ensure_profile(person_id: uuid.UUID) -> _ProfileAccumulator | None:
        profile = profiles.get(person_id)
        if profile is not None:
            return profile
        previous = previous_by_id.get(person_id)
        if previous is None:
            return None
        profile = _ProfileAccumulator(
            person_id=previous.person_id,
            name=previous.name,
            description=previous.description,
            draft_ids=set(),
            created_at=previous.created_at,
            model_provider="deterministic",
            model_name="correction",
            prompt_version=DIRECT_PROMPT_VERSION,
        )
        profiles[person_id] = profile
        return profile

    for correction in effective_corrections(company.corrections):
        if correction.kind not in PERSON_CORRECTION_KINDS:
            continue
        subject = profiles.get(correction.subject_person_id)
        previous = previous_by_id.get(correction.subject_person_id)
        if not _evidence_is_current(correction, subject, previous):
            stale.append(correction)
            continue
        assert subject is not None

        if correction.kind == "merge_persons":
            target = (
                ensure_profile(correction.target_person_id)
                if correction.target_person_id is not None
                else None
            )
            if target is None or target.person_id == subject.person_id:
                stale.append(correction)
                continue
            target.draft_ids.update(subject.draft_ids)
            target.correction_ids.append(correction.correction_id)
            target.touched = True
            subject.merged_into_person_id = target.person_id
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        elif correction.kind == "reassign_draft":
            target = (
                ensure_profile(correction.target_person_id)
                if correction.target_person_id is not None
                else None
            )
            moved = set(correction.draft_ids)
            if (
                target is None
                or len(moved) != 1
                or not moved <= subject.draft_ids
                or subject.draft_ids == moved
            ):
                stale.append(correction)
                continue
            subject.draft_ids -= moved
            target.draft_ids |= moved
            for profile in (subject, target):
                profile.correction_ids.append(correction.correction_id)
                profile.touched = True

        elif correction.kind == "split_person":
            moved = set(correction.draft_ids)
            name = str(correction.payload.get("name", "")).strip()
            if not moved or not moved < subject.draft_ids or name == "":
                stale.append(correction)
                continue
            new_id = person_id_for(company.status.company_id, name)
            target = ensure_profile(new_id)
            if target is None:
                target = _ProfileAccumulator(
                    person_id=new_id,
                    name=name,
                    description=None,
                    draft_ids=set(),
                    created_at=created_at,
                    model_provider="deterministic",
                    model_name="correction",
                    prompt_version=DIRECT_PROMPT_VERSION,
                )
                profiles[new_id] = target
            subject.draft_ids -= moved
            target.draft_ids |= moved
            target.name = name
            for profile in (subject, target):
                profile.correction_ids.append(correction.correction_id)
                profile.touched = True

        elif correction.kind in ("approve_suggestion", "reject_suggestion"):
            suggestion_id = _nullable_payload_uuid(correction.payload.get("suggestion_id"))
            suggestion = suggestions_by_id.get(suggestion_id) if suggestion_id else None
            if suggestion is None or suggestion.input_hash not in current_input_hashes:
                stale.append(correction)
                continue
            if correction.kind == "approve_suggestion":
                for other in profiles.values():
                    if other is not subject:
                        other.draft_ids -= set(suggestion.draft_ids)
                subject.draft_ids |= set(suggestion.draft_ids)
                subject.name = suggestion.name
                subject.description = suggestion.description
                subject.suggestion_id = suggestion.suggestion_id
            else:
                subject.name = _deterministic_name(subject, observations_by_id)
                subject.description = None
                subject.suggestion_id = None
                subject.model_provider = "deterministic"
                subject.model_name = "rejected-suggestion"
                subject.prompt_version = DIRECT_PROMPT_VERSION
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        elif correction.kind == "override_field":
            if "name" in correction.payload:
                name = str(correction.payload["name"]).strip()
                if name == "":
                    stale.append(correction)
                    continue
                subject.name = name
            if "description" in correction.payload:
                value = correction.payload["description"]
                subject.description = None if value is None else str(value).strip() or None
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        applied.append(correction)

    for profile in profiles.values():
        if profile.touched and not profile.draft_ids and profile.merged_into_person_id is None:
            raise ValueError(
                "Corrections would remove all evidence from a person, which the "
                f"append-only main table cannot represent: {profile.person_id}"
            )
    return CorrectionOutcome(applied=tuple(applied), stale=tuple(stale))


def _nullable_payload_uuid(value: object) -> uuid.UUID | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return None
```

Import `ZERO_HASH`, `PERSON_CORRECTION_KINDS`, `effective_corrections`, `effective_company_corrections_cte` from `corrections.py`.

- [ ] **Step 5: Route both normalization paths through a shared finalize step**

Refactor `_normalize_single_source_company` so it builds `profiles: dict[uuid.UUID, _ProfileAccumulator]` (one per name key, `touched=True`) instead of writes, then both paths call:

```python
def _finalize_company_profiles(
    company: CompanyPersonWork,
    profiles: dict[uuid.UUID, _ProfileAccumulator],
    *,
    current_input_hashes: frozenset[str],
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], dict[str, int]]:
    previous_by_id = {item.person_id: item for item in company.previous_profiles}
    outcome = apply_person_corrections(
        profiles,
        company=company,
        current_input_hashes=current_input_hashes,
        created_at=created_at,
    )
    writes: list[PersonProfileWrite] = []
    unchanged_profile_count = 0
    for profile in sorted(profiles.values(), key=lambda item: str(item.person_id)):
        if not profile.touched:
            continue
        previous = previous_by_id.get(profile.person_id)
        if not _profile_changed(profile, previous):
            unchanged_profile_count += 1
            continue
        writes.append(
            PersonProfileWrite(
                person_id=profile.person_id,
                company_id=company.status.company_id,
                name=profile.name,
                description=profile.description,
                draft_ids=tuple(sorted(profile.draft_ids)),
                model_provider=profile.model_provider,
                model_name=profile.model_name,
                prompt_version=profile.prompt_version,
                created_at=profile.created_at,
                suggestion_id=profile.suggestion_id,
                correction_ids=tuple(sorted(set(profile.correction_ids), key=str)),
                merged_into_person_id=profile.merged_into_person_id,
            )
        )
    return writes, {
        "unchanged_profile_count": unchanged_profile_count,
        "applied_correction_count": len(outcome.applied),
        "stale_correction_count": len(outcome.stale),
    }
```

and `_profile_changed` compares, in addition to name/description/draft_ids: `tuple(sorted(set(profile.correction_ids), key=str)) != tuple(sorted(previous_correction_ids, key=str))` where previous correction ids are loaded from `se_company_person` (add `correction_ids` to `build_existing_profiles_sql` and `ExistingPersonProfile.correction_ids: tuple[uuid.UUID, ...] = ()`), plus `profile.merged_into_person_id != previous.merged_into_person_id`.

In the multi-source path, `current_input_hashes` = the set of `input_hash` values computed per batch in Task 4; in the single-source path it is `frozenset()`.

`normalize_companies` sums `applied_correction_count` and `stale_correction_count` into metrics (initialise both to 0).

- [ ] **Step 6: Make "no writes" a settled state, not an error**

In `materialize_se_company_people`:

- keep `processed_company_ids: set[str] = set()`; pass `"processed_company_ids": tuple(processed_company_ids) or ("",)` in the pending query parameters; after each batch `processed_company_ids.update(status.company_id for status in statuses)`.
- replace the `RuntimeError("...made no publish progress...")` with:

```python
        if not writes:
            normalization_metrics["settled_company_count"] += len(companies)
            if log is not None:
                log(
                    "Sweden company-person batch produced no new profiles "
                    "(stale corrections or unchanged evidence): companies=%s",
                    [status.company_id for status in statuses[:10]],
                )
```

and still insert suggestion writes and advance `selected_company_count`. Initialise `"settled_company_count": 0`, `"applied_correction_count": 0`, `"stale_correction_count": 0` in `normalization_metrics`.

- [ ] **Step 7: Run the full company_people test set**

Run: `uv run pytest tests/test_se_company_person_normalization.py tests/test_se_company_person_corrections.py tests/test_se_company_person_roles.py -q`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/normalization.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_normalization.py
git commit -m "feat(company_people): apply ledger corrections to Sweden person profiles with staleness guards"
```

---

### Task 6: Role corrections and merged-person exclusion in `roles.py`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/roles.py` (`ROLE_COLUMNS` line 54, `build_role_assignments_insert_sql` line 403, `_role_assignment_quality_sql` line 501, `_publish_role_assignments_sql` line 541, `materialize_se_company_person_roles` line 601)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_roles.py`

**Interfaces:**
- Consumes: `ROLE_CORRECTION_KINDS` from `corrections.py`.
- Produces: `ROLE_COLUMNS` gains `"correction_ids"` after `"person_draft_ids"`; `build_stale_role_corrections_sql(company_ids) -> str`; metadata `applied_role_correction_count`, `stale_role_correction_count`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_person_roles.py`:

```python
from dagster_v3.defs.company_people.roles import build_stale_role_corrections_sql


def test_role_assignments_apply_ledger_role_corrections_and_skip_merged_people() -> None:
    sql = build_role_assignments_insert_sql("`corpscout`.`_tmp_roles`", ["5565200028"])

    assert "role_corrections AS (" in sql
    assert "correction_kind IN ('set_role', 'remove_role')" in sql
    assert "arrayJoin(ledger.draft_ids) AS person_draft_id" in sql
    assert "JSONExtractString(ledger.payload, 'role_code')" in sql
    assert "WHERE is_active = 1" in sql
    assert "people.merged_into_person_id IS NULL" in sql
    assert "corrections.correction_kind != 'remove_role'" in sql
    assert "arraySort(groupUniqArray(corrections.correction_id))" in sql
    assert "correction_ids" in sql
    for column in ROLE_COLUMNS:
        assert column in sql
    assert "correction_ids" in ROLE_COLUMNS


def test_stale_role_corrections_are_counted_not_applied() -> None:
    sql = build_stale_role_corrections_sql(["5565200028"])

    assert "FROM corpscout.se_company_person_correction" in sql
    assert "correction_kind IN ('set_role', 'remove_role')" in sql
    assert "supersedes_correction_id IS NOT NULL" in sql
    assert "count()" in sql
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_se_company_person_roles.py -q`
Expected: FAIL with `ImportError: cannot import name 'build_stale_role_corrections_sql'`

- [ ] **Step 3: Implement**

In `roles.py`:

- Add `"correction_ids",` to `ROLE_COLUMNS` after `"person_draft_ids",`.
- Add `from dagster_v3.defs.company_people.corrections import ROLE_CORRECTION_KINDS` and a helper:

```python
def _role_kinds_sql() -> str:
    return ", ".join(f"'{kind}'" for kind in ROLE_CORRECTION_KINDS)


def _role_corrections_cte(company_ids: Sequence[str]) -> str:
    """Live set_role / remove_role decisions, one row per draft they bind to.

    A correction applies only when the draft currently belongs to its subject
    person (enforced by the join in the assignment query) and, for set_role,
    when the requested role_code is active.
    """
    company_filter = _company_filter("ledger.company_id", company_ids)
    return f"""role_corrections AS (
    SELECT
        ledger.correction_id,
        ledger.company_id,
        ledger.subject_person_id,
        ledger.correction_kind,
        arrayJoin(ledger.draft_ids) AS person_draft_id,
        JSONExtractString(ledger.payload, 'role_code') AS role_code,
        if(
            JSONHas(ledger.payload, 'fiscal_year')
                AND JSONType(ledger.payload, 'fiscal_year') != 'Null',
            toNullable(toUInt16(JSONExtractUInt(ledger.payload, 'fiscal_year'))),
            CAST(NULL, 'Nullable(UInt16)')
        ) AS fiscal_year_filter,
        ledger.created_at
    FROM corpscout.se_company_person_correction AS ledger
    WHERE {company_filter}
      AND ledger.correction_kind IN ({_role_kinds_sql()})
      AND ledger.correction_id NOT IN (
          SELECT supersedes_correction_id
          FROM corpscout.se_company_person_correction
          WHERE supersedes_correction_id IS NOT NULL
      )
      AND (
          ledger.correction_kind = 'remove_role'
          OR JSONExtractString(ledger.payload, 'role_code') IN (
              SELECT role_code
              FROM corpscout.company_person_role_type FINAL
              WHERE is_active = 1
          )
      )
)"""
```

- In `build_role_assignments_insert_sql`, add the CTE to the `WITH` list, change `person_evidence` to `WHERE {company_filter} AND people.merged_into_person_id IS NULL` and select `people.person_id` as before, and replace the `assignments` CTE with:

```sql
corrected_roles AS (
    SELECT
        evidence.person_id,
        evidence.company_id,
        evidence.person_draft_id,
        roles.role_draft_id,
        roles.source,
        roles.fiscal_year,
        roles.source_observed_at,
        if(
            corrections.correction_kind = 'set_role',
            corrections.role_code,
            roles.role_code
        ) AS role_code,
        corrections.correction_id,
        corrections.correction_kind
    FROM person_evidence AS evidence
    INNER JOIN latest_role_drafts AS roles
        ON roles.person_draft_id = evidence.person_draft_id
       AND roles.company_id = evidence.company_id
    LEFT JOIN (
        SELECT *
        FROM role_corrections
        ORDER BY created_at DESC, correction_id DESC
        LIMIT 1 BY company_id, subject_person_id, person_draft_id
    ) AS corrections
        ON corrections.company_id = evidence.company_id
       AND corrections.subject_person_id = evidence.person_id
       AND corrections.person_draft_id = evidence.person_draft_id
       AND (
           corrections.fiscal_year_filter IS NULL
           OR corrections.fiscal_year_filter = roles.fiscal_year
       )
    WHERE corrections.correction_kind != 'remove_role'
),
assignments AS (
    SELECT
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'se-company-person-role-v2\n',
            company_id, '\n',
            toString(person_id), '\n',
            role_code, '\n',
            ifNull(toString(fiscal_year), 'undated')
        ))), 1, 32))) AS role_id,
        person_id,
        company_id,
        role_code,
        arraySort(groupUniqArray(role_draft_id)) AS role_draft_ids,
        arraySort(groupUniqArray(person_draft_id)) AS person_draft_ids,
        arraySort(groupUniqArrayIf(
            corrections.correction_id,
            toString(corrections.correction_id) != '{_ZERO_UUID}'
        )) AS correction_ids,
        arraySort(groupUniqArray(toString(source))) AS sources,
        fiscal_year,
        min(source_observed_at) AS first_observed_at,
        max(source_observed_at) AS last_observed_at
    FROM corrected_roles AS corrections
    GROUP BY person_id, company_id, role_code, fiscal_year
)
```

(`LEFT JOIN` with the default-empty `correction_kind = ''` keeps uncorrected rows; `!= 'remove_role'` drops removed ones.) Add `assignments.correction_ids,` to the final SELECT after `assignments.person_draft_ids,`, and `existing.correction_ids` / `staged.correction_ids` in the two branches of `_publish_role_assignments_sql`.

- Add:

```python
def build_stale_role_corrections_sql(company_ids: Sequence[str] = ()) -> str:
    """Live role corrections that bound to no current (person, draft) pair."""
    company_filter = _company_filter("ledger.company_id", company_ids)
    return f"""WITH live AS (
    SELECT ledger.correction_id, ledger.company_id, ledger.subject_person_id,
           arrayJoin(ledger.draft_ids) AS person_draft_id
    FROM corpscout.se_company_person_correction AS ledger
    WHERE {company_filter}
      AND ledger.correction_kind IN ({_role_kinds_sql()})
      AND ledger.correction_id NOT IN (
          SELECT supersedes_correction_id
          FROM corpscout.se_company_person_correction
          WHERE supersedes_correction_id IS NOT NULL
      )
),
bound AS (
    SELECT people.person_id, people.company_id, arrayJoin(people.draft_ids) AS person_draft_id
    FROM corpscout.se_company_person AS people FINAL
    WHERE people.merged_into_person_id IS NULL
)
SELECT
    uniqExactIf(live.correction_id, toString(bound.person_id) = '{_ZERO_UUID}') AS stale_count,
    uniqExactIf(live.correction_id, toString(bound.person_id) != '{_ZERO_UUID}') AS applied_count,
    count()
FROM live
LEFT JOIN bound
    ON bound.company_id = live.company_id
   AND bound.person_id = live.subject_person_id
   AND bound.person_draft_id = live.person_draft_id"""
```

- In `materialize_se_company_person_roles`, after the publish step, execute it and add `"stale_role_correction_count": int(stale[0])`, `"applied_role_correction_count": int(stale[1])` to metadata and the log line.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_se_company_person_roles.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/roles.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_roles.py
git commit -m "feat(company_people): apply set_role/remove_role corrections and skip merged people in role assignments"
```

---

### Task 7: Review job and correction sensor

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py`

**Interfaces:**
- Produces: `se_company_person_review_job`, `se_company_person_correction_cursor(clickhouse) -> str`, `touched_company_ids_since(clickhouse, since: str) -> tuple[str, ...]`, `se_company_person_correction_sensor`, `defs`.

- [ ] **Step 1: Write the failing tests**

```python
import dagster as dg

from dagster_v3.defs.company_people.corrections import (
    build_correction_cursor_sql,
    build_touched_companies_sql,
    review_run_request,
)


def test_review_job_selects_person_and_role_assets_without_draft_import() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    keys = {
        key.path[-1]
        for key in repository.get_job("se_company_person_review_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "se_company_person_clickhouse",
        "se_company_person_role_draft_clickhouse",
        "se_company_person_role_clickhouse",
    }
    sensor = repository.get_sensor_def("se_company_person_correction_sensor")
    assert sensor.job_name == "se_company_person_review_job"
    assert sensor.minimum_interval_seconds == 60


def test_sensor_sql_reads_cursor_and_touched_companies() -> None:
    assert "argMax(correction_id, (created_at, correction_id))" in build_correction_cursor_sql()
    assert "toString(max(created_at))" in build_correction_cursor_sql()
    touched = build_touched_companies_sql()
    assert "WHERE created_at > parseDateTime64BestEffort(%(since)s, 3)" in touched
    assert "SELECT DISTINCT company_id" in touched


def test_run_request_scopes_every_asset_to_touched_companies() -> None:
    request = review_run_request("3:abc:2026-08-22 09:00:00.000", ("5565200028",))

    assert request.run_key == "se-company-person-correction:3:abc:2026-08-22 09:00:00.000"
    assert request.run_config == {
        "ops": {
            "se_company_person_clickhouse": {"config": {"company_ids": ["5565200028"]}},
            "se_company_person_role_draft_clickhouse": {"config": {"company_ids": ["5565200028"]}},
            "se_company_person_role_clickhouse": {"config": {"company_ids": ["5565200028"]}},
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_se_company_person_corrections.py -q`
Expected: FAIL with `ImportError: cannot import name 'build_correction_cursor_sql'`

- [ ] **Step 3: Implement**

Append to `corrections.py` (add `import dagster as dg` and `from dagster_clickhouse import ClickhouseResource`):

```python
REVIEW_ASSET_NAMES = (
    "se_company_person_clickhouse",
    "se_company_person_role_draft_clickhouse",
    "se_company_person_role_clickhouse",
)

se_company_person_review_job = dg.define_asset_job(
    "se_company_person_review_job",
    selection=dg.AssetSelection.assets(*REVIEW_ASSET_NAMES),
)


def build_correction_cursor_sql() -> str:
    return f"""SELECT
    count(),
    if(count() = 0, '', toString(argMax(correction_id, (created_at, correction_id)))),
    if(count() = 0, '', toString(max(created_at)))
FROM {QUALIFIED_CORRECTION_TABLE}"""


def build_touched_companies_sql() -> str:
    return f"""SELECT DISTINCT company_id
FROM {QUALIFIED_CORRECTION_TABLE}
WHERE created_at > parseDateTime64BestEffort(%(since)s, 3)
ORDER BY company_id"""


def se_company_person_correction_cursor(clickhouse: ClickhouseResource) -> str:
    """`count:last_id:last_created_at`; advances for every appended ledger row."""
    with clickhouse.get_connection() as client:
        rows = client.execute(build_correction_cursor_sql())
    if not rows or int(rows[0][0]) == 0:
        return ""
    return f"{int(rows[0][0])}:{rows[0][1]}:{rows[0][2]}"


def touched_company_ids_since(
    clickhouse: ClickhouseResource, since: str
) -> tuple[str, ...]:
    with clickhouse.get_connection() as client:
        rows = client.execute(build_touched_companies_sql(), {"since": since})
    return tuple(str(row[0]) for row in rows)


def review_run_request(cursor: str, company_ids: Sequence[str]) -> dg.RunRequest:
    return dg.RunRequest(
        run_key=f"se-company-person-correction:{cursor}",
        run_config={
            "ops": {
                asset_name: {"config": {"company_ids": list(company_ids)}}
                for asset_name in REVIEW_ASSET_NAMES
            }
        },
    )


@dg.sensor(
    name="se_company_person_correction_sensor",
    job=se_company_person_review_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    required_resource_keys={"clickhouse"},
)
def se_company_person_correction_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    cursor = se_company_person_correction_cursor(context.resources.clickhouse)
    if cursor == "":
        return dg.SkipReason("No Sweden company-person corrections exist")
    if cursor == context.cursor:
        return dg.SkipReason("No new Sweden company-person corrections")
    previous_created_at = (
        context.cursor.split(":", 2)[2] if context.cursor else "1970-01-01 00:00:00.000"
    )
    company_ids = touched_company_ids_since(
        context.resources.clickhouse, previous_created_at
    )
    if not company_ids:
        return dg.SensorResult(run_requests=[], cursor=cursor)
    return dg.SensorResult(
        run_requests=[review_run_request(cursor, company_ids)],
        cursor=cursor,
    )


defs = dg.Definitions(
    jobs=[se_company_person_review_job],
    sensors=[se_company_person_correction_sensor],
)
```

- [ ] **Step 4: Run tests and `dg check defs`**

Run: `uv run pytest tests/test_se_company_person_corrections.py -q && uv run dg check defs`
Expected: PASS / green

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py
git commit -m "feat(company_people): re-run touched companies when the correction ledger grows"
```

---

### Task 8: Backoffice pure validator for ledger rows

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-person-corrections.ts`
- Test: `corpscout/services/backoffice/tests/se-person-corrections.test.ts`

**Interfaces:**
- Produces:
  - `SE_PERSON_CORRECTION_KINDS` (readonly tuple of the nine kinds), `SePersonCorrectionKind`.
  - `SePersonCorrectionValidationError extends Error`.
  - `SePersonCorrectionInput` `{ companyId: string; kind: string; subjectPersonId: string; targetPersonId?: string | null; draftIds?: string[]; payload?: Record<string, unknown>; evidenceHash: string; reason: string; supersedesCorrectionId?: string | null; activeRoleCodes: ReadonlySet<string> }`.
  - `validateSePersonCorrection(input): SePersonCorrectionDraft` returning `{ company_id, correction_kind, subject_person_id, target_person_id, draft_ids, payload (string), evidence_hash, reason, supersedes_correction_id }` — everything but `correction_id`, `decided_by`, `created_at`.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/se-person-corrections.test.ts
import { describe, expect, it } from "vitest";
import {
  SE_PERSON_CORRECTION_KINDS,
  SePersonCorrectionValidationError,
  validateSePersonCorrection,
} from "~/lib/se-person-corrections";

const SUBJECT = "43234b7d-0184-16b5-de47-dc086a2b0ed9";
const TARGET = "6942ffc1-e104-ebea-7aa0-ef7377e8a508";
const DRAFT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const HASH = "a".repeat(64);
const base = {
  companyId: "5565200028",
  subjectPersonId: SUBJECT,
  evidenceHash: HASH,
  reason: "Reviewer note",
  activeRoleCodes: new Set(["board_member", "board_chair"]),
};

describe("validateSePersonCorrection", () => {
  it("lists the nine kinds", () => {
    expect(SE_PERSON_CORRECTION_KINDS).toEqual([
      "merge_persons", "reassign_draft", "split_person", "approve_suggestion",
      "reject_suggestion", "override_field", "set_role", "remove_role", "undo",
    ]);
  });

  it("builds an override row with a JSON payload", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "override_field", payload: { name: " Anna K. Svensson " },
    });
    expect(row).toEqual({
      company_id: "5565200028",
      correction_kind: "override_field",
      subject_person_id: SUBJECT,
      target_person_id: null,
      draft_ids: [],
      payload: JSON.stringify({ name: "Anna K. Svensson" }),
      evidence_hash: HASH,
      reason: "Reviewer note",
      supersedes_correction_id: null,
    });
  });

  it("requires exactly one draft and a distinct target for reassign_draft", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: SUBJECT, draftIds: [DRAFT] }),
    ).toThrow(SePersonCorrectionValidationError);
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: TARGET, draftIds: [DRAFT, DRAFT] }),
    ).toThrow("exactly one");
    expect(
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: TARGET, draftIds: [DRAFT] }).draft_ids,
    ).toEqual([DRAFT]);
  });

  it("requires a non-empty name for split_person", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "split_person", draftIds: [DRAFT], payload: { name: " " } }),
    ).toThrow("name");
  });

  it("accepts only active role codes for set_role and rejects unknown payload keys", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "set_role", draftIds: [DRAFT], payload: { role_code: "ceo" } }),
    ).toThrow("active canonical role");
    const row = validateSePersonCorrection({
      ...base, kind: "set_role", draftIds: [DRAFT], payload: { role_code: "board_chair", fiscal_year: 2023 },
    });
    expect(JSON.parse(row.payload)).toEqual({ role_code: "board_chair", fiscal_year: 2023 });
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "override_field", payload: { nickname: "x" } }),
    ).toThrow("not allowed");
  });

  it("requires supersedes for undo and a uuid suggestion id for approvals", () => {
    expect(() => validateSePersonCorrection({ ...base, kind: "undo" })).toThrow("supersedes");
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "approve_suggestion", payload: { suggestion_id: "nope" } }),
    ).toThrow("suggestion_id");
  });

  it("rejects bad company ids, hashes, kinds and empty reasons", () => {
    expect(() => validateSePersonCorrection({ ...base, companyId: "123", kind: "override_field", payload: { name: "A" } })).toThrow("10-digit");
    expect(() => validateSePersonCorrection({ ...base, evidenceHash: "xyz", kind: "override_field", payload: { name: "A" } })).toThrow("evidence");
    expect(() => validateSePersonCorrection({ ...base, kind: "delete_person" })).toThrow("Unknown correction");
    expect(() => validateSePersonCorrection({ ...base, reason: " ", kind: "override_field", payload: { name: "A" } })).toThrow("Reason");
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run (from `corpscout/services/backoffice`): `npx vitest run tests/se-person-corrections.test.ts`
Expected: FAIL — module not found

- [ ] **Step 3: Implement**

```ts
// app/lib/se-person-corrections.ts
export const SE_PERSON_CORRECTION_KINDS = [
  "merge_persons",
  "reassign_draft",
  "split_person",
  "approve_suggestion",
  "reject_suggestion",
  "override_field",
  "set_role",
  "remove_role",
  "undo",
] as const;

export type SePersonCorrectionKind = (typeof SE_PERSON_CORRECTION_KINDS)[number];

export class SePersonCorrectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SePersonCorrectionValidationError";
  }
}

export interface SePersonCorrectionInput {
  companyId: string;
  kind: string;
  subjectPersonId: string;
  targetPersonId?: string | null;
  draftIds?: string[];
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
  activeRoleCodes: ReadonlySet<string>;
}

export interface SePersonCorrectionDraft {
  company_id: string;
  correction_kind: SePersonCorrectionKind;
  subject_person_id: string;
  target_person_id: string | null;
  draft_ids: string[];
  payload: string;
  evidence_hash: string;
  reason: string;
  supersedes_correction_id: string | null;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const ALLOWED_PAYLOAD_KEYS: Record<SePersonCorrectionKind, readonly string[]> = {
  merge_persons: [],
  reassign_draft: [],
  split_person: ["name"],
  approve_suggestion: ["suggestion_id"],
  reject_suggestion: ["suggestion_id", "note"],
  override_field: ["name", "description"],
  set_role: ["role_code", "fiscal_year"],
  remove_role: [],
  undo: [],
};

function fail(message: string): never {
  throw new SePersonCorrectionValidationError(message);
}

function uuidOrFail(value: string | null | undefined, label: string): string {
  const clean = (value ?? "").trim().toLowerCase();
  if (!UUID_PATTERN.test(clean)) fail(`${label} must be a UUID.`);
  return clean;
}

function isKind(value: string): value is SePersonCorrectionKind {
  return (SE_PERSON_CORRECTION_KINDS as readonly string[]).includes(value);
}

export function validateSePersonCorrection(
  input: SePersonCorrectionInput,
): SePersonCorrectionDraft {
  const companyId = input.companyId.replace(/[^0-9]/g, "");
  if (companyId.length !== 10 || companyId !== input.companyId.trim()) {
    fail("Company must be a 10-digit Swedish organization number.");
  }
  if (!isKind(input.kind)) fail("Unknown correction kind.");
  const kind = input.kind;
  const subject = uuidOrFail(input.subjectPersonId, "Subject person");
  const evidenceHash = input.evidenceHash.trim().toLowerCase();
  if (!HASH_PATTERN.test(evidenceHash)) fail("The evidence hash is missing or malformed.");
  const reason = input.reason.trim();
  if (reason === "" || reason.length > 1000) fail("Reason is required (max 1000 characters).");

  const payload = input.payload ?? {};
  for (const key of Object.keys(payload)) {
    if (!ALLOWED_PAYLOAD_KEYS[kind].includes(key)) {
      fail(`Payload key "${key}" is not allowed for ${kind}.`);
    }
  }
  const draftIds = [...new Set((input.draftIds ?? []).map((id) => uuidOrFail(id, "Draft")))];
  let target: string | null = null;
  const cleanPayload: Record<string, unknown> = {};

  switch (kind) {
    case "merge_persons":
    case "reassign_draft": {
      target = uuidOrFail(input.targetPersonId, "Target person");
      if (target === subject) fail("Target person must differ from the subject.");
      if (kind === "reassign_draft" && draftIds.length !== 1) {
        fail("Reassign moves exactly one draft.");
      }
      if (kind === "merge_persons" && draftIds.length !== 0) {
        fail("Merge does not take draft ids.");
      }
      break;
    }
    case "split_person": {
      if (draftIds.length === 0) fail("Select at least one draft to split out.");
      const name = String(payload.name ?? "").trim();
      if (name === "") fail("Split needs the new person's name.");
      cleanPayload.name = name;
      break;
    }
    case "approve_suggestion":
    case "reject_suggestion": {
      cleanPayload.suggestion_id = uuidOrFail(
        typeof payload.suggestion_id === "string" ? payload.suggestion_id : null,
        "suggestion_id",
      );
      if (kind === "reject_suggestion" && typeof payload.note === "string" && payload.note.trim()) {
        cleanPayload.note = payload.note.trim();
      }
      break;
    }
    case "override_field": {
      if ("name" in payload) {
        const name = String(payload.name ?? "").trim();
        if (name === "") fail("Override name cannot be empty.");
        cleanPayload.name = name;
      }
      if ("description" in payload) {
        const description = payload.description;
        cleanPayload.description =
          description === null || String(description).trim() === ""
            ? null
            : String(description).trim();
      }
      if (Object.keys(cleanPayload).length === 0) fail("Override needs a name or description.");
      break;
    }
    case "set_role":
    case "remove_role": {
      if (draftIds.length === 0) fail("Select at least one draft for the role change.");
      if (kind === "set_role") {
        const roleCode = String(payload.role_code ?? "").trim();
        if (!input.activeRoleCodes.has(roleCode)) fail("Select an active canonical role.");
        cleanPayload.role_code = roleCode;
        if ("fiscal_year" in payload && payload.fiscal_year !== null) {
          const year = Number(payload.fiscal_year);
          if (!Number.isInteger(year) || year < 1800 || year > 9999) fail("fiscal_year must be a year.");
          cleanPayload.fiscal_year = year;
        }
      }
      break;
    }
    case "undo": {
      if (!input.supersedesCorrectionId) fail("Undo needs the correction it supersedes.");
      break;
    }
  }

  return {
    company_id: companyId,
    correction_kind: kind,
    subject_person_id: subject,
    target_person_id: target,
    draft_ids: draftIds,
    payload: JSON.stringify(cleanPayload),
    evidence_hash: evidenceHash,
    reason,
    supersedes_correction_id: input.supersedesCorrectionId
      ? uuidOrFail(input.supersedesCorrectionId, "Superseded correction")
      : null,
  };
}
```

- [ ] **Step 4: Run the tests**

Run: `npx vitest run tests/se-person-corrections.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-person-corrections.ts \
        corpscout/services/backoffice/tests/se-person-corrections.test.ts
git commit -m "feat(backoffice): validate Sweden company-person correction rows"
```

---

### Task 9: Backoffice writer and server module for the review page

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/clickhouse.server.ts:94-104` (add writer function next to `chInsertPersonCorrections`)
- Create: `corpscout/services/backoffice/app/lib/se-company-person.server.ts`
- Test: `corpscout/services/backoffice/tests/clickhouse-writer.server.test.ts`, `corpscout/services/backoffice/tests/se-company-person.server.test.ts`

**Interfaces:**
- Consumes: `validateSePersonCorrection`, `SePersonCorrectionDraft` (Task 8).
- Produces:
  - `chInsertSeCompanyPersonCorrections<T extends object>(values: T[]): Promise<void>`.
  - `seCompanyPersonId(companyId: string, name: string): string` — mirrors Python `person_id_for`.
  - `getSeCompanyPerson(companyId, personId): Promise<SeCompanyPersonDetail | null>` with `{ person, drafts, roles, suggestions, corrections }`.
  - `appendSeCompanyPersonCorrection(input: SePersonCorrectionInput): Promise<{ correctionId: string }>` — re-reads the person's `draft_set_hash`; throws `SePersonCorrectionValidationError("The evidence changed while you were reviewing. Reload and decide again.")` on mismatch (except `undo`, which carries the zero hash).

- [ ] **Step 1: Write the failing tests**

Add to `tests/clickhouse-writer.server.test.ts`:

```ts
import { chInsertSeCompanyPersonCorrections } from "~/lib/clickhouse.server";

it("writes Sweden company-person corrections with the writer client", async () => {
  vi.stubEnv("CLICKHOUSE_WRITE_USER", "correction_writer");
  vi.stubEnv("CLICKHOUSE_WRITE_PASSWORD", "writer-secret");
  clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
  clickhouse.insert.mockResolvedValue(undefined);

  await chInsertSeCompanyPersonCorrections([{ correction_id: "test" }]);

  expect(clickhouse.insert).toHaveBeenCalledWith({
    table: "se_company_person_correction",
    values: [{ correction_id: "test" }],
    format: "JSONEachRow",
  });
});
```

New `tests/se-company-person.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyPersonCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  appendSeCompanyPersonCorrection,
  seCompanyPersonId,
} from "~/lib/se-company-person.server";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

const COMPANY = "5565200028";
const PERSON = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

describe("seCompanyPersonId", () => {
  it("matches the Dagster person_id_for hash", () => {
    expect(seCompanyPersonId(COMPANY, "David Mindus")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "  david   MINDUS ")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "Anna Karin Svensson")).toBe(
      "6942ffc1-e104-ebea-7aa0-ef7377e8a508",
    );
  });
});

describe("appendSeCompanyPersonCorrection", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("refuses when the published evidence hash moved", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "b".repeat(64) }]);

    await expect(
      appendSeCompanyPersonCorrection({
        companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
        payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
        reason: "spelling", activeRoleCodes: new Set(),
      }),
    ).rejects.toThrow(SePersonCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("appends one row with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "a".repeat(64) }]);
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await appendSeCompanyPersonCorrection({
      companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
      payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
      reason: "spelling", activeRoleCodes: new Set(),
    });

    expect(result.correctionId).toMatch(/^[0-9a-f-]{36}$/);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      correction_id: result.correctionId,
      company_id: COMPANY,
      correction_kind: "override_field",
      subject_person_id: PERSON,
      payload: JSON.stringify({ name: "David G. Mindus" }),
      decided_by: "backoffice",
    });
    expect(rows[0].created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run tests/clickhouse-writer.server.test.ts tests/se-company-person.server.test.ts`
Expected: FAIL — missing exports

- [ ] **Step 3: Add the writer**

In `app/lib/clickhouse.server.ts` after `chInsertPersonCorrections`:

```ts
/** Append reviewed decisions to the Sweden company-person correction ledger. */
export async function chInsertSeCompanyPersonCorrections<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_person_correction",
    values,
    format: "JSONEachRow",
  });
}
```

- [ ] **Step 4: Create `se-company-person.server.ts`**

```ts
import { createHash, randomUUID } from "node:crypto";
import {
  chInsertSeCompanyPersonCorrections,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  SePersonCorrectionValidationError,
  validateSePersonCorrection,
  type SePersonCorrectionInput,
} from "~/lib/se-person-corrections";

export const ZERO_EVIDENCE_HASH = "0".repeat(64);
const CORRECTION_ACTOR = "backoffice";

export interface SeCompanyPersonRow {
  person_id: string;
  company_id: string;
  name: string;
  description: string | null;
  draft_ids: string[];
  draft_set_hash: string;
  correction_ids: string[];
  suggestion_id: string | null;
  merged_into_person_id: string | null;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  updated_at: string;
}

export interface SeCompanyPersonDraftRow {
  draft_id: string;
  source: string;
  name: string;
  role_original: string;
  fiscal_year: number | null;
  source_observed_at: string;
  source_value_json: string;
}

export interface SeCompanyPersonRoleRow {
  role_id: string;
  role_code: string;
  fiscal_year: number | null;
  sources: string[];
  role_draft_ids: string[];
  person_draft_ids: string[];
  correction_ids: string[];
  is_current: number;
}

export interface SeCompanyPersonSuggestionRow {
  suggestion_id: string;
  input_hash: string;
  draft_ids: string[];
  suggestion: string;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  created_at: string;
  is_published: number;
}

export interface SeCompanyPersonCorrectionRow {
  correction_id: string;
  correction_kind: string;
  subject_person_id: string;
  target_person_id: string | null;
  draft_ids: string[];
  payload: string;
  evidence_hash: string;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  created_at: string;
  is_current: number;
  is_stale: number;
  is_applied: number;
}

export interface SeCompanyPersonDetail {
  person: SeCompanyPersonRow;
  drafts: SeCompanyPersonDraftRow[];
  roles: SeCompanyPersonRoleRow[];
  suggestions: SeCompanyPersonSuggestionRow[];
  corrections: SeCompanyPersonCorrectionRow[];
}

/** Same hash as dagster_v3 normalization.person_id_for: sha256 of company + first|last token key. */
export function seCompanyPersonId(companyId: string, name: string): string {
  const tokens = name.trim().replace(/\s+/g, " ").toLowerCase().split(" ").filter(Boolean);
  const key =
    tokens.length < 2 ? (tokens[0] ?? "") : `${tokens[0]}|${tokens[tokens.length - 1]}`;
  const hex = createHash("sha256")
    .update(`se-company-person-v1\n${companyId}\n${key}`)
    .digest("hex")
    .slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

const PERSON_SQL = `SELECT
  toString(person_id) AS person_id, company_id, name, description,
  arrayMap(id -> toString(id), draft_ids) AS draft_ids,
  toString(draft_set_hash) AS draft_set_hash,
  arrayMap(id -> toString(id), correction_ids) AS correction_ids,
  toString(suggestion_id) AS suggestion_id,
  toString(merged_into_person_id) AS merged_into_person_id,
  toString(model_provider) AS model_provider, model_name, prompt_version,
  toString(updated_at) AS updated_at
FROM corpscout.se_company_person FINAL
WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
LIMIT 1`;

const DRAFTS_SQL = `SELECT
  toString(draft_id) AS draft_id, toString(source) AS source,
  JSONExtractString(source_value_json, 'name') AS name,
  multiIf(
    source = 'bolagsverket', JSONExtractString(source_value_json, 'role_original'),
    source = 'esef', JSONExtractString(source_value_json, 'role'),
    JSONExtractString(source_value_json, 'role_label')
  ) AS role_original,
  fiscal_year, toString(source_observed_at) AS source_observed_at, source_value_json
FROM corpscout.se_company_person_draft FINAL
WHERE company_id = {companyId:String} AND draft_id IN {draftIds:Array(UUID)}
ORDER BY source, fiscal_year, draft_id`;

const ROLES_SQL = `SELECT
  toString(role_id) AS role_id, role_code, fiscal_year, sources,
  arrayMap(id -> toString(id), role_draft_ids) AS role_draft_ids,
  arrayMap(id -> toString(id), person_draft_ids) AS person_draft_ids,
  arrayMap(id -> toString(id), correction_ids) AS correction_ids,
  toUInt8(is_current) AS is_current
FROM corpscout.se_company_person_role FINAL
WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
ORDER BY is_current DESC, fiscal_year DESC NULLS LAST, role_code`;

const SUGGESTIONS_SQL = `SELECT
  toString(s.suggestion_id) AS suggestion_id, toString(s.input_hash) AS input_hash,
  arrayMap(id -> toString(id), s.draft_ids) AS draft_ids, s.suggestion,
  toString(s.model_provider) AS model_provider, s.model_name, s.prompt_version,
  toString(s.created_at) AS created_at,
  toUInt8(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}) AS is_published
FROM corpscout.se_company_person_suggestion AS s
WHERE s.company_id = {companyId:String} AND s.person_id = {personId:UUID}
ORDER BY s.created_at DESC
LIMIT 50`;

const CORRECTIONS_SQL = `WITH superseded AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_person_correction
  WHERE company_id = {companyId:String} AND supersedes_correction_id IS NOT NULL
)
SELECT
  toString(c.correction_id) AS correction_id, c.correction_kind,
  toString(c.subject_person_id) AS subject_person_id,
  toString(c.target_person_id) AS target_person_id,
  arrayMap(id -> toString(id), c.draft_ids) AS draft_ids,
  c.payload, toString(c.evidence_hash) AS evidence_hash, c.reason, c.decided_by,
  toString(c.supersedes_correction_id) AS supersedes_correction_id,
  toString(c.created_at) AS created_at,
  toUInt8(c.correction_id NOT IN (SELECT id FROM superseded)) AS is_current,
  toUInt8(
    c.correction_id NOT IN (SELECT id FROM superseded)
    AND toString(c.evidence_hash) != {zeroHash:String}
    AND toString(c.evidence_hash) != {draftSetHash:String}
  ) AS is_stale,
  toUInt8(has({appliedIds:Array(String)}, toString(c.correction_id))) AS is_applied
FROM corpscout.se_company_person_correction AS c
WHERE c.company_id = {companyId:String}
  AND (c.subject_person_id = {personId:UUID} OR c.target_person_id = {personId:UUID})
ORDER BY c.created_at DESC, c.correction_id DESC
LIMIT 200`;

export async function getSeCompanyPerson(
  companyId: string,
  personId: string,
): Promise<SeCompanyPersonDetail | null> {
  const [person] = await chQuery<SeCompanyPersonRow>(PERSON_SQL, { companyId, personId });
  if (!person) return null;
  const [drafts, roles, suggestions, corrections] = await Promise.all([
    chQuery<SeCompanyPersonDraftRow>(DRAFTS_SQL, { companyId, draftIds: person.draft_ids }),
    chQuery<SeCompanyPersonRoleRow>(ROLES_SQL, { companyId, personId }),
    chQuery<SeCompanyPersonSuggestionRow>(SUGGESTIONS_SQL, {
      companyId, personId, publishedSuggestionId: person.suggestion_id,
    }),
    chQuery<SeCompanyPersonCorrectionRow>(CORRECTIONS_SQL, {
      companyId, personId, zeroHash: ZERO_EVIDENCE_HASH,
      draftSetHash: person.draft_set_hash, appliedIds: person.correction_ids,
    }),
  ]);
  return { person, drafts, roles, suggestions, corrections };
}

function correctionTimestamp(): string {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

export async function appendSeCompanyPersonCorrection(
  input: SePersonCorrectionInput,
): Promise<{ correctionId: string }> {
  const draft = validateSePersonCorrection(input);
  if (draft.correction_kind !== "undo") {
    const [current] = await chQuery<{ draft_set_hash: string }>(
      `SELECT toString(draft_set_hash) AS draft_set_hash
       FROM corpscout.se_company_person FINAL
       WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
       LIMIT 1`,
      { companyId: draft.company_id, personId: draft.subject_person_id },
    );
    if (!current) {
      throw new SePersonCorrectionValidationError("This person is not published.");
    }
    if (current.draft_set_hash !== draft.evidence_hash) {
      throw new SePersonCorrectionValidationError(
        "The evidence changed while you were reviewing. Reload and decide again.",
      );
    }
  }
  const correctionId = randomUUID();
  await chInsertSeCompanyPersonCorrections([
    {
      correction_id: correctionId,
      ...draft,
      decided_by: CORRECTION_ACTOR,
      created_at: correctionTimestamp(),
    },
  ]);
  return { correctionId };
}
```

- [ ] **Step 5: Run tests and typecheck**

Run: `npx vitest run tests/clickhouse-writer.server.test.ts tests/se-company-person.server.test.ts && pnpm typecheck`
Expected: PASS / no type errors

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/backoffice/app/lib/clickhouse.server.ts \
        corpscout/services/backoffice/app/lib/se-company-person.server.ts \
        corpscout/services/backoffice/tests/clickhouse-writer.server.test.ts \
        corpscout/services/backoffice/tests/se-company-person.server.test.ts
git commit -m "feat(backoffice): read Sweden company people from ClickHouse and append ledger corrections"
```

---

### Task 10: Review page route, component and links

**Files:**
- Create: `corpscout/services/backoffice/app/routes/admin-se-people-person.tsx`
- Create: `corpscout/services/backoffice/app/components/admin/se-person-review-workspace.tsx`
- Modify: `corpscout/services/backoffice/app/routes.ts:95-113` (admin block)
- Modify: `corpscout/services/backoffice/app/routes/admin-se-people.tsx:159-168` (decorate Draft 2 rows with `se_person_id`), `corpscout/services/backoffice/app/components/admin/people-draft-tables.tsx:604-611` (add the review link next to the LLM-input link)
- Test: `corpscout/services/backoffice/tests/admin-se-people-person.test.tsx`

**Interfaces:**
- Consumes: `getSeCompanyPerson`, `appendSeCompanyPersonCorrection`, `seCompanyPersonId`, `ZERO_EVIDENCE_HASH` (Task 9); `getCompanyPersonRoleTypes` (`~/lib/company-roles.server`); `SePersonCorrectionValidationError` (Task 8).
- Produces: route `admin/se/people/person/:companyId/:personId`; exported component `SePersonReviewWorkspace` with props `{ detail: SeCompanyPersonDetail; activeRoleCodes: string[]; result: { ok: true; correctionId: string } | { ok: false; error: string } | null }`.

- [ ] **Step 1: Write the failing component test**

```tsx
// tests/admin-se-people-person.test.tsx
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SePersonReviewWorkspace } from "~/components/admin/se-person-review-workspace";

const detail = {
  person: {
    person_id: "43234b7d-0184-16b5-de47-dc086a2b0ed9",
    company_id: "5565200028",
    name: "David Mindus",
    description: null,
    draft_ids: ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
    draft_set_hash: "a".repeat(64),
    correction_ids: [],
    suggestion_id: null,
    merged_into_person_id: null,
    model_provider: "deterministic",
    model_name: "single-source:bolagsverket",
    prompt_version: "single-source-copy-v2",
    updated_at: "2026-08-22 09:00:00.000",
  },
  drafts: [
    {
      draft_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      source: "bolagsverket",
      name: "David Mindus",
      role_original: "Verkställande direktör",
      fiscal_year: 2024,
      source_observed_at: "2026-08-01 00:00:00.000",
      source_value_json: "{}",
    },
  ],
  roles: [],
  suggestions: [],
  corrections: [],
};

function render(result: Parameters<typeof SePersonReviewWorkspace>[0]["result"] = null) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SePersonReviewWorkspace
            detail={detail}
            activeRoleCodes={["board_member", "chief_executive_officer"]}
            result={result}
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: ["/admin/se/people/person/5565200028/43234b7d-0184-16b5-de47-dc086a2b0ed9"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("Sweden company-person review page", () => {
  it("shows evidence, provenance and every correction form with the evidence hash", () => {
    const html = render();

    expect(html).toContain("David Mindus");
    expect(html).toContain("Verkställande direktör");
    expect(html).toContain("deterministic");
    for (const kind of ["override_field", "merge_persons", "reassign_draft", "split_person", "set_role", "remove_role"]) {
      expect(html).toContain(`value="${kind}"`);
    }
    expect(html).toContain(`name="evidence_hash" value="${"a".repeat(64)}"`);
    expect(html).toContain("chief_executive_officer");
  });

  it("confirms a saved correction and says Dagster will re-run the company", () => {
    const html = render({ ok: true, correctionId: "55555555-5555-4555-8555-555555555555" });

    expect(html).toContain("Saved");
    expect(html).toContain("re-run company 5565200028");
  });

  it("shows a validation error", () => {
    const html = render({ ok: false, error: "The evidence changed while you were reviewing." });

    expect(html).toContain("The evidence changed while you were reviewing.");
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run tests/admin-se-people-person.test.tsx`
Expected: FAIL — module not found

- [ ] **Step 3: Register the route and write the route module**

In `app/routes.ts`, inside the `route("admin", ...)` block after `route("se/people", "routes/admin-se-people.tsx"),` add:

```ts
    route(
      "se/people/person/:companyId/:personId",
      "routes/admin-se-people-person.tsx",
    ),
```

`app/routes/admin-se-people-person.tsx`:

```tsx
import { data } from "react-router";
import type { Route } from "./+types/admin-se-people-person";
import { SePersonReviewWorkspace } from "~/components/admin/se-person-review-workspace";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";
import {
  appendSeCompanyPersonCorrection,
  getSeCompanyPerson,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-company-person.server";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

export async function loader({ params }: Route.LoaderArgs) {
  const [detail, roleTypes] = await Promise.all([
    getSeCompanyPerson(params.companyId, params.personId),
    getCompanyPersonRoleTypes(),
  ]);
  if (!detail) throw data("Person not found", { status: 404 });
  return {
    detail,
    activeRoleCodes: roleTypes.filter((r) => r.is_active === 1).map((r) => r.role_code),
  };
}

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function optionalText(form: FormData, name: string): string | null {
  return form.has(name) ? text(form, name) : null;
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const kind = text(form, "correction_kind");
  const roleTypes = await getCompanyPersonRoleTypes();
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    if (text(form, "name").trim() !== "") payload.name = text(form, "name");
    if (form.has("description")) payload.description = text(form, "description") || null;
  } else if (kind === "split_person") {
    payload.name = text(form, "name");
  } else if (kind === "approve_suggestion" || kind === "reject_suggestion") {
    payload.suggestion_id = text(form, "suggestion_id");
    const note = optionalText(form, "note");
    if (note) payload.note = note;
  } else if (kind === "set_role") {
    payload.role_code = text(form, "role_code");
    const year = text(form, "fiscal_year").trim();
    if (year !== "") payload.fiscal_year = Number(year);
  }
  try {
    const result = await appendSeCompanyPersonCorrection({
      companyId: params.companyId,
      kind,
      subjectPersonId: params.personId,
      targetPersonId: optionalText(form, "target_person_id"),
      draftIds: form.getAll("draft_id").map(String),
      payload,
      evidenceHash: kind === "undo" ? ZERO_EVIDENCE_HASH : text(form, "evidence_hash"),
      reason: text(form, "reason"),
      supersedesCorrectionId: optionalText(form, "supersedes_correction_id"),
      activeRoleCodes: new Set(
        roleTypes.filter((r) => r.is_active === 1).map((r) => r.role_code),
      ),
    });
    return { ok: true as const, correctionId: result.correctionId };
  } catch (error) {
    if (error instanceof SePersonCorrectionValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.detail.person.name ?? "Person"} review | CompanyCollect` }];
}

export default function AdminSwedenPersonReview({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <SePersonReviewWorkspace
      detail={loaderData.detail}
      activeRoleCodes={loaderData.activeRoleCodes}
      result={actionData ?? null}
    />
  );
}
```

- [ ] **Step 4: Write the workspace component**

`app/components/admin/se-person-review-workspace.tsx` — one card per section, one `<Form method="post">` per action, every form carrying hidden `correction_kind` and `evidence_hash`. Use existing shadcn primitives (`Card`, `Alert`, `Badge`, `Button`, `Input`, `Textarea`, `Checkbox`, `Select`) already under `~/components/ui`. Skeleton (fill in with the same markup style as `person-profile-suggestion-card.tsx`):

```tsx
import { CheckCircle2Icon, TriangleAlertIcon } from "lucide-react";
import { Form, Link } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import type { SeCompanyPersonDetail } from "~/lib/se-company-person.server";

type ActionResult = { ok: true; correctionId: string } | { ok: false; error: string } | null;

function HiddenCommon({ kind, evidenceHash }: { kind: string; evidenceHash: string }) {
  return (
    <>
      <input type="hidden" name="correction_kind" value={kind} />
      <input type="hidden" name="evidence_hash" value={evidenceHash} />
    </>
  );
}

function DraftCheckboxes({ drafts }: { drafts: SeCompanyPersonDetail["drafts"] }) {
  return (
    <div className="flex flex-col gap-1">
      {drafts.map((draft) => (
        <label key={draft.draft_id} className="flex items-center gap-2 text-sm">
          <Checkbox name="draft_id" value={draft.draft_id} />
          <span>{draft.source} · {draft.name} · {draft.role_original || "—"} · {draft.fiscal_year ?? "undated"}</span>
        </label>
      ))}
    </div>
  );
}

export function SePersonReviewWorkspace({
  detail,
  activeRoleCodes,
  result,
}: {
  detail: SeCompanyPersonDetail;
  activeRoleCodes: string[];
  result: ActionResult;
}) {
  const { person, drafts, roles, suggestions, corrections } = detail;
  const hash = person.draft_set_hash;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">ClickHouse</Badge>
          <Badge variant="secondary">{person.model_provider} · {person.model_name}</Badge>
          {person.correction_ids.length > 0 ? <Badge>reviewed</Badge> : null}
          {person.merged_into_person_id ? (
            <Badge variant="destructive">merged into {person.merged_into_person_id}</Badge>
          ) : null}
        </div>
        <h1 className="text-2xl font-semibold tracking-tight">{person.name}</h1>
        <p className="text-sm text-muted-foreground">
          Company{" "}
          <Link className="underline" to={`/company/se/${person.company_id}`}>{person.company_id}</Link>
          {" "}· {drafts.length} source observations · evidence <code className="font-mono text-xs">{hash.slice(0, 12)}</code>
        </p>
        {person.description ? <p className="text-sm">{person.description}</p> : null}
      </header>

      {result?.ok ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Saved</AlertTitle>
          <AlertDescription>
            Correction {result.correctionId} is in the ledger. Dagster will re-run company {person.company_id} within a minute; reload to see the result.
          </AlertDescription>
        </Alert>
      ) : null}
      {result && !result.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Not saved</AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}

      {/* Evidence */}
      <Card>
        <CardHeader><CardTitle>Source observations</CardTitle></CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead><tr><th>Source</th><th>Name</th><th>Role</th><th>Year</th><th>Draft</th></tr></thead>
            <tbody>
              {drafts.map((d) => (
                <tr key={d.draft_id}>
                  <td>{d.source}</td><td>{d.name}</td><td>{d.role_original || "—"}</td>
                  <td>{d.fiscal_year ?? "—"}</td><td className="font-mono text-xs">{d.draft_id}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Roles */}
      <Card>
        <CardHeader><CardTitle>Roles</CardTitle></CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {roles.length === 0 ? <span className="text-sm text-muted-foreground">No role rows.</span> : null}
          {roles.map((r) => (
            <Badge key={r.role_id} variant={r.is_current ? "default" : "outline"}>
              {r.role_code} · {r.fiscal_year ?? "undated"}{r.correction_ids.length ? " · corrected" : ""}
            </Badge>
          ))}
        </CardContent>
      </Card>

      {/* Suggestions */}
      <Card>
        <CardHeader>
          <CardTitle>Model suggestions</CardTitle>
          <CardDescription>Newest first. Approve pins one; reject falls back to the deterministic name.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {suggestions.length === 0 ? <span className="text-sm text-muted-foreground">No suggestions recorded.</span> : null}
          {suggestions.map((s) => (
            <div key={s.suggestion_id} className="rounded-lg border p-3">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{s.model_provider} · {s.model_name}</Badge>
                {s.is_published ? <Badge>published</Badge> : null}
                <span className="text-xs text-muted-foreground">{s.created_at}</span>
              </div>
              <pre className="mt-2 whitespace-pre-wrap text-xs">{s.suggestion}</pre>
              <div className="mt-2 flex gap-2">
                {(["approve_suggestion", "reject_suggestion"] as const).map((kind) => (
                  <Form key={kind} method="post" className="flex items-center gap-2">
                    <HiddenCommon kind={kind} evidenceHash={hash} />
                    <input type="hidden" name="suggestion_id" value={s.suggestion_id} />
                    <Input name="reason" placeholder="Reason" required className="h-8 w-56" />
                    <Button size="sm" variant={kind === "approve_suggestion" ? "default" : "outline"} type="submit">
                      {kind === "approve_suggestion" ? "Approve" : "Reject"}
                    </Button>
                  </Form>
                ))}
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* Corrections */}
      <Card>
        <CardHeader><CardTitle>Correct this person</CardTitle></CardHeader>
        <CardContent className="grid gap-6 lg:grid-cols-2">
          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="override_field" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Override name / description</h3>
            <Input name="name" defaultValue={person.name} />
            <Textarea name="description" defaultValue={person.description ?? ""} />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit">Save override</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="merge_persons" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Merge into another person (same company)</h3>
            <Input name="target_person_id" placeholder="Target person_id (UUID)" required />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">Merge</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="reassign_draft" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Move one observation to another person</h3>
            <DraftCheckboxes drafts={drafts} />
            <Input name="target_person_id" placeholder="Target person_id (UUID)" required />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">Reassign</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="split_person" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Split observations into a new person</h3>
            <DraftCheckboxes drafts={drafts} />
            <Input name="name" placeholder="Name of the new person" required />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">Split</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="set_role" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Set role for observations</h3>
            <DraftCheckboxes drafts={drafts} />
            <select name="role_code" className="h-9 rounded-md border bg-background px-2 text-sm" required>
              {activeRoleCodes.map((code) => <option key={code} value={code}>{code}</option>)}
            </select>
            <Input name="fiscal_year" placeholder="Fiscal year (optional)" inputMode="numeric" />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">Set role</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="remove_role" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Remove role from observations</h3>
            <DraftCheckboxes drafts={drafts} />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">Remove role</Button>
          </Form>
        </CardContent>
      </Card>

      {/* Ledger */}
      <Card>
        <CardHeader><CardTitle>Ledger</CardTitle></CardHeader>
        <CardContent className="flex flex-col gap-2">
          {corrections.length === 0 ? <span className="text-sm text-muted-foreground">No corrections yet.</span> : null}
          {corrections.map((c) => (
            <div key={c.correction_id} className="flex flex-wrap items-center gap-2 rounded-lg border p-2 text-sm">
              <Badge variant={c.is_current ? "default" : "outline"}>{c.correction_kind}</Badge>
              {c.is_stale ? <Badge variant="destructive">stale</Badge> : null}
              {c.is_applied ? <Badge variant="secondary">applied</Badge> : null}
              <span>{c.reason}</span>
              <span className="text-xs text-muted-foreground">{c.decided_by} · {c.created_at}</span>
              <code className="font-mono text-xs">{c.payload}</code>
              {c.is_current && c.correction_kind !== "undo" ? (
                <Form method="post" className="ml-auto flex items-center gap-2">
                  <input type="hidden" name="correction_kind" value="undo" />
                  <input type="hidden" name="supersedes_correction_id" value={c.correction_id} />
                  <Input name="reason" placeholder="Why undo" required className="h-8 w-40" />
                  <Button size="sm" variant="ghost" type="submit">Undo</Button>
                </Form>
              ) : null}
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
```

(`~/components/ui/input.tsx` and `textarea.tsx` exist in this project's shadcn set; if `textarea` is missing, add it with `npx shadcn@latest add textarea`.)

- [ ] **Step 5: Link Draft 2 rows to the review page**

In `app/routes/admin-se-people.tsx` loader, import `seCompanyPersonId` from `~/lib/se-company-person.server` and decorate rows:

```ts
    rows: draftTwoPage.rows.map((row) => ({
      ...row,
      has_llm_suggestion: rowsWithLlmSuggestions.has(row.draft_2_id),
      se_person_id: seCompanyPersonId(row.company_id, row.name),
    })),
```

Add `se_person_id?: string;` to `SwedenPeopleDraftTwoRow` in `app/lib/sweden-people-draft-two.server.ts` (next to `has_llm_suggestion`). In `app/components/admin/people-draft-tables.tsx` beside the existing LLM-input `<Link>` (line 606) add:

```tsx
{person.se_person_id ? (
  <Link
    className={buttonVariants({ variant: "ghost", size: "sm" })}
    to={`/admin/se/people/person/${person.company_id}/${person.se_person_id}`}
  >
    Review in ClickHouse
  </Link>
) : null}
```

(`person` is the row variable already in scope at that line; if the component names it `row`, use that.)

- [ ] **Step 6: Run tests and typecheck**

Run: `npx vitest run tests/admin-se-people-person.test.tsx tests/admin-people-curation.test.tsx && pnpm typecheck`
Expected: PASS / clean

- [ ] **Step 7: Commit (these files only — the rest of the admin area stays uncommitted)**

```bash
git add corpscout/services/backoffice/app/routes.ts \
        corpscout/services/backoffice/app/routes/admin-se-people-person.tsx \
        corpscout/services/backoffice/app/components/admin/se-person-review-workspace.tsx \
        corpscout/services/backoffice/tests/admin-se-people-person.test.tsx
git commit -m "feat(backoffice): ClickHouse-backed Sweden person review page writing ledger corrections"
```

`admin-se-people.tsx`, `people-draft-tables.tsx` and `sweden-people-draft-two.server.ts` are part of the uncommitted admin WIP; their edits ride along with whatever commit the owner makes of that work. Note this in the task hand-off.

---

### Task 11: Apply, deploy and verify end to end

**Files:** none created; operational checklist.

- [ ] **Step 1: Apply migrations 000295 and 000296 on the ClickHouse host** the same way 000288–000293 were applied (golang-migrate against `corpscout`; see `corpscout/clickhouse/`). Verify:

```sql
SELECT name FROM system.columns WHERE database='corpscout' AND table='se_company_person' AND name IN ('correction_ids','correction_set_hash','suggestion_id','merged_into_person_id');
SHOW GRANTS FOR corpscout_person_correction_writer;
```

Expected: four column names; grants list INSERT on `country_person_correction`, `se_company_person_correction`, `se_company_person_suggestion`.

- [ ] **Step 2: Deploy Dagster** (`cd corpscout/services/dagster_v3/ansible && ansible-playbook -i inventory.ini light_sync.yml`), confirm `se_company_person_correction_sensor` shows RUNNING in the Dagster UI.

- [ ] **Step 3: Smoke the pipeline with no ledger rows.** Launch `se_company_person_publish_job` with `company_ids: ["5592990765"]`. Expected metadata: `applied_correction_count = 0`, `stale_correction_count = 0`, `settled_company_count` ≥ 0, no exception; `se_company_person_suggestion` gains rows for that company if it is multi-source.

- [ ] **Step 4: End-to-end correction.** In the backoffice open `/admin/se/people/person/5592990765/<person_id>` (take the id from the Draft 2 "Review in ClickHouse" link), submit an `override_field` with a new name. Within ~2 minutes:

```sql
SELECT name, correction_ids, model_provider FROM corpscout.se_company_person FINAL
WHERE company_id='5592990765' AND person_id='<person_id>';
```

Expected: new name, `correction_ids` containing the ledger id. Then submit `undo` on that row; expect the name to revert and `correction_ids = []` after the next sensor run.

- [ ] **Step 5: Record the outcome** in `docs/superpowers/specs/2026-08-21-se-company-person-corrections-design.md` §8 (one line: date verified, company used) and commit that doc change by explicit path.

---

## Self-review

**Spec coverage**
- §2.1 ledger table → Task 2. §2.2 suggestions → Task 2/4. §2.3 provenance columns → Task 2/5/6. §2.4 grants → Task 2 (000296).
- §3 nine kinds → Task 3 constants, Task 5 (six person kinds), Task 6 (two role kinds), undo via `effective_corrections`.
- §4.1 effective set + order → Task 3 `effective_corrections`, `KIND_ORDER`. §4.2 identity under merge/split → Task 5 (`merged_into_person_id`, `person_id_for`). §4.3 staleness rules → Task 5 `_evidence_is_current` + per-kind checks; role staleness → Task 6 `build_stale_role_corrections_sql` and the active-role filter. §4.4 idempotency → Task 5 `_company_status_ctes`, `processed_company_ids`. §4.5 suggestions + provider from settings → Task 4. §4.6 job + sensor → Task 7.
- §5.1 write path and validator → Tasks 8–9. §5.2 review page → Task 10. §5.3 navigation → Task 10 step 5.
- §6 error handling: no-progress relaxed → Task 5 step 6; invalid/stale never abort → Tasks 5/6; single-row inserts → Task 9.
- §7 testing → each task; E2E → Task 11.

**Placeholder scan** — no TBD/TODO; every code step carries code. The component in Task 10 is the full skeleton the test exercises.

**Type consistency** — `normalize_companies` returns a 3-tuple everywhere (Tasks 4, 5 and the updated existing tests); `llm_model` is threaded to `_normalize_multi_source_company`; `person_id_for` is the public name used by tests and Task 9's TS mirror; `ROLE_COLUMNS`/`PERSON_COLUMNS` additions match the migration in Task 2; `SePersonCorrectionInput` fields used in Task 10 match Task 8.
