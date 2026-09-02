# SE Field Registry — Part 5: Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move production from the hand-written `se_company_info` publisher to the registry model — snapshot the old rows, apply the migrations, run the registry export, every candidates extractor and one `resolve_all`, prove parity, then retire the old publisher and switch the automation on — with a rollback path at every step.

**Architecture:** Parts 1–4 shipped the registry, the candidates extractors, the resolver, the parity check and the backoffice resolve path; nothing of that is changed here. This plan adds one baseline table + asset (`se_company_info_parity_snapshot`), then sequences operations on the prod ClickHouse (`companycollect`) and the prod Dagster host (`dagster`), and finally deletes the old publisher (`info.py` asset/scan/config/schedule, `info_rules.py` merge rules) and re-points the backoffice Pipeline sheet at the new assets. Migrations are additive only; the old asset stays deployed until parity is proven, so every step before Task 7 can be rolled back by re-running it.

**Tech Stack:** ClickHouse 26.5 (golang-migrate via `make -C corpscout clickhouse-migrate-*`, docker `clickhouse-clickhouse-1` on host `companycollect`), Dagster 1.13.9 (`uv run dg`, prod webserver GraphQL at `localhost:3000/graphql` on host `dagster`, systemd unit `corpscout-dagster-dev`, deploy via the `light_sync` ansible playbook from a pristine worktree), Python 3.14 / pytest, TypeScript / React Router / vitest for the backoffice (local dev server on :5183).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-02-se-company-field-registry-design.md` — this plan implements sections 8.4 (the sensor keeps its cursor and launches the resolver), 10 (serving survives the cutover), 12 (migrations, cutover, backfill, rollback, parity) and records 13 (out of scope). Executors read the spec beside this plan.

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands run from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice` (`npx vitest run <file>`, `npm run typecheck` clean before every commit). Definitions-loading pytest modules need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment.
- **Heavy work runs on the prod Dagster host, never locally.** Every materialization in Tasks 4–6 and 8 is launched on host `dagster` (UI `http://dagster:3000`, or `ssh -L 3000:localhost:3000 dagster` then `http://localhost:3000`). Local runs are for `dg check defs` and pytest only. Never kill an in-flight run to relocate it.
- **No `--force`, no `DROP`, no `TRUNCATE` by hand.** `make clickhouse-migrate-force` is not used anywhere in this plan. No `DROP TABLE`/`DROP COLUMN` appears in any up migration, any direct SQL, or any Python this plan writes. The parity snapshot table is kept after the cutover. (Down migrations follow the ledger convention and carry the reversing `DROP TABLE IF EXISTS`; none is applied here.)
- **Migrations one at a time:** every ledger step is `make -C corpscout clickhouse-migrate-up-one` followed by `make -C corpscout clickhouse-migrate-version`, with the expected number written beside it. A bare `clickhouse-migrate-up` is never run.
- **Owner fallback:** the auto-mode classifier may block the agent on `make clickhouse-migrate-*`, on `ssh companycollect … clickhouse-client` SELECTs, on `ssh dagster …` and on pytest. Every operational step therefore carries the exact command; when blocked, the agent posts it and the owner runs it as `! <command>` in the Claude Code terminal and pastes the output back. The backoffice dev server (React Router dev on :5183, run from the main checkout) is restarted by the owner in their own terminal; it is never an agent action.
- **Names are the spec's, verbatim:** assets `se_company_field_registry_clickhouse`, `se_company_field_candidates_<source>`, `se_company_field_resolved_clickhouse`; tables `se_company_field_registry`, `se_company_field_candidate`, `se_company_field`; sensor `se_company_info_field_value_sensor` (unchanged name); schedule `se_company_fields_weekly`; group `se_company_fields`. New in this plan: table `corpscout.se_company_info_parity_snapshot`, asset `se_company_info_parity_snapshot_clickhouse`.
- **Commits:** Conventional Commits, staged by explicit path (the tree carries other WIP; never `git add -A`). Every commit message ends with these two trailers, each on its own line:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- **Scratch directory** for query files and the deploy worktree: `/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover` (spelled `$S` after `S=…` at the top of each command block; shell state does not persist between tool calls, so every block re-declares it).
- **Migration numbers:** parts 1–4 own 000373 (three tables + writer grants), 000374 (wide columns), 000375 (widened decision CHECKs); this plan owns the next free number, assumed **000376** below. Verify with `ls corpscout/clickhouse/migrations | tail -8` at execution and renumber to `max + 1` if another branch landed first (memory: `clickhouse-migration-renumber-before-merge`).

## What parts 1–4 provide (assumed interfaces — verify at Task 3 step 1)

- Assets (group `se_company_fields`): `se_company_field_registry_clickhouse`; `se_company_field_candidates_scb`, `_bolagsverket`, `_esef`, `_wikidata`, `_ratsit`, `_domains`, `_llm`; `se_company_field_resolved_clickhouse`.
- Configs: candidates `CandidateExtractConfig(execute: bool = False, company_ids: list[str] = [], max_companies, company_batch_size, since: str = "")`; the LLM extractor additionally takes an `llm` block with the `LlmProfileConfig` fields (`provider`, `model`, `base_url`, `temperature`, `max_tokens`, `prompt_version`, `concurrency`) where `provider` and `model` are required (spec 5.3: "Provider and model remain required run config (no default)"); resolver `SECompanyFieldResolveConfig(execute: bool = False, company_ids, max_companies, company_batch_size, resolve_all: bool = False, resolve_all_before: str = "", fields: list[str] = [])`.
- Jobs: `se_company_fields_job` (registry export → every candidates asset → resolver; the weekly's job) and `se_company_field_resolve_job` (resolver only; the sensor's job). These two names are this plan's assumption; Task 3 step 1 pins them by reading the fields package, and Task 7 writes them into the backoffice.
- Asset check `se_company_field_parity_check` on `se_company_field_resolved_clickhouse`, reading `corpscout.se_company_info_parity_snapshot` (this plan's Task 1 table).
- Sensor `se_company_info_field_value_sensor` (now defined once, in the fields package, launching `se_company_field_resolve_job`; `default_status=STOPPED`), schedule `se_company_fields_weekly` (`default_status=STOPPED`), optionally sensor `se_company_field_candidate_sensor`.
- Migrations 000373, 000374, 000375 committed on main, not yet applied on prod (prod ledger at 372).

## Order of execution

Task 1 (code) → Task 2 (ledger) → Task 3 (deploy side by side) → Task 4 (registry, snapshot, candidates) → Task 5 (`resolve_all`) → Task 6 (parity: go/no-go) → Task 7 (code: retirement) → Task 8 (deploy retirement, automation on, smoke). Task 9 is the rollback reference and is read before Task 2, not executed.

---

### Task 1: Parity snapshot — migration 000376 and `se_company_info_parity_snapshot_clickhouse`

**Files:**
- Create: `corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.up.sql`
- Create: `corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/parity_snapshot.py`
- Create: `corpscout/services/dagster_v3/tests/test_se_company_parity_snapshot.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` tuple after the `"000375_…"` entry; one content test appended before `_migration_sql`)

**Interfaces:**
- Consumes: `assert_clickhouse_tables_exist(clickhouse, *, database, tables)` from `dagster_v3.defs.clickhouse.resolved`; `ClickhouseResource.get_connection()` yielding a client with `.execute(sql, params=None) -> list[tuple]`; `tests.se_company_ddl.declared_columns(table) -> list[str]` (reads the CREATE TABLE block of whichever migration declares `table`; requires the line `CREATE TABLE IF NOT EXISTS corpscout.<table>\n` and four-space-indented `name Type` column lines).
- Produces: table `corpscout.se_company_info_parity_snapshot` (MergeTree ORDER BY company_id; columns exactly `company_id, description, description_sv, llm_enhanced, suggestion_id, description_source_count, correction_ids, legal_name, legal_form_code, status, incorporation_date, primary_sni_code, primary_nace_code, resolved_at`, typed as on `se_company_info`); module constants `PARITY_SNAPSHOT_COLUMNS: tuple[str, ...]`, `SE_COMPANY_INFO_PARITY_SNAPSHOT = "se_company_info_parity_snapshot"`; `build_parity_snapshot_sql() -> str`; `materialize_parity_snapshot(*, clickhouse, execute: bool, overwrite: bool) -> dict[str, object]`; asset `se_company_info_parity_snapshot_clickhouse` with config `SECompanyInfoParitySnapshotConfig(execute: bool = False, overwrite: bool = False)`. The parity check (part 4) reads this table by name.

- [ ] **Step 1: Confirm the next free migration number**

Run: `ls /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations | tail -8`
Expected: the last lines are `000375_…up.sql`/`.down.sql` (part 4's decision-CHECK migration). If the tail shows a higher number, use `max + 1` everywhere this task says `000376`.

- [ ] **Step 2: Write the failing ledger + content test**

In `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`, add `"000376_corpscout_se_company_info_parity_snapshot",` as the last entry of `EXPECTED_MIGRATIONS` (directly after the `"000375_…"` line), and append this test after `test_the_correction_ledger_is_retired_by_000372_reversibly` (before `test_every_migration_ends_with_a_statement_not_a_comment`):

```python
def test_se_company_info_parity_snapshot_is_a_kept_baseline_of_the_compared_columns() -> None:
    """2026-09-02 field-registry cutover (spec section 12 step 4): the old publisher's rows
    are copied once, BEFORE the registry resolver rewrites se_company_info, so the parity
    asset check has something to compare against. The migration creates the table only --
    the asset fills it (INSERT ... SELECT FROM se_company_info FINAL) -- and the up file
    drops nothing: the baseline is kept after the cutover."""
    from dagster_v3.defs.se_company.parity_snapshot import PARITY_SNAPSHOT_COLUMNS

    up = _migration_sql("000376_corpscout_se_company_info_parity_snapshot.up.sql")
    down = _migration_sql("000376_corpscout_se_company_info_parity_snapshot.down.sql")

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_info_parity_snapshot\n" in up
    for column in PARITY_SNAPSHOT_COLUMNS:
        assert f"\n    {column} " in up, column
    assert "ENGINE = MergeTree" in up
    assert "ORDER BY company_id;" in up
    assert "INSERT" not in up and "DROP" not in up and "TRUNCATE" not in up

    assert "DROP TABLE IF EXISTS corpscout.se_company_info_parity_snapshot;" in down
```

- [ ] **Step 3: Run the ledger test to verify it fails**

Run (from `corpscout/services/dagster_v3`): `uv run pytest tests/test_clickhouse_migrations.py -q -k "parity_snapshot or expected_migrations or ends_with_a_statement"`
Expected: FAIL — `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.parity_snapshot'` on the new test, and the ledger-matching test fails because no `000376_…` file exists.
Owner fallback: `! cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && uv run pytest tests/test_clickhouse_migrations.py -q -k "parity_snapshot or expected_migrations or ends_with_a_statement"`

- [ ] **Step 4: Write the up migration**

`corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.up.sql` — exactly this content (the final line must be a statement, never a comment: golang-migrate's splitter hands ClickHouse every `;`-chunk and an all-comment chunk is `code 62 Empty query` after the real statements ran):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Parity baseline for the SE field-registry cutover (design 2026-09-02, section 12
-- step 4). One row per published company, copied from se_company_info FINAL by the
-- asset se_company_info_parity_snapshot_clickhouse BEFORE se_company_field_resolved_clickhouse
-- rewrites the wide table from the registry model. The parity asset check compares the
-- rewritten row against this copy: for llm_enhanced = false the description must be equal;
-- for llm_enhanced = true the new description must equal the stored observation named by
-- suggestion_id; a row that applied a reviewer decision (length(correction_ids) > 0) must
-- keep its OLD text whatever the flag says; a row published with several description
-- sources and no suggestion (description_source_count > 1, suggestion_id NULL) is the
-- pilot's unfinished model pass -- a changed text there is reported as
-- description_model_pending_changed and never fails; legal facts and industry codes must
-- be equal for every company.
--
-- Types are the wide table's own (000297, 000301, 000304) so the copy is lossless.
-- resolved_at is the OLD row's stamp: it tells the check which publisher wrote the
-- baseline. Plain MergeTree: the asset refuses to fill a non-empty table unless told to
-- overwrite, so this holds exactly one snapshot. It is never dropped by the ledger --
-- the baseline stays for audit after the cutover.
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_parity_snapshot
(
    company_id String,
    description Nullable(String),
    description_sv Nullable(String),
    llm_enhanced Bool,
    suggestion_id Nullable(UUID),
    description_source_count UInt8,
    correction_ids Array(UUID),
    legal_name String,
    legal_form_code Nullable(String),
    status LowCardinality(String),
    incorporation_date Nullable(Date32),
    primary_sni_code String,
    primary_nace_code String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = MergeTree
ORDER BY company_id;
```

- [ ] **Step 5: Write the down migration**

`corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_info_parity_snapshot;
```

- [ ] **Step 6: Write the failing asset tests**

`corpscout/services/dagster_v3/tests/test_se_company_parity_snapshot.py`:

```python
"""The cutover parity baseline: its one statement, its column contract with migration
000376 and with the wide table it copies, and the execute/overwrite gates -- against a
scripted ClickHouse client, since this repo has no live ClickHouse in CI."""

from contextlib import contextmanager

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.parity_snapshot import (
    PARITY_SNAPSHOT_COLUMNS,
    build_parity_snapshot_sql,
    materialize_parity_snapshot,
)
from tests.se_company_ddl import declared_columns

PUBLISHED = 3_552_806  # prod se_company_info FINAL on 2026-09-02 (000372's gate note)


class _FakeClient:
    """Answers the counts the asset asks for and records every statement it is sent."""

    def __init__(self, *, existing: int, published: int) -> None:
        self.existing = existing
        self.published = published
        self.executed: list[str] = []

    def execute(self, sql: str, params=None):
        self.executed.append(sql)
        head = sql.lstrip()
        if head.startswith("SELECT name"):  # assert_clickhouse_tables_exist
            return [("se_company_info",), ("se_company_info_parity_snapshot",)]
        if head.startswith("SELECT count()") and "se_company_info_parity_snapshot" in head:
            return [(self.existing,)]
        if head.startswith("SELECT count()") and "se_company_info FINAL" in head:
            return [(self.published,)]
        if head.startswith("TRUNCATE TABLE corpscout.se_company_info_parity_snapshot"):
            self.existing = 0
            return []
        if head.startswith("INSERT INTO corpscout.se_company_info_parity_snapshot"):
            self.existing = self.published
            return []
        raise AssertionError(f"unexpected statement: {sql}")

    def writes(self) -> list[str]:
        return [sql.split()[0] for sql in self.executed if sql.startswith(("TRUNCATE", "INSERT"))]


@pytest.fixture
def resource(monkeypatch):
    def bind(client: _FakeClient) -> ClickhouseResource:
        @contextmanager
        def fake_get_connection(self):
            yield client

        monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
        return ClickhouseResource(host="localhost")

    return bind


def test_the_snapshot_sql_copies_the_compared_columns_from_a_final_read() -> None:
    columns = ", ".join(PARITY_SNAPSHOT_COLUMNS)
    assert build_parity_snapshot_sql() == (
        f"INSERT INTO corpscout.se_company_info_parity_snapshot ({columns})\n"
        f"SELECT {columns}\n"
        "FROM corpscout.se_company_info FINAL"
    )


def test_the_columns_match_000376_in_order_and_all_exist_on_the_wide_table() -> None:
    assert list(PARITY_SNAPSHOT_COLUMNS) == declared_columns("se_company_info_parity_snapshot")
    assert set(PARITY_SNAPSHOT_COLUMNS) <= set(declared_columns("se_company_info"))


def test_a_preview_reads_the_counts_and_writes_nothing(resource) -> None:
    client = _FakeClient(existing=0, published=PUBLISHED)
    metadata = materialize_parity_snapshot(clickhouse=resource(client), execute=False, overwrite=False)
    assert metadata == {"preview": True, "existing_count": 0, "published_count": PUBLISHED}
    assert client.writes() == []


def test_a_real_run_fills_an_empty_baseline_without_truncating(resource) -> None:
    client = _FakeClient(existing=0, published=PUBLISHED)
    metadata = materialize_parity_snapshot(clickhouse=resource(client), execute=True, overwrite=False)
    assert metadata["preview"] is False
    assert metadata["snapshot_count"] == PUBLISHED and metadata["replaced_count"] == 0
    assert client.writes() == ["INSERT"]


def test_a_filled_baseline_is_refused_without_overwrite(resource) -> None:
    client = _FakeClient(existing=PUBLISHED, published=PUBLISHED)
    with pytest.raises(ValueError, match=f"already holds {PUBLISHED} rows"):
        materialize_parity_snapshot(clickhouse=resource(client), execute=True, overwrite=False)
    assert client.writes() == []


def test_overwrite_truncates_then_copies(resource) -> None:
    client = _FakeClient(existing=10, published=PUBLISHED)
    metadata = materialize_parity_snapshot(clickhouse=resource(client), execute=True, overwrite=True)
    assert client.writes() == ["TRUNCATE", "INSERT"]
    assert metadata["replaced_count"] == 10 and metadata["snapshot_count"] == PUBLISHED


def test_an_empty_wide_table_takes_no_baseline(resource) -> None:
    client = _FakeClient(existing=0, published=0)
    with pytest.raises(ValueError, match="no baseline was taken"):
        materialize_parity_snapshot(clickhouse=resource(client), execute=True, overwrite=False)
```

- [ ] **Step 7: Run the asset tests to verify they fail**

Run: `uv run pytest tests/test_se_company_parity_snapshot.py -q`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.parity_snapshot'`.

- [ ] **Step 8: Write the asset module**

`corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/parity_snapshot.py` (no `from __future__ import annotations` — it would stringize the context hint and break Dagster's context-type validation):

```python
"""One-time parity baseline for the SE field-registry cutover.

Before ``se_company_field_resolved_clickhouse`` rewrites every ``se_company_info`` row
from the registry model, this asset copies the columns the parity check compares -- the
published description pair with its LLM provenance, the source count and the applied
decision ids that decide which parity rule a row falls under, the legal facts and the
industry codes -- into ``se_company_info_parity_snapshot`` (migration 000376), read FINAL so one
row per company is copied. ``se_company_field_parity_check`` then compares the rewritten
wide row against this table (design 2026-09-02, section 12 step 4).

The table is a baseline, never a pipeline output: the asset refuses to run on a filled
table unless ``overwrite`` is set, because a second snapshot taken AFTER the cutover would
compare the new publisher with itself. Nothing here drops the table; the baseline is kept
for audit once the cutover is done.

Gate: ``execute`` is False by default, so a bare "Materialize" click reports the counts it
would copy and writes nothing.

Assets
  se_company_info_parity_snapshot_clickhouse -> corpscout.se_company_info_parity_snapshot
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

DATABASE = "corpscout"
GROUP_NAME = "se_company_fields"
SE_COMPANY_INFO = "se_company_info"
SE_COMPANY_INFO_PARITY_SNAPSHOT = "se_company_info_parity_snapshot"
# Insert columns in 000376's DDL order -- pinned against the migration by the test, and
# every name is also a column of se_company_info, so the SELECT below can be a plain copy.
PARITY_SNAPSHOT_COLUMNS = (
    "company_id", "description", "description_sv", "llm_enhanced", "suggestion_id",
    "description_source_count", "correction_ids",
    "legal_name", "legal_form_code", "status", "incorporation_date",
    "primary_sni_code", "primary_nace_code", "resolved_at",
)


def build_parity_snapshot_sql() -> str:
    """The one statement that fills the baseline: a FINAL read of the wide table, so
    exactly one version per company is copied whatever the merge state is."""
    columns = ", ".join(PARITY_SNAPSHOT_COLUMNS)
    return (
        f"INSERT INTO {DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT} ({columns})\n"
        f"SELECT {columns}\n"
        f"FROM {DATABASE}.{SE_COMPANY_INFO} FINAL"
    )


def materialize_parity_snapshot(
    *, clickhouse: ClickhouseResource, execute: bool, overwrite: bool,
) -> dict[str, object]:
    """Copy the baseline -- or, with ``execute`` false, only report what would be copied."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(SE_COMPANY_INFO, SE_COMPANY_INFO_PARITY_SNAPSHOT))
    with clickhouse.get_connection() as client:
        existing = int(client.execute(
            f"SELECT count() FROM {DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT}")[0][0])
        published = int(client.execute(
            f"SELECT count() FROM {DATABASE}.{SE_COMPANY_INFO} FINAL")[0][0])
        if not execute:
            return {"preview": True, "existing_count": existing, "published_count": published}
        if existing and not overwrite:
            raise ValueError(
                f"{DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT} already holds {existing} rows; "
                "a baseline is taken once, before the cutover. Run with overwrite: true only "
                "to deliberately replace it.")
        if existing:
            client.execute(f"TRUNCATE TABLE {DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT}")
        client.execute(build_parity_snapshot_sql())
        snapshot = int(client.execute(
            f"SELECT count() FROM {DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT}")[0][0])
    if snapshot == 0:
        raise ValueError(
            f"{DATABASE}.{SE_COMPANY_INFO} FINAL returned no rows; no baseline was taken")
    return {"preview": False, "snapshot_count": snapshot, "published_count": published,
            "replaced_count": existing, "snapshot_at": datetime.now(UTC).isoformat()}


class SECompanyInfoParitySnapshotConfig(dg.Config):
    # False = preview: read the two counts, write nothing.
    execute: bool = False
    # True = replace a filled baseline. Off by default so a repeated run after the
    # cutover cannot silently turn the baseline into a copy of the new publisher.
    overwrite: bool = False


@dg.asset(
    name="se_company_info_parity_snapshot_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT}"},
    description=(
        "One-time copy of the old publisher's se_company_info rows (compared columns only) "
        "taken before the field-registry resolver rewrites them; the parity asset check "
        "reads it. Refuses to refill unless overwrite=true; execute=false previews."
    ),
)
def se_company_info_parity_snapshot_clickhouse(
    context: dg.AssetExecutionContext, config: SECompanyInfoParitySnapshotConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """se_company_info FINAL -> se_company_info_parity_snapshot, once."""
    metadata = materialize_parity_snapshot(
        clickhouse=clickhouse, execute=config.execute, overwrite=config.overwrite)
    context.log.info("se_company_info_parity_snapshot: %s", metadata)
    return dg.MaterializeResult(
        metadata={**metadata, "table": f"{DATABASE}.{SE_COMPANY_INFO_PARITY_SNAPSHOT}"})


defs = dg.Definitions(assets=[se_company_info_parity_snapshot_clickhouse])
```

- [ ] **Step 9: Run both test files and the definitions check**

Run: `uv run pytest tests/test_se_company_parity_snapshot.py tests/test_clickhouse_migrations.py -q`
Expected: all PASS (the ledger file's own count is large; the last line reads `… passed`).
Run: `uv run dg check defs`
Expected: exit 0; the output reports the definitions loaded without an error (no traceback; a `company_domain_suggestions` adapter warning is known noise).
Owner fallback: `! cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && uv run pytest tests/test_se_company_parity_snapshot.py tests/test_clickhouse_migrations.py -q && uv run dg check defs`

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.up.sql \
        corpscout/clickhouse/migrations/000376_corpscout_se_company_info_parity_snapshot.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/parity_snapshot.py \
        corpscout/services/dagster_v3/tests/test_se_company_parity_snapshot.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(se): parity snapshot table and asset for the field-registry cutover

Migration 000376 creates corpscout.se_company_info_parity_snapshot; the asset copies
the compared columns from se_company_info FINAL once, refusing to refill unless
overwrite is set. The parity asset check compares the registry resolver's output
against it (design 2026-09-02, section 12 step 4).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: Apply migrations 000373–000376 on prod, one at a time

**Files:** none changed. Reads `corpscout/clickhouse/migrations/` from the committed tree (the Makefile mounts the working tree's migrations directory read-only — every file present rides along, which is why `up 1` is used and why Task 1 is committed first).

**Interfaces:**
- Consumes: `make -C corpscout clickhouse-migrate-up-one` (`migrate/migrate:v4.17.0 … up 1` against `CLICKHOUSE_MIGRATE_URL` from `corpscout/.env`), `make -C corpscout clickhouse-migrate-version` (prints the ledger version, e.g. `372`, and `(dirty)` when a migration half-applied).
- Produces: prod ClickHouse at ledger 376 with tables `se_company_field_registry`, `se_company_field_candidate`, `se_company_field`, `se_company_info_parity_snapshot`, the eight new `se_company_info` columns and the widened `se_company_info_field_value` CHECKs.

- [ ] **Step 1: Confirm the tree, main and prod agree on the starting point**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect && git status --porcelain corpscout/clickhouse/migrations && ls corpscout/clickhouse/migrations | tail -8
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1\""
```
Expected: `git status` prints nothing for the migrations directory (no uncommitted migration files can ride along); the tail lists `000373_…`, `000374_…`, `000375_…`, `000376_…` up/down pairs; prod answers `372	0`. If prod answers a version other than 372 or `dirty = 1`, stop and report — nothing below is applied on a dirty or unexpected ledger.
Owner fallback: `! ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1\""`

- [ ] **Step 2: Apply 000373 (registry, candidate and resolved tables + writer grants)**

Run: `make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-up-one && make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-version`
Expected: the first command prints the migration's name (`373/u corpscout_se_company_field_registry…` in golang-migrate's own line format) and exits 0; the second prints `373`.
Owner fallback: `! make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-up-one && make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-version`

Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT name, engine FROM system.tables WHERE database = 'corpscout' AND name IN ('se_company_field_registry', 'se_company_field_candidate', 'se_company_field') ORDER BY name\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SHOW GRANTS FOR corpscout_person_correction_writer\""
```
Expected: three rows — `se_company_field	ReplacingMergeTree`, `se_company_field_candidate	ReplacingMergeTree`, `se_company_field_registry	ReplacingMergeTree`; the grants list includes `GRANT INSERT ON corpscout.se_company_field TO corpscout_person_correction_writer` (000373's exact grant set is part 1's; `se_company_field` must be there for the backoffice resolve path, spec section 9).

- [ ] **Step 3: Apply 000374 (new wide columns)**

Run: `make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-up-one && make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-version`
Expected: exit 0; version `374`.

Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT name, type FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info' AND name IN ('industry_label_en', 'website', 'employee_count', 'employee_count_as_of', 'latest_revenue_amount', 'latest_revenue_currency', 'latest_revenue_amount_usd', 'latest_revenue_fiscal_year') ORDER BY position\""
```
Expected: exactly 8 rows, types as spec 8.3 (`String`, `Nullable(String)`, `Nullable(UInt64)`, `Nullable(Date32)`, `Nullable(Decimal(38, 2))`, `LowCardinality(String)`, `Nullable(Decimal(38, 2))`, `Nullable(UInt16)`).

- [ ] **Step 4: Apply 000375 (widened decision CHECKs)**

Run: `make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-up-one && make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-version`
Expected: exit 0; version `375`.

Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SHOW CREATE TABLE corpscout.se_company_info_field_value\""
```
Expected: the `known_field` CHECK lists the twelve registry fields (`legal_name`, `legal_form_code`, `status`, `incorporation_date`, `description`, `description_sv`, `primary_sni_code`, `primary_nace_code`, `industry_label_en`, `website`, `employee_count`, `latest_revenue`) and `known_source` lists `scb`, `bolagsverket`, `esef`, `wikidata`, `ratsit`, `domains`, `llm`, `reviewer`.

- [ ] **Step 5: Apply 000376 (parity snapshot table)**

Run: `make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-up-one && make -C /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout clickhouse-migrate-version`
Expected: exit 0; version `376`.

Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT engine, sorting_key, total_rows FROM system.tables WHERE database = 'corpscout' AND name = 'se_company_info_parity_snapshot'\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info_parity_snapshot' ORDER BY position)\""
```
Expected: one row `MergeTree	company_id	0`; then the 14 names in order: `['company_id','description','description_sv','llm_enhanced','suggestion_id','description_source_count','correction_ids','legal_name','legal_form_code','status','incorporation_date','primary_sni_code','primary_nace_code','resolved_at']`.

- [ ] **Step 6: Final ledger state**

Run: `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1\""`
Expected: `376	0`. If any step above left `dirty = 1`: stop; do not run `clickhouse-migrate-force`; report the failing statement from the make output to the owner (the fix is a forward repair migration per the dagster_v3 CLAUDE.md rule, never a rewind).

---

### Task 3: Deploy dagster_v3 with old and new assets side by side, automation stopped

**Files:** none changed. Uses the pristine-worktree recipe (memory `se-worktree-deploy-recipe`): `light_sync` rsyncs the working tree with `--delete-after`, so a dirty main tree would ship WIP.

**Interfaces:**
- Consumes: main at the commit that contains parts 1–4 and Task 1; `corpscout/services/dagster_v3/.env` (gitignored; `dg` needs it in the worktree); `corpscout/services/dagster_v3/ansible/light_sync.yml` with `inventory.ini` (host `dagster`, user `graovic`).
- Produces: the prod code location `dagster_v3` serving both `se_company_info_clickhouse` (old) and the `se_company_fields` group (new); every SE info/fields instigator STOPPED.

**Operational notes:**
- `se_company_field_registry_clickhouse` must be materialized before any resolve run -- plan 3's `load_registry_statements` refuses on a registry version mismatch (Task 4 materializes it first, before the candidates assets or `se_company_field_resolved_clickhouse`).
- Confirm the prod ClickHouse server/session timezone is UTC: `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT timezone()\""` should answer `UTC`. `resolved_at` now binds as `{resolved_at:DateTime64(3, 'UTC')}` regardless of the session timezone, but this check documents the assumption the fix was written against.
- The backoffice's resolve-after-decision `INSERT ... SELECT` (spec section 9) needs `SELECT` on seven source tables that the INSERT-only `corpscout_person_correction_writer` role (000373's grant set) does not carry -- it works today only because the backoffice connects to ClickHouse as the Dagster account (2026-08-23 owner decision), not as that writer role. The role alone cannot run the resolve.

- [ ] **Step 1: Pin the names this plan assumes from parts 1–4**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
rg -n "define_asset_job\(|ScheduleDefinition\(|ledger_sensor\(|name=\"se_company_field" src/dagster_v3/defs/se_company/fields/
rg -n "class .*Config\(dg.Config\)" -A 12 src/dagster_v3/defs/se_company/fields/ | rg -n "class|execute|company_ids|max_companies|company_batch_size|since|resolve_all|resolve_all_before|fields|llm|provider|model"
```
Expected: the job names `se_company_fields_job` and `se_company_field_resolve_job`, the schedule `se_company_fields_weekly`, the sensor `se_company_info_field_value_sensor`, and the config fields listed under "What parts 1–4 provide". If a job name differs, write the real names down now: Task 4 (`jobName` in every launch file), Task 7 step B1 (backoffice constants) and Task 8 use them. If `max_companies` shows an upper bound (`le=`), note it: Tasks 4 and 5 say what to do with a cap.

- [ ] **Step 2: Stop the old automation before anything is deployed**

The instance-level instigator state persists across deploys, so this is done once, explicitly. UI path: `http://dagster:3000/automation` → toggle `se_company_info_field_value_sensor` and `se_company_info_weekly` off. GraphQL path — read the ids, then stop:

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover; mkdir -p $S
cat > $S/instigators.json <<'EOF'
{"query": "query Names($r: RepositorySelector!) { sensorsOrError(repositorySelector: $r) { ... on Sensors { results { name sensorState { id selectorId status } } } } schedulesOrError(repositorySelector: $r) { ... on Schedules { results { name scheduleState { id selectorId status } } } } }",
 "variables": {"r": {"repositoryLocationName": "dagster_v3", "repositoryName": "__repository__"}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/instigators.json | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(*[(s['name'], s['sensorState']['status'], s['sensorState']['id'], s['sensorState']['selectorId']) for s in d['sensorsOrError']['results'] if 'se_company' in s['name']], *[(s['name'], s['scheduleState']['status'], s['scheduleState']['id'], s['scheduleState']['selectorId']) for s in d['schedulesOrError']['results'] if 'se_company' in s['name']], sep='\n')"
```
Expected: one line per SE sensor/schedule with its status, id and selectorId. For each of `se_company_info_field_value_sensor` and `se_company_info_weekly` that is `RUNNING`, stop it (substitute the printed id/selectorId):

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/stop-sensor.json <<'EOF'
{"query": "mutation Stop($origin: String!, $selector: String!) { stopSensor(jobOriginId: $origin, jobSelectorId: $selector) { __typename ... on StopSensorMutationResult { instigationState { status } } ... on PythonError { message } } }",
 "variables": {"origin": "PASTE_SENSOR_STATE_ID", "selector": "PASTE_SENSOR_SELECTOR_ID"}}
EOF
cat > $S/stop-schedule.json <<'EOF'
{"query": "mutation Stop($origin: String!, $selector: String!) { stopRunningSchedule(scheduleOriginId: $origin, scheduleSelectorId: $selector) { __typename ... on ScheduleStateResult { scheduleState { status } } ... on PythonError { message } } }",
 "variables": {"origin": "PASTE_SCHEDULE_STATE_ID", "selector": "PASTE_SCHEDULE_SELECTOR_ID"}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/stop-sensor.json; echo
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/stop-schedule.json; echo
```
Expected: `{"data":{"stopSensor":{"__typename":"StopSensorMutationResult","instigationState":{"status":"STOPPED"}}}}` and `{"data":{"stopRunningSchedule":{"__typename":"ScheduleStateResult","scheduleState":{"status":"STOPPED"}}}}`. Re-run the names query: every `se_company_*` sensor and schedule reads `STOPPED`.
Owner fallback: the same `ssh dagster "curl …" < file` lines with `!` in front.

- [ ] **Step 3: Confirm no SE info run is in flight**

Run:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/inflight.json <<'EOF'
{"query": "query Inflight($f: RunsFilter!) { runsOrError(filter: $f, limit: 20) { ... on Runs { results { runId jobName status } } } }",
 "variables": {"f": {"statuses": ["QUEUED", "STARTING", "STARTED", "CANCELING"]}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/inflight.json; echo
```
Expected: no result whose `jobName` is `se_company_info_job` or `se_company_info_review_job`. If one is running, wait for it (never cancel in-flight work to make room).

- [ ] **Step 4: Build the pristine worktree and validate it**

Run (one block; every line must exit 0 before the next):
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover; mkdir -p $S
cd /Users/graovic/pulsarpoint/ppoint/companycollect && git worktree add $S/deploy-worktree HEAD
cp corpscout/services/dagster_v3/.env $S/deploy-worktree/corpscout/services/dagster_v3/.env
cd $S/deploy-worktree/corpscout/services/dagster_v3
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
```
Expected: `git worktree add` prints `HEAD is now at <sha> …`; both `dbt parse` end with `Done.`; `refresh-defs-state` may print the known non-fatal `company_domain_suggestions` adapter traceback; `dg check defs` exits 0 with no error. A `dg check defs` failure with a YAML/column error means the `.env` copy was skipped.

- [ ] **Step 5: Run the hot-sync and capture its exit code explicitly**

Run:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cd $S/deploy-worktree/corpscout/services/dagster_v3/ansible
ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > $S/light_sync-side-by-side.log 2>&1; RC=$?; echo "RC=$RC"; tail -15 $S/light_sync-side-by-side.log
```
Expected: `RC=0` and a `PLAY RECAP` line for `dagster` with `failed=0` and `unreachable=0`. Never judge this by `ansible-playbook | tail` — `tail` masks the exit code. A become-timeout failure means the host is IO-starved; re-run once, and if it fails again check the host per Task 4 step 2's wedge commands before retrying.
Owner fallback: `! cd /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover/deploy-worktree/corpscout/services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml; echo RC=$?`

- [ ] **Step 6: Verify the prod code location serves both generations**

Run:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/group.json <<'EOF'
{"query": "query Group($g: AssetGroupSelector!) { assetNodes(group: $g) { assetKey { path } jobNames } }",
 "variables": {"g": {"groupName": "se_company_fields", "repositoryLocationName": "dagster_v3", "repositoryName": "__repository__"}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/group.json; echo
cat > $S/old.json <<'EOF'
{"query": "query Old($keys: [AssetKeyInput!]!) { assetNodes(assetKeys: $keys) { assetKey { path } jobNames } }",
 "variables": {"keys": [{"path": ["se_company_info_clickhouse"]}]}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/old.json; echo
```
Expected: the group answer lists all ten new keys (`se_company_field_registry_clickhouse`, the seven `se_company_field_candidates_*`, `se_company_field_resolved_clickhouse`, `se_company_info_parity_snapshot_clickhouse`) and the second answer still lists `se_company_info_clickhouse` with `jobNames` containing `se_company_info_job`. Then re-run `$S/instigators.json` from step 2: `se_company_fields_weekly` and both sensors read `STOPPED`.

- [ ] **Step 7: Remove the worktree**

Run: `cd /Users/graovic/pulsarpoint/ppoint/companycollect && git worktree remove /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover/deploy-worktree && git worktree prune`
Expected: no output; `git worktree list` no longer shows `deploy-worktree`.

---

### Task 4: Materialize the registry export, the parity snapshot and every candidates asset

**Files:** none changed. All runs on the prod Dagster host, launched one at a time, each waited to `SUCCESS` before the next — the host has wedged under memory pressure before (memory `dagster-host-wedge-recovery`), and one extractor at a time is the cheapest guard.

**Interfaces:**
- Consumes: the jobs and config fields pinned in Task 3 step 1; the Dagster Launchpad (asset page → **Materialize** dropdown → **Open launchpad**, paste YAML, **Launch**) or the GraphQL `launchRun` shape below; `BACKOFFICE_LAUNCH_RUN_MUTATION`'s selector shape (`repositoryLocationName: "dagster_v3"`, `repositoryName: "__repository__"`).
- Produces: `corpscout.se_company_field_registry` filled (12 field rows + the `*` projection row for `info`/`SE`); `corpscout.se_company_info_parity_snapshot` holding 3,552,806-ish rows; `corpscout.se_company_field_candidate` filled from all seven sources.

Common launch file (used by every step; only `jobName`, `assetSelection` and `runConfigData` change). Run status and materialization metadata are read with the two query files defined once here:

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover; mkdir -p $S
cat > $S/run.json <<'EOF'
{"query": "query Run($id: ID!) { runOrError(runId: $id) { __typename ... on Run { runId status startTime endTime } ... on RunNotFoundError { message } } }",
 "variables": {"id": "PASTE_RUN_ID"}}
EOF
cat > $S/mat.json <<'EOF'
{"query": "query Mat($keys: [AssetKeyInput!]!) { assetNodes(assetKeys: $keys) { assetKey { path } assetMaterializations(limit: 1) { runId timestamp metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on BoolMetadataEntry { boolValue } ... on FloatMetadataEntry { floatValue } ... on JsonMetadataEntry { jsonString } } } } }",
 "variables": {"keys": [{"path": ["PASTE_ASSET_NAME"]}]}}
EOF
```
Reading them: `sed "s/PASTE_RUN_ID/<runId>/" $S/run.json | ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql"; echo` and `sed "s/PASTE_ASSET_NAME/<asset>/" $S/mat.json | ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql"; echo`. A run is done when `status` is `SUCCESS`; `FAILURE` stops this task — read the run page (`http://dagster:3000/runs/<runId>`) and report. Preferred: launch from the UI Launchpad (the schema panel on the right lists the exact config keys the asset accepts) and read the run page; the GraphQL files below are the equivalent for a terminal.

- [ ] **Step 1: Registry export**

Launchpad YAML for `se_company_field_registry_clickhouse` (the export takes no run config; if the Launchpad's schema panel shows an `execute` key, set `execute: true`):
```yaml
{}
```
GraphQL:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/launch-registry.json <<'EOF'
{"query": "mutation Launch($p: ExecutionParams!) { launchRun(executionParams: $p) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { errors { message path } } ... on PythonError { message } ... on InvalidSubsetError { message } ... on PipelineNotFoundError { message } } }",
 "variables": {"p": {"selector": {"repositoryLocationName": "dagster_v3", "repositoryName": "__repository__", "jobName": "se_company_fields_job", "assetSelection": [{"path": ["se_company_field_registry_clickhouse"]}]}, "runConfigData": {}, "mode": "default", "executionMetadata": {"tags": [{"key": "cutover", "value": "se-field-registry"}]}}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/launch-registry.json; echo
```
Expected: `{"data":{"launchRun":{"__typename":"LaunchRunSuccess","run":{"runId":"…","status":"QUEUED"}}}}`; `SUCCESS` within a minute. Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT field, value_type, policy_name, registry_version, length(sources) FROM corpscout.se_company_field_registry FINAL WHERE datatype = 'info' AND country = 'SE' ORDER BY field\""
```
Expected: 13 rows — the twelve registry fields (spec 4.2) plus `*	projection	…`; `registry_version` = `se-info-v1` on every row; `length(sources)` matches the spec table (e.g. `description` 4, `legal_form_code` 2, `latest_revenue` 3).

- [ ] **Step 2: Parity snapshot (before any candidate or resolve run)**

Launchpad YAML for `se_company_info_parity_snapshot_clickhouse`:
```yaml
ops:
  se_company_info_parity_snapshot_clickhouse:
    config:
      execute: true
```
GraphQL: copy `$S/launch-registry.json` to `$S/launch-snapshot.json`, replace the asset path with `se_company_info_parity_snapshot_clickhouse` and `"runConfigData": {}` with `"runConfigData": {"ops": {"se_company_info_parity_snapshot_clickhouse": {"config": {"execute": true}}}}`, launch the same way.
Expected: `SUCCESS` within a few minutes; metadata `snapshot_count` ≈ `published_count` ≈ 3,552,806 (prod count on 2026-09-02), `replaced_count` 0. Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count(), countIf(llm_enhanced) AS llm_rows, countIf(length(correction_ids) > 0) AS reviewed_rows, countIf(description_source_count > 1 AND suggestion_id IS NULL) AS model_pending_rows, toString(max(resolved_at)) FROM corpscout.se_company_info_parity_snapshot\""
```
Expected: count ≈ 3.55M; `llm_rows` in the low thousands (the pilot's model-written descriptions); `reviewed_rows` a handful (0 of 3,552,806 on 2026-09-02 per 000372's gate, plus whatever Use-this decisions the field-values smoke wrote since) — these rows must keep their old text in Task 6; `model_pending_rows` the multi-source companies the pilot's model pass never reached — their text may change in Task 6 and is reported, never failed; `max(resolved_at)` older than now — this is the OLD publisher's last stamp.

Host-health check to run before each extractor from here on (the wedge symptoms: a run `STARTED` with no log progress for > 10 min, or a slow webserver):
```bash
ssh dagster "free -m | head -2; cat /proc/pressure/memory; ps -eo pid,stat,wchan:32,cmd | awk '\$2 ~ /D/'"
```
Expected: swap used near 0, memory PSI `full avg10` well below 10, no D-state process. A permanent D-state `dg` process with `wchan` `folio_wait_bit_common` is the swap-in livelock: owner recovery per memory `dagster-host-wedge-recovery` (`sudo swapoff -a` with RAM free; the unit refuses manual stop, so never `systemctl restart`). Do not launch the next extractor into a wedged host.

- [ ] **Step 3: Candidates — scb, bolagsverket, esef, wikidata, ratsit, domains (in this order, one at a time)**

Launchpad YAML, replacing `<source>` per run (no `company_ids` = every company; no `max_companies` = the config default, which parts 1–4 leave unbounded; no `since` = every company qualifies on an empty candidate table):
```yaml
ops:
  se_company_field_candidates_<source>:
    config:
      execute: true
```
GraphQL: copy `$S/launch-registry.json` to `$S/launch-cand-<source>.json`, set the asset path to `se_company_field_candidates_<source>` and `"runConfigData": {"ops": {"se_company_field_candidates_<source>": {"config": {"execute": true}}}}`, launch, wait for `SUCCESS`, run the host-health check, then the next source.

If Task 3 step 1 showed a cap on `max_companies` (`le=…`): set `max_companies` to that cap and simply relaunch the same run until its metadata reports fewer selected companies than the cap — extractors select only companies whose source rows are newer than their candidates, so each run continues where the previous stopped.

Expected durations: scb and bolagsverket 10–60 min each (3.5M / 2.9M companies staged through `publish_with_stage`); esef, wikidata, domains minutes; ratsit depends on crawl coverage. Expected magnitudes, checked after each run:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT source, count() AS rows, uniqExact(company_id) AS companies, uniqExact(field) AS fields FROM corpscout.se_company_field_candidate GROUP BY source ORDER BY source\""
```
- `scb`: companies = `SELECT uniqExact(company_id) FROM corpscout.se_company_info_scb` (≈ 3.55M); rows ≈ 6–9 per company (20–32M); fields 9.
- `bolagsverket`: companies ≈ the Bolagsverket-sourced registry rows (≈ 2.9M); rows ≈ 4–6 per company; fields 6 (legal_name, legal_form_code, status, incorporation_date, employee_count, latest_revenue — the last two only for companies with a filed statement).
- `esef`: hundreds (≈ 300–900 rows; a few hundred Swedish ESEF filers × up to 3 fields).
- `wikidata`: thousands of companies (≈ 5–10k), rows ≈ 10–50k; fields 6.
- `ratsit`: companies ≈ `SELECT uniqExact(company_id) FROM corpscout.se_ratsit_company_industry_codes`; fields 5.
- `domains`: companies ≈ `SELECT uniqExact(company_id) FROM corpscout.company_domains WHERE review_status IN ('confirmed_primary', 'suggested_primary')` for Swedish ids; fields 1 (`website`).
A source whose `rows` is 0 after `SUCCESS` is a stop-and-report: an extractor that found nothing is a bug, not a state.

- [ ] **Step 4: Candidates — llm, preview first, then the real run**

The LLM extractor reuses stored observations by `input_hash` (`se_company_info_enrichment_observation`), so most of its answers cost nothing; a preview says how many paid calls a real run makes. Launchpad YAML for `se_company_field_candidates_llm`, first WITHOUT `execute` (preview):
```yaml
ops:
  se_company_field_candidates_llm:
    config:
      llm:
        provider: deepseek
        model: deepseek-v4-flash
        base_url: https://api.deepseek.com
        temperature: 0
        max_tokens: 6000
        prompt_version: se-company-info-description-v3
        concurrency: 2
```
(The `llm` block is the pilot's `DEFAULT_LLM_PROFILE` with concurrency 2; the host reads `DEEPSEEK_API_KEY` for provider `deepseek`. If the Launchpad schema panel shows part 3's config keyed differently — e.g. `provider`/`model` at the top level in the ESEF style — use the panel's shape; a wrong shape is rejected as `RunConfigValidationInvalid` before any work starts.)
Before the preview, check the two ways a payload can differ from what `info.py` hashed (a mismatch means the stored observation is missed and the description is paid for again): `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT countIf(position(nace_rev2_class_code, '.') > 0) AS dotted, count() FROM corpscout.se_industries WHERE is_primary = 1\""` (expect `dotted = 0`; the candidates carry the dot-less code) and `... -q "SELECT countIf(lowerUTF8(trim(activity_description)) IN ('-', '--', '.', 'n/a', 'null', 'none')) FROM corpscout.se_company_info_scb FINAL"` (expect 0; placeholders are scrubbed from candidates but were not from the old payload). Then read the preview's `would_reuse_count` against `would_call_model_count`: with an unchanged prompt and dot-less codes nearly every multi-source company should reuse; a large `would_call_model_count` is a payload drift to investigate before spending.

Read the preview's materialization metadata (`$S/mat.json` with `se_company_field_candidates_llm`): note the selected-company count and the estimated call count it reports. Then launch again with `execute: true` added under `config`. Expected: `SUCCESS`; rows with `source = 'llm'` ≈ the number of companies with two or more non-LLM description candidates (the pilot's multi-source set, low thousands), two rows per company (`description`, `description_sv`); paid calls ≈ the preview's estimate.

- [ ] **Step 5: Record the candidate totals**

Run the per-source count from step 3 once more and paste the table into the session notes; Task 6 compares the parity check's per-source counts against it.

---

### Task 5: `resolve_all` — one run over every company

**Files:** none changed.

**Interfaces:**
- Consumes: `SECompanyFieldResolveConfig` (`execute`, `resolve_all`, `company_batch_size`, `max_companies`, `resolve_all_before`); `corpscout.se_company_field` and the wide table's `resolved_at` for progress.
- Produces: `corpscout.se_company_field` with one row per company × resolved field (≈ 3.55M companies × 6–10 fields with a winner = 20–35M rows) and every `se_company_info` row rewritten with a `resolved_at` newer than the snapshot's.

- [ ] **Step 1: Note the start instant**

Run: `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT toString(now64(3, 'UTC'))\""`
Expected: a stamp like `2026-09-03 07:15:42.123`; write it down as `T0` — every progress query below compares against it, and a capped multi-run pass uses it as `resolve_all_before`.

- [ ] **Step 2: Launch**

Launchpad YAML for `se_company_field_resolved_clickhouse` (job `se_company_field_resolve_job`):
```yaml
ops:
  se_company_field_resolved_clickhouse:
    config:
      execute: true
      resolve_all: true
      company_batch_size: 20000
```
GraphQL: copy `$S/launch-registry.json` to `$S/launch-resolve-all.json`, set `jobName` to `se_company_field_resolve_job`, the asset path to `se_company_field_resolved_clickhouse` and `"runConfigData": {"ops": {"se_company_field_resolved_clickhouse": {"config": {"execute": true, "resolve_all": true, "company_batch_size": 20000}}}}`, launch. Expected: `LaunchRunSuccess`.

If `max_companies` is capped (Task 3 step 1): add `max_companies: <cap>` and `resolve_all_before: "<T0>"` to the config and relaunch the identical run until a run's metadata reports 0 selected companies — the cutoff is what stops run 2 from re-selecting what run 1 rewrote.

- [ ] **Step 3: Monitor**

Expected duration: 3.55M / 20,000 ≈ 178 pages × (12 field statements + one pivot) — plan for 2–4 hours; a page that takes longer than 5 minutes, or no growth for 10 minutes, means look at the host (Task 4 step 2's health check) and the run logs. Every 15–30 minutes:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() AS resolved_rows, uniqExact(company_id) AS companies FROM corpscout.se_company_field WHERE resolved_at >= toDateTime64('T0', 3, 'UTC')\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT countIf(resolved_at >= toDateTime64('T0', 3, 'UTC')) AS rewritten, toString(max(resolved_at)) FROM corpscout.se_company_info\""
ssh dagster "journalctl -u corpscout-dagster-dev --since '-15m' --no-pager | grep -i 'se_company_field' | tail -20"
```
(substitute `T0`). Expected: `companies` and `rewritten` climb in steps of 20,000 per page; the journal shows one page-summary line per page. The run page `http://dagster:3000/runs/<runId>` shows the same lines under the asset's step.

- [ ] **Step 4: Completion**

Expected when `status` is `SUCCESS`: `rewritten` = the wide table's published count (≈ 3,552,806 — every company was rewritten; a company with no `legal_name` candidate from `scb`/`bolagsverket` is still not published, as before); `resolved_rows` ≈ 20–35M; the run's materialization metadata (`$S/mat.json` with `se_company_field_resolved_clickhouse`) carries per-field rows resolved, rows from decisions, rows per winning source and companies with no row. Paste the metadata into the session notes. Then check the wide table's coverage against the baseline:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() AS baseline, countIf(new.company_id = '') AS vanished, countIf(new.resolved_at <= old.resolved_at) AS not_rewritten FROM corpscout.se_company_info_parity_snapshot AS old LEFT JOIN (SELECT company_id, resolved_at FROM corpscout.se_company_info FINAL) AS new ON new.company_id = old.company_id\""
```
Expected: `vanished` 0 and `not_rewritten` 0. Either being non-zero is a Task 6 no-go: it means the resolver skipped companies; do not proceed to retirement.

Serving check (spec section 10 — the view keeps its base): `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT view, status, toString(last_success_time), exception FROM system.view_refreshes WHERE view = 'se_companies_serving'\""`. Expected: `status` `Scheduled` (or `Running`), `last_success_time` advancing on the hourly cadence after the resolve finished, `exception` empty. The new wide columns are not yet in the serving SELECT; that extension is a separate staged-swap migration (memory `se-companies-serving-view`) and not part of this cutover.

- [ ] **Step 5: Apply the serving-view column migration (plan 3 Task 8, 000377) now that the new wide columns are filled**

Run: `make -C corpscout clickhouse-migrate-up-one` then `make -C corpscout clickhouse-migrate-version`
Expected: `377` (not dirty). If the classifier blocks the agent: `! make -C corpscout clickhouse-migrate-up-one` in the owner's terminal.

Then force one refresh and verify the new columns are served:

```
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SYSTEM REFRESH VIEW corpscout.se_companies_serving; SYSTEM WAIT VIEW corpscout.se_companies_serving; SELECT countIf(website IS NOT NULL) AS with_website, countIf(employee_count IS NOT NULL) AS with_employees, countIf(latest_revenue_amount IS NOT NULL) AS with_revenue, count() AS total FROM corpscout.se_companies_serving\""
```

Expected: `total` equals the pre-cutover serving row count (Task 5 step 4), and the three `with_*` counts are non-zero and within the magnitudes the candidate counts of Task 4 predict (website ≤ the `domains` + `wikidata` candidate count; revenue ≤ the `bolagsverket` + `esef` + `ratsit` latest_revenue candidate count). `SYSTEM WAIT VIEW` can exceed the client timeout on this view (see the serving memory note); if it does, poll `system.view_refreshes` until `last_success_time` advances instead.

---

### Task 6: Parity check — go/no-go

**Files:** none changed.

**Interfaces:**
- Consumes: asset check `se_company_field_parity_check` on `se_company_field_resolved_clickhouse` (part 4), which compares `se_company_info FINAL` against `se_company_info_parity_snapshot` per spec 12 step 4 — plus part 3's two refinements: a row with `length(correction_ids) > 0` must keep its old text, and a row with `description_source_count > 1` and no `suggestion_id` is reported as `description_model_pending_changed` and never fails — and reports counts of rows per field per source; `corpscout.se_company_info_enrichment_observation` (`suggestion_id`, `suggestion` JSON with `description`/`description_sv`).
- Produces: the go/no-go decision that gates Task 7. Nothing is written.

- [ ] **Step 1: Execute the check on the final table state**

The resolve run may already have executed the check (asset checks run with their asset by default); execute it again explicitly so the evaluation is on the table as it stands now. UI: asset `se_company_field_resolved_clickhouse` → **Checks** tab → `se_company_field_parity_check` → **Execute**. GraphQL:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/launch-check.json <<'EOF'
{"query": "mutation Launch($p: ExecutionParams!) { launchRun(executionParams: $p) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } ... on InvalidSubsetError { message } ... on RunConfigValidationInvalid { errors { message path } } } }",
 "variables": {"p": {"selector": {"repositoryLocationName": "dagster_v3", "repositoryName": "__repository__", "jobName": "se_company_field_resolve_job", "assetSelection": [], "assetCheckSelection": [{"assetKey": {"path": ["se_company_field_resolved_clickhouse"]}, "name": "se_company_field_parity_check"}]}, "runConfigData": {}, "mode": "default", "executionMetadata": {"tags": [{"key": "cutover", "value": "se-field-registry"}]}}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/launch-check.json; echo
```
Expected: `LaunchRunSuccess`; the run finishes in minutes (three joins over 3.55M rows).

- [ ] **Step 2: Read the evaluation**

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/check.json <<'EOF'
{"query": "query Check($key: AssetKeyInput!, $name: String!) { assetCheckExecutions(assetKey: $key, checkName: $name, limit: 1) { runId status evaluation { severity metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on FloatMetadataEntry { floatValue } ... on JsonMetadataEntry { jsonString } } } } }",
 "variables": {"key": {"path": ["se_company_field_resolved_clickhouse"]}, "name": "se_company_field_parity_check"}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/check.json | python3 -m json.tool
```
Expected: `status` `SUCCEEDED`; the metadata entries carry the compared-company count, one mismatch count per compared column (description for `llm_enhanced = false` rows, description vs the stored observation for `llm_enhanced = true` rows, description for reviewed rows — `length(correction_ids) > 0` — against the old text, `legal_name`, `legal_form_code`, `status`, `incorporation_date`, `primary_sni_code`, `primary_nace_code`), the informational `description_model_pending_changed` count (rows with `description_source_count > 1` and no suggestion whose text changed — expected non-zero where the LLM extractor wrote a candidate the pilot never had; it does not fail the check) and the rows-per-field-per-source table. Paste the whole evaluation into the session notes beside Task 4 step 5's candidate totals.

- [ ] **Step 3: Independent confirmation (not the check's own labels)**

```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() AS compared, countIf(NOT old.llm_enhanced AND NOT (old.description_source_count > 1 AND old.suggestion_id IS NULL) AND ifNull(old.description, '') <> ifNull(new.description, '')) AS description_plain_mismatch, countIf(NOT old.llm_enhanced AND NOT (old.description_source_count > 1 AND old.suggestion_id IS NULL) AND ifNull(old.description_sv, '') <> ifNull(new.description_sv, '')) AS description_sv_plain_mismatch, countIf(length(old.correction_ids) > 0 AND (ifNull(old.description, '') <> ifNull(new.description, '') OR ifNull(old.description_sv, '') <> ifNull(new.description_sv, ''))) AS description_reviewed_mismatch, countIf(old.description_source_count > 1 AND old.suggestion_id IS NULL AND ifNull(old.description, '') <> ifNull(new.description, '')) AS description_model_pending_changed, countIf(old.legal_name <> new.legal_name) AS legal_name_mismatch, countIf(ifNull(old.legal_form_code, '') <> ifNull(new.legal_form_code, '')) AS legal_form_code_mismatch, countIf(old.status <> new.status) AS status_mismatch, countIf(ifNull(toString(old.incorporation_date), '') <> ifNull(toString(new.incorporation_date), '')) AS incorporation_date_mismatch, countIf(old.primary_sni_code <> new.primary_sni_code) AS sni_mismatch, countIf(old.primary_nace_code <> new.primary_nace_code) AS nace_mismatch FROM corpscout.se_company_info_parity_snapshot AS old INNER JOIN (SELECT company_id, description, description_sv, legal_name, legal_form_code, status, incorporation_date, primary_sni_code, primary_nace_code FROM corpscout.se_company_info FINAL) AS new ON new.company_id = old.company_id\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() AS llm_rows, countIf(ifNull(new.description, '') <> JSONExtractString(obs.suggestion, 'description')) AS description_llm_mismatch, countIf(new.llm_enhanced = false) AS lost_llm_flag FROM corpscout.se_company_info_parity_snapshot AS old INNER JOIN (SELECT company_id, description, llm_enhanced FROM corpscout.se_company_info FINAL) AS new ON new.company_id = old.company_id INNER JOIN corpscout.se_company_info_enrichment_observation AS obs ON obs.suggestion_id = old.suggestion_id WHERE old.llm_enhanced\""
```
Expected: `compared` ≈ 3,552,806 and every `*_mismatch` column 0 (`description_reviewed_mismatch` included: a reviewer's decision is a `se_company_info_field_value` row, which the resolver applies by construction); `description_model_pending_changed` is informational — non-zero is expected wherever the LLM extractor wrote a candidate for a multi-source company the pilot never modelled, and it is NOT a gate; `llm_rows` = the snapshot's `llm_rows` and `description_llm_mismatch` 0, `lost_llm_flag` 0.

- [ ] **Step 4: Decide (spec 12 step 4 is the rule)**

**GO** only when all of these hold: the check's `status` is `SUCCEEDED`; every mismatch count in its metadata AND in step 3 is 0 (`description_model_pending_changed` is not a mismatch count — record its value, it may be non-zero); Task 5 step 4's `vanished` and `not_rewritten` are 0. Then continue to Task 7.

**NO-GO** on anything else: stop; do not start Task 7; the old publisher stays deployed and Task 9's "before retirement" rollback applies if the wide rows must be restored before the investigation ends. Investigate per column with these queries (each lists 20 examples with both values; adapt the column name):
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT old.company_id, old.legal_name AS old_value, new.legal_name AS new_value, f.source, f.source_record_uid FROM corpscout.se_company_info_parity_snapshot AS old INNER JOIN (SELECT company_id, legal_name FROM corpscout.se_company_info FINAL) AS new ON new.company_id = old.company_id LEFT JOIN (SELECT company_id, source, source_record_uid FROM corpscout.se_company_field FINAL WHERE field = 'legal_name') AS f ON f.company_id = old.company_id WHERE old.legal_name <> new.legal_name LIMIT 20\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT source, count() FROM corpscout.se_company_field FINAL WHERE field = 'legal_name' AND company_id IN (SELECT old.company_id FROM corpscout.se_company_info_parity_snapshot AS old INNER JOIN (SELECT company_id, legal_name FROM corpscout.se_company_info FINAL) AS new ON new.company_id = old.company_id WHERE old.legal_name <> new.legal_name) GROUP BY source\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT company_id, field, source, source_record_uid, toString(observed_at), value FROM corpscout.se_company_field_candidate WHERE company_id = 'PASTE_ONE_MISMATCHED_ID' AND field = 'legal_name' ORDER BY source\""
```
Reading the result: (a) a mismatch whose winning `source` is `bolagsverket` while the baseline came from `scb` is the registry's ranking (`legal_name`: scb, bolagsverket, wikidata — spec 4.2 after the 2026-09-02 scb-first reorder, so this case now means the scb artifact carried no value for that field) disagreeing with the old publisher's "SCB is authoritative" — a design decision for the owner: either reorder the registry (part 1 code, registry version bump, redeploy via Task 3, re-run Task 5) or accept the class explicitly in the check (part 4 code); never accept it silently here; (b) a mismatch with the same winning source is an extractor or resolver bug (compare the candidate row's `value` with the artifact's value) — fix in parts 2–4, redeploy, re-run the affected candidates asset and Task 5; (c) `description_llm_mismatch` > 0 means the LLM extractor did not reuse the stored observation the old row was built from — check `source_record_uid` of the `llm` candidate against `old.suggestion_id`. After any fix, Task 5 (with `resolve_all`) and this task run again in full; the snapshot is NOT retaken (it is the baseline).

---

### Task 7: Retire the old publisher (dagster_v3) and re-point the backoffice

Two commits: 7.A dagster_v3, 7.B backoffice. `dg check defs` and the full backoffice `npm run typecheck` must be clean before each.

**Files (7.A — dagster_v3, paths under `corpscout/services/dagster_v3/`):**
- Delete or trim: `src/dagster_v3/defs/se_company/info.py` (asset, scan, config, jobs, schedule, `defs`; see A2 for what may survive)
- Modify: `src/dagster_v3/defs/se_company/info_rules.py` (trim to the three symbols the address side imports)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:223-232` (the info leaf and its comment)
- Modify: `src/dagster_v3/defs/se_company/fields/<the module defining se_company_fields_job>` (the weekly's job must include the three artifact assets — A4)
- Delete: `tests/test_se_company_info.py`
- Modify: `tests/test_se_company_info_rules.py` (keep only `test_evidence_set_hash_for_is_order_independent`)
- Modify: `tests/test_se_company_info_clickhouse_local.py` (drop the final/scan parts, keep the artifact passes)
- Create: `tests/test_se_company_info_retirement.py`

**Files (7.B — backoffice, paths under `corpscout/services/backoffice/`):**
- Modify: `app/lib/dagster.server.ts:48-54` (constant values)
- Modify: `app/lib/se-company-info-pipeline.ts` (add `CANDIDATE_SOURCES`)
- Modify: `app/lib/se-company-info-pipeline.server.ts` (scan port → candidates CTE; run-config builders for the resolver and the LLM extractor; counts interface)
- Modify: `app/routes/admin-se-companies-pipeline.ts` (the two model intents → resolve intent + LLM-candidates intent; confirmation copy)
- Modify: `app/components/admin/se-company-info-pipeline.tsx` (stats cards and the two launch forms)
- Modify: `app/lib/se-company-info-pipeline.server.test.ts`, `app/routes/admin-se-companies-pipeline.test.ts`, `tests/admin-se-company-info-pipeline-sheet.test.tsx`

**Interfaces:**
- Consumes: from the fields package — assets `se_company_field_resolved_clickhouse`, `se_company_field_candidates_llm`, jobs `se_company_fields_job` / `se_company_field_resolve_job` (Task 3 step 1's pinned names), sensor `se_company_info_field_value_sensor`, schedule `se_company_fields_weekly`; `ClickhouseLeaf(asset_key, tables, max_age)` and `WEEKLY` from `clickhouse_checks.py`; backoffice `launchRun(input: {job, assetSelection?, runConfig, tags?})`, `chQuery`.
- Produces: no asset `se_company_info_clickhouse`, no jobs `se_company_info_job`/`se_company_info_review_job`, no schedule `se_company_info_weekly`; `info_rules.py` exporting exactly `ArtifactRow`, `_text`, `evidence_set_hash_for`; backoffice constants `SE_COMPANY_INFO_JOB = "se_company_fields_job"`, `SE_COMPANY_INFO_REVIEW_JOB = "se_company_field_resolve_job"`, `SE_COMPANY_INFO_ASSET = "se_company_field_resolved_clickhouse"`, `SE_COMPANY_INFO_SCHEDULE = "se_company_fields_weekly"`; `INFO_ASSET = "se_company_field_resolved_clickhouse"`, `LLM_CANDIDATES_ASSET = "se_company_field_candidates_llm"`, `buildInfoRunConfig({maxCompanies, companyIds?})`, `buildLlmCandidatesRunConfig({maxCompanies, companyIds?, llm})`, `CANDIDATE_SOURCES`, `SeCompanyInfoSelectionCounts {companyCount, changedCount, neverPublishedCount, newEvidenceCounts: Record<CandidateSource, number>, ledgerPendingCount, llmPendingCount}`.

#### 7.A — dagster_v3

- [ ] **Step A1: Grep before deleting**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "from dagster_v3.defs.se_company.info import|from dagster_v3.defs.se_company import info\b|se_company\.info\b|se_company.info_rules|from dagster_v3.defs.se_company.info_rules import" corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests corpscout/services/backoffice/app --glob '!**/se_company/info.py' --glob '!**/tests/test_se_company_info.py' --glob '!**/tests/test_se_company_info_rules.py' --glob '!**/tests/test_se_company_info_clickhouse_local.py'
rg -n "se_company_info_clickhouse|se_company_info_job|se_company_info_review_job|se_company_info_weekly" corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests --glob '!**/se_company/info.py' --glob '!**/tests/test_se_company_info.py'
```
Expected (as of 2026-09-02): `info_rules` importers are `address_rules.py` (`ArtifactRow, _text, evidence_set_hash_for`), `address.py` (`ArtifactRow`) and `tests/test_se_company_address_rules.py`; `common.py:41` and `company_people/merge.py:137` mention `se_company.info` in comments only; the old names appear in `clickhouse_checks.py:232` (leaf), `tests/test_se_company_common.py` (its own test doubles — leave them), `address.py:800` and `tests/test_se_company_address.py:699` (comments about the cron slot). Any importer of `se_company.info` that is NOT one of the three deleted test files (for example the fields package importing `LlmProfileConfig`, `build_llm_client`, `DESCRIPTION_PROMPT_VERSION`, `map_ordered`, `OBSERVATION_COLUMNS` for the LLM extractor) names symbols that survive: write them down for A2.

- [ ] **Step A2: Delete the publisher from `info.py`**

If A1 found no importer of `se_company.info` outside the three deleted tests: `git rm corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/info.py`.

Otherwise keep the file and delete every publisher-only definition — `EPOCH_SQL`, `_clickhouse_stamp`, `MULTI_SOURCE_SQL`, `PENDING_MODEL_SQL`, `ARTIFACT_ENVELOPE`, `UNREAD_ARTIFACT_COLUMNS`, `_read_columns`, `ARTIFACT_READS`, `ARTIFACT_TABLES`, `SELECTION_REASONS`, `SELECTION_COLUMNS`, `INSERT_COLUMNS`, `build_changed_companies_sql`, `build_artifact_rows_sql`, `_artifact_row_from_row`, `build_field_values_sql`, `_field_value_from_row`, `_final_row`, `_Prepared`, `_call_model`, `_resolve_page`, `materialize_se_company_info`, `SECompanyInfoConfig`, `se_company_info_clickhouse`, `se_company_info_job`, `se_company_info_review_job`, `AUTOMATED_RUN_CONFIG`, `se_company_info_weekly`, `defs` — plus any of `SE_COMPANY_INFO`, `SE_COMPANY_INFO_FIELD_VALUE`, `SE_COMPANY_INFO_OBSERVATION`, `OBSERVATION_FLUSH_ROWS`, `FALLBACK_*`, `build_description_request`, `_source_entry`, `_publish_observations`, `build_token_averages_sql`, `token_averages` and the `info_rules`/`scb`/`esef`/`wikidata` imports that A1 did not list as imported. Replace the module docstring with:

```python
"""LLM profile and description-request helpers shared by the SE field-registry LLM
extractor (``dagster_v3.defs.se_company.fields``).

The publisher that used to live here -- the se_company_info asset, its change scan, its
config, jobs, sensor and weekly schedule -- was retired on the 2026-09-02 field-registry
cutover: candidates are extracted per source, precedence is generated SQL from the
registry, and ``se_company_field_resolved_clickhouse`` writes the wide table.
"""
```
Then `uv run python -c "import dagster_v3.defs.se_company.info"` must succeed (unused imports removed).

- [ ] **Step A3: Trim `info_rules.py` to what the address side imports**

Replace the whole file with:

```python
"""Shared artifact-row helpers for the SE company datatypes.

What remains of the retired info publisher's rules (2026-09-02, field-registry cutover):
the address side (``address.py``, ``address_rules.py``) imports ``ArtifactRow``, ``_text``
and ``evidence_set_hash_for`` and nothing else. The description merge, the source
precedence and ``apply_field_values`` are gone -- precedence now lives in the field
registry (``dagster_v3.defs.se_company.fields``) and runs as generated ClickHouse SQL.
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

    Must equal the final table's MATERIALIZED ``evidence_set_hash`` column:
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

- [ ] **Step A4: The weekly job must still refresh the three artifacts**

`se_company_info_job` used to run `se_company_info_scb_clickhouse`, `se_company_info_esef_clickhouse` and `se_company_info_wikidata_clickhouse` before the final, and the freshness leaves in `clickhouse_checks.py` expect them WEEKLY. Run `rg -n "se_company_fields_job = " -A 6 corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/`. If its `AssetSelection.assets(...)` does not name those three artifact assets, add them at the front of the selection:

```python
se_company_fields_job = dg.define_asset_job(
    "se_company_fields_job",
    selection=dg.AssetSelection.assets(
        # The three per-source artifacts the retired se_company_info_job refreshed: the
        # scb/esef/wikidata candidates read them, and their freshness leaves are WEEKLY.
        "se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
        "se_company_info_wikidata_clickhouse",
        "se_company_field_registry_clickhouse",
        "se_company_field_candidates_scb", "se_company_field_candidates_bolagsverket",
        "se_company_field_candidates_esef", "se_company_field_candidates_wikidata",
        "se_company_field_candidates_ratsit", "se_company_field_candidates_domains",
        "se_company_field_candidates_llm",
        "se_company_field_resolved_clickhouse",
    ),
)
```
(keep whatever else part 4's definition carries — tags, description, config — and only widen the selection).

- [ ] **Step A5: Re-point the freshness leaf**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, replace the comment at lines 223-225 and the line `ClickhouseLeaf("se_company_info_clickhouse", ("se_company_info",), WEEKLY),` with:

```python
    # se_company — three per-source artifacts and, since the 2026-09-02 field-registry
    # cutover, the registry resolver's two tables in place of the retired
    # se_company_info_clickhouse. All refreshed by se_company_fields_weekly, so a missed
    # week turns the freshness check red.
    ClickhouseLeaf("se_company_info_scb_clickhouse", ("se_company_info_scb",), WEEKLY),
    ClickhouseLeaf(
        "se_company_info_esef_clickhouse", ("se_company_info_esef",), WEEKLY
    ),
    ClickhouseLeaf(
        "se_company_info_wikidata_clickhouse", ("se_company_info_wikidata",), WEEKLY
    ),
    ClickhouseLeaf(
        "se_company_field_resolved_clickhouse", ("se_company_field", "se_company_info"), WEEKLY
    ),
```
If part 4 already added a `se_company_field_resolved_clickhouse` leaf, keep exactly one.

- [ ] **Step A6: Tests — delete, trim, add**

1. `git rm corpscout/services/dagster_v3/tests/test_se_company_info.py`.
2. `tests/test_se_company_info_rules.py`: keep the module docstring, the imports `from dagster_v3.defs.se_company.info_rules import evidence_set_hash_for` only, and `test_evidence_set_hash_for_is_order_independent`; delete every other test and the `uuid`/`datetime`/`StoredObservation`/`ArtifactRow` imports they used.
3. `tests/test_se_company_info_clickhouse_local.py` — keep the artifact passes, drop the final/scan parts:
   - imports: drop `INSERT_COLUMNS, build_artifact_rows_sql, build_changed_companies_sql` (the whole `from dagster_v3.defs.se_company.info import (...)`) and `from dagster_v3.defs.se_company.info_rules import evidence_set_hash_for`;
   - module constants/functions: delete `EVIDENCE_HASHES`, `_final_row_values`, `_final_rows_sql`, `FINAL_ROW_SQL`, `T_BEFORE_CUTOFF`, `RESOLVE_ALL_CUTOFF`, `T_AFTER_CUTOFF`, `CUTOFF_ROWS_SQL`, `NO_CUTOFF`, `_changed_params`;
   - in `_script`: delete every `parts.append(...)` whose section label starts with `changed_`, `rows`, `final_description`, `final_legal_form`, `final_llm_enhanced`, `evidence_set_hash`, `resolve_all_`, and the `parts.append(FINAL_ROW_SQL)` / `parts.append(CUTOFF_ROWS_SQL)` lines with the `SETTLE` immediately before each; keep `final_columns` (it reads `system.columns` and needs no row);
   - delete the test functions that read those sections: `test_the_final_row_carries_both_legal_form_labels`, `test_resolve_all_skips_companies_already_rewritten_past_its_cutoff`, `test_resolve_all_re_selects_settled_companies`, `test_changed_companies_scan_tracks_publication_and_pending_model`, `test_artifact_rows_sql_returns_one_row_per_source_for_alpha`, `test_final_row_evidence_set_hash_matches_info_rules`, `test_the_final_row_carries_both_description_languages`, `test_the_final_row_round_trips_the_llm_enhanced_flag`; in `test_a_translation_arriving_after_publication_re_selects_the_company` delete the two `changed_*` assertions and rename it `test_a_translation_arriving_after_publication_appends_one_version`;
   - in the `MIGRATIONS` comments replace mentions of `INSERT_COLUMNS`/`FINAL_ROW_SQL` with "the final table (empty in this harness since the 2026-09-02 cutover)";
   - `rg -n "changed_|final_description|final_legal_form|final_llm_enhanced|evidence_set_hash\"|resolve_all_|INSERT_COLUMNS|EVIDENCE_HASHES" tests/test_se_company_info_clickhouse_local.py` must then print nothing.
4. Create `tests/test_se_company_info_retirement.py`:

```python
"""The 2026-09-02 field-registry cutover retired the hand-written se_company_info
publisher. These pin that it is gone from the repository, that the automation now hangs
off the registry resolver, and that the weekly still refreshes the three artifacts."""

import dagster as dg


def test_the_old_publisher_is_gone_and_the_registry_resolver_owns_its_triggers() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES, WEEKLY

    repository = load_defs().get_repository_def()
    assert not repository.asset_graph.has(dg.AssetKey("se_company_info_clickhouse"))
    assert not repository.has_job("se_company_info_job")
    assert not repository.has_job("se_company_info_review_job")
    assert not repository.has_schedule_def("se_company_info_weekly")

    sensor = repository.get_sensor_def("se_company_info_field_value_sensor")
    assert sensor.job_name == "se_company_field_resolve_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    resolve_keys = {k.path[-1] for k in repository.get_job("se_company_field_resolve_job").asset_layer.executable_asset_keys}
    assert resolve_keys == {"se_company_field_resolved_clickhouse"}

    weekly = repository.get_schedule_def("se_company_fields_weekly")
    weekly_keys = {k.path[-1] for k in repository.get_job(weekly.job_name).asset_layer.executable_asset_keys}
    assert {"se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
            "se_company_info_wikidata_clickhouse", "se_company_field_registry_clickhouse",
            "se_company_field_resolved_clickhouse"} <= weekly_keys
    assert {f"se_company_field_candidates_{s}" for s in (
        "scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")} <= weekly_keys

    leaves = {leaf.asset_key: leaf for leaf in CLICKHOUSE_LEAVES}
    assert "se_company_info_clickhouse" not in leaves
    assert leaves["se_company_field_resolved_clickhouse"].tables == ("se_company_field", "se_company_info")
    assert leaves["se_company_field_resolved_clickhouse"].max_age == WEEKLY


def test_info_rules_exports_only_what_the_address_side_imports() -> None:
    from dagster_v3.defs.se_company import info_rules

    public = {name for name in dir(info_rules) if not name.startswith("__")}
    assert {"ArtifactRow", "evidence_set_hash_for", "_text"} <= public
    for retired in ("merge_company_info", "apply_field_values", "InfoOutcome", "FieldValueRow",
                    "DESCRIPTION_PRIORITY", "INFO_VALUE_FIELDS", "INFO_VALUE_SOURCES"):
        assert not hasattr(info_rules, retired), retired
```

- [ ] **Step A7: Run the tests and the definitions check**

Run (from `corpscout/services/dagster_v3`):
```bash
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_se_company_info_retirement.py tests/test_se_company_info_rules.py tests/test_se_company_address_rules.py tests/test_se_company_address.py tests/test_se_company_common.py tests/test_se_company_parity_snapshot.py tests/test_schedule_cron_contracts.py -q
uv run pytest tests/test_se_company_info_clickhouse_local.py -q -m integration
uv run dg check defs
```
Expected: every file PASS (the clickhouse-local file skips with a reason if `clickhouse-local` is unusable on this machine; that is acceptable only when the skip message says so, never on an assertion); `dg check defs` exit 0.
Owner fallback: `! cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_se_company_info_retirement.py tests/test_se_company_info_rules.py tests/test_se_company_address_rules.py tests/test_se_company_address.py tests/test_se_company_common.py tests/test_se_company_parity_snapshot.py tests/test_schedule_cron_contracts.py -q && uv run dg check defs`

- [ ] **Step A8: Commit 7.A**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/info_rules.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields \
        corpscout/services/dagster_v3/tests/test_se_company_info_rules.py \
        corpscout/services/dagster_v3/tests/test_se_company_info_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_se_company_info_retirement.py
git add -u corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/info.py corpscout/services/dagster_v3/tests/test_se_company_info.py
git commit -m "refactor(se): retire the se_company_info publisher after the field-registry cutover

Parity proved on prod (Task 6 of the cutover plan): the registry resolver owns
se_company_info now. Deletes the old asset, change scan, config, jobs and weekly;
trims info_rules.py to the artifact-row helpers the address side imports; the
freshness leaf and the weekly job follow the resolver.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

#### 7.B — backoffice

- [ ] **Step B1: Constants**

`app/lib/dagster.server.ts` lines 48-54 become:

```ts
/** The registry chain (artifacts → registry export → candidates → resolver): the weekly's job. */
export const SE_COMPANY_INFO_JOB = "se_company_fields_job";
/** The resolver alone: what the field-value sensor and this page's resolve launch run. */
export const SE_COMPANY_INFO_REVIEW_JOB = "se_company_field_resolve_job";
export const SE_COMPANY_INFO_ASSET = "se_company_field_resolved_clickhouse";

/** The two instigators that drive THIS pipeline. The repository has 52 schedules
 * and 15 sensors; a page that renders all of them tells its reader nothing. */
export const SE_COMPANY_INFO_SCHEDULE = "se_company_fields_weekly";
export const SE_COMPANY_INFO_SENSOR = "se_company_info_field_value_sensor";
```
(`app/lib/dagster.server.test.ts` passes job names as literal strings through the transport and asserts them back; it needs no change.)

- [ ] **Step B2: Client-safe module — the candidate sources**

Append to `app/lib/se-company-info-pipeline.ts` after `INFO_ARTIFACT_SOURCES`:

```ts
/** Every candidate source the registry knows, in the registry's own spelling. The
 * change scan counts new evidence per source, so the sheet renders one badge each. */
export const CANDIDATE_SOURCES = [
  "scb",
  "bolagsverket",
  "esef",
  "wikidata",
  "ratsit",
  "domains",
  "llm",
] as const;

export type CandidateSource = (typeof CANDIDATE_SOURCES)[number];
```

- [ ] **Step B3: Failing server tests**

Replace the `describe("the selection query")`, `describe("loadSeCompanyInfoPipelineStats")` and `describe("buildInfoRunConfig")` blocks of `app/lib/se-company-info-pipeline.server.test.ts` with (keep `describe("buildArtifactRunConfig")` and the imports it needs; add `buildLlmCandidatesRunConfig`, `LLM_CANDIDATES_ASSET` to the import list and drop `DEFAULT_MAX_TOKENS`/`DEFAULT_PROMPT_VERSION` only if no remaining test uses them):

```ts
describe("the selection query", () => {
  it("ports the registry resolver's change scan: candidates, ledger, published", () => {
    const sql = INFO_SELECTION_COUNTS_SQL;
    expect(sql).toContain("FROM corpscout.se_company_info AS final FINAL");
    // The artifacts CTE became a candidates CTE (spec 8.4): one long table, one
    // per-source freshness column. No artifact table is read any more.
    expect(sql).toContain("FROM corpscout.se_company_field_candidate");
    expect(sql).not.toContain("FROM corpscout.se_company_info_scb");
    for (const source of ["scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm"]) {
      expect(sql).toContain(`maxIf(extracted_at, source = '${source}') AS ${source}_extracted_at`);
      expect(sql).toContain(`candidates.${source}_extracted_at > ${PUBLISHED_AT} AS new_evidence_${source}`);
    }
    expect(sql).toContain("FROM corpscout.se_company_info_field_value");
    expect(sql).toContain("max(created_at) AS latest_correction_at");
    expect(sql).toContain("ifNull(published.company_id, '') = '' AS never_published");
    expect(sql).toContain(`ifNull(ledger.latest_correction_at, ${EPOCH}) > ${PUBLISHED_AT} AS ledger_pending`);
    expect(sql).not.toContain("> published.resolved_at");
    // The model term is gone from the resolver; what remains is the LLM extractor's
    // own selection (spec 5.3): two or more non-LLM description candidates newer than
    // the last llm candidate.
    expect(sql).toContain("HAVING uniqIf(source, source <> 'llm') >= 2");
    expect(sql).toContain("AND maxIf(extracted_at, source <> 'llm') > maxIf(extracted_at, source = 'llm')");
    expect(sql).not.toContain("pending_model");
    expect(sql).not.toContain("description_source_count");
  });

  it("counts the two selections the page's two launches make", () => {
    const sql = INFO_SELECTION_COUNTS_SQL;
    const changed =
      "never_published OR new_evidence_scb OR new_evidence_bolagsverket OR new_evidence_esef OR new_evidence_wikidata OR new_evidence_ratsit OR new_evidence_domains OR new_evidence_llm OR ledger_pending";
    expect(sql).toContain(`toString(countIf(${changed})) AS changed_count`);
    expect(sql).toContain("toString(countIf(llm_pending)) AS llm_pending_count");
    expect(sql.match(/toString\(countIf\(/g)).toHaveLength(11);
    expect(sql).toContain("toString(count()) AS company_count");
    expect(sql).toContain("FROM selection");
  });

  it("reads artifact freshness and the observed cost per model", () => {
    expect(INFO_ARTIFACT_FRESHNESS_SQL).toContain("toString(max(observed_at)) AS latest_observed_at");
    expect(INFO_ARTIFACT_FRESHNESS_SQL.match(/UNION ALL/g)).toHaveLength(2);
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("WHERE prompt_tokens > 0");
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("AS avg_prompt_tokens");
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("AS avg_completion_tokens");
    expect(INFO_OBSERVATION_AVERAGES_SQL).not.toMatch(/\) AS prompt_tokens/);
    expect(INFO_OBSERVATION_AVERAGES_SQL).not.toMatch(/\) AS completion_tokens/);
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("GROUP BY model_name");
  });
});

describe("loadSeCompanyInfoPipelineStats", () => {
  it("shapes the three reads into the numbers the page renders", async () => {
    const queryImpl = vi.fn(async (sql: string) => {
      if (sql.startsWith("WITH candidates")) {
        return [
          {
            company_count: "3500000",
            changed_count: "1240",
            never_published_count: "12",
            new_evidence_scb_count: "800",
            new_evidence_bolagsverket_count: "300",
            new_evidence_esef_count: "40",
            new_evidence_wikidata_count: "60",
            new_evidence_ratsit_count: "70",
            new_evidence_domains_count: "20",
            new_evidence_llm_count: "9",
            ledger_pending_count: "5",
            llm_pending_count: "410",
          },
        ];
      }
      if (sql.startsWith("SELECT 'scb' AS source")) {
        return [{ source: "scb", latest_observed_at: "2026-08-22 03:00:00.000", row_count: "7000000" }];
      }
      return [
        { model_name: "deepseek-v4-flash", call_count: "1200", avg_prompt_tokens: "640", avg_completion_tokens: "240" },
      ];
    });

    const stats = await loadSeCompanyInfoPipelineStats({ queryImpl });

    expect(queryImpl).toHaveBeenCalledTimes(3);
    expect(stats.selection).toEqual({
      companyCount: 3_500_000,
      changedCount: 1_240,
      neverPublishedCount: 12,
      newEvidenceCounts: { scb: 800, bolagsverket: 300, esef: 40, wikidata: 60, ratsit: 70, domains: 20, llm: 9 },
      ledgerPendingCount: 5,
      llmPendingCount: 410,
    });
    expect(stats.artifacts).toEqual([
      { source: "scb", latestObservedAt: "2026-08-22 03:00:00.000", rowCount: 7_000_000 },
    ]);
    expect(stats.models[0].promptTokens).toBe(640);
  });

  it("reads zeros rather than NaN when nothing has been published yet", async () => {
    const queryImpl = vi.fn(async () => []);
    const stats = await loadSeCompanyInfoPipelineStats({ queryImpl });
    expect(stats.selection.changedCount).toBe(0);
    expect(stats.selection.newEvidenceCounts.esef).toBe(0);
    expect(stats.selection.llmPendingCount).toBe(0);
    expect(stats.artifacts).toEqual([]);
    expect(stats.models).toEqual([]);
  });
});

describe("buildInfoRunConfig", () => {
  it("sends execute: true, the cap and the scope, and nothing model-shaped", () => {
    expect(buildInfoRunConfig({ maxCompanies: 1000 })).toEqual({
      ops: { [INFO_ASSET]: { config: { execute: true, max_companies: 1000, company_ids: [] } } },
    });
    expect(INFO_ASSET).toBe("se_company_field_resolved_clickhouse");
  });

  it("scopes the run to the picked companies, normalised the way the form sent them", () => {
    const config = buildInfoRunConfig({
      maxCompanies: 1000,
      companyIds: [" 5560125220 ", "5565200028", "5560125220", ""],
    }) as { ops: Record<string, { config: { company_ids: string[] } }> };
    expect(config.ops[INFO_ASSET].config.company_ids).toEqual(["5560125220", "5565200028"]);
  });
});

describe("buildLlmCandidatesRunConfig", () => {
  it("targets the LLM candidates extractor with the whole profile and no credential", () => {
    const config = buildLlmCandidatesRunConfig({ maxCompanies: 500, llm: PROFILE });
    expect(config).toEqual({
      ops: {
        [LLM_CANDIDATES_ASSET]: {
          config: {
            execute: true,
            max_companies: 500,
            company_ids: [],
            llm: {
              provider: "deepseek",
              model: "deepseek-v4-flash",
              base_url: "https://api.deepseek.com",
              temperature: 0,
              max_tokens: DEFAULT_MAX_TOKENS,
              prompt_version: DEFAULT_PROMPT_VERSION,
              concurrency: 4,
            },
          },
        },
      },
    });
    expect(LLM_CANDIDATES_ASSET).toBe("se_company_field_candidates_llm");
    const serialized = JSON.stringify(config).toLowerCase();
    expect(serialized).not.toContain("api_key");
    expect(serialized).not.toContain("apikey");
    expect(serialized).not.toContain("secret");
  });

  it("clamps the concurrency it is handed rather than passing it through", () => {
    const config = buildLlmCandidatesRunConfig({
      maxCompanies: 10,
      llm: { ...PROFILE, concurrency: 99 },
    }) as { ops: Record<string, { config: { llm: { concurrency: number } } }> };
    expect(config.ops[LLM_CANDIDATES_ASSET].config.llm.concurrency).toBe(8);
  });
});
```

Run: `npx vitest run app/lib/se-company-info-pipeline.server.test.ts`
Expected: FAIL — `buildLlmCandidatesRunConfig`/`LLM_CANDIDATES_ASSET` undefined, `INFO_ASSET` still `se_company_info_clickhouse`, the SQL still reads the artifact tables.

- [ ] **Step B4: Server module — scan port and builders**

In `app/lib/se-company-info-pipeline.server.ts`:

1. Header comment: replace the first paragraph's "PORT of Dagster's `build_changed_companies_sql` (dagster_v3/defs/se_company/info.py): the same three CTEs, the same per-source `maxIf` freshness" with "PORT of the registry resolver's change scan (dagster_v3/defs/se_company/fields, spec 8.4): the `candidates` CTE in place of the old `artifacts` one, the same per-source `maxIf` freshness (now over `extracted_at`)" and add a third deliberate difference after the two listed: "and it does not count registry/policy-version drift — that is an operator event (a deploy that bumps `se-info-vN`), applied by a `resolve_all` run, not something a reviewer's re-resolve click should promise". Update the cost paragraph: "one FINAL read of `se_company_info` (3.5M rows) joined to a GROUP BY over `se_company_field_candidate` (tens of millions of rows)".
2. Imports: add `CANDIDATE_SOURCES, type CandidateSource` to the import from `~/lib/se-company-info-pipeline`.
3. Replace `export const INFO_ASSET = "se_company_info_clickhouse";` with:
```ts
/** The resolver's op key in a run config, and the LLM extractor's. */
export const INFO_ASSET = "se_company_field_resolved_clickhouse";
export const LLM_CANDIDATES_ASSET = "se_company_field_candidates_llm";
```
4. Replace `InfoRunOptions` and `buildInfoRunConfig` with:
```ts
export interface InfoRunOptions {
  maxCompanies: number;
  /** The companies picked on the list, or [] for "every changed company". */
  companyIds?: readonly string[];
}

/**
 * The run config for one `se_company_field_resolved_clickhouse` run, in Dagster's
 * own snake_case. `execute: true` is written here and nowhere else -- without it
 * the asset runs a preview that writes nothing. The resolver calls no model: LLM
 * text reaches it as candidates written by `buildLlmCandidatesRunConfig`'s run.
 */
export function buildInfoRunConfig(options: InfoRunOptions): Record<string, unknown> {
  return {
    ops: {
      [INFO_ASSET]: {
        config: {
          execute: true,
          max_companies: clampMaxCompanies(options.maxCompanies),
          company_ids: normalizeCompanyIdScope(options.companyIds ?? []),
        },
      },
    },
  };
}

export interface LlmCandidatesRunOptions {
  maxCompanies: number;
  companyIds?: readonly string[];
  llm: PipelineLlmProfile;
}

/**
 * The run config for one `se_company_field_candidates_llm` run: the extractor that
 * writes `source = 'llm'` candidates for description / description_sv. The `llm`
 * block carries the profile and never a key: the Dagster host resolves
 * `dagsterApiKeyVariable(provider)` from its own environment.
 */
export function buildLlmCandidatesRunConfig(
  options: LlmCandidatesRunOptions,
): Record<string, unknown> {
  return {
    ops: {
      [LLM_CANDIDATES_ASSET]: {
        config: {
          execute: true,
          max_companies: clampMaxCompanies(options.maxCompanies),
          company_ids: normalizeCompanyIdScope(options.companyIds ?? []),
          llm: {
            provider: options.llm.provider,
            model: options.llm.model,
            base_url: options.llm.baseUrl,
            temperature: DEFAULT_TEMPERATURE,
            max_tokens: DEFAULT_MAX_TOKENS,
            prompt_version: DEFAULT_PROMPT_VERSION,
            concurrency: clampConcurrency(options.llm.concurrency),
          },
        },
      },
    },
  };
}
```
5. Delete `MULTI_SOURCE`, `PENDING_MODEL`, `CHANGED` and the `EVIDENCE_CHANGED` definition; replace `newEvidence`, `LEDGER_PENDING`, `NEVER_PUBLISHED` and the two SQL constants `INFO_SELECTION_CTE_SQL` / `INFO_SELECTION_COUNTS_SQL` with:
```ts
/** New evidence from one candidate source: its newest candidate is newer than the
 * published resolution. `maxIf` over no rows is 1970, so a source the company has no
 * candidate from never reads as new evidence. `extracted_at` IS the candidate table's
 * ReplacingMergeTree version, so the max needs no FINAL. */
function newEvidence(source: CandidateSource): string {
  return `candidates.${source}_extracted_at > ${PUBLISHED_AT}`;
}

const LEDGER_PENDING = `ifNull(ledger.latest_correction_at, ${EPOCH}) > ${PUBLISHED_AT}`;
const NEVER_PUBLISHED = "ifNull(published.company_id, '') = ''";

/** Everything a resolve run picks up: never published, a newer candidate from any
 * source, or a newer decision. The registry-version term of the Dagster scan is
 * deliberately absent -- see the header. */
const CHANGED = [
  "never_published",
  ...CANDIDATE_SOURCES.map((source) => `new_evidence_${source}`),
  "ledger_pending",
].join(" OR ");

export const INFO_SELECTION_CTE_SQL = `WITH candidates AS (
  SELECT company_id,
    ${CANDIDATE_SOURCES.map(
      (source) => `maxIf(extracted_at, source = '${source}') AS ${source}_extracted_at`,
    ).join(",\n    ")}
  FROM corpscout.se_company_field_candidate
  GROUP BY company_id
),
ledger AS (
  SELECT company_id, max(created_at) AS latest_correction_at
  FROM corpscout.se_company_info_field_value
  GROUP BY company_id
),
published AS (
  SELECT final.company_id AS company_id, final.resolved_at AS resolved_at
  FROM corpscout.se_company_info AS final FINAL
),
llm_pending AS (
  SELECT company_id
  FROM corpscout.se_company_field_candidate
  WHERE field = 'description'
  GROUP BY company_id
  HAVING uniqIf(source, source <> 'llm') >= 2
    AND maxIf(extracted_at, source <> 'llm') > maxIf(extracted_at, source = 'llm')
),
selection AS (
  SELECT candidates.company_id AS company_id,
    ${NEVER_PUBLISHED} AS never_published,
    ${CANDIDATE_SOURCES.map(
      (source) => `${newEvidence(source)} AS new_evidence_${source}`,
    ).join(",\n    ")},
    ${LEDGER_PENDING} AS ledger_pending,
    ifNull(llm_pending.company_id, '') <> '' AS llm_pending
  FROM candidates
  LEFT JOIN published ON published.company_id = candidates.company_id
  LEFT JOIN ledger ON ledger.company_id = candidates.company_id
  LEFT JOIN llm_pending ON llm_pending.company_id = candidates.company_id
)`;

/** Counts are read back as strings: a UInt64 over 2^53 would lose precision as a
 * JSON number, and every other list page in this app reads counts the same way. */
export const INFO_SELECTION_COUNTS_SQL = `${INFO_SELECTION_CTE_SQL}
SELECT
  toString(count()) AS company_count,
  toString(countIf(${CHANGED})) AS changed_count,
  toString(countIf(never_published)) AS never_published_count,
  ${CANDIDATE_SOURCES.map(
    (source) => `toString(countIf(new_evidence_${source})) AS new_evidence_${source}_count`,
  ).join(",\n  ")},
  toString(countIf(ledger_pending)) AS ledger_pending_count,
  toString(countIf(llm_pending)) AS llm_pending_count
FROM selection`;
```
6. Replace `SeCompanyInfoSelectionCounts` with:
```ts
export interface SeCompanyInfoSelectionCounts {
  companyCount: number;
  /** What a "Re-resolve changed companies" run selects. */
  changedCount: number;
  neverPublishedCount: number;
  newEvidenceCounts: Record<CandidateSource, number>;
  ledgerPendingCount: number;
  /** What a "Run the model pass" (LLM candidates) run selects: companies with two
   * or more non-LLM description candidates newer than their last llm candidate. */
  llmPendingCount: number;
}
```
7. In `loadSeCompanyInfoPipelineStats`, replace the `selection:` object with:
```ts
    selection: {
      companyCount: num(row, "company_count"),
      changedCount: num(row, "changed_count"),
      neverPublishedCount: num(row, "never_published_count"),
      newEvidenceCounts: Object.fromEntries(
        CANDIDATE_SOURCES.map((source) => [source, num(row, `new_evidence_${source}_count`)]),
      ) as Record<CandidateSource, number>,
      ledgerPendingCount: num(row, "ledger_pending_count"),
      llmPendingCount: num(row, "llm_pending_count"),
    },
```

Run: `npx vitest run app/lib/se-company-info-pipeline.server.test.ts`
Expected: PASS.

- [ ] **Step B5: Route — two intents, two builders**

In `app/routes/admin-se-companies-pipeline.ts`:

1. Import `buildLlmCandidatesRunConfig` and `LLM_CANDIDATES_ASSET` from `~/lib/se-company-info-pipeline.server` (keep `buildInfoRunConfig`, `buildArtifactRunConfig`, `infoArtifactAsset`).
2. Replace `modelIntent` with:
```ts
  /** Re-resolve: the registry resolver on its own job. No model, no profile. */
  const resolveIntent = (run: ReturnType<typeof modelRun>): LaunchIntent => ({
    job: SE_COMPANY_INFO_REVIEW_JOB,
    runConfig: buildInfoRunConfig({ maxCompanies: run.maxCompanies, companyIds: run.companyIds }),
  });

  /** The model pass: the LLM candidates extractor, narrowed to its own asset on the
   * chain job. The resolver picks its rows up on the next resolve (sensor, weekly, or
   * a Re-resolve click). */
  const llmIntent = (run: ReturnType<typeof modelRun>): LaunchIntent => ({
    job: SE_COMPANY_INFO_JOB,
    assetSelection: [LLM_CANDIDATES_ASSET],
    runConfig: buildLlmCandidatesRunConfig({
      maxCompanies: run.maxCompanies,
      companyIds: run.companyIds,
      llm: {
        provider: run.profile?.provider ?? "deepseek",
        model: run.profile?.model ?? "deepseek-v4-flash",
        baseUrl: run.profile?.baseUrl ?? "https://api.deepseek.com",
        concurrency: run.concurrency,
      },
    }),
  });
```
3. In `modelRun`, `useModel` is only meaningful for the model pass now: change its first line to `const useModel = pendingModelOnly;` (the resolve launch never calls a model, whatever the old checkbox said).
4. In the `confirm-resolve` / `confirm-model-pass` branch replace the `selected`/`bounded`/`calls` computation and the `lines` array with:
```ts
      const { selection } = statsResult.stats;
      const selected = pendingModelOnly ? selection.llmPendingCount : selection.changedCount;
      const bounded = Math.min(selected, maxCompanies);
      const scoped = companyIds.length > 0;
      const scopeLine = scoped
        ? `Scoped to ${describeCompanyScope(companyIds)}; of those, only the ones the change scan still selects are ${pendingModelOnly ? "sent to the model" : "resolved"}. This run stops after ${nf.format(maxCompanies)}.`
        : `${nf.format(selected)} companies match right now; this run stops after ${nf.format(maxCompanies)}, so it will ${pendingModelOnly ? "send" : "resolve"} ${nf.format(bounded)}.`;
      const modelLine = pendingModelOnly
        ? `Each enters the model step, ${concurrency} call${concurrency === 1 ? "" : "s"} at a time. Answers already stored for the same request are reused and cost nothing.`
        : "The resolver calls no model: it picks a value per field from the stored candidates and the reviewer's decisions.";
      const confirmation: PipelineConfirmation = {
        intent: pendingModelOnly ? "launch-model-pass" : "launch-resolve",
        title: pendingModelOnly ? "Run the model pass" : "Re-resolve changed companies",
        lines: [
          scopeLine,
          modelLine,
          pendingModelOnly && profile
            ? `Model ${profile.model} (${profile.provider}) at ${profile.baseUrl}; the key comes from ${keyVariable} on the Dagster host.`
            : "No model is called.",
          pendingModelOnly
            ? "The run writes llm candidates to corpscout.se_company_field_candidate and, when the model answers, to se_company_info_enrichment_observation; the next resolve publishes them."
            : "The run writes to corpscout.se_company_field and corpscout.se_company_info.",
        ],
        fields: {
          use_model: useModel ? "1" : "",
          max_companies: String(maxCompanies),
          concurrency: String(concurrency),
          profile_id: profile?.profileId ?? "",
          company_ids: formatCompanyIdScope(companyIds),
        },
      };
```
5. In the `launch-resolve` / `launch-model-pass` branch: `return await launch(pendingModelOnly ? llmIntent(run) : resolveIntent(run));` (keep the profile refusal for `run.useModel && !run.profile`).

- [ ] **Step B6: Route tests**

In `app/routes/admin-se-companies-pipeline.test.ts`:
- `STATS.selection` becomes `{ companyCount: 3_500_000, changedCount: 1_240, neverPublishedCount: 12, newEvidenceCounts: { scb: 800, bolagsverket: 300, esef: 40, wikidata: 60, ratsit: 70, domains: 20, llm: 9 }, ledgerPendingCount: 5, llmPendingCount: 410 }`.
- "launches the confirmed run with execute: true and the pilot tag": the config read becomes `input.runConfig.ops.se_company_field_resolved_clickhouse.config`; keep the `execute`/`max_companies`/`company_ids` assertions; replace the `resolve_multi_source_with_llm`, `pending_model_only` and `llm` assertions with `expect(config.llm).toBeUndefined();` and `expect(input.job).toBe("se_company_field_resolve_job");`.
- "scopes the launch to the picked companies": read `ops.se_company_field_resolved_clickhouse.config`.
- "scopes a model pass the same way": read `ops.se_company_field_candidates_llm.config`; replace `expect(config.pending_model_only).toBe(true)` with `expect(input.job).toBe("se_company_fields_job"); expect(input.assetSelection).toEqual(["se_company_field_candidates_llm"]); expect(config.llm).toEqual({ provider: "deepseek", model: "deepseek-v4-flash", base_url: "https://api.deepseek.com", temperature: 0, max_tokens: 6000, prompt_version: "se-company-info-description-v3", concurrency: 2 });` (declare `input` with `job`, `assetSelection` and `runConfig` in the cast).
- "binds an artifact refresh to its own asset": `expect(input.job).toBe("se_company_fields_job")`.
- "restates the numbers it just re-read": replace `expect(result.confirmation?.lines[1]).toContain("340")` with `expect(result.confirmation?.lines[1]).toContain("calls no model")`; add a model-pass case: `const pass = await post({ intent: "confirm-model-pass", ...RESOLVE_FIELDS }); expect(pass.confirmation?.lines[0]).toContain("410 companies match right now");`.
- "says what a SCOPED run covers": keep; the `lines[1]` assertion becomes `expect(result.confirmation?.lines[1]).toContain("calls no model")`.
- "refuses a model run whose provider cannot name a key variable": post `intent: "confirm-model-pass"` instead of `confirm-resolve` (the resolve launch no longer needs a profile).

Run: `npx vitest run app/routes/admin-se-companies-pipeline.test.ts`
Expected: PASS.

- [ ] **Step B7: Component and sheet test**

In `app/components/admin/se-company-info-pipeline.tsx`:
- import `CANDIDATE_SOURCES` beside `INFO_ARTIFACT_SOURCES`;
- "Changed companies" card: `<Stat label="Selected" value={nf.format(stats.selection.changedCount)} hint="What a resolve run selects" />`; the badge loop iterates `CANDIDATE_SOURCES` (`new {source}` badges, seven of them);
- "Model work" card: one `Stat` — `label="Owed an LLM description" value={nf.format(stats.selection.llmPendingCount)} hint="What the model pass selects: two or more text candidates newer than the last llm candidate"`; delete the "Would call the model" stat;
- "Re-resolve changed companies" form: delete the "Call the model" checkbox, `<ProfileSelect>` and `<ConcurrencyField />` from THIS form only (the model pass form keeps them); its `CardDescription` becomes "Everything the resolver picks up: new candidates from any source, new decisions, never-published companies — within the scope above. No model is called.";
- "Run the model pass" `CardDescription` becomes "Only the companies with two or more text candidates newer than their last llm candidate — within the scope above. Writes llm candidates; the next resolve publishes them."

In `tests/admin-se-company-info-pipeline-sheet.test.tsx`: `STATS.selection` as in B6; line 145's `expect(html).toContain("900 without the model term")` becomes `expect(html).toContain("What a resolve run selects")`; lines 151-152 become `expect(html).toContain("410"); expect(html).toContain("bolagsverket");` (React SSR splits `new {source}` into two text nodes, so the badge is matched by the source name alone); the `instigators.schedules[0].name` fixture becomes `"se_company_fields_weekly"` with `cronSchedule` unchanged.

Run: `npx vitest run tests/admin-se-company-info-pipeline-sheet.test.tsx app/lib/se-company-info-pipeline.test.ts app/lib/dagster.server.test.ts && npm run typecheck`
Expected: all PASS; typecheck clean.
Owner fallback: `! cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice && npx vitest run tests/admin-se-company-info-pipeline-sheet.test.tsx app/lib/se-company-info-pipeline.test.ts app/lib/dagster.server.test.ts app/lib/se-company-info-pipeline.server.test.ts app/routes/admin-se-companies-pipeline.test.ts && npm run typecheck`

- [ ] **Step B8: Commit 7.B**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/backoffice/app/lib/dagster.server.ts \
        corpscout/services/backoffice/app/lib/se-company-info-pipeline.ts \
        corpscout/services/backoffice/app/lib/se-company-info-pipeline.server.ts \
        corpscout/services/backoffice/app/lib/se-company-info-pipeline.server.test.ts \
        corpscout/services/backoffice/app/routes/admin-se-companies-pipeline.ts \
        corpscout/services/backoffice/app/routes/admin-se-companies-pipeline.test.ts \
        corpscout/services/backoffice/app/components/admin/se-company-info-pipeline.tsx \
        corpscout/services/backoffice/tests/admin-se-company-info-pipeline-sheet.test.tsx
git commit -m "refactor(backoffice): point the SE pipeline sheet at the field-registry assets

The old se_company_info publisher is retired: the sheet's resolve launch runs
se_company_field_resolved_clickhouse on se_company_field_resolve_job, the model pass
runs se_company_field_candidates_llm, and the change-scan port reads the candidates
table (spec 8.4) instead of the three artifacts.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 8: Deploy the retirement, switch the automation on, smoke-test one company

**Files:** none changed.

**Interfaces:**
- Consumes: main at the Task 7 commits; the same deploy recipe as Task 3; GraphQL `startSensor(sensorSelector)` / `startSchedule(scheduleSelector)`; the backoffice Info page `http://localhost:5183/admin/se/company/5020077862/info` (Use this / Release — the field-values flow), its synchronous resolve-after-decision path (spec section 9).
- Produces: prod without the old publisher; `se_company_info_field_value_sensor` (and `se_company_field_candidate_sensor` if part 3 created it) RUNNING; `se_company_fields_weekly` RUNNING; one verified decision round-trip.

- [ ] **Step 1: Deploy exactly as Task 3 steps 4–7**

Same worktree recipe from the merged HEAD (`git worktree add … HEAD`, `.env` copy, `uv sync --frozen`, both `dbt parse`, `refresh-defs-state`, `dg check defs`, `ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > $S/light_sync-retirement.log 2>&1; RC=$?; echo RC=$RC`), then verify:
```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/old.json; echo
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/instigators.json | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(*[(s['name'], s['sensorState']['status']) for s in d['sensorsOrError']['results'] if 'se_company' in s['name']], *[(s['name'], s['scheduleState']['status']) for s in d['schedulesOrError']['results'] if 'se_company' in s['name']], sep='\n')"
```
Expected: `RC=0`; `$S/old.json` answers `{"data":{"assetNodes":[]}}` (no `se_company_info_clickhouse`); the instigator list no longer contains `se_company_info_weekly`, and `se_company_info_field_value_sensor` / `se_company_fields_weekly` read `STOPPED`. Remove the worktree afterwards.

- [ ] **Step 2: Restart the backoffice on the merged code (owner)**

The owner restarts the React Router dev server on :5183 from the main checkout in their own terminal. Then open `http://localhost:5183/admin/se/companies`, open the **Pipeline** sheet: the "Changed companies" card renders seven `new <source>` badges and the recent-runs table lists the cutover runs (`se_company_field_resolve_job`) — this executes the new scan port against prod.

- [ ] **Step 3: Start the sensors and the weekly**

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/cutover
cat > $S/start-sensor.json <<'EOF'
{"query": "mutation Start($s: SensorSelector!) { startSensor(sensorSelector: $s) { __typename ... on Sensor { name sensorState { status } } ... on SensorNotFoundError { message } ... on PythonError { message } ... on UnauthorizedError { message } } }",
 "variables": {"s": {"repositoryLocationName": "dagster_v3", "repositoryName": "__repository__", "sensorName": "se_company_info_field_value_sensor"}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/start-sensor.json; echo
sed 's/se_company_info_field_value_sensor/se_company_field_candidate_sensor/' $S/start-sensor.json | ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql"; echo
cat > $S/start-schedule.json <<'EOF'
{"query": "mutation Start($s: ScheduleSelector!) { startSchedule(scheduleSelector: $s) { __typename ... on ScheduleStateResult { scheduleState { status } } ... on PythonError { message } ... on UnauthorizedError { message } } }",
 "variables": {"s": {"repositoryLocationName": "dagster_v3", "repositoryName": "__repository__", "scheduleName": "se_company_fields_weekly"}}}
EOF
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < $S/start-schedule.json; echo
```
Expected: `{"data":{"startSensor":{"__typename":"Sensor","name":"se_company_info_field_value_sensor","sensorState":{"status":"RUNNING"}}}}`; the second call answers `SensorNotFoundError` if part 3 created no candidate sensor (that is fine) or `RUNNING`; `{"data":{"startSchedule":{"__typename":"ScheduleStateResult","scheduleState":{"status":"RUNNING"}}}}`. The sensor's first tick re-reads its kept cursor (spec 8.4: the name is unchanged, so the instance keeps the cursor) — expect a `SkipReason` "No new rows" tick, not a run, unless decisions were written during the cutover.
Owner fallback: the same `ssh dagster "curl …" < file` lines with `!` in front; UI alternative: `http://dagster:3000/automation` toggles.

- [ ] **Step 4: Smoke — Use this**

Open `http://localhost:5183/admin/se/company/5020077862/info`. On the description card, pick a non-live source candidate (an SCB or ESEF text that is not the current value) and click **Use this**. Expected on the page: it reloads immediately showing that text as the resolved description with the chosen source's chip (the backoffice resolve-after-decision path, not a Dagster run). Verify in ClickHouse:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT toString(value_id), field, source, source_ref, toString(created_at) FROM corpscout.se_company_info_field_value WHERE company_id = '5020077862' ORDER BY created_at DESC LIMIT 3\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT field, source, source_record_uid, toString(decision_id), toString(resolved_at), substring(value, 1, 60) FROM corpscout.se_company_field FINAL WHERE company_id = '5020077862' AND field = 'description'\""
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT substring(description, 1, 60), llm_enhanced, toString(resolved_at) FROM corpscout.se_company_info FINAL WHERE company_id = '5020077862'\""
```
Expected: the newest field-value row is the decision (source = the picked candidate's source); the resolved row's `decision_id` equals that `value_id`, its `value` is the picked text and its `resolved_at` is seconds old; the wide row shows the same text with `llm_enhanced = false` (the source is not `llm`).

- [ ] **Step 5: Smoke — the sensor re-resolves as a same-value version**

Within 60 s the sensor ticks (`http://dagster:3000/automation` → `se_company_info_field_value_sensor` → Ticks: one tick with one run request; the run on `se_company_field_resolve_job` carries `company_ids: ["5020077862"]` and `execute: true` in its config). After that run is `SUCCESS`:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() AS versions, uniqExact(value) AS distinct_values, toString(max(resolved_at)) AS newest FROM corpscout.se_company_field WHERE company_id = '5020077862' AND field = 'description'\""
```
Expected: `distinct_values` 1 (the sensor's resolve produced the same value — a same-value version), `newest` later than step 4's `resolved_at`; `versions` ≥ 1 (background merges may already have collapsed the two). The FINAL row from step 4's second query is unchanged apart from `resolved_at`.

- [ ] **Step 6: Smoke — Release**

On the same page click **Release** on the description field. Expected: the page reloads immediately showing the pipeline's winner (the highest-ranked candidate: `llm` where an llm candidate exists, else esef/wikidata/scb per spec 4.2) with no reviewer marker; `SELECT toString(decision_id), source FROM corpscout.se_company_field FINAL WHERE company_id = '5020077862' AND field = 'description'` answers `\N` (NULL) and the winner's source; within 60 s the sensor launches one more scoped run, which again lands as a same-value version (repeat step 5's query: `distinct_values` stays 1 after the run, `newest` advances).

- [ ] **Step 7: Record the state**

Paste into the session notes: the deploy RC, the three instigator statuses, the two sensor run ids, and the final `se_company_field` / `se_company_info` counts:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT (SELECT count() FROM corpscout.se_company_field FINAL) AS resolved_rows, (SELECT count() FROM corpscout.se_company_info FINAL) AS published, (SELECT count() FROM corpscout.se_company_info_parity_snapshot) AS baseline_kept\""
```
Expected: `published` ≈ 3,552,806, `baseline_kept` the same number (the snapshot is kept, never dropped).

---

### Task 9: Rollback (reference — read before Task 2, executed only on a no-go)

**Files:** none. Nothing here drops or truncates anything; the migrations 000373–000376 are additive and stay applied in every rollback.

**Interfaces:**
- Consumes: the old asset `se_company_info_clickhouse` (deployed until Task 7 ships) with `SECompanyInfoConfig` (`execute`, `resolve_all`, `resolve_all_before`, `max_companies` capped at 1,000,000, `resolve_multi_source_with_llm`, `llm`); `git revert`.
- Produces: `se_company_info` rows written by the old publisher again, as NEWER versions — the wide table is `ReplacingMergeTree(resolved_at)`, so whichever publisher wrote last wins, and a rollback is therefore always "make the old publisher write again", never a restore of old parts.

- [ ] **Before Task 7 (old asset still deployed):** re-run the old publisher over every company. Launchpad YAML for `se_company_info_clickhouse` on `se_company_info_review_job`:
```yaml
ops:
  se_company_info_clickhouse:
    config:
      execute: true
      resolve_all: true
      resolve_all_before: "T_ROLLBACK"
      max_companies: 1000000
      resolve_multi_source_with_llm: true
      llm:
        provider: deepseek
        model: deepseek-v4-flash
        base_url: https://api.deepseek.com
        temperature: 0
        max_tokens: 6000
        prompt_version: se-company-info-description-v3
        concurrency: 2
```
where `T_ROLLBACK` is `SELECT toString(now64(3, 'UTC'))` taken before the first launch (`"2026-09-03 12:00:00.000"` style). `max_companies` is capped at 1,000,000 by the old config, so this is FOUR identical launches (3.55M companies), each waited to `SUCCESS`; the fixed cutoff is what makes run 2 continue past run 1's million instead of rewriting it. `resolve_multi_source_with_llm: true` with the pilot profile REUSES every stored observation by `input_hash` (no paid call for an unchanged request) and restores `llm_enhanced = true` on the model-written rows; switching the model off would publish the deterministic pick and lose them. Verify with Task 6 step 3's first query: every mismatch column 0 against the snapshot (the snapshot IS the old publisher's output). `se_company_field` and `se_company_field_candidate` keep their rows — nothing reads them for publication while the old asset writes the wide table, and the sensor stays STOPPED. The serving MV re-picks the restored rows on its next hourly refresh.

- [ ] **After Task 7 / Task 8 (old asset deleted):** `git revert` the two Task 7 commits (7.B then 7.A, `git revert --no-edit <sha-7B> <sha-7A>`), stop `se_company_info_field_value_sensor` and `se_company_fields_weekly` (Task 3 step 2's stop files), redeploy with Task 3's recipe, restart the backoffice on the reverted code (owner), then run the four-launch `resolve_all` above — the newer `resolved_at` overwrites the registry resolver's rows. Start `se_company_info_field_value_sensor` again only after the old publisher's pass has finished (its job is `se_company_info_review_job` again after the revert).

- [ ] **Never in a rollback:** `clickhouse-migrate-down`, `clickhouse-migrate-force`, `DROP`/`TRUNCATE` of `se_company_field*`, `se_company_info_parity_snapshot` or any column added by 000374 — the old publisher ignores columns it does not name, and the ledger stays at 376.

---

## Self-review

**Spec coverage.** 8.4 sensor keeps its cursor and launches the resolver → Task 8 step 3 (name unchanged; first tick reads the kept cursor) and Task 7 A6's wiring test. 10 serving survives → Task 5 step 4 (`system.view_refreshes` check; new-column extension is plan 3 Task 8, migration 000377, applied in Task 5 step 4 after the backfill). 12 migrations additive, no DROP → Task 1 (000376) and Task 2 (one at a time, version after each); cutover steps 1–6 → Tasks 2–8 in the spec's order; parity per 12 step 4 → Task 6 (check + independent SQL + go/no-go); rollback "old asset republishes from artifacts, newer resolved_at wins" → Task 9 (with the four-launch cap recipe and the model-reuse subtlety). 13 out of scope → nothing here touches financial/jobs registries, public views, other countries, or the clock-skew note.

**Placeholder scan.** Every `PASTE_*` token is an explicit substitution of a value the previous step prints (run ids, instigator ids, company ids); every "if part N differs" branch is a verification with the command that decides it (Task 3 step 1, Task 4 step 4's Launchpad schema panel), not deferred work. No TBD/TODO. Code steps carry full code; operational steps carry exact commands and expected outputs.

**Type consistency.** `PARITY_SNAPSHOT_COLUMNS` (14 names: `company_id, description, description_sv, llm_enhanced, suggestion_id, description_source_count, correction_ids, legal_name, legal_form_code, status, incorporation_date, primary_sni_code, primary_nace_code, resolved_at`), `SE_COMPANY_INFO_PARITY_SNAPSHOT`, `materialize_parity_snapshot(*, clickhouse, execute, overwrite)` are used identically in Task 1's module and DDL, its tests, Task 2 step 5, Task 4 step 2 and Task 6 step 3. Job names `se_company_fields_job` / `se_company_field_resolve_job` are the same in Task 3 step 1, Task 4/5/6 launch files, Task 7 A4/A6/B1/B5/B6 and Task 8. `SeCompanyInfoSelectionCounts` fields (`changedCount`, `neverPublishedCount`, `newEvidenceCounts`, `ledgerPendingCount`, `llmPendingCount`) match between B4's interface, B4's loader, B3/B6/B7's fixtures and B5's route. `INFO_ASSET`/`LLM_CANDIDATES_ASSET` values equal `SE_COMPANY_INFO_ASSET` and the asset selected in B5.
