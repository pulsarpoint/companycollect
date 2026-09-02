# SE Field Registry, Part 3: Resolve Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `se_company_field_resolved_clickhouse`: the registry-driven resolve that runs the exported per-field `resolve_sql` statements in company batches, re-pivots `corpscout.se_company_info` through `publish_with_stage`, is launched by the (re-pointed) field-value sensor, a new candidate sensor and a weekly schedule, carries the cutover parity asset check, and extends `se_companies_serving` with the new wide columns.

**Architecture:** One asset reads every statement it executes from `corpscout.se_company_field_registry` (the contract shared with the backoffice), never from the Python renderers. Per batch it runs each field's statement in registry order, then the `field = '*'` projection through the stage table, then one counts query for metadata. Its change scan is the old `build_changed_companies_sql` with the `artifacts` CTE replaced by a `candidates` CTE and a `versions` CTE that compares stamped policy/registry versions against the registry table. The statements' `{name:Type}` parameters are bound server-side through a driver client opened with `server_side_params=True` from the resource's own fields; the clickhouse-local harness binds the same statements with `SET param_*`.

**Tech Stack:** Python 3.14 + uv, Dagster 1.13.9 (`dg.asset`, `dg.asset_check`, `ledger_sensor`), clickhouse-driver 0.2.10 via `dagster_clickhouse.ClickhouseResource`, ClickHouse 26.5 (clickhouse-local harness through Docker image `clickhouse/clickhouse-server:26.5`), golang-migrate ledger, pytest.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-02-se-company-field-registry-design.md`, sections 7.4 (as consumer), 8 (all), 10 (serving view), 12 (parity check, pivot and resolve tests). Plans 1 (registry + tables + migrations) and 2 (policies, generated SQL, extractors) are assumed merged; this plan imports their names and never re-creates them.

## Global Constraints

- Python 3.14 + uv. Every command runs from `corpscout/services/dagster_v3`: `uv run --frozen --no-sync pytest <file> -q -p no:warnings`. Any test that loads `dagster_v3.definitions` (and `uv run --frozen --no-sync dg check defs`) needs `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment.
- Dagster assets, jobs, sensors, schedules and checks are autoloaded from `src/dagster_v3/defs/` through each module's `defs = dg.Definitions(...)`; no `definitions.py` edit. No `from __future__ import annotations` in a module that defines a `@dg.asset` / `@dg.asset_check`.
- ClickHouse 26.5. `FINAL` only on ReplacingMergeTree tables. Every `LEFT JOIN` miss is read through `ifNull` so the SQL answers identically under `join_use_nulls = 0` and `1`.
- **Parameter binding.** The registry statements use server-side named parameters (`{field:String}`, `{company_ids:Array(String)}`, `{source_run_id:String}`, `{resolved_at:DateTime64(3, 'UTC')}`). clickhouse-driver forwards those over the native protocol only from a `Client` created with `settings={"server_side_params": True}` -- it is a client-level setting (popped at construction; `substitute_params` consults `client_settings`), there is no per-`execute()` toggle, and `param_*` entries in `execute(settings=...)` are NOT parameters (`Code: 456 Substitution not set`). So the resolve asset opens its own client from the resource's fields (`open_resolve_client`) and passes a params dict. Two driver quirks, verified 2026-09-02 against `clickhouse/clickhouse-server:26.5` with the pinned driver: (1) a Python `list` is double-quoted on the wire (`Code: 27`), and a pre-rendered `str` is double-escaped (`Code: 26`) -- the driver's `escape_param(for_server=True)` escapes str values twice and quotes non-str values' `str()` without escaping. Hence `ServerSideLiteral`: a non-str wrapper whose `__str__` returns the array literal escaped exactly once (works for elements with quotes, backslashes and newlines, empty arrays and 20,000-element arrays). (2) a `datetime` goes through `escape_datetime` (server-timezone conversion, seconds precision), so `resolved_at` is passed as its millisecond text `YYYY-MM-DD HH:MM:SS.mmm`. Plain `str` and `int` values pass through unchanged. `execute(sql)`, `execute(sql, None)` and `execute(sql, {})` all work on such a client, `%` inside a statement is left alone, and per-call `settings=` are still honoured. The backoffice side (clickhouse-js over HTTP) is unaffected.
- Heavy runs (`resolve_all`, the cutover rebuild, the parity check against 3.5M rows, the serving-view rebuild) execute on the prod Dagster / ClickHouse hosts, never locally; local runs are the FakeClient tests and the clickhouse-local harness.
- `corpscout.se_company_info` keeps its name, engine (`ReplacingMergeTree(resolved_at) ORDER BY (company_id)`) and every column; plan 1's additive migration adds the eight new columns from spec 8.3.
- Names are fixed by the spec and the coordinator: asset `se_company_field_resolved_clickhouse` (group `se_company_fields`); jobs `se_company_fields_job` (the weekly chain) and `se_company_field_resolve_job` (the resolve asset alone, used by the sensors and the backoffice launch); sensor `se_company_info_field_value_sensor` (name unchanged: the backoffice's `dagster.server.ts:55` names it); sensor `se_company_field_candidate_sensor`; schedule `se_company_fields_weekly` at `50 6 * * 1` UTC (the slot `se_company_info_weekly` leaves; `tests/test_schedule_cron_contracts.py` forbids sharing a `(minute, hour)` pair); check `se_company_field_parity_check`.
- `info.py` and `info_rules.py` are NOT deleted here (the cutover plan does that); the old asset `se_company_info_clickhouse` and its jobs `se_company_info_job` / `se_company_info_review_job` stay registered beside the new asset. Only the sensor and the schedule move out of `info.py`, because both names are taken over.
- Migrations: `corpscout/clickhouse/migrations/`, golang-migrate, forward-only ledger; first line `CREATE DATABASE IF NOT EXISTS corpscout;`, file ends with a statement, both `up` and `down`, one entry in `EXPECTED_MIGRATIONS` (`tests/test_clickhouse_migrations.py`) plus a content test. Number = `max(existing) + 1` at execution time (expected `000377`; verify with `ls corpscout/clickhouse/migrations | tail -2`).
- Conventional Commits, staged by explicit path (`git add <paths>`), never `git add -A`. Every commit message ends with these two trailer lines, each on its own line:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Per repo rule: commit existing unrelated working-tree changes (or leave them unstaged) before starting; never sweep them into this plan's commits.

## Cross-plan contract

### Consumed (plans 1 and 2 -- import, never re-create)

| Name | Module | Used for |
| --- | --- | --- |
| `SE_COMPANY_FIELD_REGISTRY`, `SE_COMPANY_FIELD_CANDIDATE`, `SE_COMPANY_FIELD`, `SE_COMPANY_INFO` (qualified table names, e.g. `SE_COMPANY_FIELD == "corpscout.se_company_field"`) | `dagster_v3.defs.se_company.fields.tables` | every statement this plan renders |
| `SE_COMPANY_FIELD_COLUMNS`, `SE_COMPANY_FIELD_CANDIDATE_COLUMNS` | `fields.tables` | harness read-backs |
| `INFO_REGISTRY` (a `DatatypeRegistry` with `.datatype == "info"`, `.country == "SE"`, `.version == "se-info-v1"`, `.fields: tuple[FieldSpec, ...]`), `FieldSpec`, `DatatypeRegistry`, `field_names(registry) -> tuple[str, ...]` (registry order), `field_by_name(registry, name) -> FieldSpec` | `fields.registry` | field order, datatype/country, version guard |
| `policy_for(field: FieldSpec) -> FieldPolicy` with `.name`, `.version` | `fields.policies` | harness seeds the registry rows |
| `render_resolve_sql(registry, field: FieldSpec) -> str` (params `{field:String}`, `{company_ids:Array(String)}`, `{source_run_id:String}`, `{resolved_at:DateTime64(3, 'UTC')}`; an `INSERT INTO corpscout.se_company_field ...`), `render_projection_sql(registry) -> str` (param `{company_ids:Array(String)}`; an `INSERT INTO corpscout.se_company_info (<columns>) ...`) | `fields.sql` | harness seeds the registry rows; a header-shape test. The asset itself reads the statements from the registry table (plan 1's decision CTE fix -- `argMax((value, source, ...), ...)` -- therefore needs nothing here) |
| `corpscout.se_company_field_registry` rows per spec 4.3, one row per field plus the `field = '*'` projection row; consumers read `argMax(..., version)` | migration (plan 1) | `load_registry_statements`, the scan's `versions` CTE |
| `corpscout.se_company_field_candidate` (spec 5.1), `corpscout.se_company_field` (spec 8.1), `corpscout.se_company_info_field_value` (000371, CHECKs widened by plan 1), the eight new `se_company_info` columns | migrations (plan 1) | scan, resolve, projection, serving view |
| assets `se_company_field_registry_clickhouse`, `se_company_field_candidates_{scb,bolagsverket,esef,wikidata,ratsit,domains,llm}` | plans 1 and 2 | deps and the weekly job |

### Produced (plans 4 and 5 import these names)

| Name | Module (all under `src/dagster_v3/defs/se_company/fields/` unless noted) |
| --- | --- |
| `SECompanyFieldResolveConfig`, `ResolveSummary`, `FieldStats`, `materialize_se_company_fields(context, client, config, *, registry, now)`, `build_changed_companies_sql(registry)`, `open_resolve_client(clickhouse)`, `ServerSideLiteral`, `server_array`, `server_params`, `split_insert_header`, `load_registry_statements`, `RESOLVE_ASSET`, `REGISTRY_ASSET`, `ARTIFACT_ASSETS`, `CANDIDATE_ASSETS`, `LLM_CANDIDATES_ASSET`, `PARITY_CHECK_NAME`, `SE_COMPANY_INFO_FIELD_VALUE`, `AUTOMATED_RUN_CONFIG`, `SELECTION_REASONS`, `SELECTION_COLUMNS`, asset `se_company_field_resolved_clickhouse` | `resolve.py` |
| `PARITY_SNAPSHOT`, `build_parity_snapshot_sql()`, `build_parity_sql()`, `build_rows_per_field_source_sql()`, `PARITY_COLUMNS`, `parity_result(...)`, `run_parity_check(client)`, check `se_company_field_parity_check` | `parity.py` |
| `se_company_field_resolve_job` = `AssetSelection.assets("se_company_field_resolved_clickhouse") - AssetSelection.checks(<parity check>)`; `se_company_fields_job` = `AssetSelection.assets(*WEEKLY_ASSETS) - AssetSelection.checks(<parity check>)` with `WEEKLY_ASSETS = ("se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse", "se_company_info_wikidata_clickhouse", "se_company_field_registry_clickhouse", <the seven candidate assets>, "se_company_field_resolved_clickhouse")`; `WEEKLY_ASSETS` | `jobs.py` |
| `se_company_info_field_value_sensor`, `se_company_field_candidate_sensor`, `candidate_sensor(...)`, `build_candidate_cursor_sql`, `build_candidate_touched_sql`, `MAX_SCOPED_COMPANY_IDS` | `sensors.py` |
| `se_company_fields_weekly`, `LLM_CANDIDATES_RUN_CONFIG` | `schedules.py` |
| `publish_with_stage(..., client=)` (additive) | `se_company/common.py` |
| `build_se_companies_serving_sql()` projecting the eight new columns; migration `0003NN_corpscout_se_companies_serving_field_registry_columns` | `sweden_company/companies_current.py`, `corpscout/clickhouse/migrations/` |

### Decisions taken where the spec or the prompt left room

1. **Parameter binding** is option (a) of the coordinator's note -- a dedicated `server_side_params=True` client built from `ClickhouseResource`'s fields through `dagster_clickhouse.resource.client_kwargs_from_resource_config` (the same helper `get_connection` uses) -- with the two driver-quirk workarounds recorded in Global Constraints. Every statement of a run goes through that one client, `publish_with_stage` included (Task 1's `client=`), so the projection's `{company_ids:Array(String)}` binds inside the stage `INSERT ... SELECT` too. The harness proves the same statements under `SET param_*`.
2. **Version comparison** in the scan: a per-field comparison against the registry TABLE (`current_registry` CTE, `argMax(..., version)`), not `{registry_version}` / `{policy_versions:Map}` parameters. The statements executed come from the table too, so "stamped version differs from the table" is exactly "resolved by an older statement"; comparing against the CODE's version instead would flag every company on a deploy whose export has not run yet and then stamp them with the old version again (endless churn). The code/table mismatch is caught separately: `load_registry_statements` refuses to run when the table's `registry_version` is not `INFO_REGISTRY.version`, naming `se_company_field_registry_clickhouse` as the fix. Fields no longer in the registry are ignored by the `INNER JOIN`.
3. **"SCB row mandatory" (spec 8.3 last paragraph)** is a pre-filter in the SCAN, not a WHERE in the projection: the `candidates` CTE carries `has_register_name = countIf(field = 'legal_name' AND source IN ('bolagsverket', 'scb')) > 0` and the scan's WHERE requires it. A company without a register name is never resolved either (no long rows, no weekly churn), matching the old asset's `skipped_no_register_count` behaviour.
4. **Second sensor.** `ledger_sensor` cannot serve `se_company_field_candidate` (it hard-codes `created_at` and `toUUID(<id column>)`; the candidate table has `extracted_at` and no UUID column), so `sensors.py` adds a small `candidate_sensor` factory with cursor `count:max(extracted_at)`. When more than `MAX_SCOPED_COMPANY_IDS` (20,000) companies were touched it launches an UNSCOPED run (the scan finds them through `new_candidates`) rather than stuffing millions of ids into run config. Default STOPPED like every SE sensor.
5. **The weekly job includes all seven candidate assets and the three old artifact assets** (coordinator). Spec 5.3 makes the LLM extractor's provider/model required run config, so `se_company_fields_weekly` carries `LLM_CANDIDATES_RUN_CONFIG` for it (the pinned production profile, spelled out in `schedules.py`) beside the resolve asset's `execute: true`; the Definitions test validates the schedule's run config against the job with `dg.validate_run_config`, so a key that plan 2's config class does not accept fails the suite by name.
6. **Parity rules (spec 12 step 4)** are refined by two snapshot columns, `description_source_count` and `correction_ids`: (a) a company whose old row already carried an applied decision (`length(correction_ids) > 0`) must match its OLD text, not the observation's (the decision, not the model, wrote it); (b) a company published with several sources but no suggestion (model-off initial load / failed call) is EXPECTED to change once the LLM extractor supplies a candidate, so it is reported as `description_model_pending_changed` and never fails the check. Pass = zero mismatches on legal facts and codes, zero on copied descriptions, zero on decided descriptions, zero on LLM descriptions against the stored observation, and no company missing after the rebuild.
7. **`render_resolve_sql(registry, field)`** is called with a `FieldSpec` (the only reading consistent with `policy_for(field)`); the harness is its only caller here.
8. **Jobs live in `fields/jobs.py`**: both jobs subtract the parity check from their selection (a cutover-only instrument must not run -- and fail -- on every sensor tick), and Dagster 1.13.9 raises `DagsterInvalidSubsetError` at repository build when a subtracted check key is undefined, so the jobs must import the check definition. `parity.py` imports only constants from `resolve.py`; `jobs.py` imports both. The leaf row-count check (`clickhouse_tables_non_empty`) stays in both jobs.
9. **`se_company_info_weekly` and the field-value sensor leave `info.py`** in Tasks 5-6 (the old asset and its two jobs stay). The freshness leaf for the new asset is registered with `max_age=None` (row-count check only) until the cutover plan starts the schedule; the old leaf is left untouched (it dies with the old asset).
10. **Serving view (spec 10).** The eight new columns are projected straight off `se_company_info` (`website` folded to `''` like every other string column; numeric/date NULLs kept). The rebuilt `_next` view carries the CURRENT cadence `REFRESH EVERY 1 HOUR OFFSET 45 MINUTE` (000366), not 000347's 15 minutes. The migration is applied AFTER the resolve backfill (cutover step 5): the columns exist from plan 1's migration but are empty until the rebuild, so an earlier apply is harmless yet wastes one full refresh.
11. **Deploy note for the cutover plan:** the sensor keeps its name, so Dagster keeps its RUNNING state and cursor across the deploy; cutover step 1 must stop `se_company_info_field_value_sensor` BEFORE deploying this branch, or the first decision after the deploy launches the new asset against empty candidate tables.

## File structure

| File | Responsibility |
| --- | --- |
| `src/dagster_v3/defs/se_company/common.py` (modify) | `publish_with_stage` accepts a caller-supplied `client` |
| `src/dagster_v3/defs/se_company/fields/resolve.py` (create) | config, server-side client + parameter encoding, registry-statement loader, changed-company scan, batch loop, summary, the asset |
| `src/dagster_v3/defs/se_company/fields/parity.py` (create) | snapshot DDL, parity SQL, pure result builder, the asset check |
| `src/dagster_v3/defs/se_company/fields/jobs.py` (create) | `se_company_field_resolve_job`, `se_company_fields_job` |
| `src/dagster_v3/defs/se_company/fields/sensors.py` (create) | the moved field-value sensor, the candidate sensor factory + instance |
| `src/dagster_v3/defs/se_company/fields/schedules.py` (create) | `se_company_fields_weekly` |
| `src/dagster_v3/defs/se_company/info.py` (modify) | drop the sensor and the schedule (asset + jobs stay) |
| `src/dagster_v3/defs/common/clickhouse_checks.py` (modify) | leaf for the new asset |
| `src/dagster_v3/defs/sweden_company/companies_current.py` (modify) | the serving SELECT gains the eight columns |
| `corpscout/clickhouse/migrations/0003NN_corpscout_se_companies_serving_field_registry_columns.{up,down}.sql` (create) | staged swap of the serving MV |
| `tests/test_se_company_common.py` (modify) | `publish_with_stage` client test |
| `tests/test_se_company_field_resolve.py` (create) | SQL pinned as text, encoding, loader, FakeClient batch tests, Definitions wiring |
| `tests/test_se_company_field_sensors.py` (create) | both sensors and the schedule |
| `tests/test_se_company_field_parity.py` (create) | parity SQL and result |
| `tests/test_se_company_field_resolve_clickhouse_local.py` (create) | the executed scan / resolve / projection / parity harness |
| `tests/test_se_company_info.py` (modify) | sensor/schedule assertions move out |
| `tests/test_se_companies_serving_mv.py`, `tests/test_se_companies_serving_sql.py`, `tests/test_clickhouse_migrations.py` (modify) | serving drift pin, executable suite, ledger entry + content test |

---

### Task 1: `publish_with_stage` runs on a caller-supplied client

**Files:**
- Modify: `src/dagster_v3/defs/se_company/common.py:75-165` (`publish_with_stage`)
- Test: `tests/test_se_company_common.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `publish_with_stage(*, clickhouse: ClickhouseResource | None = None, target, insert_columns, rows=None, select_sql=None, select_parameters=None, invalid_condition, allow_shrink=False, new_versions_only=False, client: Any | None = None) -> PublishCounts`. Exactly one of `clickhouse` / `client`; with `client` every statement runs on it and no connection is opened. Every existing caller passes `clickhouse=` by keyword and is unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_common.py` (after `test_publish_with_stage_new_versions_only_counts_the_anti_join_before_inserting`):

```python
def test_publish_with_stage_runs_on_a_caller_supplied_client() -> None:
    """The field-registry resolve asset holds one server-side-params client for a whole
    run and passes it in: every statement must then go through THAT client -- no
    connection of its own -- and the staged SELECT's parameters must reach the driver
    untouched (the server binds `{company_ids:Array(String)}` from them)."""
    client = FakeClient(answers=[[(2, 0)], [(10,)], [(12,)]])  # validation, existing, final
    counts = publish_with_stage(
        client=client, target="se_company_info",
        insert_columns=("company_id", "legal_name"),
        select_sql="SELECT company_id, legal_name FROM corpscout.x WHERE company_id IN {company_ids:Array(String)}",
        select_parameters={"company_ids": "['5565200028']"},
        invalid_condition="trim(legal_name) = ''",
        new_versions_only=False,
    )
    assert counts == PublishCounts(staged=2, inserted=2, total=12)
    sql = [entry[0] for entry in client.executed]
    assert sql[0].startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_")
    assert sql[1].startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_")
    assert sql[1].endswith("WHERE company_id IN {company_ids:Array(String)}")
    assert client.executed[1][1] == {"company_ids": "['5565200028']"}
    assert any(s.startswith("INSERT INTO `corpscout`.`se_company_info` (company_id,") for s in sql)
    assert sql[-1].startswith("DROP TABLE IF EXISTS `corpscout`.`_tmp_se_company_info_")


def test_publish_with_stage_needs_exactly_one_of_resource_or_client() -> None:
    client = FakeClient(answers=[])
    with pytest.raises(ValueError, match="exactly one of clickhouse or client"):
        publish_with_stage(target="t", insert_columns=("a",), rows=[("x",)], invalid_condition="a = ''")
    with pytest.raises(ValueError, match="exactly one of clickhouse or client"):
        publish_with_stage(clickhouse=FakeClickhouse(client), client=client, target="t",
                           insert_columns=("a",), rows=[("x",)], invalid_condition="a = ''")
    assert client.executed == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_common.py -q -p no:warnings -k "caller_supplied or exactly_one_of"`
Expected: FAIL -- `TypeError: publish_with_stage() got an unexpected keyword argument 'client'` and `missing 1 required keyword-only argument: 'clickhouse'`.

- [ ] **Step 3: Implement**

In `src/dagster_v3/defs/se_company/common.py` add `from contextlib import nullcontext` to the imports, then replace the signature, the guard and the connection line of `publish_with_stage` (the body under `with connection as client:` is unchanged):

```python
def publish_with_stage(
    *,
    clickhouse: ClickhouseResource | None = None,
    target: str,
    insert_columns: Sequence[str],
    rows: Sequence[tuple[Any, ...]] | None = None,
    select_sql: str | None = None,
    select_parameters: Mapping[str, Any] | None = None,
    invalid_condition: str,
    allow_shrink: bool = False,
    new_versions_only: bool = False,
    anti_join_columns: Sequence[str] = ("company_id", "source_record_uid", "evidence_hash"),  # added by plan 2 Task 1; keep it
    client: Any | None = None,
) -> PublishCounts:
    """Stage -> validate -> insert -> drop stage; shrink-guard the published table.

    When ``new_versions_only`` is True the final copy is a left-anti-join on
    ``(company_id, source_record_uid, evidence_hash)`` against the target, so
    a version of a row already published with the same evidence is never
    re-inserted. The stage is created with ``CREATE TABLE stage AS target``,
    so the target's MATERIALIZED ``evidence_hash`` is computed on the stage
    by ClickHouse itself -- it is never re-expressed in Python.

    ``client`` is an already-open driver client to run every statement on,
    for a caller that holds one connection across many publishes (the field
    resolve asset, whose client binds server-side parameters); ``clickhouse``
    opens and closes one here. Exactly one of the two.
    """
    if (rows is None) == (select_sql is None):
        raise ValueError("publish_with_stage needs exactly one of rows or select_sql")
    if (clickhouse is None) == (client is None):
        raise ValueError("publish_with_stage needs exactly one of clickhouse or client")
    qualified_target = qualified(target)
    qualified_stage = qualified(f"_tmp_{target}_{uuid.uuid4().hex}")
    columns = _columns_sql(insert_columns)
    connection = nullcontext(client) if client is not None else clickhouse.get_connection()
    with connection as client:
```

- [ ] **Step 4: Run the whole common suite**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_common.py -q -p no:warnings`
Expected: PASS (the two new tests and every existing `publish_with_stage` test, whose calls pass `clickhouse=` by keyword).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/common.py tests/test_se_company_common.py
git commit -m "feat(se): publish_with_stage runs on a caller-supplied client

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: `resolve.py` foundations -- config, server-side client, parameter encoding, registry statements, changed-company scan

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/resolve.py`
- Test: `tests/test_se_company_field_resolve.py`

**Interfaces:**
- Consumes: `fields.tables.{SE_COMPANY_FIELD_REGISTRY, SE_COMPANY_FIELD_CANDIDATE, SE_COMPANY_FIELD, SE_COMPANY_INFO}`, `fields.registry.{INFO_REGISTRY, DatatypeRegistry, field_names}`, `dagster_clickhouse.resource.client_kwargs_from_resource_config`, `clickhouse_driver.Client`; tests also use `fields.sql.render_projection_sql` and `tests/se_company_ddl.declared_columns`.
- Produces (all in `resolve.py`): constants `DATABASE`, `GROUP_NAME`, `RESOLVE_ASSET`, `REGISTRY_ASSET`, `ARTIFACT_ASSETS`, `CANDIDATE_ASSETS`, `LLM_CANDIDATES_ASSET`, `PARITY_CHECK_NAME`, `SE_COMPANY_INFO_FIELD_VALUE`, `PROJECTION_FIELD`, `REGISTER_NAME_FIELD`, `REGISTER_NAME_SOURCES`, `EPOCH_SQL`, `WIDE_INVALID_CONDITION`, `SELECTION_REASONS`, `SELECTION_COLUMNS`, `AUTOMATED_RUN_CONFIG`; `class SECompanyFieldResolveConfig(dg.Config)`; `clickhouse_stamp(moment) -> str`; `open_resolve_client(clickhouse) -> Iterator[Client]` (context manager); `class ServerSideLiteral(text)` with `.text`, `__str__`, `__eq__`; `server_array(items) -> ServerSideLiteral`; `server_params(*, company_ids, **scalars) -> dict[str, object]`; `@dataclass(frozen=True) InsertHeader(table, columns, body)`; `split_insert_header(sql) -> InsertHeader`; `build_registry_statements_sql(registry) -> str`; `@dataclass(frozen=True) RegistryStatements(registry_version, resolve_sql: Mapping[str, str], projection_sql)`; `load_registry_statements(client, registry) -> RegistryStatements`; `build_changed_companies_sql(registry) -> str`; `build_batch_stats_sql() -> str`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_se_company_field_resolve.py`:

```python
"""The registry-driven resolve asset: the server-side parameter encoding, the
registry-statement loader, the changed-company scan (SQL pinned as text; executed in
test_se_company_field_resolve_clickhouse_local.py), the batch loop through the
scripted FakeClient, and the Definitions wiring."""

import re
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.fields import resolve
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_names
from dagster_v3.defs.se_company.fields.resolve import (
    EPOCH_SQL,
    PROJECTION_FIELD,
    SELECTION_COLUMNS,
    SELECTION_REASONS,
    SECompanyFieldResolveConfig,
    ServerSideLiteral,
    build_batch_stats_sql,
    build_changed_companies_sql,
    build_registry_statements_sql,
    clickhouse_stamp,
    load_registry_statements,
    open_resolve_client,
    server_array,
    server_params,
    split_insert_header,
)
from dagster_v3.defs.se_company.fields.sql import render_projection_sql
from tests.se_company_ddl import declared_columns
from tests.test_se_company_common import FakeClient

NOW = datetime(2026, 9, 2, 10, 0, 0, tzinfo=UTC)
HANDELSBANKEN = "5020077862"
OTHER_COMPANY = "5560125220"
SOLE_TRADER = "196408233412"
PUBLISHED_AT = f"ifNull(published.resolved_at, {EPOCH_SQL})"


def test_server_side_literals_are_escaped_exactly_once_for_the_driver() -> None:
    """clickhouse-driver's server_side_params path quotes a non-str value's str() as-is
    and escapes a str twice, so the array literal is handed over as a non-str whose
    str() carries ONE level of escaping (verified against 26.5, see the module doc)."""
    plain = server_array(["5020077862", "5560125220"])
    assert plain.text == "['5020077862','5560125220']"
    assert str(plain) == "[\\'5020077862\\',\\'5560125220\\']"
    tricky = server_array(["it's", "a\\b"])
    assert tricky.text == "['it\\'s','a\\\\b']"
    assert str(tricky) == "[\\'it\\\\\\'s\\',\\'a\\\\\\\\b\\']"
    assert server_array(()).text == "[]" and str(server_array(())) == "[]"
    assert server_array(["x"]) == ServerSideLiteral("['x']") and server_array(["x"]) != ServerSideLiteral("['y']")
    assert repr(ServerSideLiteral("['x']")) == "ServerSideLiteral(\"['x']\")"

    params = server_params(company_ids=[HANDELSBANKEN], field="legal_name", source_run_id="run",
                           resolved_at=NOW, page_size=20_000, all_companies=0)
    assert params == {"company_ids": ServerSideLiteral("['5020077862']"), "field": "legal_name",
                      "source_run_id": "run", "resolved_at": "2026-09-02 10:00:00.000",
                      "page_size": 20_000, "all_companies": 0}
    assert clickhouse_stamp(NOW) == "2026-09-02 10:00:00.000"


def test_open_resolve_client_builds_a_server_side_params_client_from_the_resource(monkeypatch) -> None:
    built: list[dict] = []

    class _Client:
        def __init__(self, **kwargs) -> None:
            built.append(kwargs)
            self.disconnected = False

        def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(resolve, "Client", _Client)
    resource = ClickhouseResource(host="ch.local", port=9440, user="u", password="p", database="corpscout",
                                  secure=True, settings={"max_execution_time": 600})
    with open_resolve_client(resource) as client:
        assert isinstance(client, _Client) and client.disconnected is False
    assert built == [{"host": "ch.local", "port": 9440, "user": "u", "password": "p", "database": "corpscout",
                      "secure": True, "settings": {"max_execution_time": 600, "server_side_params": True}}]
    assert client.disconnected is True


def test_split_insert_header_reads_the_projection_target_and_columns() -> None:
    header = split_insert_header(
        "INSERT INTO corpscout.se_company_info (\n    company_id, legal_name,\n    `status`\n)\n"
        "WITH x AS (SELECT 1)\nSELECT company_id, legal_name, status FROM x")
    assert header.table == "corpscout.se_company_info"
    assert header.columns == ("company_id", "legal_name", "status")
    assert header.body == "WITH x AS (SELECT 1)\nSELECT company_id, legal_name, status FROM x"
    with pytest.raises(ValueError, match="INSERT INTO"):
        split_insert_header("SELECT 1")


def test_the_projection_header_names_every_wide_column_in_ddl_order() -> None:
    """The stage INSERT binds the projection's SELECT positionally to the header's
    column list, so that list must be the deployed table minus its MATERIALIZED hash."""
    header = split_insert_header(render_projection_sql(INFO_REGISTRY))
    assert header.table == "corpscout.se_company_info"
    assert list(header.columns) == [c for c in declared_columns("se_company_info") if c != "evidence_set_hash"]


def test_changed_companies_sql_reads_candidates_decisions_published_and_versions() -> None:
    sql = build_changed_companies_sql(INFO_REGISTRY)
    assert sql.startswith("WITH current_registry AS (")
    # The registry table is the version authority: what a company was resolved WITH.
    assert "argMax(registry_version, version) AS registry_version" in sql
    assert "argMax(policy_version, version) AS policy_version" in sql
    assert f"WHERE datatype = 'info' AND country = 'SE' AND field != '{PROJECTION_FIELD}'" in sql
    # candidates replaces the old artifacts CTE: max(extracted_at) IS the version column,
    # so no FINAL; the register-name gate rides on the same aggregate.
    assert "SELECT company_id, max(extracted_at) AS latest_extracted_at," in sql
    assert "countIf(field = 'legal_name' AND source IN ('bolagsverket', 'scb')) > 0 AS has_register_name" in sql
    assert ("FROM corpscout.se_company_field_candidate\n"
            "    WHERE ({all_companies:UInt8} = 1 OR company_id IN {company_ids:Array(String)})") in sql
    assert "se_company_field_candidate FINAL" not in sql
    # The decisions CTE keeps its alias: the backoffice Pipeline page mirrors this SQL.
    assert ("SELECT company_id, max(created_at) AS latest_correction_at\n"
            "    FROM corpscout.se_company_info_field_value") in sql
    assert "FROM corpscout.se_company_info AS final FINAL" in sql
    assert ("FROM corpscout.se_company_field AS resolved FINAL\n"
            "    INNER JOIN current_registry ON current_registry.field = resolved.field") in sql
    assert ("toUInt8(countIf(resolved.registry_version != current_registry.registry_version\n"
            "                        OR resolved.policy_version != current_registry.policy_version) > 0)"
            " AS version_changed") in sql
    assert "WHERE candidates.has_register_name\n  AND (" in sql
    assert "ifNull(published.company_id, '') = ''\n     OR " in sql
    assert (f"OR ({{resolve_all:UInt8}} = 1 AND {PUBLISHED_AT} < "
            "parseDateTime64BestEffort({resolve_all_before:String}, 3, 'UTC'))") in sql
    assert f"OR candidates.latest_extracted_at > {PUBLISHED_AT}" in sql
    assert f"OR ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT}" in sql
    assert "OR ifNull(versions.version_changed, 0) = 1" in sql
    # Every LEFT JOIN miss goes through ifNull (join_use_nulls = 1 safety).
    assert "> published.resolved_at" not in sql and "ledger.latest_correction_at >" not in sql
    assert "versions.version_changed = 1" not in sql
    assert "AND candidates.company_id > {after_company_id:String}" in sql
    assert sql.endswith("ORDER BY candidates.company_id\nLIMIT {page_size:UInt32}")
    # Server-side placeholders only; no model terms survive from the old scan.
    assert "%(" not in sql and "pending_model" not in sql and "multi_source" not in sql


def test_the_scan_projects_why_each_company_was_selected() -> None:
    sql = build_changed_companies_sql(INFO_REGISTRY)
    assert SELECTION_REASONS == ("never_published", "new_candidates", "decision_pending", "version_changed")
    assert SELECTION_COLUMNS == ("company_id", *SELECTION_REASONS)
    projected = re.search(r"SELECT candidates\.company_id AS company_id,\n(.*?)\nFROM candidates", sql, re.DOTALL)
    assert projected is not None
    assert [line.split(" AS ")[-1].strip() for line in projected.group(1).split(",\n")] == list(SELECTION_REASONS)
    assert "ifNull(published.company_id, '') = '' AS never_published" in sql
    assert f"candidates.latest_extracted_at > {PUBLISHED_AT} AS new_candidates" in sql
    assert f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT} AS decision_pending" in sql
    assert "ifNull(versions.version_changed, 0) = 1 AS version_changed" in sql


PROJECTION_STATEMENT = (
    "INSERT INTO corpscout.se_company_info (company_id, legal_name, source_record_uids)\n"
    "SELECT company_id, value AS legal_name, [source_record_uid] AS source_record_uids\n"
    "FROM corpscout.se_company_field FINAL\n"
    "WHERE field = 'legal_name' AND company_id IN {company_ids:Array(String)}")


def _registry_rows(version: str = INFO_REGISTRY.version, *, drop: str = "") -> list[tuple]:
    """Scripted answer for build_registry_statements_sql: (field, resolve_sql,
    policy_version, registry_version), alphabetical like the real ORDER BY. The fake
    statements name their field in the SQL text so a test can read the order back."""
    rows = [(name, f"INSERT INTO corpscout.se_company_field SELECT '{name}' AS field, "
                   "arrayJoin({company_ids:Array(String)}) AS company_id, {field:String} AS f, "
                   "{source_run_id:String} AS source_run_id, {resolved_at:DateTime64(3, 'UTC')} AS resolved_at",
             "source_precedence-v1", version)
            for name in field_names(INFO_REGISTRY) if name != drop]
    rows.append((PROJECTION_FIELD, PROJECTION_STATEMENT, "", version))
    return sorted(rows)


def test_registry_statements_sql_and_loader_refuse_a_stale_or_partial_export() -> None:
    sql = build_registry_statements_sql(INFO_REGISTRY)
    assert sql == (
        "SELECT field,\n"
        "    argMax(resolve_sql, version) AS resolve_sql,\n"
        "    argMax(policy_version, version) AS policy_version,\n"
        "    argMax(registry_version, version) AS registry_version\n"
        "FROM corpscout.se_company_field_registry\n"
        "WHERE datatype = 'info' AND country = 'SE'\n"
        "GROUP BY field\n"
        "ORDER BY field")

    statements = load_registry_statements(FakeClient(answers=[_registry_rows()]), INFO_REGISTRY)
    assert statements.registry_version == INFO_REGISTRY.version
    assert list(statements.resolve_sql) == list(field_names(INFO_REGISTRY))  # registry order, not alphabetical
    assert statements.projection_sql == PROJECTION_STATEMENT

    with pytest.raises(ValueError, match="materialize se_company_field_registry_clickhouse first"):
        load_registry_statements(FakeClient(answers=[_registry_rows(version="se-info-v0")]), INFO_REGISTRY)
    with pytest.raises(ValueError, match=r"no row for \['website'\]"):
        load_registry_statements(FakeClient(answers=[_registry_rows(drop="website")]), INFO_REGISTRY)
    with pytest.raises(ValueError, match="no row for"):
        load_registry_statements(FakeClient(answers=[[]]), INFO_REGISTRY)


def test_batch_stats_sql_counts_this_runs_rows_per_field_source_and_decision() -> None:
    assert build_batch_stats_sql() == (
        "SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision, count() AS rows\n"
        "FROM corpscout.se_company_field\n"
        "WHERE source_run_id = {source_run_id:String} AND company_id IN {company_ids:Array(String)}\n"
        "GROUP BY field, source, from_decision\n"
        "ORDER BY field, source, from_decision")


def test_the_config_defaults_to_a_preview_and_caps_the_batch() -> None:
    config = SECompanyFieldResolveConfig()
    assert config.execute is False and config.company_ids == [] and config.fields == []
    assert config.max_companies is None and config.company_batch_size == 20_000
    assert config.resolve_all is False and config.resolve_all_before is None
    with pytest.raises(ValueError):
        SECompanyFieldResolveConfig(company_batch_size=20_001)
    with pytest.raises(ValueError):
        SECompanyFieldResolveConfig(max_companies=0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py -q -p no:warnings`
Expected: FAIL at import -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.resolve'`.

- [ ] **Step 3: Create `resolve.py` with the foundations**

Create `src/dagster_v3/defs/se_company/fields/resolve.py`:

```python
"""Registry-driven resolve of Swedish company fields, and the wide re-pivot.

Replaces se_company_info_clickhouse (info.py). Every statement this asset executes is
read from corpscout.se_company_field_registry -- the per-field ``resolve_sql`` and the
``field = '*'`` projection -- never rendered here: the export is the contract shared with
the backoffice, which runs the same statements for one company after a decision.

Per run: select the company set (config ``company_ids``, the changed-company scan, or
every company under ``resolve_all``); per batch of ``company_batch_size`` run each
field's statement in registry order, then the projection through publish_with_stage
(stage -> validate -> insert), then one counts query for the metadata.

Parameters: the statements carry ClickHouse ``{name:Type}`` placeholders, bound
SERVER-SIDE. clickhouse-driver ships them over the native protocol only from a Client
built with ``server_side_params=True`` (a client-level setting: ``open_resolve_client``
builds one from the resource's own fields). Two driver quirks, verified 2026-09-02
against 26.5 with the pinned 0.2.10: a Python list is double-quoted on the wire and a
pre-rendered str is double-escaped, so Array(String) values travel as ServerSideLiteral
-- a non-str the driver quotes without escaping, whose str() is the literal escaped
once; and a datetime is converted to the server timezone at second precision, so
``resolved_at`` travels as its millisecond text.

Gate: ``execute: true`` in the run config; a bare "Materialize" click is a preview that
runs the scan and reports what a real run would select, writing nothing.

Assets
  se_company_field_resolved_clickhouse -> corpscout.se_company_field, corpscout.se_company_info
"""

import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from clickhouse_driver import Client
from dagster_clickhouse import ClickhouseResource
from dagster_clickhouse.resource import client_kwargs_from_resource_config
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids, publish_with_stage
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, DatatypeRegistry, field_names
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD,
    SE_COMPANY_FIELD_CANDIDATE,
    SE_COMPANY_FIELD_REGISTRY,
    SE_COMPANY_INFO,
)

DATABASE = "corpscout"
GROUP_NAME = "se_company_fields"
RESOLVE_ASSET = "se_company_field_resolved_clickhouse"
REGISTRY_ASSET = "se_company_field_registry_clickhouse"
# The three per-source artifact assets the old se_company_info_job carried; the weekly
# field job carries them from now on (their freshness leaves must keep a schedule).
ARTIFACT_ASSETS = ("se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
                   "se_company_info_wikidata_clickhouse")
CANDIDATE_ASSETS = tuple(
    f"se_company_field_candidates_{source}"
    for source in ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm"))
LLM_CANDIDATES_ASSET = "se_company_field_candidates_llm"
PARITY_CHECK_NAME = "se_company_field_parity_check"
# The decisions table (000371). Named here rather than imported from info.py, which the
# cutover plan deletes.
SE_COMPANY_INFO_FIELD_VALUE = "se_company_info_field_value"
# The registry export's extra row carrying the wide projection statement (spec 4.3).
PROJECTION_FIELD = "*"
# Spec 8.3: a company without a legal name from a register is not published -- and,
# here, not resolved either (the scan's WHERE), so it never churns the long table.
REGISTER_NAME_FIELD = "legal_name"
REGISTER_NAME_SOURCES = ("bolagsverket", "scb")
# A LEFT JOIN miss reads as this instant, not as a bare NULL comparison.
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"
# The wide table's own CHECKs, spelled for the stage validation exactly as info.py did.
WIDE_INVALID_CONDITION = "trim(legal_name) = '' OR empty(source_record_uids)"
# Why the scan picked a company; overlapping counters, never a partition (a never-
# published company also has candidates newer than its epoch resolved_at).
SELECTION_REASONS = ("never_published", "new_candidates", "decision_pending", "version_changed")
SELECTION_COLUMNS = ("company_id", *SELECTION_REASONS)
# What the sensors and the schedule send: an automated run must never be a preview.
AUTOMATED_RUN_CONFIG: dict[str, Any] = {"execute": True}


class SECompanyFieldResolveConfig(dg.Config):
    # False = preview: run the scan, report the selection, write nothing.
    execute: bool = False
    company_ids: list[str] = Field(default_factory=list)
    # None = unbounded (the weekly run). A capped resolve_all pass must give
    # resolve_all_before, see below.
    max_companies: int | None = Field(default=None, ge=1)
    # Scan page size and the resolve/projection batch. 20,000 ids are ~300 KB as one
    # Array(String) parameter, which the server takes without a settings change.
    company_batch_size: int = Field(default=20_000, ge=1, le=20_000)
    # True = re-resolve every in-scope company although nothing moved (registry or
    # policy edits are caught by version_changed; this is for everything else).
    resolve_all: bool = False
    # ISO-8601 UTC cutoff for resolve_all: only companies whose published resolved_at
    # is OLDER are selected, so a pass split over several capped runs carries on where
    # it stopped instead of re-selecting the same first slice. None = the run's own
    # instant, i.e. "this pass is one run".
    resolve_all_before: str | None = None
    # Registry field names to resolve; empty = every field. The projection always
    # re-pivots every field from the long table.
    fields: list[str] = Field(default_factory=list)


def clickhouse_stamp(moment: datetime) -> str:
    """``moment`` as the millisecond text ClickHouse parses for a DateTime64(3)."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@contextmanager
def open_resolve_client(clickhouse: ClickhouseResource) -> Iterator[Client]:
    """A driver client on the resource's own connection details, with server-side
    parameters on. ``server_side_params`` is a client-level setting (it decides whether
    ``execute`` substitutes ``%(name)s`` client-side or ships the params dict to the
    server), which is why the resource's ``get_connection`` client cannot be reused."""
    kwargs = client_kwargs_from_resource_config({
        "host": clickhouse.host, "port": clickhouse.port, "user": clickhouse.user,
        "password": clickhouse.password, "database": clickhouse.database, "secure": clickhouse.secure,
        "settings": {**dict(clickhouse.settings), "server_side_params": True}})
    client = Client(**kwargs)
    try:
        yield client
    finally:
        client.disconnect()


def _quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


class ServerSideLiteral:
    """An Array(String) parameter for the driver's server-side path.

    ``text`` is the ClickHouse literal (``['a','b']``). The driver quotes a non-str
    value's ``str()`` verbatim -- no escaping -- and the server unquotes that once, so
    ``__str__`` is the literal escaped exactly once. A plain str would be escaped twice
    and a list quoted per element; both are rejected by 26.5.
    """

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text.replace("\\", "\\\\").replace("'", "\\'")

    def __repr__(self) -> str:
        return f"ServerSideLiteral({self.text!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ServerSideLiteral) and other.text == self.text


def server_array(items: Iterable[str]) -> ServerSideLiteral:
    return ServerSideLiteral("[" + ",".join(_quoted(str(item)) for item in items) + "]")


def server_params(*, company_ids: Sequence[str], **scalars: object) -> dict[str, object]:
    """The params dict for one statement: ``company_ids`` as an Array(String) literal, a
    datetime as its millisecond text, str and int values as they are."""
    params: dict[str, object] = {"company_ids": server_array(company_ids)}
    for name, value in scalars.items():
        params[name] = clickhouse_stamp(value) if isinstance(value, datetime) else value
    return params


@dataclass(frozen=True)
class InsertHeader:
    table: str
    columns: tuple[str, ...]
    body: str


_INSERT_HEADER = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<table>[A-Za-z_`][\w.`]*)\s*\((?P<columns>[^)]*)\)\s*(?P<body>.+)$",
    re.DOTALL)


def split_insert_header(sql: str) -> InsertHeader:
    """The projection statement is ``INSERT INTO <table> (<columns>) <select>``; the
    stage publish needs the three parts apart (it inserts the SELECT into the stage
    under the header's column list, then copies the stage into the target)."""
    match = _INSERT_HEADER.match(sql)
    if match is None:
        raise ValueError(f"Expected 'INSERT INTO <table> (<columns>) <select>', got: {sql[:120]!r}")
    columns = tuple(c.strip().strip("`") for c in match.group("columns").split(",") if c.strip())
    return InsertHeader(table=match.group("table").replace("`", ""), columns=columns,
                        body=match.group("body").strip())


def build_registry_statements_sql(registry: DatatypeRegistry) -> str:
    return f"""SELECT field,
    argMax(resolve_sql, version) AS resolve_sql,
    argMax(policy_version, version) AS policy_version,
    argMax(registry_version, version) AS registry_version
FROM {SE_COMPANY_FIELD_REGISTRY}
WHERE datatype = '{registry.datatype}' AND country = '{registry.country}'
GROUP BY field
ORDER BY field"""


@dataclass(frozen=True)
class RegistryStatements:
    registry_version: str
    resolve_sql: Mapping[str, str]  # field -> statement, in registry order
    projection_sql: str


def load_registry_statements(client: Any, registry: DatatypeRegistry) -> RegistryStatements:
    """The statements the export table holds for ``registry`` -- or a refusal.

    A missing field row or a version other than the code's means the export asset has
    not run since the registry changed; running the old statements would stamp rows
    with the old version and re-select them on every scan."""
    rows = client.execute(build_registry_statements_sql(registry))
    by_field = {str(row[0]): (str(row[1]), str(row[3])) for row in rows}
    expected = [*field_names(registry), PROJECTION_FIELD]
    missing = [name for name in expected if name not in by_field]
    if missing:
        raise ValueError(
            f"{SE_COMPANY_FIELD_REGISTRY} has no row for {missing}: materialize {REGISTRY_ASSET} first")
    stale = [name for name in expected if by_field[name][1] != registry.version]
    if stale:
        raise ValueError(
            f"{SE_COMPANY_FIELD_REGISTRY} is at {by_field[stale[0]][1]!r} for {stale} but the code is at "
            f"{registry.version!r}: materialize {REGISTRY_ASSET} first")
    return RegistryStatements(
        registry_version=registry.version,
        resolve_sql={name: by_field[name][0] for name in field_names(registry)},
        projection_sql=by_field[PROJECTION_FIELD][0])


SCOPE_SQL = "({all_companies:UInt8} = 1 OR company_id IN {company_ids:Array(String)})"
FINAL_SCOPE_SQL = "({all_companies:UInt8} = 1 OR final.company_id IN {company_ids:Array(String)})"
RESOLVED_SCOPE_SQL = "({all_companies:UInt8} = 1 OR resolved.company_id IN {company_ids:Array(String)})"
RESOLVE_ALL_SQL = ("({resolve_all:UInt8} = 1 AND ifNull(published.resolved_at, " + EPOCH_SQL
                   + ") < parseDateTime64BestEffort({resolve_all_before:String}, 3, 'UTC'))")


def build_changed_companies_sql(registry: DatatypeRegistry) -> str:
    """Companies to resolve again: never published, a candidate extracted after the
    published resolution, a decision created after it, or resolved rows stamped with a
    registry/policy version the registry table no longer carries (spec 8.4).

    The old info.py scan with ``artifacts`` replaced by ``candidates`` (``max(extracted_at)``
    is the candidate table's version column, so no FINAL), the ``ledger`` CTE unchanged --
    its ``latest_correction_at`` alias is read back by name by the backoffice Pipeline
    page -- and a ``versions`` CTE comparing every resolved row with the registry
    table's current versions (FINAL: an older duplicate version would otherwise flag the
    company forever). Fields no longer exported are dropped by the INNER JOIN.

    ``has_register_name`` is spec 8.3's "SCB row mandatory" rule as a pre-filter: a
    company without a bolagsverket/scb legal-name candidate is neither resolved nor
    published, so it is not re-selected every week either.

    ``resolve_all`` and its cutoff, the keyset paging and the projected reason flags
    behave exactly as in info.py: one page per call, ``after_company_id`` resumes it,
    the reasons are the WHERE's own expressions spelled twice from one constant. Every
    ``{name:Type}`` is a server-side parameter bound from ``server_params``.
    """
    published_at = f"ifNull(published.resolved_at, {EPOCH_SQL})"
    register_sources = ", ".join(f"'{source}'" for source in REGISTER_NAME_SOURCES)
    reasons = ",\n    ".join((
        "ifNull(published.company_id, '') = '' AS never_published",
        f"candidates.latest_extracted_at > {published_at} AS new_candidates",
        f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {published_at} AS decision_pending",
        "ifNull(versions.version_changed, 0) = 1 AS version_changed",
    ))
    return f"""WITH current_registry AS (
    SELECT field,
        argMax(registry_version, version) AS registry_version,
        argMax(policy_version, version) AS policy_version
    FROM {SE_COMPANY_FIELD_REGISTRY}
    WHERE datatype = '{registry.datatype}' AND country = '{registry.country}' AND field != '{PROJECTION_FIELD}'
    GROUP BY field
),
candidates AS (
    SELECT company_id, max(extracted_at) AS latest_extracted_at,
        countIf(field = '{REGISTER_NAME_FIELD}' AND source IN ({register_sources})) > 0 AS has_register_name
    FROM {SE_COMPANY_FIELD_CANDIDATE}
    WHERE {SCOPE_SQL}
    GROUP BY company_id
),
ledger AS (
    SELECT company_id, max(created_at) AS latest_correction_at
    FROM {DATABASE}.{SE_COMPANY_INFO_FIELD_VALUE}
    WHERE {SCOPE_SQL}
    GROUP BY company_id
),
published AS (
    SELECT final.company_id AS company_id, final.resolved_at AS resolved_at
    FROM {SE_COMPANY_INFO} AS final FINAL
    WHERE {FINAL_SCOPE_SQL}
),
versions AS (
    SELECT resolved.company_id AS company_id,
        toUInt8(countIf(resolved.registry_version != current_registry.registry_version
                        OR resolved.policy_version != current_registry.policy_version) > 0) AS version_changed
    FROM {SE_COMPANY_FIELD} AS resolved FINAL
    INNER JOIN current_registry ON current_registry.field = resolved.field
    WHERE {RESOLVED_SCOPE_SQL}
    GROUP BY resolved.company_id
)
SELECT candidates.company_id AS company_id,
    {reasons}
FROM candidates
LEFT JOIN published ON published.company_id = candidates.company_id
LEFT JOIN ledger ON ledger.company_id = candidates.company_id
LEFT JOIN versions ON versions.company_id = candidates.company_id
WHERE candidates.has_register_name
  AND (
        ifNull(published.company_id, '') = ''
     OR {RESOLVE_ALL_SQL}
     OR candidates.latest_extracted_at > {published_at}
     OR ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {published_at}
     OR ifNull(versions.version_changed, 0) = 1
      )
  AND candidates.company_id > {{after_company_id:String}}
ORDER BY candidates.company_id
LIMIT {{page_size:UInt32}}"""


def build_batch_stats_sql() -> str:
    """Rows this run wrote for a batch, per field, source and decision flag. No FINAL:
    a company is resolved once per run, so this run's rows are unique per (company,
    field) already."""
    return f"""SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision, count() AS rows
FROM {SE_COMPANY_FIELD}
WHERE source_run_id = {{source_run_id:String}} AND company_id IN {{company_ids:Array(String)}}
GROUP BY field, source, from_decision
ORDER BY field, source, from_decision"""
```

(The imports `defaultdict`, `asdict`, `dc_field`, `UTC`, `assert_clickhouse_tables_exist`, `normalized_se_company_ids`, `publish_with_stage`, `INFO_REGISTRY` are used by Task 3's additions to this same file; an unused-import warning until then is expected.)

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py -q -p no:warnings`
Expected: PASS (9 tests). If `test_the_projection_header_names_every_wide_column_in_ddl_order` fails, the failure names a plan-2 contract gap (the projection header must list the deployed columns minus `evidence_set_hash`): report it, do not edit `fields/sql.py`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/fields/resolve.py tests/test_se_company_field_resolve.py
git commit -m "feat(se): field resolve foundations -- scan, server-side params, registry statements

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: the batch loop, the summary and the asset

**Files:**
- Modify: `src/dagster_v3/defs/se_company/fields/resolve.py` (append)
- Test: `tests/test_se_company_field_resolve.py` (append)

**Interfaces:**
- Consumes: Task 2's builders and `server_params`; `publish_with_stage(client=...)` from Task 1; `FakeClient` from `tests/test_se_company_common.py`.
- Produces: `@dataclass(frozen=True) FieldStats(rows: int, from_decision: int, per_source: dict[str, int], no_row: int)`; `@dataclass ResolveSummary(companies_selected, companies_published, per_field: dict[str, FieldStats], per_reason: dict[str, int], stopped_at_cap: bool, preview: bool, registry_version: str, source_run_id: str, company_scope: tuple[str, ...])` with `.metadata() -> dict[str, Any]`; `materialize_se_company_fields(context, client, config, *, registry, now) -> ResolveSummary` (`context` needs only `.run_id` and `.log.info`; `client` is the server-side client); asset `se_company_field_resolved_clickhouse`; `defs = dg.Definitions(assets=[...])` (jobs come in Task 5).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_field_resolve.py`:

```python
# --- the batch loop -------------------------------------------------------------------

from dagster_v3.defs.se_company.fields.resolve import (  # noqa: E402
    RESOLVE_ASSET,
    FieldStats,
    ResolveSummary,
    materialize_se_company_fields,
)

IDS = ServerSideLiteral("['5020077862']")
RESOLVED_AT = "2026-09-02 10:00:00.000"
PUBLISH_ANSWERS = [[(1, 0)], [(0,)], [(1,)]]  # stage validation, existing count, final count


def _context(run_id: str = "run") -> SimpleNamespace:
    """What materialize_se_company_fields reads off the asset context."""
    logged: list[tuple] = []
    return SimpleNamespace(run_id=run_id, log=SimpleNamespace(info=lambda *args: logged.append(args)),
                           logged=logged)


def _selected(company_id: str, **flags: int) -> tuple:
    unknown = set(flags) - set(SELECTION_REASONS)
    assert not unknown, f"not scan reasons: {sorted(unknown)}"
    return (company_id, *(int(flags.get(name, 0)) for name in SELECTION_REASONS))


def _scans(client: FakeClient) -> list[tuple[str, dict]]:
    return [(sql, params) for sql, params in client.executed if sql.startswith("WITH current_registry AS (")]


def _resolve_inserts(client: FakeClient) -> list[tuple[str, dict]]:
    return [(sql, params) for sql, params in client.executed
            if sql.startswith("INSERT INTO corpscout.se_company_field ")]


def test_a_batch_runs_every_field_statement_in_registry_order_then_the_projection_then_the_counts() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(HANDELSBANKEN, never_published=1, new_candidates=1)],
        *PUBLISH_ANSWERS,
        [("description_sv", "reviewer", 1, 1), ("legal_name", "bolagsverket", 0, 1)],  # batch stats
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(execute=True, company_batch_size=2),
        registry=INFO_REGISTRY, now=NOW)

    statements = [sql for sql, _ in client.executed]
    inserts = _resolve_inserts(client)
    assert [re.search(r"SELECT '([a-z_]+)' AS field", sql).group(1) for sql, _ in inserts] == list(field_names(INFO_REGISTRY))
    for name, (sql, params) in zip(field_names(INFO_REGISTRY), inserts, strict=True):
        # The statement text reaches the server untouched; the values travel beside it.
        assert "{company_ids:Array(String)}" in sql and "{resolved_at:DateTime64(3, 'UTC')}" in sql
        assert params == {"company_ids": IDS, "field": name, "source_run_id": "run", "resolved_at": RESOLVED_AT}
    # All fields, THEN the projection through the stage, THEN the counts.
    stage = next(i for i, sql in enumerate(statements) if sql.startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_"))
    assert stage > statements.index(inserts[-1][0])
    stage_sql, stage_params = client.executed[stage + 1]
    assert stage_sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_")
    assert "(company_id,\n    legal_name,\n    source_record_uids)\n" in stage_sql
    assert stage_sql.endswith("WHERE field = 'legal_name' AND company_id IN {company_ids:Array(String)}")
    assert "INSERT INTO corpscout.se_company_info" not in stage_sql  # the header was split off
    assert stage_params == {"company_ids": IDS}
    assert "countIf(trim(legal_name) = '' OR empty(source_record_uids))" in statements[stage + 2]
    assert any(sql.startswith("INSERT INTO `corpscout`.`se_company_info` (company_id,") for sql in statements)
    drop = next(i for i, sql in enumerate(statements) if sql.startswith("DROP TABLE IF EXISTS"))
    stats_sql, stats_params = client.executed[-1]
    assert len(statements) - 1 > drop
    assert stats_sql.startswith("SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision")
    assert stats_params == {"company_ids": IDS, "source_run_id": "run"}
    assert all(settings is None for settings in client.settings_calls)

    assert summary.companies_selected == 1 and summary.companies_published == 1
    assert summary.per_reason == {"never_published": 1, "new_candidates": 1, "decision_pending": 0, "version_changed": 0}
    assert summary.per_field["legal_name"] == FieldStats(rows=1, from_decision=0, per_source={"bolagsverket": 1}, no_row=0)
    assert summary.per_field["description_sv"] == FieldStats(rows=1, from_decision=1, per_source={"reviewer": 1}, no_row=0)
    assert summary.per_field["website"] == FieldStats(rows=0, from_decision=0, per_source={}, no_row=1)
    assert set(summary.per_field) == set(field_names(INFO_REGISTRY))
    assert summary.preview is False and summary.stopped_at_cap is False
    assert summary.registry_version == INFO_REGISTRY.version and summary.source_run_id == "run"


def test_a_preview_scans_everything_and_writes_nothing() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(HANDELSBANKEN, never_published=1, new_candidates=1, decision_pending=1),
         _selected(OTHER_COMPANY, version_changed=1)],
        [],  # page 2: exhausted
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(company_batch_size=2), registry=INFO_REGISTRY, now=NOW)

    assert all(sql.lstrip().upper().startswith(("SELECT", "WITH")) for sql, _ in client.executed)
    assert len(_scans(client)) == 2 and _resolve_inserts(client) == []
    assert _scans(client)[0][1] == {"company_ids": ServerSideLiteral("[]"), "all_companies": 1, "resolve_all": 0,
                                    "resolve_all_before": RESOLVED_AT, "after_company_id": "", "page_size": 2}
    assert _scans(client)[1][1]["after_company_id"] == OTHER_COMPANY
    assert summary.preview is True and summary.companies_selected == 2 and summary.companies_published == 0
    assert summary.per_field == {}
    assert summary.per_reason == {"never_published": 1, "new_candidates": 1, "decision_pending": 1, "version_changed": 1}
    metadata = summary.metadata()
    assert metadata["preview"] is True and metadata["companies_selected"] == 2
    assert {reason: metadata[reason] for reason in SELECTION_REASONS} == summary.per_reason
    assert metadata["company_scope"] == [] and metadata["registry_version"] == INFO_REGISTRY.version
    assert isinstance(metadata["per_field"], dg.JsonMetadataValue)


def test_the_scan_is_paged_by_keyset_and_stops_on_a_short_page_or_at_the_cap() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(OTHER_COMPANY), _selected(HANDELSBANKEN)],  # page 1: full
        [_selected("5567890123")],                             # page 2: short -> stop
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(company_batch_size=2), registry=INFO_REGISTRY, now=NOW)
    scans = _scans(client)
    assert len(scans) == 2 and summary.companies_selected == 3 and summary.stopped_at_cap is False
    assert [params["after_company_id"] for _, params in scans] == ["", HANDELSBANKEN]
    assert [params["page_size"] for _, params in scans] == [2, 2]

    capped = FakeClient(answers=[_registry_rows(), [_selected(HANDELSBANKEN)]])
    context = _context()
    summary = materialize_se_company_fields(
        context, capped, SECompanyFieldResolveConfig(company_batch_size=1, max_companies=1),
        registry=INFO_REGISTRY, now=NOW)
    assert summary.stopped_at_cap is True and len(_scans(capped)) == 1
    assert any("max_companies cap" in str(entry[0]) for entry in context.logged)


def test_resolve_all_binds_its_cutoff_and_an_explicit_scope_is_chunked() -> None:
    def _first_scan_params(config: SECompanyFieldResolveConfig) -> dict:
        client = FakeClient(answers=[_registry_rows(), []])
        materialize_se_company_fields(_context(), client, config, registry=INFO_REGISTRY, now=NOW)
        return _scans(client)[0][1]

    # Always bound, resolve_all or not -- parseDateTime64BestEffort('') would be an error.
    assert _first_scan_params(SECompanyFieldResolveConfig())["resolve_all"] == 0
    assert _first_scan_params(SECompanyFieldResolveConfig())["resolve_all_before"] == RESOLVED_AT
    on = _first_scan_params(SECompanyFieldResolveConfig(resolve_all=True))
    assert on["resolve_all"] == 1 and on["resolve_all_before"] == RESOLVED_AT
    explicit = _first_scan_params(SECompanyFieldResolveConfig(resolve_all=True, resolve_all_before="2026-08-23 18:30:00"))
    assert explicit["resolve_all_before"] == "2026-08-23 18:30:00"

    scope = [f"55600000{index:02d}" for index in range(7)]
    chunked = FakeClient(answers=[_registry_rows(), [], [], []])  # one empty scan per chunk
    summary = materialize_se_company_fields(
        _context(), chunked, SECompanyFieldResolveConfig(company_ids=scope, company_batch_size=3),
        registry=INFO_REGISTRY, now=NOW)
    scans = _scans(chunked)
    assert [params["company_ids"] for _, params in scans] == [
        server_array(scope[0:3]), server_array(scope[3:6]), server_array(scope[6:7])]
    assert all(params["all_companies"] == 0 and params["after_company_id"] == "" for _, params in scans)
    assert summary.company_scope == tuple(scope)
    # A twelve-digit sole-trader id is a valid scope.
    materialize_se_company_fields(
        _context(), FakeClient(answers=[_registry_rows(), []]),
        SECompanyFieldResolveConfig(company_ids=[SOLE_TRADER]), registry=INFO_REGISTRY, now=NOW)


def test_a_fields_subset_runs_only_those_statements_and_still_projects() -> None:
    client = FakeClient(answers=[
        _registry_rows(), [_selected(HANDELSBANKEN)], *PUBLISH_ANSWERS,
        [("website", "domains", 0, 1)],
    ])
    summary = materialize_se_company_fields(
        _context(), client,
        SECompanyFieldResolveConfig(execute=True, fields=["website", "legal_name"], company_batch_size=2),
        registry=INFO_REGISTRY, now=NOW)
    names = [re.search(r"SELECT '([a-z_]+)' AS field", sql).group(1) for sql, _ in _resolve_inserts(client)]
    assert names == [name for name in field_names(INFO_REGISTRY) if name in {"website", "legal_name"}]  # registry order
    assert any(sql.startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_") for sql, _ in client.executed)
    assert set(summary.per_field) == {"website", "legal_name"}
    assert summary.per_field["legal_name"] == FieldStats(rows=0, from_decision=0, per_source={}, no_row=1)

    with pytest.raises(ValueError, match=r"Not registry fields: \['bogus'\]"):
        materialize_se_company_fields(
            _context(), FakeClient(answers=[]), SECompanyFieldResolveConfig(fields=["bogus"]),
            registry=INFO_REGISTRY, now=NOW)


def test_a_stale_registry_export_stops_the_run_before_the_scan() -> None:
    client = FakeClient(answers=[_registry_rows(version="se-info-v0")])
    with pytest.raises(ValueError, match="materialize se_company_field_registry_clickhouse first"):
        materialize_se_company_fields(
            _context(), client, SECompanyFieldResolveConfig(execute=True), registry=INFO_REGISTRY, now=NOW)
    assert _scans(client) == []


def test_the_asset_is_declared_with_its_deps_group_and_tables() -> None:
    from dagster_v3.defs.se_company.fields.resolve import (
        CANDIDATE_ASSETS,
        REGISTRY_ASSET,
        se_company_field_resolved_clickhouse,
    )

    assert se_company_field_resolved_clickhouse.key == dg.AssetKey(RESOLVE_ASSET)
    spec = se_company_field_resolved_clickhouse.specs_by_key[dg.AssetKey(RESOLVE_ASSET)]
    assert spec.group_name == "se_company_fields"
    assert {dep.asset_key for dep in spec.deps} == {dg.AssetKey(REGISTRY_ASSET), *(dg.AssetKey(n) for n in CANDIDATE_ASSETS)}
    assert spec.metadata["table"] == "corpscout.se_company_field"
    assert spec.metadata["wide_table"] == "corpscout.se_company_info"
    assert spec.kinds == {"clickhouse", "python"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py -q -p no:warnings`
Expected: FAIL at import -- `ImportError: cannot import name 'FieldStats'`.

- [ ] **Step 3: Append the loop, the summary and the asset to `resolve.py`**

```python
@dataclass(frozen=True)
class FieldStats:
    rows: int
    from_decision: int
    per_source: dict[str, int]
    no_row: int


class _FieldCounters:
    """Accumulates build_batch_stats_sql rows across batches."""

    def __init__(self) -> None:
        self.rows: dict[str, int] = defaultdict(int)
        self.from_decision: dict[str, int] = defaultdict(int)
        self.per_source: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.companies = 0

    def add(self, rows: Sequence[Sequence[Any]], *, companies: int) -> None:
        self.companies += companies
        for field_name, source, from_decision, count in rows:
            self.rows[str(field_name)] += int(count)
            self.per_source[str(field_name)][str(source)] += int(count)
            if int(from_decision):
                self.from_decision[str(field_name)] += int(count)

    def stats(self, fields: Sequence[str]) -> dict[str, FieldStats]:
        return {
            name: FieldStats(rows=self.rows[name], from_decision=self.from_decision[name],
                             per_source=dict(self.per_source[name]), no_row=self.companies - self.rows[name])
            for name in fields
        }


@dataclass
class ResolveSummary:
    companies_selected: int = 0
    companies_published: int = 0
    per_field: dict[str, FieldStats] = dc_field(default_factory=dict)
    per_reason: dict[str, int] = dc_field(default_factory=lambda: dict.fromkeys(SELECTION_REASONS, 0))
    stopped_at_cap: bool = False
    preview: bool = False
    registry_version: str = ""
    source_run_id: str = ""
    company_scope: tuple[str, ...] = ()

    def metadata(self) -> dict[str, Any]:
        """The MaterializeResult metadata: flat counters plus the per-field breakdown."""
        return {
            "companies_selected": self.companies_selected,
            "companies_published": self.companies_published,
            **self.per_reason,
            "stopped_at_cap": self.stopped_at_cap,
            "preview": self.preview,
            "registry_version": self.registry_version,
            "source_run_id": self.source_run_id,
            "company_scope": list(self.company_scope),
            "per_field": dg.MetadataValue.json({name: asdict(stats) for name, stats in self.per_field.items()}),
        }


def _resolve_batch(client: Any, statements: RegistryStatements, companies: Sequence[str], fields: Sequence[str],
                   counters: _FieldCounters, *, source_run_id: str, resolved_at: datetime) -> int:
    """Every field's statement in registry order, then the projection through the stage,
    then this run's rows counted. Returns the companies the projection published."""
    for name in fields:
        client.execute(statements.resolve_sql[name], server_params(
            company_ids=companies, field=name, source_run_id=source_run_id, resolved_at=resolved_at))
    header = split_insert_header(statements.projection_sql)
    if header.table != SE_COMPANY_INFO:
        raise ValueError(f"The projection statement targets {header.table}, not {SE_COMPANY_INFO}")
    # new_versions_only stays off: the wide table is keyed by company_id and a new
    # version per resolution is the point -- ReplacingMergeTree(resolved_at) keeps the newest.
    # publish_with_stage qualifies the bare table name itself (every caller passes bare).
    counts = publish_with_stage(
        client=client, target=SE_COMPANY_INFO.split(".")[-1], insert_columns=header.columns, select_sql=header.body,
        select_parameters=server_params(company_ids=companies), invalid_condition=WIDE_INVALID_CONDITION,
        new_versions_only=False)
    rows = client.execute(build_batch_stats_sql(), server_params(company_ids=companies, source_run_id=source_run_id))
    counters.add(rows, companies=len(companies))
    return counts.inserted


def materialize_se_company_fields(context: Any, client: Any, config: SECompanyFieldResolveConfig, *,
                                  registry: DatatypeRegistry, now: datetime) -> ResolveSummary:
    """Resolve the selected companies -- or, with ``execute`` false, only say which.

    ``context`` supplies ``run_id`` (the rows' source_run_id) and ``log.info``. ``client``
    is the one server-side-params client for the whole run (open_resolve_client). A
    preview runs the registry check and the scan exactly as a real run (every chunk,
    every page) and nothing else.
    """
    scope = normalized_se_company_ids(config.company_ids)
    unknown = sorted(set(config.fields) - set(field_names(registry)))
    if unknown:
        raise ValueError(f"Not registry fields: {unknown}")
    selected_fields = [name for name in field_names(registry) if not config.fields or name in config.fields]
    statements = load_registry_statements(client, registry)
    # One cutoff for the whole run, always bound (the predicate parses it whether or not
    # resolve_all is on); None -> the run's own instant, so nothing this run publishes
    # can be selected again by a later page of the same run.
    cutoff = (config.resolve_all_before or "").strip() or clickhouse_stamp(now)
    summary = ResolveSummary(preview=not config.execute, registry_version=statements.registry_version,
                             source_run_id=str(context.run_id), company_scope=scope)
    chunks = [tuple(scope[start : start + config.company_batch_size])
              for start in range(0, len(scope), config.company_batch_size)] or [()]
    counters = _FieldCounters()
    scan_sql = build_changed_companies_sql(registry)
    for chunk in chunks:
        if summary.stopped_at_cap:
            break
        after_company_id = ""
        while True:
            remaining = (config.max_companies - summary.companies_selected
                         if config.max_companies is not None else config.company_batch_size)
            if remaining <= 0:
                # Only reachable after a FULL page: the cap stopped us, not exhaustion.
                summary.stopped_at_cap = True
                break
            page_size = min(config.company_batch_size, remaining)
            page = client.execute(scan_sql, server_params(
                company_ids=chunk, all_companies=int(not chunk), resolve_all=int(config.resolve_all),
                resolve_all_before=cutoff, after_company_id=after_company_id, page_size=page_size))
            companies = [str(row[0]) for row in page]
            if not companies:
                break
            after_company_id = companies[-1]
            for row in page:
                for offset, reason in enumerate(SELECTION_REASONS, start=1):
                    if row[offset]:
                        summary.per_reason[reason] += 1
            summary.companies_selected += len(companies)
            if config.execute:
                published = _resolve_batch(client, statements, companies, selected_fields, counters,
                                           source_run_id=str(context.run_id), resolved_at=now)
                summary.companies_published += published
                context.log.info("se_company_field batch: companies=%s published=%s", len(companies), published)
            if len(companies) < page_size:
                break  # a short page means the scan is exhausted
    if summary.stopped_at_cap:
        context.log.info("se_company_field stopped at the max_companies cap (%s): changed companies may "
                         "remain, the next run resumes from the start of the scan", config.max_companies)
    summary.per_field = counters.stats(selected_fields) if config.execute else {}
    return summary


@dg.asset(
    name=RESOLVE_ASSET,
    deps=[dg.AssetKey(REGISTRY_ASSET), *(dg.AssetKey(name) for name in CANDIDATE_ASSETS)],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": SE_COMPANY_FIELD, "wide_table": SE_COMPANY_INFO},
    description=("Resolves one value per company and registry field from the candidates and the reviewer "
                 "decisions with the exported resolve statements, then re-pivots se_company_info. A UI "
                 "materialization without execute=true is a preview that writes nothing."),
)
def se_company_field_resolved_clickhouse(context: dg.AssetExecutionContext, config: SECompanyFieldResolveConfig,
                                         clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    """changed companies -> per-field resolve statements -> wide projection -> counts."""
    # The table check binds its own %(name)s parameters client-side, so it runs on the
    # resource's ordinary connection, BEFORE the server-side client is opened.
    # assert_clickhouse_tables_exist matches system.tables.name (bare), so the qualified
    # constants are split back to their bare table name for this one call.
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(
        SE_COMPANY_FIELD_REGISTRY.split(".")[-1], SE_COMPANY_FIELD_CANDIDATE.split(".")[-1],
        SE_COMPANY_FIELD.split(".")[-1], SE_COMPANY_INFO.split(".")[-1],
        SE_COMPANY_INFO_FIELD_VALUE.split(".")[-1]))
    with open_resolve_client(clickhouse) as client:
        summary = materialize_se_company_fields(context, client, config, registry=INFO_REGISTRY,
                                                now=datetime.now(UTC))
    return dg.MaterializeResult(metadata={**summary.metadata(), "table": SE_COMPANY_FIELD})


defs = dg.Definitions(assets=[se_company_field_resolved_clickhouse])
```

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py -q -p no:warnings`
Expected: PASS (16 tests).

Then: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: success -- the new asset loads beside `se_company_info_clickhouse`; no name collision (the old asset keeps its name).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/fields/resolve.py tests/test_se_company_field_resolve.py
git commit -m "feat(se): se_company_field_resolved_clickhouse resolves fields in batches and re-pivots the wide row

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 4: the parity asset check

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/parity.py`
- Test: `tests/test_se_company_field_parity.py`

**Interfaces:**
- Consumes: `resolve.{DATABASE, PARITY_CHECK_NAME, RESOLVE_ASSET}`, `fields.tables.{SE_COMPANY_FIELD, SE_COMPANY_INFO}`, `FakeClient`/`FakeClickhouse` from `tests/test_se_company_common.py`.
- Produces: `PARITY_SNAPSHOT = "se_company_info_parity_snapshot"`, `OBSERVATION_TABLE`, `SAMPLE_SIZE = 20`, `MISMATCH_CONDITIONS`, `INFORMATIONAL_CONDITIONS`, `CONDITION_NAMES`, `PARITY_COLUMNS` (the parity SELECT's output order), `build_parity_snapshot_sql() -> str`, `build_parity_sql() -> str`, `build_rows_per_field_source_sql() -> str`, `parity_result(counts: Mapping[str, int], samples: Mapping[str, Sequence[str]], rows_per_field_source: Sequence[tuple[str, str, int]]) -> dg.AssetCheckResult`, `run_parity_check(client) -> dg.AssetCheckResult`, check `se_company_field_parity_check` (on `se_company_field_resolved_clickhouse`, name `PARITY_CHECK_NAME`), `defs = dg.Definitions(asset_checks=[...])`. Task 5's jobs subtract this check.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_se_company_field_parity.py`:

```python
"""The cutover parity check (spec 12 step 4): SQL pinned as text, the pure result rule,
and the check body against a scripted client. Executed against a real engine in
test_se_company_field_resolve_clickhouse_local.py."""

import dagster as dg

from dagster_v3.defs.se_company.fields.parity import (
    CONDITION_NAMES,
    PARITY_COLUMNS,
    PARITY_SNAPSHOT,
    build_parity_snapshot_sql,
    build_parity_sql,
    build_rows_per_field_source_sql,
    parity_result,
    run_parity_check,
)
from dagster_v3.defs.se_company.fields.resolve import PARITY_CHECK_NAME, RESOLVE_ASSET
from tests.test_se_company_common import FakeClient

HANDELSBANKEN = "5020077862"
PRESENT = "ifNull(rebuilt.company_id, '') != ''"


def _zero_counts() -> dict[str, int]:
    return {"companies_compared": 0, "missing_after_rebuild": 0, **dict.fromkeys(CONDITION_NAMES, 0)}


def _parity_row(**overrides: object) -> tuple:
    """One answer row for build_parity_sql, in PARITY_COLUMNS order."""
    values: dict[str, object] = {**_zero_counts(), **{f"{name}_samples": [] for name in CONDITION_NAMES}}
    values.update(overrides)
    return tuple(values[column] for column in PARITY_COLUMNS)


def test_the_snapshot_copies_the_compared_columns_from_the_old_table() -> None:
    sql = build_parity_snapshot_sql()
    assert sql.startswith("CREATE TABLE IF NOT EXISTS corpscout.se_company_info_parity_snapshot\n"
                          "ENGINE = MergeTree ORDER BY company_id AS\n")
    assert ("SELECT company_id, legal_name, legal_form_code, status, incorporation_date,\n"
            "    description, description_sv, llm_enhanced, description_source_count, suggestion_id, correction_ids,\n"
            "    primary_sni_code, primary_nace_code, resolved_at AS snapshot_resolved_at\n"
            "FROM corpscout.se_company_info FINAL") in sql
    assert PARITY_SNAPSHOT == "se_company_info_parity_snapshot"


def test_parity_sql_compares_every_legal_fact_and_both_description_rules() -> None:
    sql = build_parity_sql()
    assert sql.startswith("WITH observation AS (")
    assert "JSONExtractString(suggestion, 'description') AS description" in sql
    assert "JSONExtractString(suggestion, 'description_sv') AS description_sv" in sql
    assert "FROM corpscout.se_company_info_enrichment_observation" in sql
    assert "FROM corpscout.se_company_info FINAL" in sql
    assert "FROM corpscout.se_company_info_parity_snapshot AS old\n" in sql
    assert "LEFT JOIN rebuilt ON rebuilt.company_id = old.company_id\n" in sql
    assert sql.endswith("LEFT JOIN observation ON observation.suggestion_id = old.suggestion_id")
    assert "count() AS companies_compared" in sql
    assert f"countIf(NOT ({PRESENT})) AS missing_after_rebuild" in sql
    # Legal facts and codes: NULL-safe text comparison of the rebuilt row with the old one.
    for column in ("legal_name", "legal_form_code", "status", "incorporation_date", "primary_sni_code", "primary_nace_code"):
        assert (f"countIf({PRESENT} AND (ifNull(toString(rebuilt.{column}), '') != "
                f"ifNull(toString(old.{column}), ''))) AS {column}") in sql
        assert f"groupArrayIf(20)(old.company_id, {PRESENT} AND (" in sql and f" AS {column}_samples" in sql
    # Copied text (single source, no decision) must match the old row ...
    assert ("(NOT old.llm_enhanced AND old.description_source_count <= 1 AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(rebuilt.description), '') != ifNull(toString(old.description), ''))) AS description_copied") in sql
    assert "AS description_sv_copied" in sql
    # ... a decided company matches the old row whatever wrote it ...
    assert ("(NOT (length(old.correction_ids) = 0) AND (ifNull(toString(rebuilt.description), '') != "
            "ifNull(toString(old.description), '') OR ifNull(toString(rebuilt.description_sv), '') != "
            "ifNull(toString(old.description_sv), '')))) AS description_decided") in sql  # inner OR group: one paren more
    # ... and a modelled one matches the stored observation, not the old row.
    assert ("(old.llm_enhanced AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(observation.suggestion_id), '00000000-0000-0000-0000-000000000000') != "
            "'00000000-0000-0000-0000-000000000000' AND "
            "ifNull(rebuilt.description, '') != ifNull(observation.description, ''))) AS description_llm") in sql
    assert "AS description_sv_llm" in sql
    # Informational: expected to change (model never answered), or the observation is gone.
    assert ("(NOT old.llm_enhanced AND old.description_source_count > 1 AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(rebuilt.description), '') != ifNull(toString(old.description), ''))) "
            "AS description_model_pending_changed") in sql
    assert "AS llm_observation_missing" in sql
    assert PARITY_COLUMNS[:2] == ("companies_compared", "missing_after_rebuild")
    assert PARITY_COLUMNS[2 : 2 + len(CONDITION_NAMES)] == CONDITION_NAMES
    assert PARITY_COLUMNS[2 + len(CONDITION_NAMES) :] == tuple(f"{name}_samples" for name in CONDITION_NAMES)
    assert build_rows_per_field_source_sql() == (
        "SELECT field, source, count() AS rows\n"
        "FROM corpscout.se_company_field FINAL\n"
        "GROUP BY field, source\n"
        "ORDER BY field, source")


def test_parity_result_passes_only_with_zero_mismatches_and_reports_informational_counts() -> None:
    clean = parity_result({**_zero_counts(), "companies_compared": 3_500_000,
                           "description_model_pending_changed": 12_000, "llm_observation_missing": 3},
                          {}, [("legal_name", "bolagsverket", 3_400_000), ("legal_name", "scb", 100_000)])
    assert clean.passed is True and clean.severity == dg.AssetCheckSeverity.ERROR
    assert clean.metadata["companies_compared"] == dg.MetadataValue.int(3_500_000)
    assert clean.metadata["description_model_pending_changed"] == dg.MetadataValue.int(12_000)
    assert clean.metadata["rows_per_field_per_source"] == dg.MetadataValue.json(
        [{"field": "legal_name", "source": "bolagsverket", "rows": 3_400_000},
         {"field": "legal_name", "source": "scb", "rows": 100_000}])
    assert clean.metadata["failing"] == dg.MetadataValue.json({})

    for column in ("missing_after_rebuild", "legal_name", "primary_sni_code", "description_copied",
                   "description_decided", "description_llm", "description_sv_llm"):
        failed = parity_result({**_zero_counts(), "companies_compared": 1, column: 1},
                               {column: [HANDELSBANKEN]}, [])
        assert failed.passed is False, column
        assert failed.metadata["failing"] == dg.MetadataValue.json({column: 1})
        assert failed.metadata["samples"] == dg.MetadataValue.json({column: [HANDELSBANKEN]})


def test_run_parity_check_reads_counts_and_samples_from_the_client() -> None:
    client = FakeClient(answers=[
        [(1,)],  # the snapshot exists
        [_parity_row(companies_compared=2, primary_sni_code=1, primary_sni_code_samples=[HANDELSBANKEN])],
        [("description", "llm", 2), ("legal_name", "bolagsverket", 2)],
    ])
    result = run_parity_check(client)
    assert result.passed is False
    assert result.metadata["primary_sni_code"] == dg.MetadataValue.int(1)
    assert result.metadata["samples"] == dg.MetadataValue.json({"primary_sni_code": [HANDELSBANKEN]})
    assert result.metadata["rows_per_field_per_source"] == dg.MetadataValue.json(
        [{"field": "description", "source": "llm", "rows": 2}, {"field": "legal_name", "source": "bolagsverket", "rows": 2}])
    statements = [sql for sql, _ in client.executed]
    assert statements[0] == ("SELECT count() FROM system.tables WHERE database = 'corpscout' "
                             "AND name = 'se_company_info_parity_snapshot'")
    assert statements[1] == build_parity_sql() and statements[2] == build_rows_per_field_source_sql()


def test_run_parity_check_fails_clearly_without_a_snapshot() -> None:
    client = FakeClient(answers=[[(0,)]])
    result = run_parity_check(client)
    assert result.passed is False
    assert "corpscout.se_company_info_parity_snapshot does not exist" in str(result.metadata["error"].value)
    assert len(client.executed) == 1  # nothing else was asked


def test_the_check_is_registered_on_the_resolve_asset() -> None:
    from dagster_v3.defs.se_company.fields.parity import se_company_field_parity_check

    assert PARITY_CHECK_NAME == "se_company_field_parity_check"
    assert se_company_field_parity_check.check_keys == {dg.AssetCheckKey(dg.AssetKey(RESOLVE_ASSET), PARITY_CHECK_NAME)}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_parity.py -q -p no:warnings`
Expected: FAIL at import -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.parity'`.

- [ ] **Step 3: Create `parity.py`**

```python
"""Cutover parity (spec 12 step 4): the rebuilt se_company_info against a snapshot of the
old one.

The cutover plan creates corpscout.se_company_info_parity_snapshot with
``build_parity_snapshot_sql()`` from the OLD table before the rebuild (a scratch table,
direct SQL, not a ledger migration), runs the resolve with ``resolve_all``, then executes
this check on its own -- it is subtracted from both jobs (fields/jobs.py) because it is
meaningless on an ordinary sensor run and would show red forever.

Rules, per old row: legal facts and codes must be equal; a description copied from a
single source with no decision must be equal; a company with an applied decision
(``correction_ids`` non-empty) must equal its old text whatever wrote it; a modelled
description with no decision must equal the STORED OBSERVATION's text (the LLM
candidate reuses it by input_hash). A company published with several sources but no
suggestion is expected to change once the LLM extractor supplies a candidate -- reported
as description_model_pending_changed, never a failure. Under join_use_nulls = 1 every
rebuilt/observation column of a missing join is NULL, hence the ifNull everywhere.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.fields.resolve import DATABASE, PARITY_CHECK_NAME, RESOLVE_ASSET
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD, SE_COMPANY_INFO

PARITY_SNAPSHOT = "se_company_info_parity_snapshot"
OBSERVATION_TABLE = "se_company_info_enrichment_observation"
SAMPLE_SIZE = 20
ZERO_UUID = "00000000-0000-0000-0000-000000000000"
PRESENT = "ifNull(rebuilt.company_id, '') != ''"
NO_DECISION = "length(old.correction_ids) = 0"
OBSERVATION_PRESENT = f"ifNull(toString(observation.suggestion_id), '{ZERO_UUID}') != '{ZERO_UUID}'"


def _differs(column: str) -> str:
    return f"ifNull(toString(rebuilt.{column}), '') != ifNull(toString(old.{column}), '')"


# (name, condition over old/rebuilt/observation). A non-zero count fails the check.
MISMATCH_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("legal_name", _differs("legal_name")),
    ("legal_form_code", _differs("legal_form_code")),
    ("status", _differs("status")),
    ("incorporation_date", _differs("incorporation_date")),
    ("primary_sni_code", _differs("primary_sni_code")),
    ("primary_nace_code", _differs("primary_nace_code")),
    ("description_copied",
     f"NOT old.llm_enhanced AND old.description_source_count <= 1 AND {NO_DECISION} AND {_differs('description')}"),
    ("description_sv_copied",
     f"NOT old.llm_enhanced AND old.description_source_count <= 1 AND {NO_DECISION} AND {_differs('description_sv')}"),
    ("description_decided",
     f"NOT ({NO_DECISION}) AND ({_differs('description')} OR {_differs('description_sv')})"),
    ("description_llm",
     f"old.llm_enhanced AND {NO_DECISION} AND {OBSERVATION_PRESENT} AND "
     "ifNull(rebuilt.description, '') != ifNull(observation.description, '')"),
    ("description_sv_llm",
     f"old.llm_enhanced AND {NO_DECISION} AND {OBSERVATION_PRESENT} AND "
     "ifNull(rebuilt.description_sv, '') != ifNull(observation.description_sv, '')"),
)
# Reported, never failing.
INFORMATIONAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("description_model_pending_changed",
     f"NOT old.llm_enhanced AND old.description_source_count > 1 AND {NO_DECISION} AND {_differs('description')}"),
    ("llm_observation_missing", f"old.llm_enhanced AND {NO_DECISION} AND NOT ({OBSERVATION_PRESENT})"),
)
CONDITION_NAMES = tuple(name for name, _ in (*MISMATCH_CONDITIONS, *INFORMATIONAL_CONDITIONS))
# The parity SELECT's output order -- run_parity_check reads the one row by position.
PARITY_COLUMNS = ("companies_compared", "missing_after_rebuild", *CONDITION_NAMES,
                  *(f"{name}_samples" for name in CONDITION_NAMES))


def build_parity_snapshot_sql() -> str:
    """The cutover plan runs this against the OLD table before the rebuild."""
    return f"""CREATE TABLE IF NOT EXISTS {DATABASE}.{PARITY_SNAPSHOT}
ENGINE = MergeTree ORDER BY company_id AS
SELECT company_id, legal_name, legal_form_code, status, incorporation_date,
    description, description_sv, llm_enhanced, description_source_count, suggestion_id, correction_ids,
    primary_sni_code, primary_nace_code, resolved_at AS snapshot_resolved_at
FROM {SE_COMPANY_INFO} FINAL"""


def build_parity_sql() -> str:
    conditions = (*MISMATCH_CONDITIONS, *INFORMATIONAL_CONDITIONS)
    counts = ",\n    ".join(f"countIf({PRESENT} AND ({condition})) AS {name}" for name, condition in conditions)
    samples = ",\n    ".join(
        f"groupArrayIf({SAMPLE_SIZE})(old.company_id, {PRESENT} AND ({condition})) AS {name}_samples"
        for name, condition in conditions)
    return f"""WITH observation AS (
    SELECT suggestion_id,
        JSONExtractString(suggestion, 'description') AS description,
        JSONExtractString(suggestion, 'description_sv') AS description_sv
    FROM {DATABASE}.{OBSERVATION_TABLE}
),
rebuilt AS (
    SELECT company_id, legal_name, legal_form_code, status, incorporation_date, description, description_sv,
        primary_sni_code, primary_nace_code
    FROM {SE_COMPANY_INFO} FINAL
)
SELECT count() AS companies_compared,
    countIf(NOT ({PRESENT})) AS missing_after_rebuild,
    {counts},
    {samples}
FROM {DATABASE}.{PARITY_SNAPSHOT} AS old
LEFT JOIN rebuilt ON rebuilt.company_id = old.company_id
LEFT JOIN observation ON observation.suggestion_id = old.suggestion_id"""


def build_rows_per_field_source_sql() -> str:
    return f"""SELECT field, source, count() AS rows
FROM {SE_COMPANY_FIELD} FINAL
GROUP BY field, source
ORDER BY field, source"""


def parity_result(counts: Mapping[str, int], samples: Mapping[str, Sequence[str]],
                  rows_per_field_source: Sequence[tuple[str, str, int]]) -> dg.AssetCheckResult:
    """Pass iff no company is missing and every MISMATCH_CONDITIONS count is zero."""
    failing = {name: int(counts[name]) for name, _ in MISMATCH_CONDITIONS if counts[name]}
    if counts["missing_after_rebuild"]:
        failing = {"missing_after_rebuild": int(counts["missing_after_rebuild"]), **failing}
    metadata: dict[str, Any] = {
        "companies_compared": dg.MetadataValue.int(int(counts["companies_compared"])),
        "missing_after_rebuild": dg.MetadataValue.int(int(counts["missing_after_rebuild"])),
        **{name: dg.MetadataValue.int(int(counts[name])) for name in CONDITION_NAMES},
        "failing": dg.MetadataValue.json(failing),
        "samples": dg.MetadataValue.json({name: [str(c) for c in ids] for name, ids in samples.items() if ids}),
        "rows_per_field_per_source": dg.MetadataValue.json(
            [{"field": field, "source": source, "rows": int(rows)} for field, source, rows in rows_per_field_source]),
    }
    return dg.AssetCheckResult(passed=not failing, severity=dg.AssetCheckSeverity.ERROR, metadata=metadata)


def run_parity_check(client: Any) -> dg.AssetCheckResult:
    exists = client.execute(
        f"SELECT count() FROM system.tables WHERE database = '{DATABASE}' AND name = '{PARITY_SNAPSHOT}'")
    if int(exists[0][0]) == 0:
        return dg.AssetCheckResult(passed=False, metadata={"error": dg.MetadataValue.text(
            f"{DATABASE}.{PARITY_SNAPSHOT} does not exist: run build_parity_snapshot_sql() against the OLD "
            "table before the rebuild (cutover step 3)")})
    row = client.execute(build_parity_sql())[0]
    values = dict(zip(PARITY_COLUMNS, row, strict=True))
    counts = {name: int(values[name]) for name in PARITY_COLUMNS if not name.endswith("_samples")}
    samples = {name[: -len("_samples")]: [str(c) for c in values[name]]
               for name in PARITY_COLUMNS if name.endswith("_samples")}
    per_source = [(str(field), str(source), int(rows)) for field, source, rows in client.execute(build_rows_per_field_source_sql())]
    return parity_result(counts, samples, per_source)


@dg.asset_check(
    asset=dg.AssetKey(RESOLVE_ASSET),
    name=PARITY_CHECK_NAME,
    description=("Cutover parity of the rebuilt se_company_info against se_company_info_parity_snapshot: "
                 "legal facts, codes and descriptions per spec 12; run on demand, not by the jobs."),
)
def se_company_field_parity_check(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        return run_parity_check(client)


defs = dg.Definitions(asset_checks=[se_company_field_parity_check])
```

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_parity.py -q -p no:warnings`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/fields/parity.py tests/test_se_company_field_parity.py
git commit -m "feat(se): se_company_field_parity_check compares the rebuilt wide table with its snapshot

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 5: the jobs, the sensors, and the field-value sensor leaves `info.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/jobs.py`
- Create: `src/dagster_v3/defs/se_company/fields/sensors.py`
- Modify: `src/dagster_v3/defs/se_company/info.py:1-33` (docstring), `:45-55` (imports), `:964-981` (sensor block and `defs`)
- Modify: `tests/test_se_company_info.py` (`test_definitions_wire_final_jobs_sensor_schedule_and_leaves`; delete `test_the_field_value_sensor_launches_a_real_run_not_a_preview`)
- Test: `tests/test_se_company_field_sensors.py`

**Interfaces:**
- Consumes: `resolve.{ARTIFACT_ASSETS, CANDIDATE_ASSETS, REGISTRY_ASSET, RESOLVE_ASSET, AUTOMATED_RUN_CONFIG, SE_COMPANY_INFO_FIELD_VALUE}`, `parity.se_company_field_parity_check`, `common.{DATABASE, EPOCH, ledger_sensor}`, `fields.tables.SE_COMPANY_FIELD_CANDIDATE`, `_FakeLedgerClient` from `tests/test_se_company_common.py`.
- Produces: `jobs.WEEKLY_ASSETS`, `jobs.se_company_field_resolve_job`, `jobs.se_company_fields_job`; `sensors.MAX_SCOPED_COMPANY_IDS = 20_000`, `sensors.build_candidate_cursor_sql(table)`, `sensors.build_candidate_touched_sql(table)`, `sensors.candidate_sensor(*, name, table, job, asset_names, default_status=STOPPED, extra_config=None, max_scoped_company_ids=MAX_SCOPED_COMPANY_IDS) -> dg.SensorDefinition`, `sensors.se_company_info_field_value_sensor` (same name as before, now on `se_company_field_resolve_job`), `sensors.se_company_field_candidate_sensor`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_se_company_field_sensors.py`:

```python
"""The field-registry sensors: the field-value sensor re-pointed under its old name
(the backoffice names it), and the candidate sensor with its own cursor on
extracted_at. Mirrors the ledger-sensor tests in test_se_company_common.py."""

import uuid
from contextlib import contextmanager

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.common import EPOCH
from dagster_v3.defs.se_company.fields.resolve import RESOLVE_ASSET
from dagster_v3.defs.se_company.fields.sensors import (
    MAX_SCOPED_COMPANY_IDS,
    build_candidate_cursor_sql,
    build_candidate_touched_sql,
    candidate_sensor,
    se_company_field_candidate_sensor,
    se_company_info_field_value_sensor,
)
from tests.test_se_company_common import _FakeLedgerClient

COMPANY = "5020077862"
OTHER = "5560125220"
T0 = "2026-08-20 11:59:59.000"
T1 = "2026-08-20 12:00:00.000"
T2 = "2026-08-20 12:00:01.000"


def _patch(monkeypatch, client) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self):
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def _tick(sensor: dg.SensorDefinition, resource: ClickhouseResource, cursor: str | None):
    return sensor.evaluate_tick(dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource}))


def test_the_field_value_sensor_keeps_its_name_and_launches_the_resolve_asset_for_real(monkeypatch) -> None:
    """A new decision must re-resolve its company through the registry-driven asset,
    for real (execute) -- and under the name dagster.server.ts starts and stops, so the
    backoffice needs no change and Dagster keeps the sensor's cursor across the deploy."""
    ledger = _FakeLedgerClient()
    ledger.append(COMPANY, str(uuid.UUID(int=7)), T1)
    resource = _patch(monkeypatch, ledger)

    assert se_company_info_field_value_sensor.name == "se_company_info_field_value_sensor"
    assert se_company_info_field_value_sensor.job_name == "se_company_field_resolve_job"
    assert se_company_info_field_value_sensor.default_status == dg.DefaultSensorStatus.STOPPED
    data = _tick(se_company_info_field_value_sensor, resource, None)
    assert data.run_requests is not None and data.run_requests[0].run_config == {
        "ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": [COMPANY]}}}}
    statements = [sql for sql, _ in ledger.executed]
    assert any("corpscout.se_company_info_field_value" in sql for sql in statements)
    assert all("correction_id" not in sql for sql in statements)
    assert any("argMax(value_id, (created_at, value_id))" in sql for sql in statements)


class _FakeCandidateClient:
    """In-memory candidate table answering candidate_sensor's two queries."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str]] = []  # (company_id, extracted_at)
        self.executed: list[tuple[str, object]] = []

    def append(self, company_id: str, extracted_at: str) -> None:
        self.rows.append((company_id, extracted_at))

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        self.executed.append((sql, params))
        if "max(extracted_at)" in sql:
            if not self.rows:
                return [(0, "")]
            return [(len(self.rows), max(extracted_at for _, extracted_at in self.rows))]
        if "SELECT DISTINCT company_id" in sql:
            assert params is not None
            touched = sorted({company for company, extracted_at in self.rows if extracted_at > str(params["since"])})
            return [(company,) for company in touched[: int(params["limit"])]]
        raise AssertionError(sql)


def test_candidate_sensor_sql_shapes_and_declaration() -> None:
    assert build_candidate_cursor_sql("se_company_field_candidate") == (
        "SELECT count(), if(count() = 0, '', toString(max(extracted_at)))\n"
        "FROM corpscout.se_company_field_candidate")
    assert build_candidate_touched_sql("se_company_field_candidate") == (
        "SELECT DISTINCT company_id\n"
        "FROM corpscout.se_company_field_candidate\n"
        "WHERE extracted_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')\n"
        "ORDER BY company_id\n"
        "LIMIT %(limit)s")
    assert MAX_SCOPED_COMPANY_IDS == 20_000
    assert se_company_field_candidate_sensor.name == "se_company_field_candidate_sensor"
    assert se_company_field_candidate_sensor.job_name == "se_company_field_resolve_job"
    assert se_company_field_candidate_sensor.default_status == dg.DefaultSensorStatus.STOPPED
    assert se_company_field_candidate_sensor.minimum_interval_seconds == 60


def test_candidate_sensor_first_tick_uses_the_epoch_boundary_and_scopes_the_run(monkeypatch) -> None:
    client = _FakeCandidateClient()
    client.append(COMPANY, T1)
    client.append(OTHER, T2)
    resource = _patch(monkeypatch, client)

    data = _tick(se_company_field_candidate_sensor, resource, None)

    touched = next(entry for entry in client.executed if "SELECT DISTINCT company_id" in entry[0])
    assert touched[1] == {"since": EPOCH, "limit": MAX_SCOPED_COMPANY_IDS + 1}
    assert data.cursor == f"2:{T2}"
    assert data.run_requests is not None and len(data.run_requests) == 1
    assert data.run_requests[0].run_key == f"se_company_field_candidate:2:{T2}"
    assert data.run_requests[0].run_config == {
        "ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": [COMPANY, OTHER]}}}}


def test_candidate_sensor_skips_on_an_empty_table_and_on_an_unchanged_cursor(monkeypatch) -> None:
    empty = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, _FakeCandidateClient()), None)
    assert empty.skip_message == "No rows in se_company_field_candidate"

    client = _FakeCandidateClient()
    client.append(COMPANY, T1)
    unchanged = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, client), f"1:{T1}")
    assert unchanged.skip_message == "No new rows in se_company_field_candidate" and not unchanged.run_requests
    assert len(client.executed) == 1  # the unchanged cursor never issues the touched scan


def test_candidate_sensor_advances_without_a_run_when_nothing_is_newer_than_the_boundary(monkeypatch) -> None:
    client = _FakeCandidateClient()
    client.append(COMPANY, T1)
    client.append(OTHER, T0)  # clock skew: a row older than the cursored boundary
    data = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, client), f"1:{T1}")
    assert data.cursor == f"2:{T1}" and data.run_requests == []


def test_candidate_sensor_launches_an_unscoped_run_past_the_id_cap(monkeypatch) -> None:
    """A full extraction touches millions of companies: rather than carrying them all in
    run config, the sensor launches an unscoped run and the changed-company scan finds
    them through new_candidates."""
    sensor = candidate_sensor(
        name="capped", table="se_company_field_candidate",
        job=dg.define_asset_job("capped_job", selection=dg.AssetSelection.assets(RESOLVE_ASSET)),
        asset_names=(RESOLVE_ASSET,), extra_config={"execute": True}, max_scoped_company_ids=2)
    client = _FakeCandidateClient()
    for index in range(3):
        client.append(f"55600000{index:02d}", T1)
    data = _tick(sensor, _patch(monkeypatch, client), None)
    touched = next(entry for entry in client.executed if "SELECT DISTINCT company_id" in entry[0])
    assert touched[1]["limit"] == 3  # cap + 1: one row more than the cap is the overflow signal
    assert data.run_requests is not None
    assert data.run_requests[0].run_config == {"ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": []}}}}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sensors.py -q -p no:warnings`
Expected: FAIL at import -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.sensors'`.

- [ ] **Step 3: Create `jobs.py`**

```python
"""The field registry's jobs.

se_company_fields_job is the weekly chain: the three per-source artifacts (they feed the
scb/esef/wikidata extractors), the registry export, the seven candidate extractors and
the resolve. se_company_field_resolve_job is the resolve alone -- what the sensors and
the backoffice launch for a scoped run.

Both subtract se_company_field_parity_check: it is a cutover instrument (compares the
rebuilt wide table with a snapshot the cutover plan creates) and would fail on every
ordinary run. Dagster resolves the subtraction at repository build and refuses an
undefined check key, hence the import of the definition itself. The leaf row-count
check on the resolve asset stays in both jobs.
"""

import dagster as dg

from dagster_v3.defs.se_company.fields.parity import se_company_field_parity_check
from dagster_v3.defs.se_company.fields.resolve import (
    ARTIFACT_ASSETS,
    CANDIDATE_ASSETS,
    REGISTRY_ASSET,
    RESOLVE_ASSET,
)

WEEKLY_ASSETS = (*ARTIFACT_ASSETS, REGISTRY_ASSET, *CANDIDATE_ASSETS, RESOLVE_ASSET)
_PARITY_CHECK = dg.AssetSelection.checks(se_company_field_parity_check)

se_company_field_resolve_job = dg.define_asset_job(
    "se_company_field_resolve_job", selection=dg.AssetSelection.assets(RESOLVE_ASSET) - _PARITY_CHECK)
se_company_fields_job = dg.define_asset_job(
    "se_company_fields_job", selection=dg.AssetSelection.assets(*WEEKLY_ASSETS) - _PARITY_CHECK)

defs = dg.Definitions(jobs=[se_company_field_resolve_job, se_company_fields_job])
```

- [ ] **Step 4: Create `sensors.py`**

```python
"""The field registry's sensors.

se_company_info_field_value_sensor keeps its name -- the backoffice's dagster.server.ts
starts, stops and reads it by name, and Dagster keys the RUNNING state and the cursor on
it -- but launches the registry-driven resolve now, for real (execute), scoped to the
companies the new decisions touched. Built by common.ledger_sensor exactly as before.

se_company_field_candidate_sensor watches the candidate table so an extractor run
outside the weekly job (the LLM pass, a backoffice refresh) is followed by a resolve.
ledger_sensor cannot serve it: that factory hard-codes ``created_at`` and a UUID id
column, and the candidate table has ``extracted_at`` and no UUID. The cursor here is
``count:max(extracted_at)``; the touched set is every company with a candidate newer
than the cursored instant. Past MAX_SCOPED_COMPANY_IDS the run is launched UNSCOPED:
the changed-company scan finds those companies through new_candidates, and a run config
of millions of ids is not something to store in Postgres.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE, EPOCH, ledger_sensor
from dagster_v3.defs.se_company.fields.jobs import se_company_field_resolve_job
from dagster_v3.defs.se_company.fields.resolve import (
    AUTOMATED_RUN_CONFIG,
    RESOLVE_ASSET,
    SE_COMPANY_INFO_FIELD_VALUE,
)
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_CANDIDATE

# One batch of the resolve asset: more touched companies than this means "resolve
# whatever the scan finds" rather than a run config carrying every id.
MAX_SCOPED_COMPANY_IDS = 20_000

se_company_info_field_value_sensor = ledger_sensor(
    name="se_company_info_field_value_sensor", table=SE_COMPANY_INFO_FIELD_VALUE, id_column="value_id",
    job=se_company_field_resolve_job, asset_names=(RESOLVE_ASSET,), extra_config=AUTOMATED_RUN_CONFIG)


def build_candidate_cursor_sql(table: str) -> str:
    """``max(extracted_at)`` is the candidate table's version column: no FINAL needed."""
    return f"""SELECT count(), if(count() = 0, '', toString(max(extracted_at)))
FROM {DATABASE}.{table}"""


def build_candidate_touched_sql(table: str) -> str:
    return f"""SELECT DISTINCT company_id
FROM {DATABASE}.{table}
WHERE extracted_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')
ORDER BY company_id
LIMIT %(limit)s"""


def candidate_sensor(
    *,
    name: str,
    table: str,
    job: dg.JobDefinition,
    asset_names: Sequence[str],
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
    extra_config: Mapping[str, Any] | None = None,
    max_scoped_company_ids: int = MAX_SCOPED_COMPANY_IDS,
) -> dg.SensorDefinition:
    """A sensor that wakes every asset in ``asset_names`` for the companies with a
    candidate extracted since the last cursor -- unscoped past the id cap."""
    shared_config = dict(extra_config or {})

    @dg.sensor(
        name=name,
        job=job,
        default_status=default_status,
        minimum_interval_seconds=60,
        required_resource_keys={"clickhouse"},
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult | dg.SkipReason:
        # ``table`` (e.g. SE_COMPANY_FIELD_CANDIDATE) is database-qualified; the SQL
        # builders below expect a bare name and prefix it with DATABASE themselves.
        bare_table = table.split(".")[-1]
        with context.resources.clickhouse.get_connection() as client:
            count, latest = client.execute(build_candidate_cursor_sql(bare_table))[0]
            if int(count) == 0:
                return dg.SkipReason(f"No rows in {bare_table}")
            cursor = f"{int(count)}:{latest}"
            if cursor == context.cursor:
                return dg.SkipReason(f"No new rows in {bare_table}")
            since = context.cursor.split(":", 1)[1] if context.cursor else EPOCH
            rows = client.execute(build_candidate_touched_sql(bare_table),
                                  {"since": since, "limit": max_scoped_company_ids + 1})
        company_ids = [str(row[0]) for row in rows]
        if not company_ids:
            # The table grew but nothing is newer than the boundary (clock skew): advance
            # the cursor anyway, or this tick would re-evaluate the same boundary forever.
            return dg.SensorResult(run_requests=[], cursor=cursor)
        scope = [] if len(company_ids) > max_scoped_company_ids else company_ids
        return dg.SensorResult(
            run_requests=[dg.RunRequest(
                run_key=f"{bare_table}:{cursor}",
                run_config={"ops": {asset: {"config": {**shared_config, "company_ids": scope}}
                                    for asset in asset_names}})],
            cursor=cursor)

    return _sensor


se_company_field_candidate_sensor = candidate_sensor(
    name="se_company_field_candidate_sensor", table=SE_COMPANY_FIELD_CANDIDATE,
    job=se_company_field_resolve_job, asset_names=(RESOLVE_ASSET,), extra_config=AUTOMATED_RUN_CONFIG)

defs = dg.Definitions(sensors=[se_company_info_field_value_sensor, se_company_field_candidate_sensor])
```

- [ ] **Step 5: Take the old sensor out of `info.py`**

Three edits in `src/dagster_v3/defs/se_company/info.py`:

1. Docstring, replace the three `Trigger:` lines (`Trigger: se_company_info_weekly after the artifacts; se_company_info_field_value_sensor` / `(new field values -> scoped review job); manual runs from the backoffice Pipeline page,` / `scoped by company_ids.`) with:

```
Trigger: manual runs from the backoffice Pipeline page, scoped by company_ids. The
field-value sensor and the weekly schedule launch the registry-driven
se_company_field_resolved_clickhouse now (fields/sensors.py, fields/schedules.py);
this asset is deleted by the cutover plan.
```

2. Imports: delete the line `    ledger_sensor,` from the `from dagster_v3.defs.se_company.common import (` block.

3. Delete the sensor block

```python
se_company_info_field_value_sensor = ledger_sensor(
    name="se_company_info_field_value_sensor", table=SE_COMPANY_INFO_FIELD_VALUE,
    id_column="value_id", job=se_company_info_review_job,
    asset_names=("se_company_info_clickhouse",), extra_config=AUTOMATED_RUN_CONFIG)
```

and rewrite the module's last statement as

```python
defs = dg.Definitions(assets=[se_company_info_clickhouse], jobs=[se_company_info_job, se_company_info_review_job],
                      schedules=[se_company_info_weekly])
```

(`AUTOMATED_RUN_CONFIG` stays: the backoffice Pipeline page and Task 6's test read it. The comment above it that mentions "the two automated triggers" still holds for the schedule until Task 6.)

- [ ] **Step 6: Move the old sensor assertions**

In `tests/test_se_company_info.py`:

1. In `test_definitions_wire_final_jobs_sensor_schedule_and_leaves`, replace

```python
    sensor = repository.get_sensor_def("se_company_info_field_value_sensor")
    assert sensor.job_name == "se_company_info_review_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
```

with

```python
    # The field-value sensor keeps its name but launches the registry-driven resolve
    # (fields/sensors.py); tests/test_se_company_field_sensors.py owns it.
    assert repository.get_sensor_def("se_company_info_field_value_sensor").job_name == "se_company_field_resolve_job"
```

2. Delete the whole function `test_the_field_value_sensor_launches_a_real_run_not_a_preview` (its replacement is `test_the_field_value_sensor_keeps_its_name_and_launches_the_resolve_asset_for_real`).

- [ ] **Step 7: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sensors.py tests/test_se_company_common.py -q -p no:warnings`
Expected: PASS (6 new tests; the common suite unchanged).

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_info.py tests/test_se_company_field_resolve.py tests/test_se_company_field_parity.py -q -p no:warnings`
Expected: PASS -- one sensor of that name in the repository, on the new job.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: success (two jobs, two sensors, the parity check subtracted, no duplicate names).

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/se_company/fields/jobs.py src/dagster_v3/defs/se_company/fields/sensors.py \
        src/dagster_v3/defs/se_company/info.py tests/test_se_company_field_sensors.py tests/test_se_company_info.py
git commit -m "feat(se): field jobs and sensors -- the field-value sensor launches the registry resolve

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 6: the weekly schedule replaces `se_company_info_weekly`; the freshness leaf; the Definitions wiring test

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/schedules.py`
- Modify: `src/dagster_v3/defs/se_company/info.py` (`se_company_info_weekly` block and `defs`)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:232` (leaf after `se_company_info_clickhouse`)
- Modify: `tests/test_se_company_info.py` (`test_definitions_wire_final_jobs_sensor_schedule_and_leaves`, `test_the_config_gates_the_run_and_pins_the_profile_the_automation_sends`)
- Test: `tests/test_se_company_field_resolve.py` (append the Definitions test)

**Interfaces:**
- Consumes: `jobs.se_company_fields_job`, `resolve.{AUTOMATED_RUN_CONFIG, LLM_CANDIDATES_ASSET, RESOLVE_ASSET}`.
- Produces: `schedules.LLM_CANDIDATES_RUN_CONFIG: dict[str, Any]` (`{"execute": True, "llm": {...}}`), `schedules.se_company_fields_weekly` (cron `50 6 * * 1`, `UTC`, STOPPED, run config `{"ops": {RESOLVE_ASSET: {"config": {"execute": True}}, <each non-LLM candidate asset>: {"config": {"execute": True}}, LLM_CANDIDATES_ASSET: {"config": LLM_CANDIDATES_RUN_CONFIG}}}` -- plan 2's `CandidateExtractConfig.execute` defaults to a preview exactly like the resolve asset, so every extractor the weekly job runs must be told `execute`; the registry export and the three artifact assets have no gate); a `ClickhouseLeaf("se_company_field_resolved_clickhouse", ("se_company_field",), None)`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_se_company_field_resolve.py`:

```python
# --- Definitions wiring ---------------------------------------------------------------


def test_definitions_wire_the_resolve_asset_jobs_sensors_and_schedule() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES, ROW_COUNT_CHECK_NAME
    from dagster_v3.defs.se_company.fields.jobs import WEEKLY_ASSETS
    from dagster_v3.defs.se_company.fields.resolve import (
        ARTIFACT_ASSETS,
        CANDIDATE_ASSETS,
        LLM_CANDIDATES_ASSET,
        PARITY_CHECK_NAME,
        REGISTRY_ASSET,
    )
    from dagster_v3.defs.se_company.fields.schedules import LLM_CANDIDATES_RUN_CONFIG, se_company_fields_weekly

    repository = load_defs().get_repository_def()
    asset = repository.asset_graph.get(dg.AssetKey(RESOLVE_ASSET))
    assert asset.group_name == "se_company_fields"
    assert asset.parent_keys == {dg.AssetKey(REGISTRY_ASSET), *(dg.AssetKey(name) for name in CANDIDATE_ASSETS)}

    # The two jobs: the weekly chain and the resolve alone. Checks ride along, except
    # the parity check, which only the cutover runs.
    resolve_job = repository.get_job("se_company_field_resolve_job")
    assert {key.path[-1] for key in resolve_job.asset_layer.executable_asset_keys} == {RESOLVE_ASSET}
    weekly_job = repository.get_job("se_company_fields_job")
    assert WEEKLY_ASSETS == (*ARTIFACT_ASSETS, REGISTRY_ASSET, *CANDIDATE_ASSETS, RESOLVE_ASSET)
    assert len(WEEKLY_ASSETS) == 12 and LLM_CANDIDATES_ASSET in WEEKLY_ASSETS
    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == set(WEEKLY_ASSETS)
    for job in (resolve_job, weekly_job):
        nodes = {node.name for node in job.graph.node_defs}
        assert f"{RESOLVE_ASSET}_{PARITY_CHECK_NAME}" not in nodes
        assert f"{RESOLVE_ASSET}_{ROW_COUNT_CHECK_NAME}" in nodes

    field_value = repository.get_sensor_def("se_company_info_field_value_sensor")
    assert field_value.job_name == "se_company_field_resolve_job"
    assert field_value.default_status == dg.DefaultSensorStatus.STOPPED
    candidate = repository.get_sensor_def("se_company_field_candidate_sensor")
    assert candidate.job_name == "se_company_field_resolve_job"
    assert candidate.default_status == dg.DefaultSensorStatus.STOPPED

    # The schedule took the Monday 06:50 slot se_company_info_weekly held.
    schedule = repository.get_schedule_def("se_company_fields_weekly")
    assert schedule.cron_schedule == "50 6 * * 1" and schedule.execution_timezone == "UTC"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_fields_job"
    assert "se_company_info_weekly" not in {s.name for s in repository.schedule_defs}
    context = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 9, 7, 6, 50, tzinfo=UTC))
    run_requests = se_company_fields_weekly.evaluate_tick(context).run_requests
    # Every gated asset in the chain is told execute: the resolve asset AND the seven
    # extractors (plan 2's CandidateExtractConfig defaults to a preview too).
    assert run_requests is not None and run_requests[0].run_config == {"ops": {
        RESOLVE_ASSET: {"config": {"execute": True}},
        **{name: {"config": {"execute": True}} for name in CANDIDATE_ASSETS if name != LLM_CANDIDATES_ASSET},
        LLM_CANDIDATES_ASSET: {"config": LLM_CANDIDATES_RUN_CONFIG}}}
    assert LLM_CANDIDATES_RUN_CONFIG["execute"] is True
    assert LLM_CANDIDATES_RUN_CONFIG["llm"]["provider"] == "deepseek"
    assert LLM_CANDIDATES_RUN_CONFIG["llm"]["model"] == "deepseek-v4-flash"
    assert LLM_CANDIDATES_RUN_CONFIG["llm"]["prompt_version"] == "se-company-info-description-v3"
    # The config the daemon would submit must be one the job accepts: raises
    # DagsterInvalidConfigError, naming the key, when the LLM extractor's config class
    # (plan 2) does not take LLM_CANDIDATES_RUN_CONFIG's shape.
    dg.validate_run_config(weekly_job, run_requests[0].run_config)

    # The old asset and its two jobs stay registered beside the new ones until the cutover.
    assert repository.asset_graph.has(dg.AssetKey("se_company_info_clickhouse"))
    assert {"se_company_info_job", "se_company_info_review_job"} <= {job.name for job in repository.get_all_jobs()}

    leaf = next(leaf for leaf in CLICKHOUSE_LEAVES if leaf.asset_key == RESOLVE_ASSET)
    assert leaf.tables == ("se_company_field",) and leaf.max_age is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py -q -p no:warnings -k definitions_wire`
Expected: FAIL -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.schedules'`.

- [ ] **Step 3: Create `schedules.py`**

```python
"""The weekly field-registry chain: artifacts -> registry export -> candidates -> resolve.

Takes the Monday 06:50 UTC slot se_company_info_weekly held ((minute, hour) pairs are
unique across every schedule -- tests/test_schedule_cron_contracts.py). STOPPED by
default like its predecessor; the cutover plan starts it on the prod instance once the
rebuild is verified.

The run config spells out what an automated run must never leave to defaults: ``execute``
for the resolve asset AND for every candidate extractor (a bare run of either is a
preview -- plan 2's CandidateExtractConfig gates exactly like the resolve asset), and the
LLM extractor's model profile (spec 5.3: provider and model are required run config, no
default). The registry export and the three artifact assets have no gate.
"""

from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.fields.jobs import se_company_fields_job
from dagster_v3.defs.se_company.fields.resolve import (
    AUTOMATED_RUN_CONFIG,
    CANDIDATE_ASSETS,
    LLM_CANDIDATES_ASSET,
    RESOLVE_ASSET,
)

# Today's production model, pinned here rather than left to the extractor's defaults so
# a default change can never silently change what the weekly run calls. The values are
# info.DEFAULT_LLM_PROFILE's (which the cutover plan deletes with info.py); the key
# names are the LLM extractor's config class -- the Definitions test validates them.
# ``execute`` rides along: without it the extractor previews and writes nothing.
LLM_CANDIDATES_RUN_CONFIG: dict[str, Any] = {
    "execute": True,
    "llm": {
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "temperature": 0,
        "max_tokens": 6_000,
        "prompt_version": "se-company-info-description-v3",
        "concurrency": 1,
    },
}

se_company_fields_weekly = dg.ScheduleDefinition(
    name="se_company_fields_weekly", job=se_company_fields_job, cron_schedule="50 6 * * 1",
    execution_timezone="UTC", default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"ops": {
        RESOLVE_ASSET: {"config": dict(AUTOMATED_RUN_CONFIG)},
        **{name: {"config": dict(AUTOMATED_RUN_CONFIG)}
           for name in CANDIDATE_ASSETS if name != LLM_CANDIDATES_ASSET},
        LLM_CANDIDATES_ASSET: {"config": dict(LLM_CANDIDATES_RUN_CONFIG)},
    }})

defs = dg.Definitions(schedules=[se_company_fields_weekly])
```

If `dg.validate_run_config` in the test raises for `LLM_CANDIDATES_RUN_CONFIG` (plan 2 named the extractor's config fields differently, e.g. a flat `provider`/`model` instead of a nested `llm`), rename the KEYS of `LLM_CANDIDATES_RUN_CONFIG` to the ones the error names and keep every value; the values are the contract, the key names are plan 2's.

- [ ] **Step 4: Take the old schedule out of `info.py`**

Delete from `src/dagster_v3/defs/se_company/info.py`:

```python
# 06:50 Monday: the (minute, hour) slot must be unique across every schedule, and
# 06:45 is already taken by a Saturday schedule.
se_company_info_weekly = dg.ScheduleDefinition(
    name="se_company_info_weekly", job=se_company_info_job, cron_schedule="50 6 * * 1",
    execution_timezone="UTC", default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"se_company_info_clickhouse": {"config": dict(AUTOMATED_RUN_CONFIG)}}})
```

and rewrite the module's last statement as

```python
defs = dg.Definitions(assets=[se_company_info_clickhouse], jobs=[se_company_info_job, se_company_info_review_job])
```

Replace the comment above `AUTOMATED_RUN_CONFIG` (`# The two automated triggers are the ones that must resolve for real, so both spell` / `# out execute AND the profile they call: an automated run must never depend on the` / `# asset's field defaults, and must never be silently downgraded to a preview.`) with:

```python
# What a real run of this asset carries -- execute AND the profile it may call. Sent by
# the backoffice Pipeline page until the cutover deletes this asset; the automated
# triggers launch the registry-driven resolve now (fields/).
```

- [ ] **Step 5: Register the leaf**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, after the seven `se_company_field_candidates_*` leaves plan 2 added below `ClickhouseLeaf("se_company_info_clickhouse", ("se_company_info",), WEEKLY),` (i.e. after the `se_company_field_candidates_llm` leaf) add:

```python
    # se_company_fields -- the registry-driven resolve (se_company/fields/resolve.py)
    # writes the long resolved table and re-pivots se_company_info. Unscheduled (row-count
    # check only) until the cutover plan starts se_company_fields_weekly; it then becomes
    # WEEKLY and the se_company_info_clickhouse leaf above goes with the old asset.
    ClickhouseLeaf("se_company_field_resolved_clickhouse", ("se_company_field",), None),
```

- [ ] **Step 6: Move the old schedule assertions**

In `tests/test_se_company_info.py`:

1. In `test_definitions_wire_final_jobs_sensor_schedule_and_leaves`, replace

```python
    schedule = repository.get_schedule_def("se_company_info_weekly")
    # 06:45 Monday would collide with the existing "45 6 * * 6" slot the cron contract guards.
    assert schedule.cron_schedule == "50 6 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
```

with

```python
    # The Monday 06:50 slot belongs to se_company_fields_weekly now (fields/schedules.py).
    assert "se_company_info_weekly" not in {schedule.name for schedule in repository.schedule_defs}
```

and the comment `# se_company_info_weekly is RUNNING (phase 7): a missed week must show as stale.` with `# Refreshed weekly by se_company_fields_job now: a missed week must still show as stale.`

2. In `test_the_config_gates_the_run_and_pins_the_profile_the_automation_sends`, change the import to

```python
    from dagster_v3.defs.se_company.info import (
        AUTOMATED_RUN_CONFIG,
        DEFAULT_LLM_PROFILE,
        SECompanyInfoConfig,
    )
```

and replace the tail (from `    # The automated triggers must both spell out execute AND the profile: a` through the closing `"llm": DEFAULT_LLM_PROFILE}}}}`) with

```python
    # What the backoffice Pipeline page sends for a real run of this asset until the
    # cutover deletes it: execute AND the pinned profile. The automated triggers launch
    # the registry-driven resolve now (tests/test_se_company_field_resolve.py).
    assert AUTOMATED_RUN_CONFIG == {"execute": True, "llm": DEFAULT_LLM_PROFILE}
```

- [ ] **Step 7: Run the tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_resolve.py tests/test_se_company_field_sensors.py tests/test_se_company_info.py tests/test_schedule_cron_contracts.py tests/test_clickhouse_leaf_checks.py -q -p no:warnings`
Expected: PASS -- the cron contract sees exactly one schedule at `(50, 6)`; every leaf has its row-count check and the new unscheduled leaf has no freshness check.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: success.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/se_company/fields/schedules.py src/dagster_v3/defs/se_company/info.py \
        src/dagster_v3/defs/common/clickhouse_checks.py tests/test_se_company_field_resolve.py tests/test_se_company_info.py
git commit -m "feat(se): se_company_fields_weekly takes over the Monday slot; leaf for the resolve asset

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 7: the clickhouse-local harness -- scan reasons, resolve, projection, parity, executed

**Files:**
- Create: `tests/test_se_company_field_resolve_clickhouse_local.py`

**Interfaces:**
- Consumes: `resolve.{SELECTION_COLUMNS, build_changed_companies_sql}`, `parity.{PARITY_COLUMNS, build_parity_snapshot_sql, build_parity_sql, build_rows_per_field_source_sql}`, `fields.sql.{render_resolve_sql, render_projection_sql}`, `fields.policies.policy_for`, `fields.registry.{INFO_REGISTRY, field_names, field_by_name}`, `info.INSERT_COLUMNS` (the OLD wide insert list, still importable until the cutover), `tests/se_company_ddl.declared_columns`, `tests/test_se_company_person_clickhouse_local.{_clickhouse_local_command, _literal}`, Docker (`clickhouse/clickhouse-server:26.5`) or a local `clickhouse-local`.
- Produces: nothing importable; the executed proof that every statement this plan runs is accepted by ClickHouse 26.5 under both `join_use_nulls` settings, with the parameters bound server-side (`SET param_<name>`), and that the wide row equals the hand-written Handelsbanken row.

- [ ] **Step 1: Write the harness**

Create `tests/test_se_company_field_resolve_clickhouse_local.py`:

```python
"""Executes the field-registry resolve path end to end in a disposable clickhouse-local:
the changed-company scan for every reason, every field's resolve statement, the wide
projection, and the parity check -- against the migrations' DDL, with the parameters
bound by ClickHouse's own ``SET param_<name>`` (the server-side path the backoffice
takes), twice (join_use_nulls 0 and 1; every LEFT JOIN miss is read through ifNull).

One company, Svenska Handelsbanken (5020077862), with candidates from every source the
info registry names and one reviewer decision; a second company, BETA, with a legal name
from wikidata only, which the scan must never select (spec 8.3: no register name, no
publication). The expected wide row is hand-written below; the array provenance columns
are compared as sets (plan 2 decides their order), everything else column by column.

The resolve statements executed here are rendered by fields.sql -- the same text plan 1's
export writes into se_company_field_registry (this script inserts those rows too, so the
scan's registry-version comparison runs against real rows). The asset reads them back
from that table; the FakeClient tests in test_se_company_field_resolve.py cover that.
"""

import ast
import hashlib
import json
import re
import subprocess
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.fields.parity import (
    PARITY_COLUMNS,
    build_parity_snapshot_sql,
    build_parity_sql,
    build_rows_per_field_source_sql,
)
from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.resolve import SELECTION_COLUMNS, build_changed_companies_sql
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.info import INSERT_COLUMNS as OLD_INSERT_COLUMNS
from tests.se_company_ddl import declared_columns
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
BASE_MIGRATIONS = (
    "000150_corpscout_se_translations.up.sql",  # se_code_labels, the legal-form dictionary
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
    "000300_corpscout_se_company_info_scb_english.up.sql",
    "000301_corpscout_se_company_info_description_sv.up.sql",
    "000304_corpscout_se_company_info_llm_enhanced.up.sql",
    "000305_corpscout_se_code_labels_swedish.up.sql",
    "000306_corpscout_se_company_info_legal_form_label.up.sql",
    "000365_corpscout_se_company_info_esef_enrichment.up.sql",
    "000371_corpscout_se_company_info_field_value.up.sql",
)
# Plan 1's migrations (the three field tables, the eight wide columns, the widened
# decision CHECKs) are numbered after 000372: picked up by number so this file needs no
# edit when they land. Statements aimed at other tables -- and 000377's MATERIALIZED
# VIEW -- are filtered out by _schema_statements.
LATER_MIGRATIONS = tuple(sorted(
    path.name for path in MIGRATIONS_DIR.glob("[0-9]*.up.sql") if path.name > "000372"))
NEEDED_TABLES = frozenset({
    "se_code_labels", "se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata",
    "se_company_info", "se_company_info_field_value", "se_company_info_enrichment_observation",
    "se_company_field_registry", "se_company_field_candidate", "se_company_field",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)

HB = "5020077862"  # Svenska Handelsbanken AB
BETA = "5560125220"  # a legal name from wikidata only: never a register name, never published
LEI = "NHBDILHZTYCNBV5UYZ31"
WIKIDATA_ID = "Q1155005"
SUGGESTION_ID = uuid.UUID(int=7)
DECISION_ID = uuid.UUID(int=1)
DECISION_ID_2 = uuid.UUID(int=2)
RUN_1, RUN_2, RUN_3 = "resolve-run-1", "resolve-run-2", "resolve-run-3"
T_REGISTER = datetime(2026, 8, 1, tzinfo=UTC)
T_ESEF = datetime(2025, 4, 2, tzinfo=UTC)
T_FINANCIAL = datetime(2024, 12, 31, tzinfo=UTC)
T_DOMAIN = datetime(2026, 8, 10, tzinfo=UTC)
T_LLM = datetime(2026, 8, 15, tzinfo=UTC)
T_EXTRACT = datetime(2026, 8, 20, 12, tzinfo=UTC)
T_DECISION = datetime(2026, 8, 21, 9, tzinfo=UTC)
T_OLD_ROW = datetime(2026, 8, 25, tzinfo=UTC)  # the pre-cutover publisher's row: after the decision
T_RESOLVE_1 = datetime(2026, 9, 2, 10, tzinfo=UTC)
T_EXTRACT_2 = datetime(2026, 9, 3, 8, tzinfo=UTC)
T_RESOLVE_2 = datetime(2026, 9, 3, 10, tzinfo=UTC)
T_DECISION_2 = datetime(2026, 9, 4, 9, tzinfo=UTC)
T_RESOLVE_3 = datetime(2026, 9, 4, 10, tzinfo=UTC)
T_REGISTRY = datetime(2026, 8, 1, tzinfo=UTC)
T_REGISTRY_BUMP = datetime(2026, 9, 5, tzinfo=UTC)
NO_CUTOFF = "2099-12-31 23:59:59"
PAST_CUTOFF = "2000-01-01 00:00:00"
LLM_EN = "Handelsbanken is a Swedish bank offering retail and corporate banking across the Nordics."
LLM_SV = "Handelsbanken aer en svensk bank med privat- och foeretagsbank i Norden."
DECISION_SV = "Svenska Handelsbanken aer en svensk fullservicebank."
DECISION_EN_2 = "Handelsbanken is a Swedish full-service bank."
WIKIDATA_EN_2 = "Swedish bank and financial services group"
BV_UID, SCB_UID, WD_UID = f"bv:{HB}", f"scb:{HB}", f"wikidata:{WIKIDATA_ID}"
ESEF_UID, DOMAIN_UID, FIN_UID = "esef:doc-hb-2023", "domain:handelsbanken.com:fp-1", f"bv-fin:{HB}:2024"


def _json(**members: object) -> str:
    return json.dumps(members, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


# (field, source, source_record_uid, value, value_json, observed_at) -- spec 5.1 / 4.2.
HB_CANDIDATES = (
    ("legal_name", "bolagsverket", BV_UID, "Svenska Handelsbanken AB", _json(compare_key="svenska handelsbanken ab"), T_REGISTER),
    ("legal_name", "scb", SCB_UID, "Svenska Handelsbanken AB", _json(compare_key="svenska handelsbanken ab"), T_REGISTER),
    ("legal_name", "wikidata", WD_UID, "Svenska Handelsbanken", _json(compare_key="svenska handelsbanken"), T_REGISTER),
    ("legal_form_code", "bolagsverket", BV_UID, "AB-ORGFO", _json(compare_key="ab-orgfo"), T_REGISTER),
    ("legal_form_code", "scb", SCB_UID, "49", _json(compare_key="49"), T_REGISTER),
    ("status", "bolagsverket", BV_UID, "active", _json(compare_key="active"), T_REGISTER),
    ("status", "scb", SCB_UID, "active", _json(compare_key="active"), T_REGISTER),
    ("incorporation_date", "bolagsverket", BV_UID, "1955-06-14", _json(compare_key="1955-06-14"), T_REGISTER),
    ("incorporation_date", "scb", SCB_UID, "1955-06-14", _json(compare_key="1955-06-14"), T_REGISTER),
    ("description", "llm", str(SUGGESTION_ID), LLM_EN, _json(compare_key=LLM_EN.lower(), language="en"), T_LLM),
    ("description", "esef", ESEF_UID, "Handelsbanken is a Swedish credit institution.",
     _json(compare_key="handelsbanken is a swedish credit institution.", language="en"), T_ESEF),
    ("description", "wikidata", WD_UID, "Swedish bank", _json(compare_key="swedish bank", language="en"), T_REGISTER),
    ("description", "scb", SCB_UID, "Banking.", _json(compare_key="banking.", language="en"), T_REGISTER),
    ("description_sv", "llm", str(SUGGESTION_ID), LLM_SV, _json(compare_key=LLM_SV.lower(), language="sv"), T_LLM),
    ("description_sv", "scb", SCB_UID, "Bankverksamhet.", _json(compare_key="bankverksamhet.", language="sv"), T_REGISTER),
    ("primary_sni_code", "scb", SCB_UID, "64190", _json(compare_key="64190"), T_REGISTER),
    ("primary_nace_code", "scb", SCB_UID, "64.19", _json(compare_key="64.19"), T_REGISTER),
    ("industry_label_en", "scb", SCB_UID, "Other monetary intermediation", _json(compare_key="other monetary intermediation"), T_REGISTER),
    ("website", "domains", DOMAIN_UID, "https://www.handelsbanken.com", _json(compare_key="handelsbanken.com"), T_DOMAIN),
    ("website", "wikidata", WD_UID, "https://www.handelsbanken.se", _json(compare_key="handelsbanken.se"), T_REGISTER),
    ("employee_count", "bolagsverket", FIN_UID, "11000",
     _json(compare_key="11000", count=11000, as_of="2024-12-31", period="FY2024"), T_FINANCIAL),
    ("employee_count", "wikidata", WD_UID, "12000",
     _json(compare_key="12000", count=12000, as_of="2023-12-31", period="2023"), T_REGISTER),
    ("latest_revenue", "bolagsverket", FIN_UID, "58000000000.00 SEK",
     _json(compare_key="SEK:2024:58000000000.00", amount="58000000000.00", currency="SEK",
           amount_usd="5500000000.00", fiscal_year=2024, period_end="2024-12-31"), T_FINANCIAL),
)
BETA_CANDIDATES = (
    ("legal_name", "wikidata", "wikidata:Q2", "Beta AB", _json(compare_key="beta ab"), T_REGISTER),
)
# Which source wins each field under the registry's precedence (spec 4.2); description_sv
# is decided by the reviewer and has no candidate winner.
WINNERS = {
    "legal_name": "bolagsverket", "legal_form_code": "bolagsverket", "status": "bolagsverket",
    "incorporation_date": "bolagsverket", "description": "llm", "primary_sni_code": "scb",
    "primary_nace_code": "scb", "industry_label_en": "scb", "website": "domains",
    "employee_count": "bolagsverket", "latest_revenue": "bolagsverket",
}
WINNING_ROWS = tuple(row for row in HB_CANDIDATES if WINNERS.get(row[0]) == row[1])


def _evidence_hash(row: tuple) -> str:
    """The candidate table's MATERIALIZED evidence_hash (spec 5.1), recomputed in Python."""
    field, source, uid, value, value_json, _ = row
    return hashlib.sha256("\n".join((field, source, uid, value, value_json)).encode()).hexdigest()


EXPECTED_UIDS = {row[2] for row in WINNING_ROWS}
EXPECTED_HASHES = {_evidence_hash(row) for row in WINNING_ROWS}
WIDE_COLUMNS = tuple(c for c in declared_columns("se_company_info") if c != "evidence_set_hash")
EXPECTED_WIDE = {
    "company_id": HB, "legal_name": "Svenska Handelsbanken AB", "legal_form_code": "AB-ORGFO",
    "legal_form_label_en": "Limited company (aktiebolag)", "legal_form_label_sv": "Aktiebolag",
    "status": "active", "incorporation_date": "1955-06-14",
    "description": LLM_EN, "description_sv": DECISION_SV, "description_language": "en", "llm_enhanced": "true",
    "description_source_count": "4", "primary_nace_code": "64.19", "primary_sni_code": "64190",
    "wikidata_id": WIKIDATA_ID, "lei": LEI,
    "suggestion_id": str(SUGGESTION_ID), "model_provider": "deepseek", "model_name": "deepseek-v4-flash",
    "prompt_version": "se-company-info-description-v3", "source_run_id": RUN_1,
    "resolved_at": "2026-09-02 10:00:00.000",
    "industry_label_en": "Other monetary intermediation", "website": "https://www.handelsbanken.com",
    "employee_count": "11000", "employee_count_as_of": "2024-12-31",
    "latest_revenue_currency": "SEK", "latest_revenue_fiscal_year": "2024",
}
EXPECTED_DECIMALS = {"latest_revenue_amount": Decimal("58000000000.00"), "latest_revenue_amount_usd": Decimal("5500000000.00")}
EXPECTED_SETS = {
    "description_sources": {"llm", "esef", "wikidata", "scb"},
    "description_source_record_uids": {str(SUGGESTION_ID), ESEF_UID, WD_UID, SCB_UID},
    "correction_ids": {str(DECISION_ID)},
    "source_record_uids": EXPECTED_UIDS,
    "evidence_hashes": EXPECTED_HASHES,
}
SCAN_LABELS = ("scan_never_published", "scan_settled_1", "scan_new_candidates", "scan_settled_2",
               "scan_decision_pending", "scan_settled_3", "scan_resolve_all", "scan_resolve_all_past_cutoff",
               "scan_version_changed")


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order."""
    statements: list[str] = []
    for name in migrations:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def _string_array(values) -> str:
    return "[" + ", ".join(_literal(str(v)) for v in values) + "]"


def _param_text(value: object) -> str:
    """The text ClickHouse parses for a query parameter of the declared type."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, list | tuple):
        return "[" + ",".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in value) + "]"
    return str(value)


def _bound(sql: str, **values: object) -> str:
    """``sql`` preceded by one ``SET param_<name>`` per value: ClickHouse's own server-side
    binding, the path the backoffice's clickhouse-js query_params takes."""
    sets = "".join(f"SET param_{name} = {_literal(_param_text(value))};\n" for name, value in values.items())
    return sets + sql + ";\n"


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _marked_bound(label: str, sql: str, **values: object) -> str:
    return f"SELECT '@@{label}';\n" + _bound(sql + " FORMAT TSV", **values)


def _candidates_sql(company: str, rows: tuple, extracted_at: datetime) -> str:
    values = ",\n".join(
        f"('{company}', '{field}', '{source}', {_literal(uid)}, {_literal(value)}, {_literal(value_json)}, "
        f"{_literal(observed_at)}, {_literal(extracted_at)}, 'v1', 'extract-1')"
        for field, source, uid, value, value_json, observed_at in rows)
    return ("INSERT INTO corpscout.se_company_field_candidate (company_id, field, source, source_record_uid, value, "
            f"value_json, observed_at, extracted_at, extractor_version, source_run_id) VALUES\n{values};\n")


def _decision_sql(value_id: uuid.UUID, field: str, value: str, created_at: datetime) -> str:
    return ("INSERT INTO corpscout.se_company_info_field_value (value_id, company_id, field, value, source, source_ref, "
            f"source_at, decided_by, note, created_at) VALUES ('{value_id}', '{HB}', '{field}', {_literal(value)}, "
            f"'reviewer', '', NULL, 'backoffice', 'harness', {_literal(created_at)});\n")


def _registry_row(spec, *, policy_version: str, stamp: datetime) -> str:
    policy = policy_for(spec)
    return (f"('{INFO_REGISTRY.datatype}', '{INFO_REGISTRY.country}', '{spec.name}', '{spec.value_type}', "
            f"'{spec.display_group}', {_literal(spec.structured)}, {_literal(spec.python_only)}, "
            f"{_string_array(spec.sources)}, '{policy.name}', {_literal(policy_version)}, "
            f"{_literal(render_resolve_sql(INFO_REGISTRY, spec))}, '{INFO_REGISTRY.version}', {_literal(stamp)})")


REGISTRY_INSERT = ("INSERT INTO corpscout.se_company_field_registry (datatype, country, field, value_type, display_group, "
                   "structured, python_only, sources, policy_name, policy_version, resolve_sql, registry_version, version) VALUES\n")


def _registry_rows_sql() -> str:
    """What plan 1's export asset writes: one row per field plus the projection row."""
    rows = [_registry_row(spec, policy_version=policy_for(spec).version, stamp=T_REGISTRY) for spec in INFO_REGISTRY.fields]
    rows.append(f"('info', 'SE', '*', 'projection', '', 0, 0, [], '', '', "
                f"{_literal(render_projection_sql(INFO_REGISTRY))}, '{INFO_REGISTRY.version}', {_literal(T_REGISTRY)})")
    return REGISTRY_INSERT + ",\n".join(rows) + ";\n"


def _registry_bump_sql() -> str:
    """A newer export of legal_name under a bumped policy version: what a policy edit does."""
    spec = field_by_name(INFO_REGISTRY, "legal_name")
    return REGISTRY_INSERT + _registry_row(spec, policy_version=f"{policy_for(spec).name}-v2", stamp=T_REGISTRY_BUMP) + ";\n"


def _resolve_pass(*, run_id: str, resolved_at: datetime) -> str:
    """What one batch of the asset does for [HB]: every field's statement in registry
    order, then the projection -- the same text the registry rows above carry."""
    parts = [_bound(render_resolve_sql(INFO_REGISTRY, spec), field=spec.name, company_ids=[HB],
                    source_run_id=run_id, resolved_at=resolved_at)
             for spec in INFO_REGISTRY.fields]
    parts.append(_bound(render_projection_sql(INFO_REGISTRY), company_ids=[HB]))
    return "".join(parts)


def _scan(label: str, *, resolve_all: int = 0, resolve_all_before: str = NO_CUTOFF) -> str:
    return _marked_bound(label, build_changed_companies_sql(INFO_REGISTRY), company_ids=[], all_companies=1,
                         resolve_all=resolve_all, resolve_all_before=resolve_all_before, after_company_id="",
                         page_size=10)


def _old_row_values() -> str:
    """The pre-cutover publisher's row for HB, in the OLD insert order: every value the
    parity check must find equal, except primary_sni_code -- 64191 is the one deliberate
    mismatch, so the check is seen counting, not just passing."""
    return (f"'{HB}', 'Svenska Handelsbanken AB', 'AB-ORGFO', 'Limited company (aktiebolag)', 'Aktiebolag', "
            f"'active', '1955-06-14', {_literal(LLM_EN)}, {_literal(DECISION_SV)}, 'en', true, "
            f"['esef', 'wikidata', 'scb'], ['{ESEF_UID}', '{WD_UID}', '{SCB_UID}'], 3, "
            f"'64.19', '64191', '{WIKIDATA_ID}', '{LEI}', ['{SCB_UID}'], ['{'a' * 64}'], ['{DECISION_ID}'], "
            f"'{SUGGESTION_ID}', 'deepseek', 'deepseek-v4-flash', 'se-company-info-description-v3', 'old-run', "
            f"{_literal(T_OLD_ROW)}")


WIDE_ROW_SQL = ("SELECT " + ", ".join(f"ifNull(toString({c}), '') AS {c}" for c in WIDE_COLUMNS)
                + f" FROM corpscout.se_company_info FINAL WHERE company_id = '{HB}'")
RESOLVED_ROWS_SQL = ("SELECT field, source, ifNull(toString(decision_id), ''), value, toString(candidate_count), "
                     "toString(arraySort(agreeing_sources)), policy_version, registry_version, source_run_id "
                     f"FROM corpscout.se_company_field FINAL WHERE company_id = '{HB}' ORDER BY field")

FIXTURE = f"""
INSERT INTO corpscout.se_code_labels (code_type, code, label_en, label_sv, version)
VALUES ('legal_form', 'AB-ORGFO', 'Limited company (aktiebolag)', 'Aktiebolag', toDateTime('2026-08-01 00:00:00'));

INSERT INTO corpscout.se_company_info_wikidata
    (company_id, source_record_uid, observed_at, source_run_id, wikidata_id, wikidata_url, name)
VALUES ('{HB}', '{WD_UID}', {_literal(T_REGISTER)}, 'fixture', '{WIKIDATA_ID}',
        'https://www.wikidata.org/wiki/{WIKIDATA_ID}', 'Handelsbanken');

INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id, source_document_id, lei, entity_name, fiscal_year,
     company_description, description_language, description_confidence, products_and_services_json, business_segments_json)
VALUES ('{HB}', '{ESEF_UID}', {_literal(T_ESEF)}, 'fixture', 'doc-hb-2023', '{LEI}', 'Svenska Handelsbanken AB', 2023,
        'Handelsbanken is a Swedish credit institution.', 'en', 0.9, '[]', '[]');

INSERT INTO corpscout.se_company_info_enrichment_observation
    (suggestion_id, company_id, input_hash, suggestion, raw_response, model_provider, model_name, prompt_version,
     prompt_tokens, completion_tokens, source_run_id, created_at)
VALUES ('{SUGGESTION_ID}', '{HB}', '{'f' * 64}',
        {_literal(_json(description=LLM_EN, description_sv=LLM_SV, language="en", rationale="merged"))},
        '', 'deepseek', 'deepseek-v4-flash', 'se-company-info-description-v3', 450, 450, 'llm-run', {_literal(T_LLM)});
""".strip() + "\n"


def _script(*, join_use_nulls: int) -> str:
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;\n")
    parts.append(";\n".join(_schema_statements((*BASE_MIGRATIONS, *LATER_MIGRATIONS))) + ";\n")
    parts.append(FIXTURE)
    parts.append(_candidates_sql(HB, HB_CANDIDATES, T_EXTRACT))
    parts.append(_candidates_sql(BETA, BETA_CANDIDATES, T_EXTRACT))
    parts.append(_decision_sql(DECISION_ID, "description_sv", DECISION_SV, T_DECISION))
    parts.append(_registry_rows_sql())

    # Nothing published: HB is selected for being new (and, by construction, for its
    # candidates and its decision being newer than the epoch); BETA never is.
    parts.append(_scan("scan_never_published"))

    # The old publisher's row and the parity snapshot taken from it, before the rebuild.
    parts.append(f"INSERT INTO corpscout.se_company_info ({', '.join(OLD_INSERT_COLUMNS)}) VALUES ({_old_row_values()});\n")
    parts.append(build_parity_snapshot_sql() + ";\n")

    # Pass 1: every field, then the projection -- the rebuild.
    parts.append(_resolve_pass(run_id=RUN_1, resolved_at=T_RESOLVE_1))
    parts.append(_marked("wide_row_1", WIDE_ROW_SQL))
    parts.append(_marked("resolved_rows_1", RESOLVED_ROWS_SQL))
    parts.append(_marked("parity_1", build_parity_sql()))
    parts.append(_marked("rows_per_field_source_1", build_rows_per_field_source_sql()))
    parts.append(_scan("scan_settled_1"))

    # A re-extracted candidate (newer extracted_at, new text) re-selects HB; a pass settles it.
    parts.append(_candidates_sql(HB, (("description", "wikidata", WD_UID, WIKIDATA_EN_2,
                                       _json(compare_key=WIKIDATA_EN_2.lower(), language="en"), T_EXTRACT_2),), T_EXTRACT_2))
    parts.append(_scan("scan_new_candidates"))
    parts.append(_resolve_pass(run_id=RUN_2, resolved_at=T_RESOLVE_2))
    parts.append(_scan("scan_settled_2"))

    # A decision after publication re-selects HB; the next pass publishes it and settles.
    parts.append(_decision_sql(DECISION_ID_2, "description", DECISION_EN_2, T_DECISION_2))
    parts.append(_scan("scan_decision_pending"))
    parts.append(_resolve_pass(run_id=RUN_3, resolved_at=T_RESOLVE_3))
    parts.append(_scan("scan_settled_3"))
    parts.append(_marked("wide_row_3", WIDE_ROW_SQL))

    # resolve_all re-selects a settled company -- unless its resolved_at is past the cutoff.
    parts.append(_scan("scan_resolve_all", resolve_all=1))
    parts.append(_scan("scan_resolve_all_past_cutoff", resolve_all=1, resolve_all_before=PAST_CUTOFF))

    # A policy version bump in the registry export re-selects every company resolved under the old one.
    parts.append(_registry_bump_sql())
    parts.append(_scan("scan_version_changed"))
    return "".join(parts)


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(command, input=_script(join_use_nulls=request.param),
                                   capture_output=True, text=True, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def _flags(row: list[str]) -> tuple[bool, ...]:
    """The reason flags of one scan row, in SELECTION_COLUMNS order (UInt8 or Bool text)."""
    assert len(row) == len(SELECTION_COLUMNS)
    return tuple(value in ("1", "true") for value in row[1:])


def _scan_rows(sections: dict[str, list[list[str]]], label: str) -> dict[str, tuple[bool, ...]]:
    return {row[0]: _flags(row) for row in sections[label]}


def test_the_scan_selects_only_companies_with_a_register_name_and_names_the_reason(sections) -> None:
    """Reasons in SELECTION_REASONS order: never_published, new_candidates, decision_pending,
    version_changed. They overlap for a never-published company (its epoch resolved_at is
    older than everything). BETA has only a wikidata legal name and is never selected."""
    assert _scan_rows(sections, "scan_never_published") == {HB: (True, True, True, False)}
    assert _scan_rows(sections, "scan_settled_1") == {}
    assert _scan_rows(sections, "scan_new_candidates") == {HB: (False, True, False, False)}
    assert _scan_rows(sections, "scan_settled_2") == {}
    assert _scan_rows(sections, "scan_decision_pending") == {HB: (False, False, True, False)}
    assert _scan_rows(sections, "scan_settled_3") == {}
    for label in SCAN_LABELS:
        assert BETA not in _scan_rows(sections, label), label


def test_resolve_all_honours_its_cutoff(sections) -> None:
    assert _scan_rows(sections, "scan_resolve_all") == {HB: (False, False, False, False)}
    assert _scan_rows(sections, "scan_resolve_all_past_cutoff") == {}


def test_a_registry_policy_bump_re_selects_every_resolved_company(sections) -> None:
    assert _scan_rows(sections, "scan_version_changed") == {HB: (False, False, False, True)}


def test_the_resolved_rows_carry_winner_decision_agreement_and_versions(sections) -> None:
    rows = {row[0]: row for row in sections["resolved_rows_1"]}
    assert set(rows) == set(field_names(INFO_REGISTRY))
    for name, source in WINNERS.items():
        field, resolved_source, decision_id, value, *_ = rows[name]
        assert (resolved_source, decision_id) == (source, ""), name
        assert value == next(row[3] for row in WINNING_ROWS if row[0] == name), name
    # Rank order: bolagsverket beats scb and wikidata; the two registers agree on the name.
    assert rows["legal_name"][4:6] == ["3", "['bolagsverket','scb']"]
    assert rows["description"][4:6] == ["4", "['llm']"]
    assert rows["website"][4] == "2"
    # The decision beats the llm winner; its row names the decision and the reviewer.
    assert rows["description_sv"][1:4] == ["reviewer", str(DECISION_ID), DECISION_SV]
    # Every row is stamped with what resolved it.
    for name, row in rows.items():
        assert row[6] == policy_for(field_by_name(INFO_REGISTRY, name)).version, name
        assert row[7] == INFO_REGISTRY.version and row[8] == RUN_1, name


def test_the_wide_row_equals_the_expected_handelsbanken_row(sections) -> None:
    [values] = sections["wide_row_1"]
    row = dict(zip(WIDE_COLUMNS, values, strict=True))
    for column, expected in EXPECTED_WIDE.items():
        assert row[column] == expected, column
    for column, expected in EXPECTED_DECIMALS.items():
        assert Decimal(row[column]) == expected, column
    for column, expected in EXPECTED_SETS.items():
        assert set(ast.literal_eval(row[column])) - {""} == expected, column
    # Every deployed column has an expectation above: a new column cannot slip in unasserted.
    assert set(WIDE_COLUMNS) == set(EXPECTED_WIDE) | set(EXPECTED_DECIMALS) | set(EXPECTED_SETS)


def test_a_decision_on_the_description_replaces_the_model_text_and_its_provenance(sections) -> None:
    [values] = sections["wide_row_3"]
    row = dict(zip(WIDE_COLUMNS, values, strict=True))
    assert row["description"] == DECISION_EN_2 and row["description_sv"] == DECISION_SV
    assert row["llm_enhanced"] == "false" and row["suggestion_id"] == ""
    assert set(ast.literal_eval(row["correction_ids"])) == {str(DECISION_ID), str(DECISION_ID_2)}
    assert row["source_run_id"] == RUN_3 and row["resolved_at"] == "2026-09-04 10:00:00.000"
    # The re-extracted wikidata text is a candidate (counted), never the published one.
    assert row["description_source_count"] == "4"


def test_the_parity_check_reports_per_column_mismatches(sections) -> None:
    [values] = sections["parity_1"]
    named = dict(zip(PARITY_COLUMNS, values, strict=True))
    counts = {name: int(named[name]) for name in PARITY_COLUMNS if not name.endswith("_samples")}
    expected = dict.fromkeys(counts, 0)
    expected.update({"companies_compared": 1, "primary_sni_code": 1})
    assert counts == expected
    assert ast.literal_eval(named["primary_sni_code_samples"]) == [HB]
    assert all(ast.literal_eval(named[name]) == [] for name in PARITY_COLUMNS
               if name.endswith("_samples") and name != "primary_sni_code_samples")
    per_source = [(field, source, int(rows)) for field, source, rows in sections["rows_per_field_source_1"]]
    assert per_source == sorted([*((field, source, 1) for field, source in WINNERS.items()), ("description_sv", "reviewer", 1)])
```

- [ ] **Step 2: Run the harness**

Run (Docker must be running, or a `clickhouse`/`clickhouse-local` binary on PATH): `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve_clickhouse_local.py -q -p no:warnings -x`
Expected: PASS (14 tests: 7 per `join_use_nulls` setting). A `returncode != 0` assertion prints clickhouse-local's stderr with the failing statement: a `Substitution ... is not set` names a placeholder plan 2's statement uses that `_resolve_pass` does not bind (add it there); a `Cannot parse` on `param_company_ids` means the array text is wrong (compare with `_param_text`); a projection column error names a plan-2 contract gap -- report it, do not edit `fields/sql.py`. If `test_the_wide_row_equals_the_expected_handelsbanken_row` fails on a provenance value the spec leaves to plan 2 (`description_language` for a decided description, the `deterministic` model columns), the expected row is what changes, with a comment naming the plan-2 rule it now pins.

- [ ] **Step 3: Commit**

```bash
git add tests/test_se_company_field_resolve_clickhouse_local.py
git commit -m "test(se): execute the field resolve, projection, scan and parity in clickhouse-local

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 8: `se_companies_serving` gains the eight wide columns (staged swap)

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py:215-365` (`build_se_companies_serving_sql`)
- Create: `corpscout/clickhouse/migrations/000377_corpscout_se_companies_serving_field_registry_columns.up.sql` and `.down.sql` (number = `max(existing) + 1` at execution; if it is not 000377, use that number everywhere in this task)
- Modify: `tests/test_se_companies_serving_mv.py` (the drift pin moves to the new migration), `tests/test_se_companies_serving_sql.py` (executable suite), `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` + content test)
- Modify: `/Users/graovic/.claude/projects/-Users-graovic-pulsarpoint-ppoint-companycollect/memory/se-companies-serving-view.md` (one paragraph)

**Interfaces:**
- Consumes: the eight `se_company_info` columns from plan 1's migration (`industry_label_en String DEFAULT ''`, `website Nullable(String)`, `employee_count Nullable(UInt64)`, `employee_count_as_of Nullable(Date32)`, `latest_revenue_amount Nullable(Decimal128(2))`, `latest_revenue_currency LowCardinality(String) DEFAULT ''`, `latest_revenue_amount_usd Nullable(Decimal128(2))`, `latest_revenue_fiscal_year Nullable(UInt16)`); the 000347 staged-swap precedent; the 000366 cadence (`REFRESH EVERY 1 HOUR OFFSET 45 MINUTE`).
- Produces: `build_se_companies_serving_sql()` projecting the eight columns (between `has_job_ads` and `addresses`, in the order above; `website` folded to `''`, `latest_revenue_currency` through `toString`, the numeric/date ones as they are); the migration; the drift pin at the new migration.
- Ordering: the cutover plan applies this migration AFTER the resolve backfill (its step 5) -- the columns exist from plan 1 but are empty until then, and a `_next` refresh over 3.5M rows is not worth spending on empty columns.

- [ ] **Step 1: Write the failing tests**

In `tests/test_se_companies_serving_mv.py` change the constants and the docstring's first line to the new migration:

```python
"""Migration 000377: the field-registry wide columns served, pinned to the builder.
```

```python
MIGRATION = "000377_corpscout_se_companies_serving_field_registry_columns"
```

In `test_the_pin_is_not_vacuous`, after the `company_job_history` assertion add:

```python
    # The field-registry columns (spec 2026-09-02 section 10), straight off se_company_info.
    for column in ("industry_label_en", "website", "employee_count", "employee_count_as_of",
                   "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd",
                   "latest_revenue_fiscal_year"):
        assert f"\n  {column},\n" in embedded, column
    assert "ifNull(i.website, '') AS website" in embedded
    assert "toString(i.latest_revenue_currency) AS latest_revenue_currency" in embedded
```

In `test_the_up_migration_is_a_staged_swap_waited_on_before_the_rename` replace `assert "REFRESH EVERY 15 MINUTE" in create` with:

```python
    # 000366 moved the cadence to hourly; a _next built at 15 minutes would silently revert it.
    assert "REFRESH EVERY 1 HOUR OFFSET 45 MINUTE" in create
    assert "REFRESH EVERY 15 MINUTE" not in create
```

Rename `test_the_down_migration_swaps_back_restarts_and_discards_the_eodhd_render` to `test_the_down_migration_swaps_back_restarts_and_discards_the_registry_render` and change its last line to `assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_registry_discard" in down`.

In `tests/test_clickhouse_migrations.py` append `"000377_corpscout_se_companies_serving_field_registry_columns",` as the last entry of `EXPECTED_MIGRATIONS` (after whatever plans 1 and the cutover added), and append this content test at the end of the file:

```python
def test_se_companies_serving_serves_the_field_registry_columns_by_staged_swap() -> None:
    """000347's recipe, verbatim: stop the live view, build _next under the CURRENT
    cadence (000366's hourly), wait for its first refresh, one atomic RENAME. The
    embedded SELECT is the builder's render (drift-pinned by test_se_companies_serving_mv.py)."""
    up = _migration_sql("000377_corpscout_se_companies_serving_field_registry_columns.up.sql")
    down = _migration_sql("000377_corpscout_se_companies_serving_field_registry_columns.down.sql")

    assert "SYSTEM STOP VIEW corpscout.se_companies_serving;" in up
    assert ("CREATE MATERIALIZED VIEW corpscout.se_companies_serving_next\n"
            "REFRESH EVERY 1 HOUR OFFSET 45 MINUTE\nENGINE = MergeTree\nORDER BY company_id\nAS WITH") in up
    for column in ("industry_label_en", "website", "employee_count", "employee_count_as_of",
                   "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd",
                   "latest_revenue_fiscal_year"):
        assert f"\n  {column},\n" in up, column
    assert "SYSTEM WAIT VIEW corpscout.se_companies_serving_next;" in up
    assert "corpscout.se_companies_serving TO corpscout.se_companies_serving_retired" in up
    assert "corpscout.se_companies_serving_next TO corpscout.se_companies_serving" in up
    assert "DROP" not in "\n".join(line.split("--")[0] for line in up.splitlines()).upper()
    assert "corpscout.se_companies_serving_retired TO corpscout.se_companies_serving" in down
    assert "SYSTEM START VIEW corpscout.se_companies_serving;" in down
    assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_registry_discard;" in down
```

In `tests/test_se_companies_serving_sql.py`: add `from decimal import Decimal` to the imports; add `SCALED = "5560000066"` under `NOADDRESS`; after the `INFO_COLUMNS` constant add

```python
SCALED_COLUMNS = INFO_COLUMNS + (
    ", industry_label_en, website, employee_count, employee_count_as_of, latest_revenue_amount, "
    "latest_revenue_currency, latest_revenue_amount_usd, latest_revenue_fiscal_year"
)


def _scaled_row() -> str:
    """A company the field registry resolved: every new column populated."""
    return _info_row(SCALED, "Scaled AB")[:-1] + (
        ", 'Other monetary intermediation', 'https://www.scaled.se', 11000, '2024-12-31', "
        "58000000000.00, 'SEK', 5500000000.00, 2024)"
    )
```

in `_script`, right after the 000306 ALTER string add

```python
        # The field registry's eight wide columns (spec 2026-09-02 section 8.3), replayed the
        # same way; every pre-existing row reads them as '' / NULL.
        "ALTER TABLE corpscout.se_company_info "
        "ADD COLUMN IF NOT EXISTS industry_label_en String DEFAULT '', "
        "ADD COLUMN IF NOT EXISTS website Nullable(String), "
        "ADD COLUMN IF NOT EXISTS employee_count Nullable(UInt64), "
        "ADD COLUMN IF NOT EXISTS employee_count_as_of Nullable(Date32), "
        "ADD COLUMN IF NOT EXISTS latest_revenue_amount Nullable(Decimal128(2)), "
        "ADD COLUMN IF NOT EXISTS latest_revenue_currency LowCardinality(String) DEFAULT '', "
        "ADD COLUMN IF NOT EXISTS latest_revenue_amount_usd Nullable(Decimal128(2)), "
        "ADD COLUMN IF NOT EXISTS latest_revenue_fiscal_year Nullable(UInt16);",
```

and right after the `INSERT INTO corpscout.se_company_info ({INFO_COLUMNS}) VALUES ...` element add

```python
        f"INSERT INTO corpscout.se_company_info ({SCALED_COLUMNS}) VALUES\n{_scaled_row()};",
```

then change `test_one_row_per_company_including_the_addressless` to `assert set(rows) == {COARSE, PRECISE, NOSERVED, POSTAL_BOX, NOADDRESS, SCALED}` and append

```python
def test_the_field_registry_columns_are_served(rows: dict[str, dict]) -> None:
    """The eight columns come straight off se_company_info: strings folded to '' like every
    other served string, numbers and dates NULL when unresolved. JSONEachRow quotes UInt64
    (output_format_json_quote_64bit_integers) and prints Decimal as a number."""
    row = rows[SCALED]
    assert row["industry_label_en"] == "Other monetary intermediation"
    assert row["website"] == "https://www.scaled.se"
    assert int(row["employee_count"]) == 11000 and row["employee_count_as_of"] == "2024-12-31"
    assert Decimal(str(row["latest_revenue_amount"])) == Decimal("58000000000.00")
    assert row["latest_revenue_currency"] == "SEK"
    assert Decimal(str(row["latest_revenue_amount_usd"])) == Decimal("5500000000.00")
    assert row["latest_revenue_fiscal_year"] == 2024
    unresolved = rows[COARSE]
    assert unresolved["industry_label_en"] == "" and unresolved["website"] == ""
    assert unresolved["latest_revenue_currency"] == ""
    for column in ("employee_count", "employee_count_as_of", "latest_revenue_amount",
                   "latest_revenue_amount_usd", "latest_revenue_fiscal_year"):
        assert unresolved[column] is None, column
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q -p no:warnings -k "serving or migration_files"`
Expected: FAIL -- `FileNotFoundError` for the 000377 files, and the ledger's file-list test naming the missing entry.

- [ ] **Step 3: Extend the builder**

In `src/dagster_v3/defs/sweden_company/companies_current.py`, `build_se_companies_serving_sql`: in the OUTER select, after the line `  has_job_ads,` insert

```
  industry_label_en,
  website,
  employee_count,
  employee_count_as_of,
  latest_revenue_amount,
  latest_revenue_currency,
  latest_revenue_amount_usd,
  latest_revenue_fiscal_year,
```

and in the INNER select, after `    toUInt8(i.company_id IN ({JOB_ADS_SET})) AS has_job_ads,` insert

```
    i.industry_label_en AS industry_label_en,
    ifNull(i.website, '') AS website,
    i.employee_count AS employee_count,
    i.employee_count_as_of AS employee_count_as_of,
    i.latest_revenue_amount AS latest_revenue_amount,
    toString(i.latest_revenue_currency) AS latest_revenue_currency,
    i.latest_revenue_amount_usd AS latest_revenue_amount_usd,
    i.latest_revenue_fiscal_year AS latest_revenue_fiscal_year,
```

Add to the docstring, after the flag-semantics paragraph: `The field-registry columns (industry label, website, employee count and its as-of date, latest revenue with currency, USD twin and fiscal year -- spec 2026-09-02 section 10) are served straight off se_company_info: strings folded to '' like every other served string, numbers and dates NULL until the registry resolve has run for the company.`

- [ ] **Step 4: Write the migration from the builder's render**

Run from `corpscout/services/dagster_v3` (replace `000377` if `ls ../../clickhouse/migrations | tail -2` says the next free number is different):

```bash
uv run --frozen --no-sync python - <<'EOF'
from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql

root = Path("../../clickhouse/migrations")
name = "000377_corpscout_se_companies_serving_field_registry_columns"
up = f"""CREATE DATABASE IF NOT EXISTS corpscout;

-- Serves the field registry's eight wide columns (industry_label_en, website, employee_count,
-- employee_count_as_of, latest_revenue_amount, latest_revenue_currency, latest_revenue_amount_usd,
-- latest_revenue_fiscal_year -- spec 2026-09-02 section 10) straight off se_company_info. Same
-- staged swap as 000347, SYSTEM STOP VIEW guard included; the _next view carries the CURRENT
-- cadence (000366: hourly, offset 45) rather than 000347's 15 minutes. Applied AFTER the
-- registry resolve backfill (cutover step 5): the columns exist from the field-table migration
-- but are empty until then.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

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

-- Swaps the pre-registry view (000347's render, parked under _retired by the up file) back
-- under the serving name, restarts its refresh loop (the up file stopped it), and discards
-- the registry-columns render. Only meaningful while _retired still exists -- after the
-- follow-up drop, roll forward instead.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_registry_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_registry_discard;
"""
(root / f"{name}.up.sql").write_text(up, encoding="utf-8")
(root / f"{name}.down.sql").write_text(down, encoding="utf-8")
print("wrote", name)
EOF
```

(000348 dropped the previous `_retired`, so the name is free; the follow-up drop of this `_retired` is the cutover plan's, after the swap is verified.)

- [ ] **Step 5: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py tests/test_se_companies_serving_sql.py -q -p no:warnings`
Expected: PASS (the drift pin at 000377; the executable suite runs through clickhouse-local/Docker twice and serves the six companies).

- [ ] **Step 6: Record the change in the serving memory note**

Append to `/Users/graovic/.claude/projects/-Users-graovic-pulsarpoint-ppoint-companycollect/memory/se-companies-serving-view.md`, before the `Related:` line:

```
**Field-registry columns (000377, 2026-09)**: industry_label_en, website, employee_count(+_as_of), latest_revenue_amount/currency/amount_usd/fiscal_year projected straight off se_company_info (SE field registry plan 3, spec section 10). Staged swap per 000347 with the 000366 hourly cadence on _next; applied AFTER the registry resolve backfill (cutover step 5). Drift pin now at 000377; `_retired` dropped by the cutover plan's follow-up.
```

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/sweden_company/companies_current.py \
        ../../clickhouse/migrations/000377_corpscout_se_companies_serving_field_registry_columns.up.sql \
        ../../clickhouse/migrations/000377_corpscout_se_companies_serving_field_registry_columns.down.sql \
        tests/test_se_companies_serving_mv.py tests/test_se_companies_serving_sql.py tests/test_clickhouse_migrations.py
git commit -m "feat(se): se_companies_serving serves the field-registry columns (000377 staged swap)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 9: whole-suite verification and handoff

**Files:**
- No source changes. Reads: every file this plan touched.

**Interfaces:**
- Consumes: everything above.
- Produces: the green suite the cutover plan (plan 4) and the backoffice plan (plan 5) start from, and the name list they import (the Cross-plan contract's "Produced" table).

- [ ] **Step 1: Run the unit suites this plan touched**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_common.py tests/test_se_company_field_resolve.py tests/test_se_company_field_parity.py tests/test_se_company_field_sensors.py tests/test_se_company_info.py tests/test_se_company_layout.py tests/test_schedule_cron_contracts.py tests/test_clickhouse_leaf_checks.py tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: PASS, no skips other than the pre-existing ones.

- [ ] **Step 2: Run the integration harnesses**

Run (Docker running): `uv run --frozen --no-sync pytest tests/test_se_company_field_resolve_clickhouse_local.py tests/test_se_company_info_clickhouse_local.py tests/test_se_companies_serving_sql.py -q -p no:warnings`
Expected: PASS -- the old info harness still passes beside the new one (both read the same migrations directory; the new field tables and the eight wide columns are additive).

- [ ] **Step 3: Load the definitions the way the daemon does**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: success. Then `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg list defs | grep -E "se_company_field|se_company_fields|se_company_info_field_value_sensor|se_company_info_weekly"` and confirm: asset `se_company_field_resolved_clickhouse`; jobs `se_company_field_resolve_job`, `se_company_fields_job`; sensors `se_company_info_field_value_sensor`, `se_company_field_candidate_sensor`; schedule `se_company_fields_weekly`; check `se_company_field_parity_check`; and NO `se_company_info_weekly`.

- [ ] **Step 4: Run the whole dagster_v3 suite once**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests -q -p no:warnings -m "not integration"`
Expected: PASS. A failure outside the files this plan touched is a pre-existing one: note it in the handoff, do not fix it here.

- [ ] **Step 5: Confirm the tree is clean and hand off**

Run: `git status --short -- src tests ../../clickhouse/migrations` -- expected: empty (every change committed in Tasks 1-8). Then `git log --oneline -8` -- expected: the eight commits of this plan in order.

Handoff to the cutover plan (plan 4), in this order: stop `se_company_info_field_value_sensor` on the prod instance BEFORE deploying this branch (decision 11); apply plan 1's migrations; deploy; materialize the registry export, then every candidate asset (the LLM one with `LLM_CANDIDATES_RUN_CONFIG`'s profile); run `build_parity_snapshot_sql()` as direct SQL against the OLD `se_company_info`; run `se_company_field_resolved_clickhouse` with `{"execute": true, "resolve_all": true}` (no `max_companies`; one run) on the prod Dagster host; execute `se_company_field_parity_check` from the UI and read `failing`, `samples` and `rows_per_field_per_source`; apply 000377; start `se_company_info_field_value_sensor` and `se_company_field_candidate_sensor`; start `se_company_fields_weekly`; flip the `se_company_field_resolved_clickhouse` leaf to `WEEKLY` and delete `info.py`, `info_rules.py`, the old leaf and the old harness in the same branch that re-points the backoffice constants (`SE_COMPANY_INFO_REVIEW_JOB` -> `se_company_field_resolve_job`, `SE_COMPANY_INFO_JOB` -> `se_company_fields_job`, `SE_COMPANY_INFO_ASSET` -> `se_company_field_resolved_clickhouse`, `SE_COMPANY_INFO_SCHEDULE` -> `se_company_fields_weekly`; the sensor constant is unchanged).

---

## Self-review

**Spec coverage.**

| Spec section | Where |
| --- | --- |
| 7.4 (generated statement, as consumer) | Task 2 `load_registry_statements` reads `resolve_sql` per field and the `*` row with `argMax(..., version)`; Task 3 `_resolve_batch` executes them verbatim with `{field, company_ids, source_run_id, resolved_at}` bound server-side; Task 7 executes the rendered text under `SET param_*` |
| 8.1 resolved table | read by the scan's `versions` CTE (Task 2), the batch stats (Task 3), the parity rows-per-source (Task 4), the harness read-back (Task 7) |
| 8.2 asset, steps 1-4 | Task 3: company set (config ids / scan / `resolve_all`), per-field statements in pages of `company_batch_size`, projection through `publish_with_stage` (Task 1's `client=`), metadata per field (rows, from decisions, per source, no row); `execute` preview and `company_ids`/`max_companies` kept |
| 8.3 wide projection, mandatory register name | projection = the registry's `*` statement (Task 3); the register-name rule as the scan's `has_register_name` pre-filter (decision 3, Task 2, executed in Task 7 with BETA); the eight new columns asserted in the harness row (Task 7) and served (Task 8) |
| 8.4 incrementality | Task 2's scan: candidates newer than `resolved_at`, decisions newer, version differs (per-field against the registry table, decision 2), never published; sensor keeps its cursor (Task 5); weekly schedule with no scope (Task 6); `resolve_all` with cutoff (Tasks 2-3, executed in Task 7) |
| 10 serving | Task 8: builder + 000377 staged swap + drift pin + executable suite |
| 12 step 4 parity | Task 4 (SQL, rules per decision 6, result, check) executed in Task 7 with one deliberate mismatch; rows per field per source reported |
| 12 tests: pivot, resolve, parity | Task 7 (Handelsbanken row column by column, sets for provenance arrays), Tasks 2-3 FakeClient (batch order, preview, `resolve_all`, metadata shape), Task 4 unit + Task 7 executed |
| 12 cutover steps 1-6 | ordered handoff in Task 9; the sensor-stop precondition in decision 11 |
| 14 naming | asset/group/jobs/sensors/schedule/check names fixed in Global Constraints and asserted in Task 6's Definitions test |

**Placeholder scan.** No TBD/TODO/"implement later"/"similar to Task N"; every code step carries the code; the two conditional instructions (Task 6: rename `LLM_CANDIDATES_RUN_CONFIG`'s keys if `validate_run_config` rejects them; Task 7: bind a placeholder `_resolve_pass` does not know) name the exact file, symbol and arbiter test, because plan 2's config field names and any extra parameters of its statements are outside this plan's contract.

**Type consistency.** `materialize_se_company_fields(context, client, config, *, registry, now)` (Task 3) is what Task 3's asset and tests call; `server_params(*, company_ids, **scalars)` (Task 2) is what Task 3 binds with, and its `ServerSideLiteral` equality is what Task 3's tests compare; `publish_with_stage(client=, select_sql=, select_parameters=)` (Task 1) is what `_resolve_batch` calls; `PARITY_CHECK_NAME`/`RESOLVE_ASSET` (Task 2) are what Task 4's check, Task 5's jobs and Task 6's test use; `se_company_field_resolve_job` (Task 5) is the job both sensors (Task 5) and the Definitions test (Task 6) name; `se_company_fields_job` (Task 5) is what the schedule (Task 6) runs; `SELECTION_COLUMNS` (Task 2) is the row shape Task 3's `_selected` and Task 7's `_flags` follow; `PARITY_COLUMNS` (Task 4) is the row shape Task 4's `run_parity_check` and Task 7's parity test read; `WEEKLY_ASSETS` (Task 5) = `(*ARTIFACT_ASSETS, REGISTRY_ASSET, *CANDIDATE_ASSETS, RESOLVE_ASSET)` as the Cross-plan contract states.
