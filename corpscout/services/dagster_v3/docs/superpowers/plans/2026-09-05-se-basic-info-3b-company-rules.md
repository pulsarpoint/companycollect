# SE Basic Info Slice 3b: Company Precedence Rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reviewer's "Use this" becomes a per-company precedence rule (`company_id, field, source, precedence`) instead of a copied value, so the chosen source's current value flows through every later fold like an automatic pick; "Release" retires the rule.

**Architecture:** The precedence table gains a company scope (migration 000380 recreates it; `''` = global rules from code). The pure fold takes the company's active rules and lets them replace the global number per source and field. The batch layer reads rules per page and counts a new rule as a change. The backoffice writes rules (never values) for Use this and Release, orders the panel by effective precedence and marks the ruled source.

**Tech Stack:** ClickHouse migrations (golang-migrate ledger), Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`, tests need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`), React Router 8 backoffice (`npx vitest run`, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`, sections 3.5, 4, 5 and 7 as amended 2026-09-05 (slice 3b).

## Global Constraints

- Dagster work happens in `corpscout/services/dagster_v3` (tests: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest <files> -q`; definitions: `uv run --frozen --no-sync dg check defs`). Backoffice work in `corpscout/services/backoffice` (`npx vitest run <files>`, `npm run typecheck`).
- Migration ledger rules: first line `CREATE DATABASE IF NOT EXISTS corpscout;`, last line a statement, no `;` inside comments, name added to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`; the DDL contract tests read column lists from the migration files through `tests/se_company_ddl.py::declared_columns`. Migration 000380 is a DROP + CREATE of a regenerable 30-row table (the only company decision so far is converted by hand in Task 5); the drop is stated in the file's comment.
- New table shape, exactly: `company_id String, field LowCardinality(String), source LowCardinality(String), precedence UInt32, removed UInt8 DEFAULT 0, decided_by LowCardinality(String) DEFAULT '', note String DEFAULT '', decided_at DateTime64(3, 'UTC')`, `ENGINE = ReplacingMergeTree(decided_at) ORDER BY (company_id, field, source)`, `CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')`.
- `tables.PRECEDENCE_COLUMNS = ("company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at")`. The export writes `company_id = ''`, `removed = 0`, `decided_by = 'code'`, `note = ''`, `decided_at = exported_at`; the stale count covers only `company_id = ''` rows.
- Effective precedence: `rules[field][source]` when present (active company rule), else `precedence_for(field, source)`; a rule may name a source the global map does not, and that source can then supply the field. Rules are `Mapping[str, Mapping[str, int]]` built only from `removed = 0` rows.
- Changed-only selection: a company is re-folded when its newest suggestion OR its newest active rule (`decided_at`) is later than its `folded_at`, or it has no main row.
- Backoffice writes: Use this inserts `(companyId, field, source, 10000, 0, 'backoffice', note, now)`; Release inserts `(companyId, field, source, 10000, 1, 'backoffice', note, now)`; `decided_at` as `YYYY-MM-DD HH:MM:SS.mmm` UTC. No reviewer-row (suggestion table) writes remain in the Info tab; `appendSeBasicInfoReviewerDecision` and its tests are deleted.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` then `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`.

---

## File structure

| File | Responsibility |
| --- | --- |
| `corpscout/clickhouse/migrations/000380_corpscout_se_company_basic_info_precedence_rules.{up,down}.sql` (new) | recreate the precedence table with the company scope |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/tables.py` (modify) | `PRECEDENCE_COLUMNS` |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/assets.py` (modify) | export writes the new columns; stale count scoped to `''` |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/fold.py` (modify) | `rules` parameter, effective precedence |
| `dagster_v3/src/dagster_v3/defs/se_company/basic_info/batch.py` (modify) | `company_rules_sql`, `rule_watermarks_sql`, rules per page, changed-only |
| `dagster_v3/tests/test_se_company_basic_info_{assets,fold,batch,tables,clickhouse_local}.py`, `tests/test_clickhouse_migrations.py` (modify) | tests |
| `backoffice/app/lib/clickhouse.server.ts` (modify) | `chInsertSeBasicInfoPrecedence` |
| `backoffice/app/lib/se-basic-info.server.ts` (modify) | precedence read with company rows, `appendSeBasicInfoRule`, delete the reviewer-row write |
| `backoffice/app/lib/se-basic-info-decision-form.ts` (modify) | `release` requires `source` |
| `backoffice/app/components/admin/se-basic-info-workspace.tsx` (modify) | effective order, "preferred by reviewer", Release on the ruled row |
| `backoffice/app/routes/admin-se-company-info.tsx` (modify) | calls `appendSeBasicInfoRule` |
| `backoffice/tests/{se-basic-info.server,se-basic-info-decision-form,admin-se-company-basic-info}.test.ts(x)` (modify) | tests |

---

### Task 1: Migration 000380 and the export

**Files:**
- Create: `corpscout/clickhouse/migrations/000380_corpscout_se_company_basic_info_precedence_rules.up.sql`, `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (append the name to `EXPECTED_MIGRATIONS`), `src/dagster_v3/defs/se_company/basic_info/tables.py`, `src/dagster_v3/defs/se_company/basic_info/assets.py`
- Test: `tests/test_se_company_basic_info_tables.py` (DDL pin), `tests/test_se_company_basic_info_assets.py`

**Interfaces:**
- Produces: `tables.PRECEDENCE_COLUMNS` (8 names above); `export_precedence(client, exported_at)` unchanged signature, rows are 8-tuples.

- [ ] **Step 1: Write the failing tests**

In `tests/test_se_company_basic_info_tables.py` find the existing pin of the precedence columns against the DDL (it reads `000379_...up.sql` through `declared_columns`) and point it at `000380_corpscout_se_company_basic_info_precedence_rules.up.sql`; add:

```python
def test_precedence_rules_table_carries_the_company_scope() -> None:
    assert tables.PRECEDENCE_COLUMNS == (
        "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
    )
    assert declared_columns("000380_corpscout_se_company_basic_info_precedence_rules.up.sql") == list(tables.PRECEDENCE_COLUMNS)
```

In `tests/test_se_company_basic_info_assets.py` change `test_export_precedence_inserts_every_pair_and_binds_a_utc_millisecond_string`: the insert SQL pins `(company_id, field, source, precedence, removed, decided_by, note, decided_at) VALUES`; every inserted row is `("", field, source, precedence, 0, "code", "", exported_at)`; the stale SQL contains `WHERE company_id = '' AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_tables.py tests/test_se_company_basic_info_assets.py tests/test_clickhouse_migrations.py -q`
Expected: FAIL (missing migration file, old column tuple, old SQL).

- [ ] **Step 3: Write the migration and the code**

`000380_corpscout_se_company_basic_info_precedence_rules.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Slice 3b (2026-09-05): the precedence table gains a company scope so a reviewer
-- decision is a rule for one company, not a copied value. The old table held only the
-- 30 rows the code exports, so it is dropped and recreated; re-run
-- se_company_basic_info_precedence_clickhouse after applying this migration.
DROP TABLE IF EXISTS corpscout.se_company_basic_info_precedence;

CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),

    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, field, source);
```

`.down.sql` recreates the 000379 shape:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_basic_info_precedence;

CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
(
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    exported_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (field, source);
```

`tables.py`:

```python
# The precedence table with its company scope (spec 3.5, amended 2026-09-05): '' is a
# global rule exported from code; a company id is a reviewer rule for that company only.
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
```

`assets.py`, `export_precedence`:

```python
    rows = [
        ("", field, source, precedence, 0, "code", "", exported_at)
        for field, source, precedence in precedence_rows()
    ]
    client.execute(
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
        rows,
    )
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE company_id = '' AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return len(rows), stale
```

Update the docstring: the export never touches company rows. Append `"000380_corpscout_se_company_basic_info_precedence_rules"` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 4: Run tests to verify they pass**

Same command as Step 2. Expected: PASS. Then `uv run --frozen --no-sync dg check defs`: definitions load.

- [ ] **Step 5: Commit**

```bash
git add corpscout/clickhouse/migrations/000380_corpscout_se_company_basic_info_precedence_rules.up.sql corpscout/clickhouse/migrations/000380_corpscout_se_company_basic_info_precedence_rules.down.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/tables.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/assets.py corpscout/services/dagster_v3/tests/test_se_company_basic_info_tables.py corpscout/services/dagster_v3/tests/test_se_company_basic_info_assets.py
git commit -m "feat(dagster): SE basic-info precedence table gains a company scope"
```

---

### Task 2: The fold takes company rules

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/fold.py`
- Test: `tests/test_se_company_basic_info_fold.py`

**Interfaces:**
- Produces: `Rules = Mapping[str, Mapping[str, int]]` (field -> source -> precedence); `fold_basic_info(company_id, suggestions, *, source_run_id, rules: Rules | None = None)`; `_winner(field, suggestions, rules)`.

- [ ] **Step 1: Write the failing tests**

Reuse the file's existing suggestion factory (it builds `Suggestion` rows per source; call it the way the neighbouring tests do). Add:

```python
def test_a_company_rule_replaces_the_global_number_for_that_source() -> None:
    scb = suggestion("scb", status="active")
    bolagsverket = suggestion("bolagsverket", status="inactive")
    row = fold_basic_info("5560000000", [scb, bolagsverket], source_run_id="r",
                          rules={"status": {"bolagsverket": 10000}})
    assert row is not None
    assert (row.status, row.status_source) == ("inactive", "bolagsverket")
    # Without the rule the global map (scb 1000 > bolagsverket 900) decides.
    plain = fold_basic_info("5560000000", [scb, bolagsverket], source_run_id="r")
    assert plain is not None and plain.status_source == "scb"


def test_a_company_rule_lets_an_unranked_source_supply_a_field() -> None:
    scb = suggestion("scb", status="active")
    wikidata = suggestion("wikidata", status="dormant")
    ruled = fold_basic_info("5560000000", [scb, wikidata], source_run_id="r",
                            rules={"status": {"wikidata": 10000}})
    assert ruled is not None and (ruled.status, ruled.status_source) == ("dormant", "wikidata")
    plain = fold_basic_info("5560000000", [scb, wikidata], source_run_id="r")
    assert plain is not None and plain.status_source == "scb"


def test_a_rule_for_a_source_without_a_value_changes_nothing() -> None:
    scb = suggestion("scb", status="active")
    bolagsverket = suggestion("bolagsverket", status=None)
    row = fold_basic_info("5560000000", [scb, bolagsverket], source_run_id="r",
                          rules={"status": {"bolagsverket": 10000}})
    assert row is not None and row.status_source == "scb"


def test_a_low_rule_demotes_a_source_for_one_company() -> None:
    scb = suggestion("scb", status="active")
    bolagsverket = suggestion("bolagsverket", status="inactive")
    row = fold_basic_info("5560000000", [scb, bolagsverket], source_run_id="r",
                          rules={"status": {"scb": 1}})
    assert row is not None and row.status_source == "bolagsverket"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `... pytest tests/test_se_company_basic_info_fold.py -q`. Expected: FAIL (`rules` is an unexpected keyword).

- [ ] **Step 3: Implement**

```python
from collections.abc import Mapping

# field -> source -> precedence; a company's active rules (spec 4, amended 2026-09-05).
Rules = Mapping[str, Mapping[str, int]]
EMPTY_RULES: Rules = {}


def _effective_precedence(field: str, source: str, rules: Rules) -> int | None:
    """The company rule for (field, source) when one exists, else the global number.
    A rule may name a source the global map does not, which lets that source supply the
    field for this company only."""
    ruled = rules.get(field, {}).get(source)
    if ruled is not None:
        return ruled
    return precedence_for(field, source)


def _winner(field: str, suggestions: list[Suggestion], rules: Rules = EMPTY_RULES) -> Suggestion | None:
    ...
        precedence = _effective_precedence(field, suggestion.source, rules)
    ...


def fold_basic_info(
    company_id: str,
    suggestions: list[Suggestion],
    *,
    source_run_id: str,
    rules: Rules | None = None,
) -> BasicInfoRow | None:
    ...
    active_rules = rules or EMPTY_RULES
    for field in tables.FOLDED_FIELDS:
        winner = _winner(field, suggestions, active_rules)
    ...
```

Update the module docstring: rules replace the global number per source and field. `FOLD_VERSION` stays (the logic for companies without rules is unchanged).

- [ ] **Step 4: Run tests to verify they pass** — whole fold file. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/fold.py corpscout/services/dagster_v3/tests/test_se_company_basic_info_fold.py
git commit -m "feat(dagster): SE basic-info fold applies per-company precedence rules"
```

---

### Task 3: The batch layer reads rules and counts them as changes

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/batch.py`
- Test: `tests/test_se_company_basic_info_batch.py`, `tests/test_se_company_basic_info_clickhouse_local.py`

**Interfaces:**
- Produces: `company_rules_sql()`, `rule_watermarks_sql()`, `rules_by_company(rows) -> dict[str, dict[str, dict[str, int]]]`; `fold_companies` passes `rules=...` per company; `_changed_company_ids` uses the later of the suggestion and rule watermarks.

- [ ] **Step 1: Write the failing tests**

In `tests/test_se_company_basic_info_batch.py` (the file drives `fold_companies` with a fake client keyed on SQL text; extend the fake to answer the two new statements):

```python
def test_company_rules_sql_reads_active_rules_for_the_page_with_final() -> None:
    sql = company_rules_sql()
    assert sql == (
        "SELECT company_id, field, source, precedence\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND removed = 0"
    )
    assert rule_watermarks_sql() == (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )
    assert rules_by_company([("5560000000", "status", "bolagsverket", 10000), ("5560000000", "status", "scb", 1)]) == {
        "5560000000": {"status": {"bolagsverket": 10000, "scb": 1}}
    }


def test_a_company_rule_decides_the_page_fold() -> None:
    # scb active (1000) vs bolagsverket inactive (900); the rule prefers bolagsverket.
    client = FakeClient(
        suggestions=[scb_row("5560000000", status="active"), bolagsverket_row("5560000000", status="inactive")],
        rules=[("5560000000", "status", "bolagsverket", 10000)],
    )
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=NOW)
    assert counts.folded == 1
    main_row = client.inserted_main_rows()[0]
    assert main_row[tables.MAIN_COLUMNS.index("status_source")] == "bolagsverket"


def test_changed_only_wakes_a_company_whose_newest_rule_is_newer_than_its_fold() -> None:
    client = FakeClient(
        suggestions=[scb_row("5560000000", status="active")],
        suggestion_watermarks={"5560000000": OLD},
        main_watermarks={"5560000000": MID},
        rule_watermarks={"5560000000": NEW},
    )
    counts = fold_companies(client, ["5560000000"], changed_only=True, source_run_id="r", folded_at=NOW)
    assert counts.considered == 1
```

(Adapt `FakeClient`, `scb_row`, `bolagsverket_row`, `OLD < MID < NEW` to the file's existing helpers; keep their names if they already exist.)

In `tests/test_se_company_basic_info_clickhouse_local.py` extend the schema list with `000380_...up.sql` (replacing `000379` if listed) and add a test that inserts one global row and one company rule row, runs `company_rules_sql()` bound to that company, and gets exactly the company row; a second insert of the same key with `removed = 1` and a later `decided_at` makes the query return nothing.

- [ ] **Step 2: Run tests to verify they fail** — expected: FAIL (missing functions).

- [ ] **Step 3: Implement**

```python
def company_rules_sql() -> str:
    return (
        "SELECT company_id, field, source, precedence\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND removed = 0"
    )


def rule_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def rules_by_company(rows: Sequence[Sequence[Any]]) -> dict[str, dict[str, dict[str, int]]]:
    """(company_id, field, source, precedence) rows -> company -> field -> source -> precedence."""
    out: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    for company_id, field, source, precedence in rows:
        out[company_id][field][source] = int(precedence)
    return {company: dict(fields) for company, fields in out.items()}
```

`_changed_company_ids`: read `rule_watermarks_sql()` too (same params and settings) and select the company when `max(suggested, ruled) > folded` (a company with only a rule and no suggestion stays out: nothing to fold). In `fold_companies`, after the suggestions read, `rules = rules_by_company(client.execute(company_rules_sql(), params, settings=ID_BOUND_QUERY_SETTINGS))` and call `fold_basic_info(..., rules=rules.get(company_id))`. Update the docstrings and `docs/basic_info-design.md` (the batch row and a sentence under Operating the fold).

- [ ] **Step 4: Run tests to verify they pass** — batch + clickhouse-local + fold + assets. Expected: PASS. `dg check defs` loads.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/batch.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/docs/basic_info-design.md corpscout/services/dagster_v3/tests/test_se_company_basic_info_batch.py corpscout/services/dagster_v3/tests/test_se_company_basic_info_clickhouse_local.py
git commit -m "feat(dagster): SE basic-info batch fold reads company rules and re-folds on new rules"
```

---

### Task 4: The backoffice writes rules instead of reviewer values

**Files:**
- Modify: `app/lib/clickhouse.server.ts`, `app/lib/se-basic-info.server.ts`, `app/lib/se-basic-info-decision-form.ts`, `app/lib/se-basic-info-fields.ts` (no change expected), `app/components/admin/se-basic-info-workspace.tsx`, `app/routes/admin-se-company-info.tsx`
- Test: `tests/se-basic-info.server.test.ts`, `tests/se-basic-info-decision-form.test.ts`, `tests/admin-se-company-basic-info.test.tsx`

**Interfaces:**
- `SeBasicInfoPrecedenceRow` gains `company_id`, `removed` (number), `decided_by`, `note`, `decided_at`; `BASIC_INFO_PRECEDENCE_SQL` reads `WHERE p.company_id IN ('', {companyId:String})` and takes `{ companyId }`; `SeBasicInfoDetail.precedence` keeps only global rows and a new `rules: SeBasicInfoPrecedenceRow[]` holds the company's active rows (`removed = 0`).
- `chInsertSeBasicInfoPrecedence(values)` (table `se_company_basic_info_precedence`).
- `appendSeBasicInfoRule(companyId, decision, now = new Date()): Promise<{ decidedAt: string }>`: `use-this` refuses (SeBasicInfoDecisionError, `<Source> has no <field> for this company.`) when the source's current suggestion row has no value for the field, then inserts the rule at 10000 with `removed 0`; `release` inserts the same key with `removed 1`. Both carry `decided_by 'backoffice'`, `note` ('' when empty), `decided_at`.
- Decision form: `release` now requires `source` (any catalogue source, reviewer included is refused with "Release needs the preferred source."); `use-this` unchanged.
- `foldPending(info?.folded_at ?? null, [...suggestions.map(s => s.suggested_at), ...rules.map(r => r.decided_at)])`.
- Route: `{ ok: true, decidedAt }` replaces `{ ok: true, suggestedAt }`; `SeBasicInfoResult` follows; "Decision saved" reads "Rule written at …".
- Panel: effective precedence per source = the company rule when present else the global row; order by it; the ruled source gets a `Badge variant="outline"` "preferred by reviewer" plus its note under the value; "Use this" on any non-active row with a value (including unranked sources); "Release" on the ruled source's row (not on the reviewer row); the reviewer row keeps only its value display.

- [ ] **Step 1: Write the failing tests** — in the three test files: pin the new precedence SQL and the `{ companyId }` binding; `loadSeBasicInfoDetail` splits global vs company rows and computes `foldPending` from rules too; `appendSeBasicInfoRule` inserts the two row shapes (assert the 8 keys) and refuses a valueless source; the parser refuses `release` without a source; the panel test fixture gains a rule `{ company_id: COMPANY, field: "status", source: "bolagsverket", precedence: 10000, removed: 0, decided_by: "backoffice", note: "register is right", decided_at: "2026-09-05 08:00:00.000" }` and asserts bolagsverket sorts before scb, carries "preferred by reviewer" and "register is right", offers "Release", and scb offers "Use this"; delete the reviewer-row tests that no longer apply.

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement** the interfaces above; delete `appendSeBasicInfoReviewerDecision`, `reviewerValues`, `VALUE_FIELDS` if unused, and `chInsertSeBasicInfoSuggestions` if nothing else uses it (grep first).

- [ ] **Step 4: Run** `npx vitest run tests/se-basic-info.server.test.ts tests/se-basic-info-decision-form.test.ts tests/admin-se-company-basic-info.test.tsx tests/admin-se-company-info-run.test.ts` and `npm run typecheck`. Expected: PASS, clean.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/clickhouse.server.ts corpscout/services/backoffice/app/lib/se-basic-info.server.ts corpscout/services/backoffice/app/lib/se-basic-info-decision-form.ts corpscout/services/backoffice/app/components/admin/se-basic-info-workspace.tsx corpscout/services/backoffice/app/routes/admin-se-company-info.tsx corpscout/services/backoffice/tests/se-basic-info.server.test.ts corpscout/services/backoffice/tests/se-basic-info-decision-form.test.ts corpscout/services/backoffice/tests/admin-se-company-basic-info.test.tsx
git commit -m "feat(backoffice): Use this and Release write per-company precedence rules"
```

---

### Task 5: Production cutover (controller, owner-gated)

- [ ] Merge to main (owner). Apply 000380 on prod (`make -C <deploy checkout>/corpscout clickhouse-migrate-up-one`), verify the ledger reads 380 and `DESCRIBE corpscout.se_company_basic_info_precedence` shows the eight columns.
- [ ] Hot-sync dagster from main; materialize `se_company_basic_info_precedence_clickhouse` (expect 30 pairs, 0 stale); `SELECT count() FROM corpscout.se_company_basic_info_precedence FINAL WHERE company_id = ''` = 30.
- [ ] Convert today's one decision (Handelsbanken 5020077862, description from bolagsverket): insert a reviewer-row version with `description`, `description_language` NULL (copy the current reviewer row's other columns) and a rule `('5020077862', 'description', 'bolagsverket', 10000, 0, 'backoffice', 'converted from the 2026-09-05 value decision', now)`; then `se_company_basic_info_fold_companies` for that id; expect `description_source = 'bolagsverket'` and one history row with `description` changed.
- [ ] Smoke on localhost:5183 after the owner merges: on Atlas Copco 5561552760 status, Use this on Bolagsverket opens the dialog, confirm writes a rule (check the table), Fold now publishes `status = inactive, status_source = bolagsverket`, the row reads "preferred by reviewer"; Release writes the `removed = 1` version and Fold now returns to SCB.
- [ ] Record counts in the ledger and the spec's section 10; archive the ledger.

## Self-review

- Spec coverage: 3.5 (table) Task 1; 4 (effective precedence, unranked sources, demotion) Task 2; 5 (rules per page, changed-only wakes on rules) Task 3; 7 (writes, panel, fold pending) Task 4; cutover Task 5.
- Placeholders: Task 4's tests are described by contract rather than pasted verbatim, because the three test files already exist with fixtures the brief cannot restate; the implementer pins the interfaces listed. Everything else carries code.
- Type consistency: `Rules` (Task 2) is what `rules_by_company` (Task 3) produces per company; `PRECEDENCE_COLUMNS` (Task 1) is the insert order used by `export_precedence` and mirrored by the backoffice's 8-key insert (Task 4); `decided_at` string format matches `clickhouseStamp` already in the server module.
