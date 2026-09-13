# SE company financial entity, slice 4a: the data-side cutover — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every ClickHouse-side Swedish reader of financial facts reads the folded entity `corpscout.se_company_financial` instead of the source tables — the cross-country `se_company_financials_latest` projection, the serving view's financial flags, the section-presence model with its reconciliation, and the filing-status view — through migration 000404 and a dagster deploy; the owner-run scripts that retire the two Sweden-only source views are authored and gated (they run after slice 4b, which removes their last readers in the backoffice).

**Architecture:** Spec section 10, data side. `company_financials_latest/sql.py` gets a hand-built Sweden SELECT over the entity's active standalone rows (newest period end, the fold stamp as tiebreak, the winners' own USD twins, no fx fallback) and its asset depends on the fold. `sweden_company/companies_current.py` swaps the three financial presence arms for three reads of the entity (active rows; register flags from `sources`), and migration 000404 re-points the refreshable serving view in place (`SYSTEM STOP VIEW` → `ALTER TABLE … MODIFY QUERY` with the builder's exact render → `SYSTEM START VIEW`, the recipe of 000403) and re-issues `se_annual_report_filing_status_current` with its data_available leg on the entity. The dbt presence leg and `publish.py`'s reconciliation count companies with an active folded period. The retirement scripts follow the person precedent with a zero-readers gate. Every text below was authored and run on 2026-09-13 in a throwaway worktree: 174 unit tests green across the touched files, the clickhouse-local serving suite 46 green, ruff clean; the tasks transcribe it.

**Tech Stack:** ClickHouse 26.5 (refreshable materialized view, `ALTER TABLE … MODIFY QUERY`, `CREATE OR REPLACE VIEW`, `FINAL`), golang-migrate ledger, Dagster + dbt (`company_serving`), pytest with `tests/clickhouse_local.py`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` — sections 10 (readers and retirement), 11 (testing), 12 item 4, 13. Slice 4b (the backoffice: workspace, shared grid, backoffice re-points and deletions, the smoke, then the owner-run drops and the ledger emptying of 000286/000364) is its own plan.

## Global Constraints

- Branch `se-financial-entity`, worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity` (at 2417f0edc, equal to main; `.env` and `.venv` in place). Paths are relative to `corpscout/services/dagster_v3` inside that worktree unless they start with `corpscout/`. Never touch the main checkout `/Users/graovic/pulsarpoint/ppoint/companycollect` (another session works there on another branch). Do not use `git stash`.
- The migration is **000404** (`000404_corpscout_se_financial_readers_entity`); 000402 and 000403 are taken. First line `CREATE DATABASE IF NOT EXISTS corpscout;`; no `;` inside a comment (the ledger test rejects it). It is added to `EXPECTED_MIGRATIONS`.
- The serving view is re-pointed IN PLACE (`SYSTEM STOP VIEW` / `ALTER TABLE … MODIFY QUERY` / `SYSTEM START VIEW`), never a staged swap, never `SYSTEM WAIT VIEW`, never a DROP; the MODIFY QUERY body is the exact render of `companies_current.build_se_companies_serving_sql()` and is drift-pinned by `tests/test_se_companies_serving_mv.py`, retargeted to 000404; the down file restores 000403's render verbatim and 000282's view verbatim.
- Spec 10, exactly: `se_company_financials_latest`'s Sweden leg = latest active standalone period per company from the entity with revenue, net result, total assets, equity, employees and their USD twins, `years_count = uniqExact(fiscal_year)`, `UPSTREAM_KEYS["se"] = ["se_company_financial_fold"]`; the presence model and the publish reconciliation count one row per company with an active entity row; `has_financial` = an active entity row OR a filed report (the 2026-08-25 widening stays), `fin_bolagsverket`/`fin_esef` from `sources` over the same rows (ruling: the restated column `bolagsverket_comparative` lights the Bolagsverket flag), `fin_reports` stays on `se_financial_reports`; the filing-status view's first leg = the entity's newest active standalone period end per company; every entity read is `FINAL` with `active = 1`.
- The two source views are NOT dropped in this slice and their migration files are NOT emptied here (that follows the owner-run drops after 4b); only the precheck/drops/postcheck scripts and their test land now. The scripts never use `SYNC`.
- Tests: `uv run --env-file .env pytest … -q` from `corpscout/services/dagster_v3`; integration tests with `-m integration` (Docker image `clickhouse/clickhouse-server:26.5` present); `uv run ruff check <files>` clean; `uv run dg check defs` clean where definitions change.
- Commit messages follow Conventional Commits and end with EXACTLY these two lines, pasted verbatim:
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013oGzirJgExBzVuy9GQBYHz
- Task 6 (prod) runs after the branch's final review and merge, on the owner's "do it" for this slice; merges go through a temporary worktree on `main` (`git worktree add <tmp> main`), never in the owner's checkout.

---

## File structure

| File | Responsibility |
|---|---|
| `src/dagster_v3/defs/company_financials_latest/sql.py`, `assets.py` (modify) | The Sweden leg over the entity; the fold as upstream |
| `tests/test_company_financials_latest.py` (modify) | Pins the Sweden leg |
| `src/dagster_v3/defs/sweden_company/companies_current.py` (modify) | The three financial presence arms read the entity |
| `corpscout/clickhouse/migrations/000404_corpscout_se_financial_readers_entity.{up,down}.sql` (new) | Serving-view repoint + filing-status view re-issue; down restores 000403 + 000282 |
| `tests/test_se_companies_serving_mv.py`, `tests/test_se_companies_serving_sql.py`, `tests/test_clickhouse_migrations.py` (modify) | Drift pin retargeted; entity stub in the engine suite; ledger registration + pin |
| `src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql`, `sources.yml`, `publish.py` (modify); `tests/test_company_serving_financials_presence.py` (new) | Presence leg and reconciliation on the entity |
| `corpscout/clickhouse/operations/se_financial_views_retirement_{precheck,drops,postcheck}.sql`, `tests/test_se_financial_views_retirement_drops.py` (new) | Owner-run retirement of the two views, gated |
| `src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md`, `docs/sweden-data-sources.md`, the spec (modify) | Docs name the entity; slice record |

---

### Task 1: `se_company_financials_latest` reads the entity

**Files:**
- Modify: `src/dagster_v3/defs/company_financials_latest/sql.py`, `src/dagster_v3/defs/company_financials_latest/assets.py`
- Test: `tests/test_company_financials_latest.py`

**Interfaces:**
- Produces: `sql._SE_SELECT`; `sql.SOURCES["se"] == {"table": "se_company_financial", "id": "company_id"}`; `build_latest_insert_sql("se")` returns `_SE_SELECT`; `assets.UPSTREAM_KEYS["se"] == ["se_company_financial_fold"]`. The asset's existence check reads `SOURCES["se"]["table"]`, so the entity table is what it asserts exists.

- [ ] **Step 1: Patch the test (the failing test first)**

From `corpscout/services/dagster_v3`, run this script with `uv run --env-file .env python - <<'PY' … PY` (paste verbatim):

```python
from pathlib import Path
test = Path("tests/test_company_financials_latest.py"); t = test.read_text()
old = """        if code == "br":
            continue  # BR's hand-built SELECT has no {tiebreak}-style placeholder
"""
new = """        if code in ("br", "se"):
            continue  # hand-built SELECTs: BR pivots, SE reads the folded entity (below)
"""
assert t.count(old) == 1; t = t.replace(old, new)
old = """def test_sweden_latest_prefers_reported_metrics_over_comparatives() -> None:
    from dagster_v3.defs.company_financials_latest.sql import build_latest_insert_sql

    sql = build_latest_insert_sql("se")

    assert "observation_kind = 'reported' DESC" in sql
    assert "source_fiscal_year DESC NULLS LAST" in sql
"""
new = """def test_sweden_latest_reads_the_entitys_newest_active_standalone_period() -> None:
    \"\"\"Spec 2026-09-11 section 10: the Sweden leg reads corpscout.se_company_financial (the
    fold's output), active standalone rows under FINAL, newest period end first with the
    fold stamp as the qualified tiebreak; the USD twins are the winners' own conversions, so
    no fx fallback; years_count counts distinct fiscal years before LIMIT 1 BY.\"\"\"
    from dagster_v3.defs.company_financials_latest.assets import UPSTREAM_KEYS
    from dagster_v3.defs.company_financials_latest.sql import SOURCES, build_latest_insert_sql

    sql = build_latest_insert_sql("se")

    assert "FROM corpscout.se_company_financial FINAL" in sql
    assert "WHERE active = 1 AND scope = 'standalone'" in sql
    assert "ORDER BY period_end DESC, `se_company_financial`.folded_at DESC" in sql
    assert "LIMIT 1 BY company_id" in sql
    assert "fx_rate_to_usd" not in sql and "se_bolagsverket_financial_metrics" not in sql
    assert "toUInt32(uniqExact(fiscal_year) OVER (PARTITION BY company_id)) AS years_count" in sql
    assert SOURCES["se"] == {"table": "se_company_financial", "id": "company_id"}
    assert UPSTREAM_KEYS["se"] == ["se_company_financial_fold"]
"""
assert t.count(old) == 1; test.write_text(t.replace(old, new)); print("test patched")
```

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run --env-file .env pytest tests/test_company_financials_latest.py -q
```

Expected: 1 failed (`test_sweden_latest_reads_the_entitys_newest_active_standalone_period`: the SQL still names `se_bolagsverket_financial_metrics`).

- [ ] **Step 3: Patch the code**

```python
from pathlib import Path
sql = Path("src/dagster_v3/defs/company_financials_latest/sql.py"); t = sql.read_text()
old = """    "se": {
        "table": "se_bolagsverket_financial_metrics",
        "id": "company_id",
        "currency": "currency",
        "rev": "revenue",
        "net": "profit_loss",
        "employees": "toFloat64(employees)",
        # se.report_period_end is Nullable(Date32) (verified live 2026-07-17),
        # not Date like the migration column -- toDate() normalizes it.
        "period_end": "toDate(report_period_end)",
        "statement_order": (
            "observation_kind = 'reported' DESC, source_fiscal_year DESC NULLS LAST, "
        ),
    },
"""
new = """    # SE reads the financial ENTITY (spec 2026-09-11 section 10), not a source table, so
    # its SELECT is hand-built (_SE_SELECT) like BR's; "table" and "id" stay here for the
    # upstream-table-existence check in assets.py and the coverage test.
    "se": {
        "table": "se_company_financial",
        "id": "company_id",
    },
"""
assert t.count(old) == 1; t = t.replace(old, new)
old = """

def build_latest_insert_sql(code: str) -> str:"""
new = """
# SE is the folded financial entity (spec 2026-09-11 section 10): the latest ACTIVE STANDALONE
# period per company, read FINAL from corpscout.se_company_financial. Every USD figure is the
# winning source's own conversion, copied by the fold -- the entity carries no row-level fx
# rate, so there is no `original * fx_rate` fallback here. `years_count` counts the company's
# distinct fiscal years over its active standalone rows, computed BEFORE `LIMIT 1 BY`
# collapses to the newest period. The tiebreak on folded_at is qualified with the table name
# for the same reason the wide template qualifies resolved_at (a bare alias would shadow it).
_SE_SELECT = \"\"\"
SELECT
  company_id AS company_id,
  toInt32(fiscal_year) AS fiscal_year,
  toDate(period_end) AS period_end_date,
  toString(currency) AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees,
  toUInt32(uniqExact(fiscal_year) OVER (PARTITION BY company_id)) AS years_count,
  now64(3) AS resolved_at
FROM corpscout.se_company_financial FINAL
WHERE active = 1 AND scope = 'standalone'
ORDER BY period_end DESC, `se_company_financial`.folded_at DESC
LIMIT 1 BY company_id
\"\"\"


def build_latest_insert_sql(code: str) -> str:"""
assert t.count(old) == 1; t = t.replace(old, new)
old = """    if code == "br":
        return _BR_SELECT
"""
new = """    if code == "br":
        return _BR_SELECT
    if code == "se":
        return _SE_SELECT
"""
assert t.count(old) == 1; sql.write_text(t.replace(old, new))
assets = Path("src/dagster_v3/defs/company_financials_latest/assets.py"); t = assets.read_text()
old = '    "se": ["se_bolagsverket_financial_metrics_clickhouse"],\n'
new = '    # The folded financial entity (spec 2026-09-11 section 10): the fold, not a source table.\n    "se": ["se_company_financial_fold"],\n'
assert t.count(old) == 1; assets.write_text(t.replace(old, new)); print("code patched")
```

- [ ] **Step 4: Run the tests and ruff**

```bash
uv run --env-file .env pytest tests/test_company_financials_latest.py -q
uv run ruff check src/dagster_v3/defs/company_financials_latest tests/test_company_financials_latest.py
```

Expected: all passed (the file's tests, including the every-country coverage and the qualified-tiebreak test that now skips `se` like `br`); ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/sql.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/assets.py corpscout/services/dagster_v3/tests/test_company_financials_latest.py
git commit -m "feat(se-financial): se_company_financials_latest reads the entity's newest active standalone period"
```

---

### Task 2: The serving view's financial flags and migration 000404

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py`
- Create: `corpscout/clickhouse/migrations/000404_corpscout_se_financial_readers_entity.up.sql`, `.down.sql` (GENERATED by the script in Step 4, never hand-written)
- Modify: `tests/test_se_companies_serving_mv.py`, `tests/test_se_companies_serving_sql.py`, `tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `companies_current.COMPANY_FINANCIAL_TABLE`, `FINANCIAL_SET`, `FINANCIAL_BOLAGSVERKET_SET`, `FINANCIAL_ESEF_SET` (kept: `FINANCIAL_REPORTS_SET`); the render contains `corpscout.se_company_financial FINAL` three times, `toUInt8(fin_entity OR fin_reports) AS has_financial`, and names no source financial table.

- [ ] **Step 1: Patch the three tests (they fail until the code and the migration exist)**

```python
from pathlib import Path
def sub(path, old, new):
    t = path.read_text(); assert t.count(old) == 1, (path.name, old[:60], t.count(old)); path.write_text(t.replace(old, new))

mv = Path("tests/test_se_companies_serving_mv.py")
t = mv.read_text()
start = t.index('"""Migration 000403'); end = t.index('"""\n\nfrom pathlib import Path') + 3
t = t[:start] + '"""Migration 000404: the serving view\'s financial flags read the financial entity, and the\nfiling-status view\'s data_available leg does too.\n\n`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list\npage reads: the info-list columns, the presence and source flags, the address JSON + primary\ngeocode summary, and (since 000338) the registered-activity translation, status-reason label\nand spine fields absorbed from the retired `se_companies_translated` view.\n\nWHAT 000404 CHANGES (financial slice 4a, spec 2026-09-11 section 10). `has_financial` becomes\n"an active row in corpscout.se_company_financial OR a filed report" (the 2026-08-25 widening\non filed reports stays), `fin_bolagsverket` and `fin_esef` become `has(sources, ...)` over the\nsame active rows (the restated column `bolagsverket_comparative` is Bolagsverket data and\nlights that flag too), and the IN-set subqueries on se_bolagsverket_financial_metrics,\nesef_financial_metrics and company_identifier leave the view. The same file re-issues\n`se_annual_report_filing_status_current` (000282) with its data_available leg reading the\nentity\'s newest active standalone period end instead of se_company_financials_latest.\n\nThe serving definition changes and nothing else does, so this is the in-place `ALTER TABLE\n... MODIFY QUERY` of 000393, 000396, 000398 and 000403, not the staged swap of 000391/000392:\nno `_next`, no `SYSTEM WAIT VIEW` (a refresh takes 13 to 15 minutes against a 300-second\nclient read timeout), no drop. The filing-status view is a plain view: CREATE OR REPLACE.\n\nThe drift pin couples the migration\'s MODIFY QUERY body to a fresh render of\ncompanies_current.build_se_companies_serving_sql -- editing either half alone turns this red.\n"""' + t[end:]
mv.write_text(t)
sub(mv, 'MIGRATION = "000403_corpscout_se_companies_serving_no_workplace"\nPREVIOUS_MIGRATION = "000398_corpscout_se_company_person_rename"\n',
        'MIGRATION = "000404_corpscout_se_financial_readers_entity"\nPREVIOUS_MIGRATION = "000403_corpscout_se_companies_serving_no_workplace"\nFILING_VIEW = "corpscout.se_annual_report_filing_status_current"\nFINANCIAL_ENTITY = "corpscout.se_company_financial"\n')
sub(mv, """def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert ADDRESS_ROW_SOURCE in body
""", """def test_the_builder_reads_the_financial_entity_for_every_financial_flag() -> None:
    \"\"\"Slice 4a's repoint, on the builder rather than on the file: the three financial arms
    read the entity's ACTIVE rows under FINAL, the register flags read the row's `sources`
    (the restated Bolagsverket column counts as Bolagsverket), the filed-reports arm stays,
    and no financial arm names a source table or company_identifier any more.\"\"\"
    sql = build_se_companies_serving_sql()

    assert sql.count(f"{FINANCIAL_ENTITY} FINAL") == 3
    assert "toUInt8(fin_entity OR fin_reports) AS has_financial" in sql
    assert "WHERE active = 1 AND hasAny(sources, ['bolagsverket', 'bolagsverket_comparative'])" in sql
    assert sql.count("has(sources, 'esef')") == 2          # the people arm and the financial arm
    assert "FROM corpscout.se_financial_reports" in sql
    for gone in ("se_bolagsverket_financial_metrics", "esef_financial_metrics", "company_identifier"):
        assert gone not in sql, gone


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert ADDRESS_ROW_SOURCE in body
    assert body.count(f"{FINANCIAL_ENTITY} FINAL") == 3
    assert "se_bolagsverket_financial_metrics" not in body and "company_identifier" not in body
""")
sub(mv, """    statements = _statements(_sql("up"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]).startswith(f"ALTER TABLE {VIEW}\\nMODIFY QUERY\\n")
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
""", """    statements = _statements(_sql("up"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]).startswith(f"ALTER TABLE {VIEW}\\nMODIFY QUERY\\n")
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    # The filing-status view, re-issued in place with its data_available leg on the entity.
    filing = _body(statements[4])
    assert filing.startswith(f"CREATE OR REPLACE VIEW {FILING_VIEW} AS")
    assert f"FROM {FINANCIAL_ENTITY} FINAL" in filing and "WHERE active = 1 AND scope = 'standalone'" in filing
    assert "se_company_financials_latest" not in filing
    assert "FROM corpscout.se_annual_report_filing_observations FINAL" in filing
""")
sub(mv, """def test_the_down_migration_restores_000398s_render() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    assert "SYSTEM WAIT VIEW" not in _executable(_sql("down"))
    # The restored query is 000398's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check) -- and it
    # is the render in which a workplace-only row still counts.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    assert "kinds = ['workplace']" not in _modify_query_body(_sql("down"))
""", """def test_the_down_migration_restores_000403s_render_and_000282s_view() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    assert "SYSTEM WAIT VIEW" not in _executable(_sql("down"))
    # The restored query is 000403's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check) -- the
    # render whose financial flags still read the source tables.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    assert FINANCIAL_ENTITY not in _modify_query_body(_sql("down"))
    assert "se_bolagsverket_financial_metrics" in _modify_query_body(_sql("down"))
    # And the filing-status view is 000282's text, verbatim modulo whitespace.
    [original] = [
        _body(s) for s in _statements(_sql_of("000282_corpscout_se_annual_report_filing_status", "up"))
        if _body(s).startswith("CREATE OR REPLACE VIEW")
    ]
    assert _normalized(_body(statements[4])) == _normalized(original)
    assert "FROM corpscout.se_company_financials_latest" in _body(statements[4])
""")
sub(mv, '    assert "migrate force 403" in up\n', '    assert "migrate force 404" in up\n')

ss = Path("tests/test_se_companies_serving_sql.py")
sub(ss, """        "CREATE TABLE corpscout.se_bolagsverket_financial_metrics (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_identifier (company_id String, issuer_scheme String, country_code String, is_current UInt8, issuer_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.esef_financial_metrics (lei String) ENGINE = MergeTree ORDER BY lei;",
        "CREATE TABLE corpscout.se_financial_reports (company_id String) ENGINE = MergeTree ORDER BY company_id;",
""", """        # The financial entity's main table (migration 000401) -- read FINAL, active rows only;
        # only the columns the three financial IN-subqueries touch. ReplacingMergeTree so the
        # stub accepts the FINAL modifier the real engine does.
        "CREATE TABLE corpscout.se_company_financial (company_id String, sources Array(String), active UInt8, scope String) ENGINE = ReplacingMergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_financial_reports (company_id String) ENGINE = MergeTree ORDER BY company_id;",
""")
sub(ss, """        f"INSERT INTO corpscout.se_bolagsverket_financial_metrics VALUES ('{PRECISE}');",
""", """        # PRECISE has an active folded period from Bolagsverket and Ratsit; UNGEOCODED's only
        # period is hidden (active 0), which must not light has_financial.
        f"INSERT INTO corpscout.se_company_financial VALUES ('{PRECISE}', ['bolagsverket', 'ratsit'], 1, 'standalone'), ('{UNGEOCODED}', ['ratsit'], 0, 'standalone');",
""")
sub(ss, """    # has_financial: PRECISE via extracted metrics, COARSE via a filed report --
    # the owner's 2026-08-25 widening -- and nothing else.
""", """    # has_financial: PRECISE via an active financial-entity row, COARSE via a filed report --
    # the owner's 2026-08-25 widening -- and nothing else; UNGEOCODED's hidden period does
    # not count.
""")

tm = Path("tests/test_clickhouse_migrations.py")
t = tm.read_text()
old = '    "000403_corpscout_se_companies_serving_no_workplace",\n'
assert t.count(old) == 1
t = t.replace(old, old + '    "000404_corpscout_se_financial_readers_entity",\n')
t += ```

- [ ] **Step 2: Run them to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q 2>&1 | tail -3
```

Expected: failures (the 000404 files do not exist; the builder still names the source tables).

- [ ] **Step 3: Patch the builder**

```python
from pathlib import Path
cc = Path("src/dagster_v3/defs/sweden_company/companies_current.py"); t = cc.read_text()
old = """BOLAGSVERKET_FINANCIAL_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_bolagsverket_financial_metrics"
)
ESEF_FINANCIAL_SET = (
    f"SELECT ci.company_id FROM {CLICKHOUSE_DATABASE}.company_identifier AS ci "
    "WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 "
    f"AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM {CLICKHOUSE_DATABASE}.esef_financial_metrics AS m)"
)
FINANCIAL_REPORTS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_financial_reports"
)
"""
new = """# The financial entity (spec 2026-09-11 section 10): one table constant, read FINAL, active
# rows only -- a period the reviewer hid or every source withdrew must not keep a flag lit.
# The per-register flags read the row's `sources` (every source that won a field): the
# restated column a later Bolagsverket filing publishes (`bolagsverket_comparative`) is
# Bolagsverket's data too, so it lights the Bolagsverket flag with the reported rows.
COMPANY_FINANCIAL_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_financial"
FINANCIAL_SET = f"SELECT company_id FROM {COMPANY_FINANCIAL_TABLE} FINAL WHERE active = 1"
FINANCIAL_BOLAGSVERKET_SET = (
    f"SELECT company_id FROM {COMPANY_FINANCIAL_TABLE} FINAL "
    "WHERE active = 1 AND hasAny(sources, ['bolagsverket', 'bolagsverket_comparative'])"
)
FINANCIAL_ESEF_SET = (
    f"SELECT company_id FROM {COMPANY_FINANCIAL_TABLE} FINAL "
    "WHERE active = 1 AND has(sources, 'esef')"
)
# Filed reports are a source fact, not a presentation: a company with a filed report but no
# extracted figure still counts as having financials (owner ruling 2026-08-25).
FINANCIAL_REPORTS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_financial_reports"
)
"""
assert t.count(old) == 1; t = t.replace(old, new)
old = "  toUInt8(fin_bolagsverket OR fin_esef OR fin_reports) AS has_financial,\n"
new = "  toUInt8(fin_entity OR fin_reports) AS has_financial,\n"
assert t.count(old) == 1; t = t.replace(old, new)
old = """    toUInt8(i.company_id IN ({BOLAGSVERKET_FINANCIAL_SET})) AS fin_bolagsverket,
    toUInt8(i.company_id IN ({ESEF_FINANCIAL_SET})) AS fin_esef,
    toUInt8(i.company_id IN ({FINANCIAL_REPORTS_SET})) AS fin_reports,
"""
new = """    toUInt8(i.company_id IN ({FINANCIAL_SET})) AS fin_entity,
    toUInt8(i.company_id IN ({FINANCIAL_BOLAGSVERKET_SET})) AS fin_bolagsverket,
    toUInt8(i.company_id IN ({FINANCIAL_ESEF_SET})) AS fin_esef,
    toUInt8(i.company_id IN ({FINANCIAL_REPORTS_SET})) AS fin_reports,
"""
assert t.count(old) == 1; t = t.replace(old, new)
old = "    has_financial is extracted metrics OR filed reports; the SCB flag is omitted -- it is 1\n"
new = "    has_financial is an active financial-entity row OR a filed report; the SCB flag is omitted -- it is 1\n"
assert t.count(old) == 1; cc.write_text(t.replace(old, new)); print("companies_current.py patched")
```

- [ ] **Step 4: Generate migration 000404 from the builder render (run from `corpscout/services/dagster_v3`, with the env so the module imports)**

```bash
uv run --env-file .env python - <<'PY'
from pathlib import Path
from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql
MIG = Path("../../clickhouse/migrations")
render = build_se_companies_serving_sql()
assert render.count("corpscout.se_company_financial FINAL") == 3
def statements(sql): return [s.strip() for s in sql.split(";") if s.strip()]
def body(st):
    lines = st.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")): lines.pop(0)
    return "\n".join(lines).strip()
def modify_body(sql):
    [st] = [s for s in statements(sql) if "MODIFY QUERY" in s]
    return st[st.index("MODIFY QUERY\n") + len("MODIFY QUERY\n"):]
prev_render = modify_body((MIG / "000403_corpscout_se_companies_serving_no_workplace.up.sql").read_text())
filing_282 = (MIG / "000282_corpscout_se_annual_report_filing_status.up.sql").read_text()
[filing_view] = [body(s) for s in statements(filing_282) if body(s).startswith("CREATE OR REPLACE VIEW")]
old_leg = "        FROM corpscout.se_company_financials_latest\n"
assert filing_view.count(old_leg) == 1
new_leg = ("        FROM (\n            SELECT company_id, max(period_end) AS period_end_date, max(folded_at) AS resolved_at\n"
           "            FROM corpscout.se_company_financial FINAL\n            WHERE active = 1 AND scope = 'standalone'\n            GROUP BY company_id\n        )\n")
new_filing_view = filing_view.replace(old_leg, new_leg)
head = """CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SWEDISH FINANCIAL READERS MOVE TO THE ENTITY (financial spec 2026-09-11 section 10,
-- slice 4a, owner decision 2026-09-11: no parallel run). corpscout.se_company_financial is
-- the fold's output -- one row per company, accounting scope and period end, folded from
-- Bolagsverket, its restated column, ESEF and Ratsit -- and since 2026-09-13 it holds
-- 4,180,595 rows for 795,434 companies. Two readers in the ledger still derive their
-- financial facts from the old source tables, and both are re-pointed here.
--
-- 1. corpscout.se_companies_serving: has_financial becomes "an active entity row OR a filed
--    report" (the 2026-08-25 widening on filed reports stays), fin_bolagsverket and fin_esef
--    become has(sources, ...) over the same active rows (the restated column
--    bolagsverket_comparative lights the Bolagsverket flag with the reported rows), and the
--    IN-set subqueries on se_bolagsverket_financial_metrics, esef_financial_metrics and
--    company_identifier leave the view. Every other column is unchanged.
--
-- 2. corpscout.se_annual_report_filing_status_current (migration 000282): its
--    data_available leg reads the entity's newest active standalone period end per company
--    instead of se_company_financials_latest (which is itself rebuilt from the entity by the
--    company_financials_latest asset from this slice on). The observations leg is unchanged.
--    A plain view, so CREATE OR REPLACE VIEW does it in place.
--
-- AN IN-PLACE REPOINT of the serving view, the recipe of 000393/000396/000398/000403, not
-- the staged swap of 000391/000392. Only the view's own SELECT changes, and ALTER TABLE's
-- MODIFY-QUERY clause does that where it stands: the refreshable view keeps the rows it is
-- already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the
-- new query. There is no _next view to build and so NO SYSTEM WAIT VIEW in this file -- a
-- refresh of this view takes 13 to 15 minutes and the migrate client's read_timeout is 300
-- seconds, so waiting on one here would drop the client with the ledger dirty. Every
-- statement below returns in milliseconds.
--
-- THE VIEW IS STOPPED FIRST so no refresh can start against the old definition and finish
-- against the new one. Apply OUTSIDE the :45 refresh window -- just after a refresh finishes
-- (about :00) -- and the STOP cannot interrupt a run.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped,
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 404 so the ledger records where the
-- database actually is. The address design doc's runbook section has the full sequence.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
"""
up = head + render + ";\n\nSYSTEM START VIEW corpscout.se_companies_serving;\n\n-- The filing-status view's data_available leg, from the entity (000282's text otherwise).\n" + new_filing_view + ";\n"
(MIG / "000404_corpscout_se_financial_readers_entity.up.sql").write_text(up)
down_head = """CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000404: the serving view goes back to the render 000403 deployed (has_financial
-- from se_bolagsverket_financial_metrics, esef_financial_metrics through company_identifier
-- and se_financial_reports) and se_annual_report_filing_status_current back to 000282's text
-- (its data_available leg from se_company_financials_latest). Same in-place recipe as the up
-- file -- stop, repoint, start -- because only the view's own SELECT changes, and for the
-- same reason there is no SYSTEM WAIT VIEW to sit through. Running this restores the
-- database only -- the deployed dagster code (companies_current.py, the
-- company_financials_latest Sweden leg) must be rolled back with it, or the next render taken
-- from the builder re-applies the repoint.
--
-- THE SELECT BELOW IS 000403's, COPIED VERBATIM rather than hand-written, and dagster_v3
-- tests/test_se_companies_serving_mv.py pins it against that file's own MODIFY-QUERY body.

SYSTEM STOP VIEW corpscout.se_companies_serving;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
"""
down = down_head + prev_render + ";\n\nSYSTEM START VIEW corpscout.se_companies_serving;\n\n-- 000282's view, verbatim.\n" + filing_view + ";\n"
(MIG / "000404_corpscout_se_financial_readers_entity.down.sql").write_text(down)
for f in (up, down):
    bad = [l for l in f.splitlines() if l.lstrip().startswith("--") and ";" in l]
    assert not bad, bad
print("000404 written:", len(up.splitlines()), "up lines;", len(down.splitlines()), "down lines")
PY
```

Expected: `000404 written: 310 up lines; 274 down lines`. Never edit the generated render by hand; if the builder changes, re-run this step.

- [ ] **Step 5: Run the tests, the engine suite and ruff**

```bash
uv run --env-file .env pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py tests/test_se_companies_current_asset.py -q 2>&1 | tail -2
uv run --env-file .env pytest tests/test_se_companies_serving_sql.py -q -m integration 2>&1 | tail -2
uv run ruff check src/dagster_v3/defs/sweden_company/companies_current.py tests/test_se_companies_serving_mv.py tests/test_se_companies_serving_sql.py tests/test_clickhouse_migrations.py
```

Expected: all passed (the migrations file has hundreds of tests; the mv file 8; the engine suite 46, under both `join_use_nulls` settings); ruff clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py corpscout/clickhouse/migrations/000404_corpscout_se_financial_readers_entity.up.sql corpscout/clickhouse/migrations/000404_corpscout_se_financial_readers_entity.down.sql corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py corpscout/services/dagster_v3/tests/test_se_companies_serving_sql.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(se-financial): migration 000404 — the serving view's financial flags and the filing-status view read the entity"
```

---

### Task 3: The section-presence model and the publish reconciliation

**Files:**
- Modify: `src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql`, `src/dagster_v3/defs/company_serving/dbt/models/sources.yml`, `src/dagster_v3/defs/company_serving/publish.py`
- Create: `tests/test_company_serving_financials_presence.py`

- [ ] **Step 1: Write the failing test**

`tests/test_company_serving_financials_presence.py`, exactly:

```python
"""Financial slice 4a (spec 2026-09-11 section 10): the section-presence model's financials
leg and publish.py's financials reconciliation read the financial entity's ACTIVE rows, keyed
by company, not se_company_financials_latest."""

from pathlib import Path

from dagster_v3.defs.company_serving import publish

DBT_MODELS = Path(publish.__file__).resolve().parent / "dbt" / "models"


def test_the_presence_model_reads_the_entitys_active_rows() -> None:
    text = (DBT_MODELS / "company_section_presence_current_build.sql").read_text(encoding="utf-8")
    # The executable lines only: the model's own comment names the projection it replaced.
    model = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    assert "FROM {{ source('corpscout', 'se_company_financial') }} AS financials FINAL" in model
    assert "WHERE financials.active = 1" in model
    assert "financials.company_id, 'financials', financials.company_id, financials.folded_at" in model
    assert "se_company_financials_latest" not in model
    sources = (DBT_MODELS / "sources.yml").read_text(encoding="utf-8")
    assert "      - name: se_company_financial\n" in sources


def test_the_publish_reconciliation_counts_the_same_rows() -> None:
    text = Path(publish.__file__).read_text(encoding="utf-8")
    assert "FROM corpscout.se_company_financial AS financials FINAL" in text
    assert "WHERE financials.active = 1" in text
    assert "corpscout.se_company_financials_latest AS financials" not in text
```

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run --env-file .env pytest tests/test_company_serving_financials_presence.py -q
```

Expected: 2 failed (the model and the reconciliation still read `se_company_financials_latest`).

- [ ] **Step 3: Patch the model, the sources and the reconciliation**

```python
from pathlib import Path
model = Path("src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql"); t = model.read_text()
old = """    SELECT '{{ var("country_code") }}', financials.company_id, 'financials', financials.company_id, financials.resolved_at
    FROM {{ source('corpscout', 'se_company_financials_latest') }} AS financials
    INNER JOIN company_anchors AS anchors ON anchors.company_id = financials.company_id
"""
new = """    -- The financial entity (spec 2026-09-11 section 10, slice 4a): one presence row per
    -- company with an ACTIVE folded period, keyed by company as before; folded_at is the
    -- observation instant. se_company_financials_latest is a projection of the same table
    -- since that slice, so the presence reads the table it is derived from.
    SELECT '{{ var("country_code") }}', financials.company_id, 'financials', financials.company_id, financials.folded_at
    FROM {{ source('corpscout', 'se_company_financial') }} AS financials FINAL
    INNER JOIN company_anchors AS anchors ON anchors.company_id = financials.company_id
    WHERE financials.active = 1
"""
assert t.count(old) == 1; model.write_text(t.replace(old, new))
src = Path("src/dagster_v3/defs/company_serving/dbt/models/sources.yml"); t = src.read_text()
old = "      - name: se_company_financials_latest\n"
new = "      - name: se_company_financials_latest\n      - name: se_company_financial\n"
assert t.count(old) == 1; src.write_text(t.replace(old, new))
pub = Path("src/dagster_v3/defs/company_serving/publish.py"); t = pub.read_text()
old = """        "financials": (
            "SELECT countDistinct(financials.company_id) "
            "FROM corpscout.se_company_financials_latest AS financials "
            f"INNER JOIN (SELECT DISTINCT company_id FROM {stages[tables.EXTERNAL_IDENTIFIERS.name]}) AS anchors "
            "ON anchors.company_id = financials.company_id"
        ),
"""
new = """        # The financial entity (spec 2026-09-11 section 10, slice 4a): the presence model
        # counts companies with an ACTIVE folded period, read FINAL, anchored like addresses.
        "financials": (
            "SELECT countDistinct(financials.company_id) "
            "FROM corpscout.se_company_financial AS financials FINAL "
            f"INNER JOIN (SELECT DISTINCT company_id FROM {stages[tables.EXTERNAL_IDENTIFIERS.name]}) AS anchors "
            "ON anchors.company_id = financials.company_id "
            "WHERE financials.active = 1"
        ),
"""
assert t.count(old) == 1; pub.write_text(t.replace(old, new)); print("presence, sources and reconciliation patched")
```

- [ ] **Step 4: Run the tests, the definitions check and ruff**

```bash
uv run --env-file .env pytest tests/test_company_serving_financials_presence.py tests/test_company_serving_dbt.py tests/test_company_serving.py -q 2>&1 | tail -2
uv run dg utils refresh-defs-state 2>&1 | tail -1
uv run dg check defs 2>&1 | tail -1
uv run ruff check src/dagster_v3/defs/company_serving/publish.py tests/test_company_serving_financials_presence.py
```

Expected: all passed; `All definitions loaded successfully.` (the dbt defs state must be refreshed after the sources.yml change — the `company_domain_suggestions` adapter traceback that `refresh-defs-state` may print is non-fatal noise); ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/publish.py corpscout/services/dagster_v3/tests/test_company_serving_financials_presence.py
git commit -m "feat(se-financial): the section-presence model and its reconciliation count active entity periods"
```

(If `dg utils refresh-defs-state` changed tracked files under `src/dagster_v3/defs/company_serving/dbt/` or a `.dg` state file, add them to the same commit — say so in the report.)

---

### Task 4: The owner-run retirement scripts for the two source views

**Files:**
- Create: `corpscout/clickhouse/operations/se_financial_views_retirement_precheck.sql`, `se_financial_views_retirement_drops.sql`, `se_financial_views_retirement_postcheck.sql`
- Create: `tests/test_se_financial_views_retirement_drops.py`

- [ ] **Step 1: Write the test**

`tests/test_se_financial_views_retirement_drops.py`, exactly:

```python
"""The financial slice-4 drop scripts say exactly what spec section 10's retirement list
says: the two Sweden-only financial source views, and nothing else.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production object. It reads the SQL, parses the object
names out, and compares them as WHOLE names -- se_company_financial is a prefix of the
entity's four sibling tables, none of which may ever appear in a DROP here.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_financial_views_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_financial_views_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_financial_views_retirement_postcheck.sql"

DROP_ORDER = (
    ("VIEW", "se_financials_bolagsverket_current"),
    ("VIEW", "se_financials_esef_current"),
)
# Never droppable: the entity's five tables, the source tables the two views read, the filed
# reports table, the cross-country projection and the serving view.
KEPT = (
    "se_company_financial_suggestion",
    "se_company_financial",
    "se_company_financial_history",
    "se_company_financial_precedence",
    "se_company_financial_rule",
    "se_bolagsverket_financial_metrics",
    "esef_financial_metrics",
    "esef_filings",
    "company_identifier",
    "se_financial_reports",
    "se_company_financials_latest",
    "se_companies_serving",
)

_DROP = re.compile(r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_two_views_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object_and_only_views() -> None:
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(KEPT)
    assert all(kind == "VIEW" for kind, _ in _statements(DROPS))


def test_no_script_uses_sync() -> None:
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_both_views() -> None:
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for _, name in DROP_ORDER:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_gates_on_zero_readers_the_engine_the_counts_and_the_repointed_serving_view() -> None:
    """Views cannot be UNDROPped (spec section 10), so the precheck is the safety: no view or
    materialized view may still read either name, both must be plain Views with their row
    counts recorded, and the serving view must already read the entity (migration 000404)."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS no_readers" in sql and "(FROM|JOIN)" in sql
    assert "engine" in sql and "FROM system.tables" in sql
    for view in ("bolagsverket", "esef"):
        assert f"SELECT count() AS se_financials_{view}_current_rows" in sql
        assert f"FROM corpscout.se_financials_{view}_current" in sql
    assert "corpscout.se_company_financial FINAL" in sql and "AS serving_reads_entity" in sql
    assert "system.view_refreshes" in sql


def test_the_postcheck_asserts_absence_and_the_kept_objects() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
    for name in KEPT:
        if name != "company_identifier" and name != "se_companies_serving":
            assert f"'{name}'" in sql, name
```

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run --env-file .env pytest tests/test_se_financial_views_retirement_drops.py -q
```

Expected: FAIL (`FileNotFoundError` on the scripts).

- [ ] **Step 3: Write the three scripts**

`corpscout/clickhouse/operations/se_financial_views_retirement_precheck.sql`, exactly:

```sql
-- SE financial slice 4 precheck. Run BEFORE se_financial_views_retirement_drops.sql, record
-- the output in the ledger, and check the three gates below. Both objects are plain VIEWs
-- (migration 000286, the ESEF one re-issued by 000364): UNDROP does not recover a view, so
-- the gates are the only safety there is.
--
-- Gate 1: nothing left in ClickHouse reads either view. The match is on a FROM or a JOIN, so
-- a provenance string literal in a view body cannot hold the gate open, and the trailing
-- boundary class ([^_a-zA-Z0-9]|$) rules out a prefix hit. Their only readers were the
-- backoffice's source-view queries, retired in slice 4b; expect no_readers = 1.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN ('se_financials_bolagsverket_current', 'se_financials_esef_current')
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_financials_bolagsverket_current|se_financials_esef_current)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine of both objects about to go (expect View, View) and their row counts, so
-- the ledger records a real number for every object destroyed. total_rows is NULL for a
-- plain View, which is why the two SELECT count() follow.
SELECT name, engine, total_rows
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_financials_bolagsverket_current', 'se_financials_esef_current')
ORDER BY name;

SELECT count() AS se_financials_bolagsverket_current_rows FROM corpscout.se_financials_bolagsverket_current;

SELECT count() AS se_financials_esef_current_rows FROM corpscout.se_financials_esef_current;

-- Gate 3: the entity that replaced them is live and read by the serving view (migration
-- 000404), and the view is healthy at its last refresh. If serving_reads_entity is 0, 000404
-- has not been applied here and the drops must not run.
SELECT
    countIf(position(create_table_query, 'corpscout.se_company_financial FINAL') > 0) > 0 AS serving_reads_entity,
    countIf(position(create_table_query, 'se_bolagsverket_financial_metrics') > 0) = 0 AS serving_off_the_metrics_table
FROM system.tables
WHERE database = 'corpscout' AND name = 'se_companies_serving';

SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS entity_tables_present, groupArray(name) AS present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_financial_suggestion', 'se_company_financial', 'se_company_financial_history',
      'se_company_financial_precedence', 'se_company_financial_rule'
  );

SELECT count() AS published_periods, uniqExact(company_id) AS published_companies
FROM corpscout.se_company_financial FINAL
WHERE active = 1;
```

`corpscout/clickhouse/operations/se_financial_views_retirement_drops.sql`, exactly:

```sql
-- SE financial slice 4: the two Sweden-only financial source views leave ClickHouse.
-- OWNER-RUN, by hand, after the slice-4b backoffice deploy is live (its source-view queries
-- were the views' only readers), migration 000404 is applied, and
-- se_financial_views_retirement_precheck.sql is clean. Nothing in this repo executes this
-- file (dev-phase ledger policy, owner ruling 2026-08-25: a drop whose gate cannot be
-- checked at write time never goes in the ledger).
--
-- Both are plain VIEWs (000286; the ESEF one re-issued in place by 000364), and UNDROP TABLE
-- does not recover a plain VIEW -- there is no window to fall back on. Their recreation DDL
-- lives today in corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.up.sql
-- and 000364_corpscout_esef_personnel_expenses.up.sql; slice 4b empties that DDL out of the
-- ledger once this drop has run, per the dev-phase ledger policy, so from then on recreating
-- either view is git history, not the live tree.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it): the entity's
-- five tables (se_company_financial_suggestion, se_company_financial,
-- se_company_financial_history, se_company_financial_precedence, se_company_financial_rule),
-- the source tables the views read (se_bolagsverket_financial_metrics, esef_financial_metrics,
-- esef_filings, company_identifier), se_financial_reports, se_company_financials_latest and
-- the serving view.

DROP VIEW IF EXISTS corpscout.se_financials_bolagsverket_current;
DROP VIEW IF EXISTS corpscout.se_financials_esef_current;
```

`corpscout/clickhouse/operations/se_financial_views_retirement_postcheck.sql`, exactly:

```sql
-- SE financial slice 4 postcheck. Run immediately after se_financial_views_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty; if it is not, re-run the drop for the
-- names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_financials_bolagsverket_current', 'se_financials_esef_current');

-- The kept objects are all still there. Expect 10.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_financial_suggestion', 'se_company_financial', 'se_company_financial_history',
      'se_company_financial_precedence', 'se_company_financial_rule',
      'se_bolagsverket_financial_metrics', 'esef_financial_metrics', 'esef_filings',
      'se_financial_reports', 'se_company_financials_latest'
  );

-- And the serving view still refreshes (re-run after the next :45).
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS serving_rows, countIf(has_financial = 1) AS with_financial
FROM corpscout.se_companies_serving;
```

- [ ] **Step 4: Run the test and ruff, and prove the precheck parses on the engine**

```bash
uv run --env-file .env pytest tests/test_se_financial_views_retirement_drops.py tests/test_se_person_retirement_drops.py -q
uv run ruff check tests/test_se_financial_views_retirement_drops.py
printf 'CREATE DATABASE IF NOT EXISTS corpscout; CREATE TABLE corpscout.se_company_financial (company_id String, active UInt8) ENGINE = ReplacingMergeTree ORDER BY company_id; CREATE VIEW corpscout.se_financials_bolagsverket_current AS SELECT 1 AS x; CREATE VIEW corpscout.se_financials_esef_current AS SELECT 1 AS x;\n' > /tmp/fin_views_fixture.sql
cat /tmp/fin_views_fixture.sql ../../clickhouse/operations/se_financial_views_retirement_precheck.sql | docker run --rm -i clickhouse/clickhouse-server:26.5 clickhouse-local --multiquery 2>&1 | tail -12
```

Expected: 6 + the person file's tests passed; ruff clean; the precheck runs to completion on clickhouse-local (its `system.view_refreshes` read returns nothing there, the `no_readers` gate prints 1, the engine listing prints `View` twice).

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/clickhouse/operations/se_financial_views_retirement_precheck.sql corpscout/clickhouse/operations/se_financial_views_retirement_drops.sql corpscout/clickhouse/operations/se_financial_views_retirement_postcheck.sql corpscout/services/dagster_v3/tests/test_se_financial_views_retirement_drops.py
git commit -m "chore(se-financial): owner-run retirement scripts for the two Sweden-only financial source views"
```

---

### Task 5: Docs and whole-slice check

**Files:**
- Modify: `src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md`, `docs/sweden-data-sources.md`, `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md`

- [ ] **Step 1: Rewrite the two docs sections**

```python
from pathlib import Path
design = Path("src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md"); t = design.read_text()
start = t.index("## Source-specific serving views"); end = t.index("## Job And Schedule")
design.write_text(t[:start] + "## Serving: the financial entity\n\nThe physical metrics table is named\n`corpscout.se_bolagsverket_financial_metrics` because every row is derived from\nBolagsverket annual-account facts. It is not a cross-source canonical table: it\nis one SOURCE of the Swedish financial entity (spec 2026-09-11), whose fold\npublishes one row per company, accounting scope and period end in\n`corpscout.se_company_financial` from Bolagsverket's reported rows, its restated\ncolumn (`bolagsverket_comparative`), ESEF and Ratsit, with a source beside every\nfigure.\n\n```text\nse_bolagsverket_financial_metrics -> se_company_financial_suggestion (source bolagsverket, bolagsverket_comparative)\nesef_financial_metrics            -> se_company_financial_suggestion (source esef)\nse_ratsit_financial_periods       -> se_company_financial_suggestion (source ratsit)\nse_company_financial_suggestion   -> se_company_financial (the fold) -> every Swedish reader\n```\n\nEvery Swedish reader -- the backoffice Financial tab and the public financials\npage, `se_company_financials_latest`, the serving view's `has_financial` and\nper-register flags, the section-presence model and the filing-status view --\nreads the entity's active rows. The two Sweden-only source views\n`se_financials_bolagsverket_current` and `se_financials_esef_current` were\nretired in slice 4 (owner-run drops, `se_financial_views_retirement_*.sql`).\nPrecedence between sources is the entity's (`se_company/financial/precedence.py`,\nRatsit first); this module never chooses a winner.\n\nThe ordered concept mappings are code-owned in\n`defs/common/financial_metric_mappings.py`. Canonical presentation keys map to\nsource concepts, while storage aliases such as ESEF `operating_profit` and\nBolagsverket `operating_profit_loss` remain source-specific.\n\n" + t[end:])
ds = Path("docs/sweden-data-sources.md"); t = ds.read_text()
start = t.index("### Financial serving boundary"); end = t.index("### Jobs and schedules")
ds.write_text(t[:start] + "### Financial serving boundary\n\nThe Financials page reads the Swedish financial ENTITY (spec 2026-09-11):\n`corpscout.se_company_financial`, one row per company, accounting scope\n(`standalone` | `consolidated`) and period end, folded from Bolagsverket's\nreported rows, its restated column, ESEF and Ratsit with a source beside every\nfigure, and `corpscout.se_company_financial_suggestion` for every source's own\nrow. A standalone (legal-entity) figure never merges with a consolidated (group)\none: the scope is inside the row key. The former same-shape views\n`se_financials_bolagsverket_current` and `se_financials_esef_current` were\nretired in slice 4 of that spec.\n\n" + t[end:])
print("docs rewritten")
```

For the record, the design doc's new section reads:

```markdown
## Serving: the financial entity

The physical metrics table is named
`corpscout.se_bolagsverket_financial_metrics` because every row is derived from
Bolagsverket annual-account facts. It is not a cross-source canonical table: it
is one SOURCE of the Swedish financial entity (spec 2026-09-11), whose fold
publishes one row per company, accounting scope and period end in
`corpscout.se_company_financial` from Bolagsverket's reported rows, its restated
column (`bolagsverket_comparative`), ESEF and Ratsit, with a source beside every
figure.

```text
se_bolagsverket_financial_metrics -> se_company_financial_suggestion (source bolagsverket, bolagsverket_comparative)
esef_financial_metrics            -> se_company_financial_suggestion (source esef)
se_ratsit_financial_periods       -> se_company_financial_suggestion (source ratsit)
se_company_financial_suggestion   -> se_company_financial (the fold) -> every Swedish reader
```

Every Swedish reader -- the backoffice Financial tab and the public financials
page, `se_company_financials_latest`, the serving view's `has_financial` and
per-register flags, the section-presence model and the filing-status view --
reads the entity's active rows. The two Sweden-only source views
`se_financials_bolagsverket_current` and `se_financials_esef_current` were
retired in slice 4 (owner-run drops, `se_financial_views_retirement_*.sql`).
Precedence between sources is the entity's (`se_company/financial/precedence.py`,
Ratsit first); this module never chooses a winner.

The ordered concept mappings are code-owned in
`defs/common/financial_metric_mappings.py`. Canonical presentation keys map to
source concepts, while storage aliases such as ESEF `operating_profit` and
Bolagsverket `operating_profit_loss` remain source-specific.

```

and the data-sources doc's:

```markdown
### Financial serving boundary

The Financials page reads the Swedish financial ENTITY (spec 2026-09-11):
`corpscout.se_company_financial`, one row per company, accounting scope
(`standalone` | `consolidated`) and period end, folded from Bolagsverket's
reported rows, its restated column, ESEF and Ratsit with a source beside every
figure, and `corpscout.se_company_financial_suggestion` for every source's own
row. A standalone (legal-entity) figure never merges with a consolidated (group)
one: the scope is inside the row key. The former same-shape views
`se_financials_bolagsverket_current` and `se_financials_esef_current` were
retired in slice 4 of that spec.

```

- [ ] **Step 2: Record code completion in the spec**

Section 12 item 4 (`4. Cutover: …`): append `Slice 4a (data side) code complete 2026-09-13 on branch se-financial-entity (plan 2026-09-13-se-company-financial-4a-readers.md); migration 000404, not 000402; prod pending. Slice 4b (backoffice) follows.` and re-wrap the item like its neighbours (three-space continuation indent, about 97 columns).

- [ ] **Step 3: Run the slice's suites and the wider suite once**

```bash
uv run --env-file .env pytest tests/test_company_financials_latest.py tests/test_se_companies_serving_mv.py tests/test_se_financial_views_retirement_drops.py tests/test_company_serving_financials_presence.py tests/test_company_serving_dbt.py tests/test_company_serving.py tests/test_clickhouse_migrations.py tests/test_se_companies_current_asset.py tests/test_sweden_financial_assets.py tests/test_clickhouse_leaf_checks.py -q 2>&1 | tail -2
uv run --env-file .env pytest tests/test_se_companies_serving_sql.py -q -m integration 2>&1 | tail -2
set -a; source .env; set +a; uv run pytest tests -q -p no:cacheprovider --ignore=tests/test_schedule_cron_contracts.py --deselect tests/test_backfill_policy_contracts.py::test_every_partitioned_asset_uses_multi_run_backfill_policy --deselect tests/test_duckdb_bulk_loading_contract.py::test_production_has_only_the_explicit_ted_executemany_debt --deselect tests/test_nace_categories.py::test_nace_assets_are_registered_as_staged_flow --deselect tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values --deselect tests/test_technology_aliases_clickhouse.py::test_catalog_asset_publishes_aliases_clears_them_and_rejects_bad_input 2>&1 | tail -3
```

Expected: all green; the wider run `N passed, 3 skipped, 5 deselected`, no failures (about 8 minutes).

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md corpscout/services/dagster_v3/docs/sweden-data-sources.md corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md
git commit -m "docs(se-financial): the serving docs name the entity; spec records slice 4a code complete"
```

---

### Task 6: Prod (after the final review and the merge)

**Preconditions:** final review clean; merged into main through a temporary worktree on `main` (re-check overlap with main's new commits and the owner's dirty files right before); the worktree fast-forwarded; prod ledger 403 clean.

- [ ] **Step 1: Apply migration 000404 outside the :45 refresh window**

Wait for the serving view's refresh to have finished (`SELECT view, status, last_success_time FROM system.view_refreshes WHERE view = 'se_companies_serving'` shows `Scheduled` with a fresh `last_success_time`; the refresh runs hourly at :45 and takes 13 to 15 minutes, so about :00 to :40 is safe). Then from `corpscout/` (needs `corpscout/.env`): `make clickhouse-migrate-up-one`; confirm `SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1` = 404, 0; confirm the view is `Scheduled` (not stopped) and that `SHOW CREATE TABLE corpscout.se_companies_serving` contains `corpscout.se_company_financial FINAL` three times and `se_bolagsverket_financial_metrics` nowhere; confirm `SHOW CREATE VIEW corpscout.se_annual_report_filing_status_current` reads the entity. If the migrate client dropped between STOP and START: `SYSTEM START VIEW corpscout.se_companies_serving`, then `make clickhouse-migrate-force VERSION=404`.

- [ ] **Step 2: Deploy dagster_v3 from the worktree**

The deploy recipe (uv sync, the two dbt parses, `dg utils refresh-defs-state` — mandatory here because `sources.yml` changed — `dg check defs`, ansible light sync); expected `RC=0`, `failed=0`. Confirm through GraphQL `assetNodes` that `se_company_financials_latest_clickhouse` now depends on `se_company_financial_fold`.

- [ ] **Step 3: Rebuild the two derived tables**

In-process on the host if the run queue is still held (`/tmp/fin_asset_run.sh <asset> <tag> <config.json>` from slice 3 with `{}` as the config): first `se_company_financials_latest_clickhouse` (expected `row_count` about 795,403, the companies with an active standalone period, against 579,766 before), then `company_serving_current` (the dbt build + publish; its reconciliation must report the financials count matching the presence model). Readouts:

```sql
SELECT count() AS latest_rows, countIf(revenue_amount_original IS NOT NULL) AS with_revenue, countIf(employees IS NOT NULL) AS with_employees, min(fiscal_year), max(fiscal_year) FROM corpscout.se_company_financials_latest;
SELECT company_id, fiscal_year, period_end_date, currency, revenue_amount_original, revenue_amount_usd, employees, years_count FROM corpscout.se_company_financials_latest WHERE company_id = '5567081699';
SELECT filing_status, count() FROM corpscout.se_annual_report_filing_status_current GROUP BY filing_status ORDER BY filing_status;
SELECT section, count() AS companies, sum(item_count) AS items FROM corpscout.company_section_presence_current WHERE section = 'financials' GROUP BY section;
```

Expected for 5567081699: fiscal_year 2025, period_end_date 2025-12-31, revenue 65,000,000 SEK from the Ratsit row, employees 19, years_count 8.

- [ ] **Step 4: The serving view after its next :45 refresh**

```sql
SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE view = 'se_companies_serving';
SELECT count() AS rows, countIf(has_financial = 1) AS with_financial, countIf(source_bolagsverket = 1) AS bolagsverket, countIf(source_esef = 1) AS esef FROM corpscout.se_companies_serving;
```

Record the `with_financial` count before (read it in Step 1 before migrating) and after; the entity covers 795,403 companies with an active standalone period plus the filed-report-only companies, so the count should rise from the Bolagsverket-and-report population to roughly that.

- [ ] **Step 5: Record**

Append the prod record to spec section 12 item 4 (ledger 404, deploy, the three rebuild counts, the smoke company's latest row, the serving counts before/after), commit on the branch as `docs(se-financial): slice 4a shipped, prod record; plan ticked`, merge through a temporary worktree on main, fast-forward, update the memory file, then write slice 4b's plan (the backoffice cutover; the owner-run drops and the ledger emptying of 000286/000364 belong to it).

---

## Self-review

**Spec coverage (section 10).** `company_financials_latest/sql.py`'s Sweden leg + `UPSTREAM_KEYS["se"]` → Task 1; the presence model's financials leg + `publish.py`'s reconciliation → Task 3; `companies_current.py`'s `has_financial`/`fin_bolagsverket`/`fin_esef` through one table constant with `fin_reports` kept, and the serving view re-pointed with `MODIFY QUERY` in the cutover migration → Task 2; `se_annual_report_filing_status_current`'s first leg re-issued in the same migration → Task 2; the owner-run drop script with precheck gating on zero readers, plus its test → Task 4; the two docs paragraphs → Task 5. Deferred to slice 4b by dependency: the backoffice readers (`company-sections.server.ts`, `queries.server.ts`, `countries.ts`) and deletions, the drops themselves, emptying 000286/000364 and extending `EMPTIED_MIGRATIONS`, deleting `test_sweden_financial_source_views_migration.py`.

**Deviations, stated.** The serving flag for Bolagsverket also counts `bolagsverket_comparative` rows (Bolagsverket data). The filing-status leg keeps 000282's synthetic `source_record_id` prefix `financials-latest:` so its readers see an unchanged key. `has_financial` keeps the filed-report arm (the owner's 2026-08-25 widening) in addition to the entity arm. `test_build_latest_insert_sql_qualifies_resolved_at_tiebreak` skips `se` (a hand-built SELECT with a `folded_at` tiebreak) as it skips `br`.

**Placeholder scan.** None: every patch is an executable script that asserts each replaced snippet exists exactly once; new files are given in full; the migration is generated from the builder and pinned by tests.

**Type consistency.** `SOURCES["se"]` keeps `table` and `id` for `assets.py`'s existence check and the coverage test; `build_latest_insert_sql("se")` returns `_SE_SELECT` whose aliases are exactly `COMPANY_FINANCIALS_LATEST_COLUMNS[:-1]` plus `resolved_at`; the mv test's `FINANCIAL_ENTITY`/`FILING_VIEW` constants match the migration text the generator writes; the engine suite's stub `se_company_financial (company_id, sources, active, scope)` carries exactly the columns the three new arms read.
