# finland_xbrl SQL transforms → dbt-duckdb (R4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Migrate the two hand-written SQL transforms in `finland_xbrl` — `eligible_financial_reports` and `fi_prh_xbrl_financial_metrics` — to a dbt-duckdb project (like `finland_resolved`), with the hardcoded Python metric map becoming a dbt **seed**.

**Architecture:** A new dbt project at `defs/finland_xbrl/dbt/` targets the same `data/finland_ytj.duckdb`, schema `finland_prh_xbrl`. Two models:
- `eligible_financial_reports` — port of `build_xbrl_eligible_financial_reports`: join `finland_prh_xbrl.financial_reports` × `finland_prhytj.all_companies`, filter active + has-website.
- `fi_prh_xbrl_financial_metrics` — port of `build_xbrl_financial_metrics`: multi-CTE pivot of `fi_prh_xbrl_facts_raw` against the metric-map **seed** (`{{ ref('xbrl_metric_map') }}`), joined to `fi_prh_xbrl_statement_documents`. The per-metric projection is a Jinja loop over the 13 metric columns.

A `@dbt_assets` op runs `dbt build` (seed + models). The translator maps model names to the **existing** asset keys so downstream stays intact: model `eligible_financial_reports` → key `finland_xbrl_eligible_financial_reports`; model `fi_prh_xbrl_financial_metrics` → key `fi_prh_xbrl_financial_metrics`. The Python download asset `finland_xbrl_raw_xml_documents` (which reads the `eligible_financial_reports` table) stays Python; only its dep declaration re-points to the dbt eligible model. `dbt_assets` carries `pool="finland_ytj_duckdb"`.

**Why no behavior change:** the models build the **same table names** in the **same schema**, so `load_eligible_financial_report_rows` (read by the download) and any metrics consumer are unaffected. The metric map seed reproduces `XBRL_FINANCIAL_METRIC_MAP` exactly; `employees` stays a NULL placeholder column (no fact maps to it, same as today).

**Critical gotchas:**
1. **Resource-key clash:** `exchange_rates_v2` uses `dbt`, `finland_resolved` uses `finland_resolved_dbt`. This project uses a third distinct key: **`finland_xbrl_dbt`** (param + defs key).
2. **Distinct duckdb env var:** use `FINLAND_XBRL_DUCKDB_PATH` (pointed at `data/finland_ytj.duckdb`) to avoid coupling with `finland_resolved`'s `FINLAND_YTJ_DUCKDB_PATH` (both target the same file; independent vars avoid import-order surprises).
3. **Partitioned → unpartitioned boundary:** the metrics model (unpartitioned dbt) depends (via sources) on the monthly-partitioned parse — same boundary that already works for the old unpartitioned metrics asset. Verify `dg check`.

**Scope:** R4 only. Out of scope: R3 (incremental report list), R6–R8.

**Reference:** mirror `src/dagster_v3/defs/finland_resolved/` (dbt project, `assets.py` wiring, translator, `get_asset_key_for_model`) and `src/dagster_v3/defs/exchange_rates_v2/dbt/` (seeds aren't used there, but the project/profile layout is the same). Read `tests/test_finland_resolved_dbt.py` for the dbt-build test pattern.

**Test command:** `uv run pytest tests/test_finland_xbrl_dbt.py -v`

---

### Task 1: Scaffold the dbt project + the metric-map seed

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/dbt/dbt_project.yml`, `profiles.yml`, `.gitignore`, `seeds/xbrl_metric_map.csv`, `models/.gitkeep`
- Test: `tests/test_finland_xbrl_dbt.py`

- [ ] **Step 1: Write the failing test (seed loads)**

Create `tests/test_finland_xbrl_dbt.py`:

```python
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_xbrl" / "dbt"
)


def _dbt(args, db_path, monkeypatch):
    monkeypatch.setenv("FINLAND_XBRL_DUCKDB_PATH", str(db_path))
    res = dbtRunner().invoke(args + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    assert res.success, res.exception


def test_metric_map_seed_loads(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _dbt(["seed"], db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select count(*), count(distinct metric_code) from finland_prh_xbrl.xbrl_metric_map"
    ).fetchone()
    assert rows == (12, 12)
    sample = conn.execute(
        "select metric_code from finland_prh_xbrl.xbrl_metric_map "
        "where concept_qname='fi_met:md103' and mcy_member_code='fi_MC:x673'"
    ).fetchone()
    assert sample == ("revenue",)
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_dbt.py -k metric_map_seed -v` → FAIL (project doesn't exist).

- [ ] **Step 3: Create the dbt project files**

`src/dagster_v3/defs/finland_xbrl/dbt/dbt_project.yml`:
```yaml
name: finland_xbrl
version: "1.0"
config-version: 2
profile: finland_xbrl

model-paths: ["models"]
seed-paths: ["seeds"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  finland_xbrl:
    +materialized: table

seeds:
  finland_xbrl:
    +schema: ""   # keep seeds in the profile's schema (finland_prh_xbrl), not a sub-schema
```
(If dbt-duckdb appends the seed schema oddly, the test asserts the table is at `finland_prh_xbrl.xbrl_metric_map`; adjust the `+schema` handling so the seed lands there and report what you used.)

`src/dagster_v3/defs/finland_xbrl/dbt/profiles.yml`:
```yaml
finland_xbrl:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('FINLAND_XBRL_DUCKDB_PATH', 'data/finland_ytj.duckdb') }}"
      schema: finland_prh_xbrl
```

`src/dagster_v3/defs/finland_xbrl/dbt/.gitignore`:
```
target/
dbt_packages/
logs/
.user.yml
```

`src/dagster_v3/defs/finland_xbrl/dbt/seeds/xbrl_metric_map.csv` (exact rows from `XBRL_FINANCIAL_METRIC_MAP`):
```csv
concept_qname,mcy_member_code,metric_code
fi_met:md103,fi_MC:x673,revenue
fi_met:md103,fi_MC:x689,operating_profit_loss
fi_met:md103,fi_MC:x740,profit_loss
fi_met:mi53,fi_MC:x360,total_assets
fi_met:mi53,fi_MC:x376,equity
fi_met:mi53,fi_MC:x424,liabilities
fi_met:mi53,fi_MC:x399,cash_and_bank
fi_met:mi53,fi_MC:x435,current_assets
fi_met:mi53,fi_MC:x1768,current_receivables
fi_met:mi53,fi_MC:x1811,current_liabilities
fi_met:md103,fi_MC:x5,personnel_expenses
fi_met:md103,fi_MC:x6,wages_and_salaries
```
Create empty `src/dagster_v3/defs/finland_xbrl/dbt/models/.gitkeep`.

VERIFY against the current `XBRL_FINANCIAL_METRIC_MAP` in `assets.py` that these 12 (concept_qname, mcy_member_code) → metric_code rows match exactly. If the live dict differs, the live dict wins — report the diff.

- [ ] **Step 4: Run the seed test**

Run: `uv run pytest tests/test_finland_xbrl_dbt.py -k metric_map_seed -v` → PASS. (If the seed lands in a different schema/table than `finland_prh_xbrl.xbrl_metric_map`, fix the `+schema` config and re-run.)

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/dbt tests/test_finland_xbrl_dbt.py
git commit -m "feat(finland_xbrl): scaffold dbt project + metric-map seed"
```

---

### Task 2: `sources.yml` + `eligible_financial_reports` model

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml`, `models/eligible_financial_reports.sql`
- Test: `tests/test_finland_xbrl_dbt.py`

- [ ] **Step 1: Write the failing test**

Add (a `_seed_eligible_inputs` fixture seeds the two source tables, then `dbt build --select eligible_financial_reports`):

```python
def _seed(db_path, sql_statements):
    conn = duckdb.connect(str(db_path))
    for sql in sql_statements:
        conn.execute(sql)
    conn.close()


def test_eligible_model(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _seed(db, [
        "create schema if not exists finland_prh_xbrl",
        "create schema if not exists finland_prhytj",
        """create table finland_prh_xbrl.financial_reports as select * from (values
            ('a','2023-12-31','2024-03-01','2024-01-01','2024-03-01','run-1', 5),
            ('b','2023-12-31','2024-03-01','2024-01-01','2024-03-01','run-1', 6)
          ) as t(business_id,financial_date,registration_date,
                 discovery_registered_date_start,discovery_registered_date_end,
                 source_run_id,source_record_number)""",
        """create table finland_prhytj.all_companies as select * from (values
            ('a','A Oy', true,  'https://a.fi'),
            ('b','B Oy', false, 'https://b.fi')
          ) as t(business_id,primary_name,is_active,website_normalized_url)""",
    ])
    _dbt(["build", "--select", "eligible_financial_reports"], db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, primary_name from finland_prh_xbrl.eligible_financial_reports order by business_id"
    ).fetchall()
    assert rows == [("a", "A Oy")]  # b is inactive -> excluded
```

- [ ] **Step 2: Run, expect failure** → FAIL (no model).

- [ ] **Step 3: Create `sources.yml`**

```yaml
version: 2

sources:
  - name: finland_prh_xbrl
    schema: finland_prh_xbrl
    tables:
      - name: financial_reports
        meta:
          dagster:
            asset_key: ["finland_xbrl_financial_reports_duckdb"]
      - name: fi_prh_xbrl_statement_documents
        meta:
          dagster:
            asset_key: ["fi_prh_xbrl_statement_documents"]
      - name: fi_prh_xbrl_facts_raw
        meta:
          dagster:
            asset_key: ["fi_prh_xbrl_facts_raw"]
  - name: finland_prhytj
    schema: finland_prhytj
    tables:
      - name: all_companies
        meta:
          dagster:
            asset_key: ["finland_ytj_all_companies_duckdb"]

models:
  - name: eligible_financial_reports
    columns:
      - name: business_id
        data_tests: [not_null]
  - name: fi_prh_xbrl_financial_metrics
    columns:
      - name: statement_key
        data_tests: [not_null]
```
NOTE: dbt 1.8 fails `build` if a `models:` entry names a model with no `.sql`. Until Task 3 adds the metrics model, include ONLY the `eligible_financial_reports` model entry here; Task 3 re-adds `fi_prh_xbrl_financial_metrics`. (Trim now, restore in Task 3.)

- [ ] **Step 4: Create `eligible_financial_reports.sql`** (port of `build_xbrl_eligible_financial_reports`)

```sql
{{ config(materialized='table') }}

select
    reports.business_id,
    reports.financial_date,
    reports.registration_date,
    companies.primary_name,
    companies.website_normalized_url,
    reports.discovery_registered_date_start,
    reports.discovery_registered_date_end,
    reports.source_run_id,
    reports.source_record_number
from {{ source('finland_prh_xbrl', 'financial_reports') }} as reports
inner join {{ source('finland_prhytj', 'all_companies') }} as companies
    on reports.business_id = companies.business_id
where companies.is_active = true
  and coalesce(companies.website_normalized_url, '') <> ''
order by reports.business_id, reports.financial_date
```

- [ ] **Step 5: Run the test** → PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml src/dagster_v3/defs/finland_xbrl/dbt/models/eligible_financial_reports.sql tests/test_finland_xbrl_dbt.py
git commit -m "feat(finland_xbrl): add dbt sources + eligible_financial_reports model"
```

---

### Task 3: `fi_prh_xbrl_financial_metrics` model (pivot + seed)

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/dbt/models/fi_prh_xbrl_financial_metrics.sql`
- Modify: `src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml` (re-add the metrics model entry)
- Test: `tests/test_finland_xbrl_dbt.py`

- [ ] **Step 1: Write the failing test** (seed facts + statements + the map seed; build metrics; assert the revenue pivot + counts):

```python
def test_financial_metrics_model(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _seed(db, [
        "create schema if not exists finland_prh_xbrl",
        # one statement, two facts: one mapped to revenue, one unmapped
        """create table finland_prh_xbrl.fi_prh_xbrl_statement_documents as select * from (values
            ('k1','a','2023-12-31','2022-10-01','2023-09-30')
          ) as t(statement_key,business_id,financial_date,reported_period_start,reported_period_end)""",
        """create table finland_prh_xbrl.fi_prh_xbrl_facts_raw as select * from (values
            ('k1','fi_met:md103','fi_MC:x673','numeric','125000', false),
            ('k1','fi_met:zzz','fi_MC:zzz','numeric','1', false)
          ) as t(statement_key,concept_qname,mcy_member_code,value_kind,numeric_value,is_comparative)""",
    ])
    _dbt(["build"], db, monkeypatch)  # seed + both models
    conn = duckdb.connect(str(db), read_only=True)
    row = conn.execute(
        "select revenue, mapped_fact_count, unmapped_numeric_fact_count, mapping_version "
        "from finland_prh_xbrl.fi_prh_xbrl_financial_metrics where statement_key='k1'"
    ).fetchone()
    assert row == (125000.0, 1, 1, "finland-prh-xbrl-metrics-v1")
```
(The minimal seed tables only need the columns the model reads — facts: statement_key, concept_qname, mcy_member_code, value_kind, numeric_value, is_comparative; statements: statement_key, business_id, financial_date, reported_period_start, reported_period_end. If the model references other columns, add them to the seed.)

- [ ] **Step 2: Run, expect failure** → FAIL.

- [ ] **Step 3: Re-add the metrics model entry to `sources.yml`** (the `models:` block now lists both `eligible_financial_reports` and `fi_prh_xbrl_financial_metrics`).

- [ ] **Step 4: Create `fi_prh_xbrl_financial_metrics.sql`** (port of `build_xbrl_financial_metrics`; Jinja loop over the 13 metric columns; map via `ref('xbrl_metric_map')`)

```sql
{{ config(materialized='table') }}

{% set metric_columns = [
    'revenue', 'operating_profit_loss', 'profit_loss', 'total_assets', 'equity',
    'liabilities', 'cash_and_bank', 'current_assets', 'current_receivables',
    'current_liabilities', 'personnel_expenses', 'wages_and_salaries', 'employees'
] %}

with fact_counts as (
    select statement_key, count(*) as source_fact_count
    from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_facts_raw') }}
    group by statement_key
),
current_numeric_facts as (
    select
        facts.statement_key,
        facts.concept_qname,
        facts.mcy_member_code,
        try_cast(nullif(facts.numeric_value, '') as double) as numeric_value,
        mapping.metric_code
    from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_facts_raw') }} as facts
    left join {{ ref('xbrl_metric_map') }} as mapping
        on facts.concept_qname = mapping.concept_qname
       and facts.mcy_member_code = mapping.mcy_member_code
    where facts.value_kind = 'numeric'
      and coalesce(facts.is_comparative, false) = false
),
metric_pivot as (
    select
        statement_key,
        count(*) filter (where metric_code is not null) as mapped_fact_count,
        count(*) filter (where metric_code is null) as unmapped_numeric_fact_count,
        {% for m in metric_columns -%}
        max(numeric_value) filter (where metric_code = '{{ m }}') as {{ m }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from current_numeric_facts
    group by statement_key
)
select
    statements.statement_key,
    statements.business_id,
    statements.financial_date,
    nullif(statements.reported_period_start, '') as period_start,
    coalesce(
        nullif(statements.reported_period_end, ''),
        nullif(statements.financial_date, '')
    ) as period_end,
    {% for m in metric_columns -%}
    metrics.{{ m }},
    {% endfor -%}
    coalesce(fact_counts.source_fact_count, 0) as source_fact_count,
    coalesce(metrics.mapped_fact_count, 0) as mapped_fact_count,
    coalesce(metrics.unmapped_numeric_fact_count, 0) as unmapped_numeric_fact_count,
    case
        when coalesce(metrics.unmapped_numeric_fact_count, 0) > 0
            then concat('["unmapped numeric facts: ', coalesce(metrics.unmapped_numeric_fact_count, 0)::varchar, '"]')
        when coalesce(metrics.mapped_fact_count, 0) = 0 then '["no mapped metrics"]'
        else '[]'
    end as metric_warnings,
    'finland-prh-xbrl-metrics-v1' as mapping_version,
    now() as built_at
from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_statement_documents') }} as statements
left join fact_counts on statements.statement_key = fact_counts.statement_key
left join metric_pivot as metrics on statements.statement_key = metrics.statement_key
order by statements.business_id, statements.financial_date, statements.statement_key
```
VERIFY column order/expressions against the current `build_xbrl_financial_metrics` SQL (the CTEs, the `filter (where ...)` aggregations, the `metric_warnings` case, `mapping_version`/`built_at`). The current code uses the literal `XBRL_FINANCIAL_METRICS_MAPPING_VERSION = 'finland-prh-xbrl-metrics-v1'`; keep that literal.

- [ ] **Step 5: Run the test + full dbt file** → PASS (`uv run pytest tests/test_finland_xbrl_dbt.py -v`).

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/dbt/models/fi_prh_xbrl_financial_metrics.sql src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml tests/test_finland_xbrl_dbt.py
git commit -m "feat(finland_xbrl): add financial_metrics dbt model (pivot via seed)"
```

---

### Task 4: Wire dbt assets into Dagster; re-point the download; remove old Python

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Test: `tests/test_finland_xbrl_assets.py`, `tests/test_finland_xbrl_dbt.py`

- [ ] **Step 1: Write the failing wiring test** (add to `tests/test_finland_xbrl_assets.py`):

```python
from dagster import AssetKey
from dagster_v3.definitions import defs as load_project_defs


def test_xbrl_transforms_are_dbt_assets():
    graph = load_project_defs().get_repository_def().asset_graph
    keys = {k.path[-1] for k in graph.get_all_asset_keys()}
    assert "finland_xbrl_eligible_financial_reports" in keys
    assert "fi_prh_xbrl_financial_metrics" in keys
    # download re-pointed to the dbt eligible model
    deps = graph.get(AssetKey(["finland_xbrl_raw_xml_documents"])).parent_keys
    assert AssetKey(["finland_xbrl_eligible_financial_reports"]) in deps
```

- [ ] **Step 2: Run, expect failure** → FAIL (still old python assets; deps unchanged at the structural level the test checks differently — confirm it fails because the dbt asset isn't registered with a dbt kind, or adapt the assertion to detect the dbt-backed asset; at minimum it must fail before wiring).

- [ ] **Step 3: Add the dbt wiring to `assets.py`**

Add imports: `from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model`; `from collections.abc import Mapping` (if not present); `import os`.

Add near the top (after constants):
```python
FINLAND_XBRL_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
_XBRL_DUCKDB_PATH = Path(LocalDuckDBResource().database_path).expanduser()
if not _XBRL_DUCKDB_PATH.is_absolute():
    _XBRL_DUCKDB_PATH = _XBRL_DUCKDB_PATH.resolve()
os.environ["FINLAND_XBRL_DUCKDB_PATH"] = str(_XBRL_DUCKDB_PATH)

finland_xbrl_dbt_project = DbtProject(
    project_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
)
finland_xbrl_dbt_project.prepare_if_dev()


class FinlandXbrlDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, props: "Mapping[str, Any]") -> dg.AssetKey:
        if props["resource_type"] == "source":
            return super().get_asset_key(props)
        name = props["name"]
        if name == "eligible_financial_reports":
            return dg.AssetKey("finland_xbrl_eligible_financial_reports")
        return dg.AssetKey(name)  # fi_prh_xbrl_financial_metrics keeps its name

    def get_group_name(self, props: "Mapping[str, Any]") -> str:
        return "finland_xbrl"


@dbt_assets(
    manifest=finland_xbrl_dbt_project.manifest_path,
    project=finland_xbrl_dbt_project,
    dagster_dbt_translator=FinlandXbrlDbtTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_xbrl_dbt_assets(context: dg.AssetExecutionContext, finland_xbrl_dbt: DbtCliResource):
    yield from finland_xbrl_dbt.cli(["build"], context=context).stream()
```

- [ ] **Step 4: Re-point the download + remove old assets/helpers**

- `finland_xbrl_raw_xml_documents`: change `deps=[finland_xbrl_eligible_financial_reports]` → `deps=["finland_xbrl_eligible_financial_reports"]` (string key; now produced by dbt). Its body is unchanged (it still calls `load_eligible_financial_report_rows` reading the `eligible_financial_reports` table).
- DELETE the assets `finland_xbrl_eligible_financial_reports` and `finland_xbrl_financial_metrics`, and the functions `build_xbrl_eligible_financial_reports`, `build_xbrl_financial_metrics`, `_create_metric_mapping_table`, `_metric_projection`, `_require_parsed_xbrl_tables`, and the constants `XBRL_FINANCIAL_METRIC_MAP`, `XBRL_FINANCIAL_METRIC_COLUMNS`, `XBRL_FINANCIAL_METRICS_MAPPING_VERSION`.
- KEEP `XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE` and `load_eligible_financial_report_rows` (the download still reads the eligible table). Confirm with `rg` that nothing else references the deleted symbols.
- Update the `defs = dg.Definitions(...)` block: remove `finland_xbrl_eligible_financial_reports` and `finland_xbrl_financial_metrics` from `assets=[...]`; add `finland_xbrl_dbt_assets`; add to `resources={...}`: `"finland_xbrl_dbt": DbtCliResource(project_dir=finland_xbrl_dbt_project, profiles_dir=FINLAND_XBRL_DBT_PROJECT_DIR)`.

- [ ] **Step 5: Generate the manifest, run the wiring test**

Run: `cd src/dagster_v3/defs/finland_xbrl/dbt && FINLAND_XBRL_DUCKDB_PATH=/tmp/fx_parse.duckdb uv run dbt parse --profiles-dir . --project-dir . ; cd -`
Run: `uv run pytest tests/test_finland_xbrl_assets.py -k "xbrl_transforms_are_dbt_assets" -v` → PASS.

- [ ] **Step 6: `dg check defs`** → no errors. Resolve any resource-key clash (`finland_xbrl_dbt` must match between the op param and the defs key) or partitioned→unpartitioned mapping error (add `AllPartitionMapping` on the metrics sources only if dg check complains) and report.

- [ ] **Step 7: Run the finland_xbrl suites + dbt file**

Run: `uv run pytest tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_dbt.py -m "not integration" -v` → PASS. Some existing tests referenced the deleted `build_xbrl_*` / metric-map symbols — update or remove them (port their intent to the dbt model tests where it still applies).

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_assets.py
git commit -m "feat(finland_xbrl): replace eligible+metrics SQL assets with dbt-duckdb models"
```

---

### Task 5: Verification

- [ ] **Step 1:** `uv run pytest tests/test_finland_xbrl_dbt.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py -m "not integration" -v` → PASS.
- [ ] **Step 2:** `uv run dg check defs` → no errors.
- [ ] **Step 3:** Lineage: `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['asset_key'],'<-',a['dependency_keys']) for a in d['assets'] if 'xbrl' in a['asset_key'].lower()]"` — confirm: `eligible_financial_reports` ← financial_reports_duckdb + finland_ytj_all_companies_duckdb; `raw_xml_documents` ← eligible; `fi_prh_xbrl_financial_metrics` ← parse tables; no `build_xbrl_*` python assets remain.
- [ ] **Step 4:** Final commit if anything adjusted.

---

## Self-Review

**Spec coverage:** eligible → dbt model (Task 2); metrics → dbt model + seed (Task 1+3); Python assets replaced + download re-pointed (Task 4); pool + distinct resource key + distinct env var (Task 4).

**Placeholder scan:** model SQL is fully specified (eligible verbatim; metrics with a Jinja loop matching the 13 `XBRL_FINANCIAL_METRIC_COLUMNS` and the seed reproducing `XBRL_FINANCIAL_METRIC_MAP`). The implementer is told to verify both against the live constants.

**Type/key consistency:** resource key `finland_xbrl_dbt` (op param == defs key); asset keys `finland_xbrl_eligible_financial_reports` / `fi_prh_xbrl_financial_metrics` preserved via the translator so `raw_xml_documents` and any metrics consumer are unaffected; table names unchanged (`eligible_financial_reports`, `fi_prh_xbrl_financial_metrics`) so `load_eligible_financial_report_rows` still reads the right table.

**Risks to verify during execution:**
1. **Seed schema** — dbt-duckdb may place the seed in a sub-schema; the Task 1 test pins it to `finland_prh_xbrl.xbrl_metric_map` — adjust `+schema` config until it lands there.
2. **`employees` column** — no fact maps to it (not in the seed), so it's always NULL — matches current behavior (verify the current `XBRL_FINANCIAL_METRIC_COLUMNS` includes `employees` but the map doesn't).
3. **Resource-key clash** with `dbt`/`finland_resolved_dbt` — handled via `finland_xbrl_dbt`; `dg check` (Task 4 Step 6) confirms.
4. **Partitioned parse → unpartitioned metrics dbt model** — verify `dg check`; add `AllPartitionMapping` only if it complains.
5. **Deleted-symbol fallout** — existing tests importing `build_xbrl_eligible_financial_reports`/`build_xbrl_financial_metrics`/`XBRL_FINANCIAL_METRIC_MAP` must be updated/removed (Task 4 Step 7).

---

## Execution Handoff

Plan complete and saved. Two options:
1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review.
2. **Inline Execution** — execute here with checkpoints.

Which approach?
