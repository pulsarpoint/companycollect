# finland_resolved → dbt-duckdb Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hand-written `finland_ytj_resolved_duckdb` asset (Python building three `create or replace` SQL tables) with a **dbt-duckdb** project of three models (`fi_companies`, `fi_websites`, `fi_industries`), mirroring the existing `exchange_rates_v2` pattern, so the transform layer is one dependency-ordered, testable unit.

**Architecture:** A new dbt project at `defs/finland_resolved/dbt/` targets the same `data/finland_ytj.duckdb` file. Source = `finland_prhytj.all_companies` (the dlt-loaded table, asset `finland_ytj_all_companies_duckdb`); models build into schema `finland_resolved` (materialized `table`, i.e. full rebuild — same semantics as today's `create or replace`). The industries model reuses the existing `_primary_industry_json` Python logic via a dbt-duckdb **plugin** (`BasePlugin.configure_connection` registers the UDF on each connection), so the intricate business-line/language logic is reused verbatim rather than re-written in SQL. A `@dbt_assets` op runs `dbt build`; `finland_ytj_resolved_clickhouse` stays a Python asset, re-pointed to depend on the three dbt models.

**Tech Stack:** dbt-duckdb `>=1.8,<1.9`, dagster-dbt `>=0.29.9`, DuckDB, DuckDB JSON functions, pytest. All deps already present.

**Scope (locked):** This plan migrates the **resolved layer only**. The raw download/load asset is unchanged. (A future plan will split download-from-load via object storage — out of scope here.)

**Lineage change:** today `finland_ytj_all_companies_duckdb → finland_ytj_resolved_duckdb → finland_ytj_resolved_clickhouse`. After: `finland_ytj_all_companies_duckdb → {finland_ytj_resolved_fi_companies, finland_ytj_resolved_fi_websites, finland_ytj_resolved_fi_industries} → finland_ytj_resolved_clickhouse`. The single `finland_ytj_resolved_duckdb` key is **removed** (only `finland_ytj_resolved_clickhouse` consumed it, and it is re-pointed).

**Reference files to mirror (read them before starting):**
- `src/dagster_v3/defs/exchange_rates_v2/assets.py` (DbtProject, `@dbt_assets`, translator, `get_asset_key_for_model`, defs wiring)
- `src/dagster_v3/defs/exchange_rates_v2/dbt/{dbt_project.yml,profiles.yml,models/sources.yml,models/identity_rates.sql}`
- `tests/test_exchange_rates_v2_dbt.py` (dbt test pattern)

**Test command:** `uv run pytest tests/test_finland_resolved_dbt.py -v` (per-task subsets noted).

**Critical gotcha (Task 5):** `exchange_rates_v2` already registers a resource named `dbt`. Two `DbtCliResource`s under the same key in merged Definitions **conflict**. This plan uses a distinct key `finland_resolved_dbt` for finland_resolved's dbt resource and asset param.

---

### Task 1: Scaffold the dbt project, the UDF plugin, and the shared industry module

**Files:**
- Create: `src/dagster_v3/defs/finland_resolved/dbt/dbt_project.yml`
- Create: `src/dagster_v3/defs/finland_resolved/dbt/profiles.yml`
- Create: `src/dagster_v3/defs/finland_resolved/dbt/.gitignore`
- Create: `src/dagster_v3/defs/finland_resolved/dbt/models/.gitkeep`
- Create: `src/dagster_v3/defs/finland_resolved/industry.py`
- Create: `src/dagster_v3/defs/finland_resolved/dbt_plugin.py`
- Test: `tests/test_finland_resolved_dbt.py`

- [ ] **Step 1: Write the failing test for the UDF plugin**

Create `tests/test_finland_resolved_dbt.py`:

```python
import json
from pathlib import Path

import duckdb

from dagster_v3.defs.finland_resolved import dbt_plugin

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_resolved" / "dbt"
)


def test_plugin_registers_primary_industry_udf() -> None:
    conn = duckdb.connect(":memory:")
    dbt_plugin.Plugin(alias="industry").configure_connection(conn)
    raw = json.dumps({"mainBusinessLine": {"code": "62010", "codeSet": "NACE_REV_2",
                                            "descriptions": [{"languageCode": "1", "description": "Ohjelmistot"}]}})
    out = conn.execute("select fi_primary_industry_json(?)", [raw]).fetchone()[0]
    parsed = json.loads(out)
    assert parsed["code"] == "62010"
    assert parsed["codeSet"] == "NACE_REV_2"
    assert parsed["description"] == "Ohjelmistot"
    assert parsed["language"] == "fi"
    assert conn.execute("select fi_primary_industry_json(null)").fetchone()[0] is None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_plugin_registers_primary_industry_udf -v`
Expected: FAIL — `dagster_v3.defs.finland_resolved.dbt_plugin` does not exist.

- [ ] **Step 3: Move the industry logic to a shared module**

Create `src/dagster_v3/defs/finland_resolved/industry.py` by moving `_primary_industry_json` and its helpers out of `assets.py` **verbatim**, renaming the public entrypoint to `primary_industry_json` (drop the leading underscore). Copy these functions exactly from the current `assets.py` (lines ~210-287): `_primary_industry_json` → `primary_industry_json`, `_normalize_business_line`, `_select_business_line_description`, `_find_description_by_language`, `_first_description`, `_business_line_language`. Add `import json` at the top.

```python
"""Primary industry (business line) extraction from raw PRH company JSON."""

import json


def primary_industry_json(raw_company: str | None) -> str | None:
    if not raw_company:
        return None
    try:
        payload = json.loads(raw_company)
    except json.JSONDecodeError:
        return None
    for key in ("businessLine", "mainBusinessLine"):
        business_line = payload.get(key)
        if isinstance(business_line, dict):
            return json.dumps(_normalize_business_line(business_line))
    business_lines = payload.get("businessLines")
    if isinstance(business_lines, list):
        for business_line in business_lines:
            if isinstance(business_line, dict):
                return json.dumps(_normalize_business_line(business_line))
    return None


def _normalize_business_line(business_line: dict[str, object]) -> dict[str, object]:
    description, language = _select_business_line_description(business_line)
    return {
        "code": business_line.get("code") or business_line.get("type"),
        "codeSet": business_line.get("codeSet") or business_line.get("typeCodeSet"),
        "description": description,
        "language": business_line.get("language") or language,
    }


def _select_business_line_description(
    business_line: dict[str, object],
) -> tuple[object, str | None]:
    direct_description = business_line.get("description")
    if direct_description:
        language = business_line.get("language")
        return direct_description, str(language) if language else None
    descriptions = business_line.get("descriptions")
    if not isinstance(descriptions, list):
        return None, None
    selected_description = _find_description_by_language(descriptions, "1")
    if selected_description is None:
        selected_description = _first_description(descriptions)
    if selected_description is None:
        return None, None
    language_code = selected_description.get("languageCode")
    return selected_description.get("description"), _business_line_language(language_code)


def _find_description_by_language(
    descriptions: list[object],
    language_code: str,
) -> dict[str, object] | None:
    for description in descriptions:
        if (
            isinstance(description, dict)
            and str(description.get("languageCode")) == language_code
            and description.get("description")
        ):
            return description
    return None


def _first_description(descriptions: list[object]) -> dict[str, object] | None:
    for description in descriptions:
        if isinstance(description, dict) and description.get("description"):
            return description
    return None


def _business_line_language(language_code: object) -> str | None:
    return {"1": "fi", "2": "sv", "3": "en"}.get(str(language_code))
```

- [ ] **Step 4: Create the dbt-duckdb plugin that registers the UDF**

Create `src/dagster_v3/defs/finland_resolved/dbt_plugin.py`:

```python
"""dbt-duckdb plugin: registers the fi_primary_industry_json UDF on each connection."""

from typing import Any

from dbt.adapters.duckdb.plugins import BasePlugin

from dagster_v3.defs.finland_resolved.industry import primary_industry_json


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        conn.create_function(
            "fi_primary_industry_json",
            primary_industry_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
```

- [ ] **Step 5: Create the dbt project files**

Create `src/dagster_v3/defs/finland_resolved/dbt/dbt_project.yml`:

```yaml
name: finland_resolved
version: "1.0"
config-version: 2
profile: finland_resolved

model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  finland_resolved:
    +materialized: table
```

Create `src/dagster_v3/defs/finland_resolved/dbt/profiles.yml`:

```yaml
finland_resolved:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('FINLAND_YTJ_DUCKDB_PATH', 'data/finland_ytj.duckdb') }}"
      schema: finland_resolved
      plugins:
        - module: dagster_v3.defs.finland_resolved.dbt_plugin
```

Create `src/dagster_v3/defs/finland_resolved/dbt/.gitignore`:

```
target/
dbt_packages/
logs/
```

Create an empty `src/dagster_v3/defs/finland_resolved/dbt/models/.gitkeep` (the `models/` dir must exist before any model is added).

- [ ] **Step 6: Run the plugin test to verify it passes**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_plugin_registers_primary_industry_udf -v`
Expected: PASS

- [ ] **Step 7: Verify dbt can parse the project**

Run: `cd src/dagster_v3/defs/finland_resolved/dbt && FINLAND_YTJ_DUCKDB_PATH=/tmp/finland_parse.duckdb uv run dbt parse --profiles-dir . --project-dir .`
Expected: "Found 0 models" (no models yet) and no errors. Return to repo root afterward.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/dbt src/dagster_v3/defs/finland_resolved/industry.py src/dagster_v3/defs/finland_resolved/dbt_plugin.py tests/test_finland_resolved_dbt.py
git commit -m "feat(finland_resolved): scaffold dbt-duckdb project and industry UDF plugin"
```

---

### Task 2: `sources.yml` + `fi_companies` model

**Files:**
- Create: `src/dagster_v3/defs/finland_resolved/dbt/models/sources.yml`
- Create: `src/dagster_v3/defs/finland_resolved/dbt/models/fi_companies.sql`
- Test: `tests/test_finland_resolved_dbt.py`

- [ ] **Step 1: Write the failing test (shared dbt-build fixture + fi_companies assertions)**

Add to `tests/test_finland_resolved_dbt.py`:

```python
import os
import duckdb as _duckdb
from dbt.cli.main import dbtRunner


def _seed_all_companies(db_path: Path) -> None:
    conn = _duckdb.connect(str(db_path))
    conn.execute("create schema if not exists finland_prhytj")
    conn.execute(
        """
        create table finland_prhytj.all_companies as select * from (values
          ('fi-1','FI','Active One Oy','2024-01-01','', 'active', true,
           'https://example.fi/path','https://example.fi/path','example.fi','/path','2024-01-02','',
           'finland_prhytj','run-1','fi-1','hash1',
           '{"mainBusinessLine":{"code":"62010","codeSet":"NACE_REV_2","descriptions":[{"languageCode":"1","description":"Ohjelmistot"}]}}'),
          ('fi-2','FI','Ceased Two Oy','2020-01-01','2025-01-01','ceased', false,
           '','','','','','',
           'finland_prhytj','run-1','fi-2','hash2','{}')
        ) as t(business_id,country_iso2,primary_name,registration_date,end_date,lifecycle_status,is_active,
                website_url,website_normalized_url,website_host,website_path,website_registered_on,website_ended_on,
                source_slug,source_run_id,source_record_id,source_payload_hash,raw_company)
        """
    )
    conn.close()


def _dbt_build(db_path: Path) -> None:
    os.environ["FINLAND_YTJ_DUCKDB_PATH"] = str(db_path)
    res = dbtRunner().invoke(
        ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    )
    assert res.success, res.exception


def test_fi_companies_model(tmp_path: Path) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db)
    conn = _duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, name, name_normalized, is_active, primary_website_host "
        "from finland_resolved.fi_companies order by business_id"
    ).fetchall()
    assert rows == [
        ("fi-1", "Active One Oy", "active one oy", True, "example.fi"),
        ("fi-2", "Ceased Two Oy", "ceased two oy", False, None),
    ]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_companies_model -v`
Expected: FAIL — no models exist (`dbt build` finds the source but no `fi_companies`).

- [ ] **Step 3: Create `sources.yml`**

Create `src/dagster_v3/defs/finland_resolved/dbt/models/sources.yml`:

```yaml
version: 2

sources:
  - name: finland_prhytj
    schema: finland_prhytj
    tables:
      - name: all_companies
        meta:
          dagster:
            asset_key: ["finland_ytj_all_companies_duckdb"]

models:
  - name: fi_companies
    columns:
      - name: business_id
        data_tests:
          - not_null
  - name: fi_websites
    columns:
      - name: business_id
        data_tests:
          - not_null
  - name: fi_industries
    columns:
      - name: business_id
        data_tests:
          - not_null
```

- [ ] **Step 4: Create `fi_companies.sql`** (port of `_fi_companies_sql`)

Create `src/dagster_v3/defs/finland_resolved/dbt/models/fi_companies.sql`:

```sql
{{ config(materialized='table') }}

select
  business_id,
  country_iso2,
  primary_name as name,
  lower(primary_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(end_date, '') as date) as end_date,
  lifecycle_status,
  coalesce(is_active, false) as is_active,
  cast(null as varchar) as legal_form_code,
  cast(null as varchar) as legal_form_description_original,
  cast(null as varchar) as legal_form_description_language,
  cast(null as varchar) as legal_form_description_en,
  cast(null as timestamp) as legal_form_description_translated_at,
  cast(null as varchar) as legal_form_description_translation_provider,
  cast(null as varchar) as legal_form_description_translation_model,
  nullif(website_normalized_url, '') as primary_website_url,
  nullif(website_host, '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('finland_prhytj', 'all_companies') }}
where business_id is not null and business_id != ''
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_companies_model -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/dbt/models/sources.yml src/dagster_v3/defs/finland_resolved/dbt/models/fi_companies.sql tests/test_finland_resolved_dbt.py
git commit -m "feat(finland_resolved): add dbt source and fi_companies model"
```

---

### Task 3: `fi_websites` model

**Files:**
- Create: `src/dagster_v3/defs/finland_resolved/dbt/models/fi_websites.sql`
- Test: `tests/test_finland_resolved_dbt.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_finland_resolved_dbt.py`:

```python
def test_fi_websites_model(tmp_path: Path) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db)
    conn = _duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, website_normalized_url, website_host, is_current, is_primary "
        "from finland_resolved.fi_websites order by business_id"
    ).fetchall()
    # Only fi-1 has a website; fi-2 is filtered out (empty normalized url)
    assert rows == [("fi-1", "https://example.fi/path", "example.fi", True, True)]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_websites_model -v`
Expected: FAIL — `Catalog Error: ... fi_websites does not exist`.

- [ ] **Step 3: Create `fi_websites.sql`** (port of `_fi_websites_sql`)

Create `src/dagster_v3/defs/finland_resolved/dbt/models/fi_websites.sql`:

```sql
{{ config(materialized='table') }}

select
  business_id,
  website_url,
  website_normalized_url,
  website_host,
  website_path,
  try_cast(nullif(website_registered_on, '') as date) as registered_on,
  try_cast(nullif(website_ended_on, '') as date) as ended_on,
  website_ended_on is null or website_ended_on = '' as is_current,
  true as is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('finland_prhytj', 'all_companies') }}
where business_id is not null
  and business_id != ''
  and website_normalized_url is not null
  and website_normalized_url != ''
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_websites_model -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/dbt/models/fi_websites.sql tests/test_finland_resolved_dbt.py
git commit -m "feat(finland_resolved): add fi_websites model"
```

---

### Task 4: `fi_industries` model (uses the UDF)

**Files:**
- Create: `src/dagster_v3/defs/finland_resolved/dbt/models/fi_industries.sql`
- Test: `tests/test_finland_resolved_dbt.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_finland_resolved_dbt.py`:

```python
def test_fi_industries_model(tmp_path: Path) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db)
    conn = _duckdb.connect(str(db), read_only=True)
    row = conn.execute(
        "select source_industry_code, source_industry_code_set, description_original, "
        "description_language, nace_revision, nace_code, nace_normalized_code, "
        "nace_mapping_method, nace_mapping_status, is_primary "
        "from finland_resolved.fi_industries where business_id = 'fi-1'"
    ).fetchone()
    assert row == (
        "62010", "NACE_REV_2", "Ohjelmistot", "fi",
        "NACE_REV_2", "62010", "62010", "direct_code", "mapped", True,
    )
    # fi-2 has empty raw_company -> no industry json -> still emitted, code null
    miss = conn.execute(
        "select source_industry_code, nace_mapping_method, nace_mapping_status "
        "from finland_resolved.fi_industries where business_id = 'fi-2'"
    ).fetchone()
    assert miss == (None, "none", "missing_source_code")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_industries_model -v`
Expected: FAIL — `fi_industries does not exist`.

- [ ] **Step 3: Create `fi_industries.sql`** (port of `_fi_industries_sql`, calling the registered UDF)

Create `src/dagster_v3/defs/finland_resolved/dbt/models/fi_industries.sql`:

```sql
{{ config(materialized='table') }}

with extracted as (
  select
    *,
    fi_primary_industry_json(raw_company) as industry_json
  from {{ source('finland_prhytj', 'all_companies') }}
), normalized as (
  select
    *,
    json_extract_string(industry_json, '$.code') as source_industry_code,
    json_extract_string(industry_json, '$.codeSet') as source_industry_code_set
  from extracted
)
select
  business_id,
  source_industry_code,
  source_industry_code_set,
  json_extract_string(industry_json, '$.description') as description_original,
  coalesce(json_extract_string(industry_json, '$.language'), 'fi') as description_language,
  cast(null as varchar) as description_en,
  cast(null as timestamp) as description_translated_at,
  cast(null as varchar) as description_translation_provider,
  cast(null as varchar) as description_translation_model,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then source_industry_code_set
    else null
  end as nace_revision,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then source_industry_code
    else null
  end as nace_code,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1')
      then regexp_replace(source_industry_code, '[^0-9A-Za-z]', '', 'g')
    else null
  end as nace_normalized_code,
  case
    when source_industry_code is null then 'none'
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then 'direct_code'
    else 'none'
  end as nace_mapping_method,
  case
    when source_industry_code is null then 'missing_source_code'
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then 'mapped'
    else 'unmapped_source_code_set'
  end as nace_mapping_status,
  true as is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from normalized
where business_id is not null and business_id != ''
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_resolved_dbt.py::test_fi_industries_model -v`
Expected: PASS

- [ ] **Step 5: Run the full dbt test file**

Run: `uv run pytest tests/test_finland_resolved_dbt.py -v`
Expected: all PASS (plugin + three models).

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/dbt/models/fi_industries.sql tests/test_finland_resolved_dbt.py
git commit -m "feat(finland_resolved): add fi_industries model via UDF plugin"
```

---

### Task 5: Wire dbt assets into Dagster; re-point ClickHouse; remove the old SQL asset

**Files:**
- Modify: `src/dagster_v3/defs/finland_resolved/assets.py` (full rewrite of the Dagster wiring; remove old SQL builders/UDF)
- Modify: `tests/test_finland_resolved_assets.py`
- Test: `tests/test_finland_resolved_assets.py`, `tests/test_finland_resolved_dbt.py`

- [ ] **Step 1: Write the failing wiring test**

Replace the contents of `tests/test_finland_resolved_assets.py` with a registration test (the old `build_finland_ytj_resolved_tables` is being removed):

```python
from dagster_v3.definitions import defs as load_project_defs


def test_dbt_models_and_clickhouse_registered() -> None:
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "finland_ytj_resolved_fi_companies" in keys
    assert "finland_ytj_resolved_fi_websites" in keys
    assert "finland_ytj_resolved_fi_industries" in keys
    assert "finland_ytj_resolved_clickhouse" in keys
    # the old single resolved asset is gone
    assert "finland_ytj_resolved_duckdb" not in keys


def test_clickhouse_depends_on_dbt_models() -> None:
    repo = load_project_defs().get_repository_def()
    graph = repo.asset_graph
    from dagster import AssetKey
    deps = graph.get(AssetKey(["finland_ytj_resolved_clickhouse"])).parent_keys
    dep_names = {k.path[-1] for k in deps}
    assert {"finland_ytj_resolved_fi_companies",
            "finland_ytj_resolved_fi_websites",
            "finland_ytj_resolved_fi_industries"} <= dep_names
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_finland_resolved_assets.py -v`
Expected: FAIL — model keys not present; `finland_ytj_resolved_duckdb` still exists.

- [ ] **Step 3: Rewrite `src/dagster_v3/defs/finland_resolved/assets.py`**

Replace the whole file with:

```python
import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
    get_asset_key_for_model,
)

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_tables_in_clickhouse,
)
from dagster_v3.defs.common.resources import LocalDuckDBResource
from dagster_v3.defs.finland_resolved import tables

GROUP_NAME = "finland_resolved"
RESOLVED_DUCKDB_SCHEMA = "finland_resolved"
FINLAND_RESOLVED_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"

# dbt's duckdb target reads FINLAND_YTJ_DUCKDB_PATH; point it at the shared file.
_DEFAULT_DUCKDB_PATH = Path(LocalDuckDBResource().database_path).expanduser()
if not _DEFAULT_DUCKDB_PATH.is_absolute():
    _DEFAULT_DUCKDB_PATH = _DEFAULT_DUCKDB_PATH.resolve()
os.environ.setdefault("FINLAND_YTJ_DUCKDB_PATH", str(_DEFAULT_DUCKDB_PATH))

finland_resolved_dbt_project = DbtProject(
    project_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
)
finland_resolved_dbt_project.prepare_if_dev()


class FinlandResolvedDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return super().get_asset_key(dbt_resource_props)
        return dg.AssetKey(f"finland_ytj_resolved_{dbt_resource_props['name']}")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return GROUP_NAME


@dbt_assets(
    manifest=finland_resolved_dbt_project.manifest_path,
    project=finland_resolved_dbt_project,
    dagster_dbt_translator=FinlandResolvedDbtTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_resolved_dbt_assets(
    context: AssetExecutionContext,
    finland_resolved_dbt: DbtCliResource,
) -> Iterator[Any]:
    yield from finland_resolved_dbt.cli(["build"], context=context).stream()


@dg.asset(
    deps=[
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_companies"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_websites"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_industries"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool="finland_ytj_duckdb",
    description="Exports resolved Finland YTJ DuckDB tables to migrated ClickHouse tables.",
)
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.FINLAND_YTJ_RESOLVED_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_tables_in_clickhouse(
            duckdb_path=ytj_duckdb.path(),
            clickhouse_client=client,
            duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table, tables.RESOLVED_TABLE_COLUMNS[table])
                for table in tables.FINLAND_YTJ_RESOLVED_TABLES
            ),
        )
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            finland_resolved_dbt_assets,
            finland_ytj_resolved_clickhouse,
        ],
        resources={
            "finland_resolved_dbt": DbtCliResource(
                project_dir=finland_resolved_dbt_project,
                profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
            ),
        },
    )
```

Notes baked in: distinct resource key `finland_resolved_dbt` (avoids the `dbt`-key clash with `exchange_rates_v2`); both ops carry `pool="finland_ytj_duckdb"` (the single-writer pool from the prior fix); `_primary_industry_json` and the `_fi_*_sql` builders are deleted (the dbt models + plugin own that logic now).

- [ ] **Step 4: Generate the dbt manifest, then run the wiring test**

Run: `cd src/dagster_v3/defs/finland_resolved/dbt && FINLAND_YTJ_DUCKDB_PATH=/tmp/finland_parse.duckdb uv run dbt parse --profiles-dir . --project-dir . ; cd -`
Then: `uv run pytest tests/test_finland_resolved_assets.py -v`
Expected: PASS (three model keys + clickhouse present; old key gone; clickhouse deps on the three models).

- [ ] **Step 5: Validate definitions load (resource-key clash is the key risk)**

Run: `uv run dg check defs`
Expected: no errors. If it reports a duplicate `dbt` resource key or a missing `finland_resolved_dbt` resource, fix the key wiring before proceeding.

- [ ] **Step 6: Confirm the dbt model tests still pass against the new project**

Run: `uv run pytest tests/test_finland_resolved_dbt.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/assets.py tests/test_finland_resolved_assets.py
git commit -m "feat(finland_resolved): replace SQL asset with dbt-duckdb models, re-point clickhouse"
```

---

### Task 6: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the finland + clickhouse suites**

Run: `uv run pytest tests/test_finland_resolved_dbt.py tests/test_finland_resolved_assets.py tests/test_finland_ytj_assets.py tests/test_clickhouse_resolved.py -m "not integration" -v`
Expected: PASS.

- [ ] **Step 2: Validate definitions + lineage**

Run: `uv run dg check defs`
Then: `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['asset_key'],'<-',a['dependency_keys']) for a in d['assets'] if 'finland_ytj_resolved' in a['asset_key'] or a['asset_key']=='finland_ytj_all_companies_duckdb']"`
Expected: three `finland_ytj_resolved_fi_*` models depend on `finland_ytj_all_companies_duckdb`; `finland_ytj_resolved_clickhouse` depends on the three models; no `finland_ytj_resolved_duckdb`.

- [ ] **Step 3: End-to-end dbt build against a seeded DuckDB (no Dagster)**

Run: `uv run pytest tests/test_finland_resolved_dbt.py -v`
Expected: PASS — confirms `dbt build` produces all three `finland_resolved.*` tables with the UDF-backed industries.

- [ ] **Step 4: Final commit if anything was adjusted**

```bash
git add -A
git commit -m "test(finland_resolved): verify dbt-duckdb migration" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- Replace hand-written resolved SQL with dbt-duckdb → Tasks 2-4 (three models) + Task 5 (wiring).
- Reuse the industries Python UDF without rewriting in SQL → Task 1 (plugin + `industry.py`) + Task 4 (model calls the UDF).
- Mirror `exchange_rates_v2` → DbtProject/`@dbt_assets`/translator/`get_asset_key_for_model`/profiles/sources all follow it.
- Keep ClickHouse export, re-pointed → Task 5.
- Same materialization semantics (full rebuild) → `+materialized: table`.

**Placeholder scan:** none — every model SQL, the plugin, the wiring, and tests are complete. The industries SQL and `industry.py` are verbatim ports of the current logic.

**Type/key consistency:** resource key `finland_resolved_dbt` matches between the `@dbt_assets` param and the `defs` resources block; asset keys `finland_ytj_resolved_fi_{companies,websites,industries}` match between the translator, the clickhouse `deps`, and both test files; `tables.RESOLVED_TABLE_COLUMNS`/`FINLAND_YTJ_RESOLVED_TABLES` are unchanged and still drive the ClickHouse export.

**Risks to verify during execution:**
1. **Resource-key clash** with `exchange_rates_v2`'s `dbt` resource — handled via the distinct `finland_resolved_dbt` key; Task 5 Step 5 explicitly checks `dg check defs`.
2. **Plugin import path** — `module: dagster_v3.defs.finland_resolved.dbt_plugin` must be importable by dbt at runtime (it is, since the package is installed in the venv). Task 1 Step 7 (`dbt parse`) surfaces any import error early.
3. **Manifest generation** — `@dbt_assets(manifest=...)` needs a parsed manifest; `prepare_if_dev()` covers local dev, and Task 5 Step 4 runs `dbt parse` explicitly so CI/tests have it.
4. **DuckDB single-writer** — both new ops carry `pool="finland_ytj_duckdb"`, extending the prior serialization fix to the resolved layer (set the pool limit to 1 once, already done in the instance).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-18-finland-resolved-dbt-duckdb.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks.
2. **Inline Execution** — execute tasks in this session with checkpoints.

Which approach?
