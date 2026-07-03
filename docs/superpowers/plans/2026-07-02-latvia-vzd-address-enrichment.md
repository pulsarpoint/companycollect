# Latvia VZD Address Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Latvia VZD State Address Register enrichment to `lv_companies` by loading `AW_EKA.CSV`, `AW_PILSETA.CSV`, and `AW_NOVADS.CSV` into DuckDB assets and joining them during the Latvia company ClickHouse export.

**Architecture:** Keep Latvia register data in the existing `latvia` Dagster group and DuckDB file. Add three source-reference DuckDB assets, then extend the existing ClickHouse export view so `lv_companies` receives normalized city, municipality, VZD address, postal, and coordinate fields. The register job remains the single company refresh job; its upstream selection will pull the address assets automatically.

**Tech Stack:** Dagster assets, DuckDB `read_csv`, ClickHouse migrations, pytest, `uv run dg check defs`.

---

## Source Facts

The official source is the VZD State Address Register open dataset on `data.gov.lv`. The dataset page says VZD publishes denormalized State Address Register text data as CSV and spatial data as SHP, and the dataset is CC-BY-4.0. The dataset also says its update frequency is daily.

Required CSV resources:

- `AW_EKA.CSV`: building and buildable-land-unit addresses.
- `AW_PILSETA.CSV`: cities.
- `AW_NOVADS.CSV`: municipalities.

Metadata URLs currently expose the direct download URLs:

- Cities: `https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/resource/ee02baa4-2bc3-4f77-a6cb-5427a3e9befe/download/aw_pilseta.csv`
- Municipalities: `https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/resource/c62c60bb-58d4-4f26-82c0-5b630769f9d1/download/aw_novads.csv`
- Buildings: `https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/resource/a510737a-18ce-400f-ad4b-04fce5228272/download/aw_eka.csv`

VZD metadata declares comma delimiter and `ISO-8859-1` encoding. Implementation must verify actual decoding with a live sample or fixture before relying on it; if source bytes are Windows-1257 despite metadata, use the encoding that preserves Latvian names correctly.

## Target Data Model

Keep existing UR fields:

- `address`
- `postal_code`
- `address_id`
- `region_code`
- `city_code`
- `atvk_code`

Add nullable ClickHouse fields to `corpscout.lv_companies`:

- `vzd_address_text Nullable(String)` from `AW_EKA.STD`
- `vzd_address_postal_code Nullable(String)` from `AW_EKA.ATRIB`
- `vzd_address_status LowCardinality(Nullable(String))` from `AW_EKA.STATUSS`
- `address_city_name Nullable(String)` from `AW_PILSETA.NOSAUKUMS`
- `address_municipality_name Nullable(String)` from `AW_NOVADS.NOSAUKUMS`
- `address_latitude Nullable(Float64)` from `AW_EKA.DD_N`
- `address_longitude Nullable(Float64)` from `AW_EKA.DD_E`

Join strategy:

- `lv_companies.address_id` -> `latvia_ur.address_buildings.address_code`
- `lv_companies.city_code` -> `latvia_ur.address_cities.address_code`, ignoring empty and `0`
- `lv_companies.atvk_code` -> `latvia_ur.address_municipalities.atvk_code`
- fallback municipality: city parent object code -> municipality address code when the ATVK join is missing

Only active VZD records should be joined by default:

- `STATUSS = 'EKS'`

Deleted/error address records should stay in DuckDB for auditability but should not enrich `lv_companies` unless no active match exists. The first implementation can use active-only joins and expose row-count metadata for inactive records.

## Files

- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/tables.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/clickhouse.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/docs/latvia_ur-design.md`
- Create: `companycollect/corpscout/clickhouse/migrations/000082_corpscout_lv_companies_vzd_address.up.sql`
- Create: `companycollect/corpscout/clickhouse/migrations/000082_corpscout_lv_companies_vzd_address.down.sql`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_tables.py`

## Task 1: Add Table Constants And ClickHouse Column Contract

**Files:**

- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/tables.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_tables.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`

- [ ] **Step 1: Write failing table-contract tests**

Add assertions that the table constants and export columns include the address fields:

```python
def test_latvia_address_table_names_are_defined():
    assert tables.ADDRESS_BUILDINGS_TABLE == "address_buildings"
    assert tables.ADDRESS_CITIES_TABLE == "address_cities"
    assert tables.ADDRESS_MUNICIPALITIES_TABLE == "address_municipalities"


def test_lv_companies_export_columns_include_vzd_address_fields():
    for column in (
        "vzd_address_text",
        "vzd_address_postal_code",
        "vzd_address_status",
        "address_city_name",
        "address_municipality_name",
        "address_latitude",
        "address_longitude",
    ):
        assert column in tables.LV_COMPANIES_EXPORT_COLUMNS
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_tables.py::test_latvia_address_table_names_are_defined tests/test_latvia_ur_assets.py::test_lv_companies_export_columns_include_vzd_address_fields -q
```

Expected: fail because constants/columns are missing.

- [ ] **Step 3: Add constants and columns**

Add to `tables.py`:

```python
ADDRESS_BUILDINGS_TABLE = "address_buildings"
ADDRESS_CITIES_TABLE = "address_cities"
ADDRESS_MUNICIPALITIES_TABLE = "address_municipalities"

LATVIA_VZD_ADDRESS_COLUMNS = (
    "vzd_address_text",
    "vzd_address_postal_code",
    "vzd_address_status",
    "address_city_name",
    "address_municipality_name",
    "address_latitude",
    "address_longitude",
)
```

Update:

```python
LV_COMPANIES_COLUMNS = (
    tuple(LATVIA_UR_ENTITIES_COLUMNS)
    + ("activity_text_original",)
    + LATVIA_VZD_ADDRESS_COLUMNS
)
```

- [ ] **Step 4: Run tests and verify pass**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_tables.py::test_latvia_address_table_names_are_defined tests/test_latvia_ur_assets.py::test_lv_companies_export_columns_include_vzd_address_fields -q
```

Expected: pass.

## Task 2: Add VZD CSV Load Helpers And Three DuckDB Assets

**Files:**

- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`

- [ ] **Step 1: Add failing CSV loader tests**

Add fixtures using comma-delimited VZD-shaped CSV. Keep the fixture ASCII unless a focused encoding test is added.

```python
VZD_BUILDINGS_CSV = (
    "KODS,TIPS_CD,STATUSS,APSTIPR,APST_PAK,VKUR_CD,VKUR_TIPS,NOSAUKUMS,SORT_NOS,"
    "ATRIB,PNOD_CD,DAT_SAK,DAT_MOD,DAT_BEIG,FOR_BUILD,PLAN_ADR,STD,KOORD_X,KOORD_Y,DD_N,DD_E\n"
    "103045133,108,EKS,Y,101,0885162,113,Building 1,Building 1,LV-4201,,2020.01.01,"
    "2026.01.01,,N,N,\"Valmiera, Building 1\",1,2,57.5380,25.4260\n"
)

VZD_CITIES_CSV = (
    "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,"
    "DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD\n"
    "100015821,104,Valmiera,100000000,100,Y,101,EKS,Valmiera,2020.01.01,2026.01.01,,0885162,Valmiera\n"
)

VZD_MUNICIPALITIES_CSV = (
    "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,"
    "DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD\n"
    "0885162,113,Valmieras novads,100000000,100,Y,101,EKS,Valmieras novads,"
    "2020.01.01,2026.01.01,,0885162,Valmieras novads\n"
)


def test_load_latvia_address_reference_rows_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        building_rows = assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_BUILDINGS_TABLE,
            download_url="https://example.test/aw_eka.csv",
            session=_FakeSession(VZD_BUILDINGS_CSV.encode("utf-8")),
        )
        city_rows = assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_CITIES_TABLE,
            download_url="https://example.test/aw_pilseta.csv",
            session=_FakeSession(VZD_CITIES_CSV.encode("utf-8")),
        )
        municipality_rows = assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_MUNICIPALITIES_TABLE,
            download_url="https://example.test/aw_novads.csv",
            session=_FakeSession(VZD_MUNICIPALITIES_CSV.encode("utf-8")),
        )

    assert (building_rows, city_rows, municipality_rows) == (1, 1, 1)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            f"select address_code, full_address, latitude, longitude "
            f"from {tables.DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}"
        ).fetchone() == ("103045133", "Valmiera, Building 1", 57.5380, 25.4260)
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_load_latvia_address_reference_rows_into_duckdb -q
```

Expected: fail because `load_latvia_address_csv` is missing.

- [ ] **Step 3: Add URLs and loader**

Add constants to `assets.py`:

```python
VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/a510737a-18ce-400f-ad4b-04fce5228272/download/aw_eka.csv"
)
VZD_ADDRESS_CITIES_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/ee02baa4-2bc3-4f77-a6cb-5427a3e9befe/download/aw_pilseta.csv"
)
VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/c62c60bb-58d4-4f26-82c0-5b630769f9d1/download/aw_novads.csv"
)
```

Add a single helper:

```python
def load_latvia_address_csv(
    *,
    duckdb_connection: Any,
    table_name: str,
    download_url: str,
    session: resources.HttpSession | None = None,
) -> int:
    import tempfile

    with tempfile.TemporaryDirectory(prefix="latvia_vzd_address_") as tmpdir:
        csv_path = Path(tmpdir) / f"{table_name}.csv"
        resources._download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=resources.DEFAULT_TIMEOUT_SECONDS,
            user_agent=resources.DEFAULT_USER_AGENT,
            session=session,
        )
        duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        if table_name == tables.ADDRESS_BUILDINGS_TABLE:
            select_sql = """
                select
                    KODS as address_code,
                    TIPS_CD as address_type_code,
                    STATUSS as status,
                    VKUR_CD as parent_address_code,
                    VKUR_TIPS as parent_address_type_code,
                    NOSAUKUMS as name,
                    ATRIB as postal_code,
                    STD as full_address,
                    try_cast(DD_N as double) as latitude,
                    try_cast(DD_E as double) as longitude
            """
        else:
            select_sql = """
                select
                    KODS as address_code,
                    TIPS_CD as address_type_code,
                    STATUSS as status,
                    VKUR_CD as parent_address_code,
                    VKUR_TIPS as parent_address_type_code,
                    NOSAUKUMS as name,
                    ATRIB as atvk_code,
                    STD as full_address,
                    cast(null as double) as latitude,
                    cast(null as double) as longitude
            """
        duckdb_connection.execute(
            f"""
            create or replace table {DLT_DATASET_NAME}.{table_name} as
            {select_sql}
            from read_csv(
                ?,
                delim=',',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"'
            )
            """,
            [str(csv_path)],
        )
        rows = duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{table_name}"
        ).fetchone()[0]
    return int(rows)
```

If live data decoding is wrong, add a narrow decode step before DuckDB import that reads bytes and writes UTF-8 into the temp file using `windows-1257`. Do not add this until verified by a failing test or live sample.

- [ ] **Step 4: Add three assets**

Add assets:

```python
@dg.asset(
    name="latvia_address_buildings_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_EKA.CSV building/buildable-land addresses loaded to DuckDB.",
)
def latvia_address_buildings_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_BUILDINGS_TABLE,
            download_url=VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}"})


@dg.asset(
    name="latvia_address_cities_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_PILSETA.CSV city address objects loaded to DuckDB.",
)
def latvia_address_cities_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_CITIES_TABLE,
            download_url=VZD_ADDRESS_CITIES_DOWNLOAD_URL,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_CITIES_TABLE}"})


@dg.asset(
    name="latvia_address_municipalities_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_NOVADS.CSV municipality address objects loaded to DuckDB.",
)
def latvia_address_municipalities_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_MUNICIPALITIES_TABLE,
            download_url=VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}"})
```

Register them in `defs.assets`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_load_latvia_address_reference_rows_into_duckdb -q
```

Expected: pass.

## Task 3: Join VZD Address Tables Into The ClickHouse Export View

**Files:**

- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/clickhouse.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`

- [ ] **Step 1: Write failing export test**

Extend `test_export_companies_includes_activity_text` or add a separate test:

```python
def test_export_companies_includes_vzd_address_enrichment(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=tmp_path / "dlt",
    )
    with duckdb.connect(str(db_path)) as conn:
        assets.load_latvia_company_activity_csv(
            duckdb_connection=conn,
            session=_FakeSession(ACTIVITY_CSV.encode("utf-8")),
        )
        assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_BUILDINGS_TABLE,
            download_url="https://example.test/aw_eka.csv",
            session=_FakeSession(VZD_BUILDINGS_CSV.encode("utf-8")),
        )
        assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_CITIES_TABLE,
            download_url="https://example.test/aw_pilseta.csv",
            session=_FakeSession(VZD_CITIES_CSV.encode("utf-8")),
        )
        assets.load_latvia_address_csv(
            duckdb_connection=conn,
            table_name=tables.ADDRESS_MUNICIPALITIES_TABLE,
            download_url="https://example.test/aw_novads.csv",
            session=_FakeSession(VZD_MUNICIPALITIES_CSV.encode("utf-8")),
        )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(db_path)) as conn:
        latvia_ur_clickhouse.export_latvia_ur_clickhouse_companies(
            duckdb_connection=conn,
            clickhouse=ClickhouseResource(host="localhost"),
        )

    _, inserted_rows = fake.inserts[0]
    row = inserted_rows[0]
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("vzd_address_text")] == "Valmiera, Building 1"
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("vzd_address_postal_code")] == "LV-4201"
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("vzd_address_status")] == "EKS"
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("address_city_name")] == "Valmiera"
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("address_municipality_name")] == "Valmieras novads"
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("address_latitude")] == 57.5380
    assert row[tables.LV_COMPANIES_EXPORT_COLUMNS.index("address_longitude")] == 25.4260
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_export_companies_includes_vzd_address_enrichment -q
```

Expected: fail because export view does not join VZD tables yet.

- [ ] **Step 3: Add asset dependencies**

Change `latvia_ur_clickhouse_companies` deps:

```python
deps=[
    dg.AssetKey(ENTITIES_ASSET_KEY),
    dg.AssetKey(COMPANY_ACTIVITY_ASSET_KEY),
    dg.AssetKey("latvia_address_buildings_duckdb"),
    dg.AssetKey("latvia_address_cities_duckdb"),
    dg.AssetKey("latvia_address_municipalities_duckdb"),
],
```

Because `latvia_ur_register_job` selects `latvia_ur_clickhouse_companies.upstream()`, the job will include these address assets automatically.

- [ ] **Step 4: Extend `_create_lv_companies_export_view`**

Update the view in `clickhouse.py`:

```sql
left join (
    select *
    from latvia_ur.address_buildings
    where status = 'EKS'
) b on b.address_code = e.address_id
left join (
    select *
    from latvia_ur.address_cities
    where status = 'EKS'
) c on c.address_code = nullif(e.city_code, '0')
left join (
    select *
    from latvia_ur.address_municipalities
    where status = 'EKS'
) m_atvk on m_atvk.atvk_code = e.atvk_code
left join (
    select *
    from latvia_ur.address_municipalities
    where status = 'EKS'
) m_parent on m_parent.address_code = c.parent_address_code
```

Add selected columns:

```sql
b.full_address as vzd_address_text,
b.postal_code as vzd_address_postal_code,
b.status as vzd_address_status,
c.name as address_city_name,
coalesce(m_atvk.name, m_parent.name) as address_municipality_name,
b.latitude as address_latitude,
b.longitude as address_longitude
```

- [ ] **Step 5: Run test and verify pass**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_export_companies_includes_vzd_address_enrichment -q
```

Expected: pass.

## Task 4: Add ClickHouse Migration

**Files:**

- Create: `companycollect/corpscout/clickhouse/migrations/000082_corpscout_lv_companies_vzd_address.up.sql`
- Create: `companycollect/corpscout/clickhouse/migrations/000082_corpscout_lv_companies_vzd_address.down.sql`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`

- [ ] **Step 1: Add failing migration test**

Add:

```python
def test_lv_companies_vzd_address_migration_adds_columns():
    migration = (
        Path(__file__).resolve().parents[2]
        / "clickhouse"
        / "migrations"
        / "000082_corpscout_lv_companies_vzd_address.up.sql"
    ).read_text()
    for ddl in (
        "ADD COLUMN IF NOT EXISTS vzd_address_text Nullable(String)",
        "ADD COLUMN IF NOT EXISTS vzd_address_postal_code Nullable(String)",
        "ADD COLUMN IF NOT EXISTS vzd_address_status LowCardinality(Nullable(String))",
        "ADD COLUMN IF NOT EXISTS address_city_name Nullable(String)",
        "ADD COLUMN IF NOT EXISTS address_municipality_name Nullable(String)",
        "ADD COLUMN IF NOT EXISTS address_latitude Nullable(Float64)",
        "ADD COLUMN IF NOT EXISTS address_longitude Nullable(Float64)",
    ):
        assert ddl in migration
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_lv_companies_vzd_address_migration_adds_columns -q
```

Expected: fail because migration is missing.

- [ ] **Step 3: Create up migration**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.lv_companies
    ADD COLUMN IF NOT EXISTS vzd_address_text Nullable(String),
    ADD COLUMN IF NOT EXISTS vzd_address_postal_code Nullable(String),
    ADD COLUMN IF NOT EXISTS vzd_address_status LowCardinality(Nullable(String)),
    ADD COLUMN IF NOT EXISTS address_city_name Nullable(String),
    ADD COLUMN IF NOT EXISTS address_municipality_name Nullable(String),
    ADD COLUMN IF NOT EXISTS address_latitude Nullable(Float64),
    ADD COLUMN IF NOT EXISTS address_longitude Nullable(Float64);
```

- [ ] **Step 4: Create down migration**

```sql
ALTER TABLE corpscout.lv_companies
    DROP COLUMN IF EXISTS address_longitude,
    DROP COLUMN IF EXISTS address_latitude,
    DROP COLUMN IF EXISTS address_municipality_name,
    DROP COLUMN IF EXISTS address_city_name,
    DROP COLUMN IF EXISTS vzd_address_status,
    DROP COLUMN IF EXISTS vzd_address_postal_code,
    DROP COLUMN IF EXISTS vzd_address_text;
```

- [ ] **Step 5: Run migration test and verify pass**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_lv_companies_vzd_address_migration_adds_columns -q
```

Expected: pass.

## Task 5: Wire Job Coverage And Documentation

**Files:**

- Modify: `companycollect/corpscout/dagster_v3/tests/test_latvia_ur_assets.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/docs/latvia_ur-design.md`

- [ ] **Step 1: Update job coverage test**

Update `test_schedules_registered_and_jobs_cover_full_chains` so `latvia_ur_register_job` includes:

```python
assert register_keys == {
    "latvia_ur_entities_duckdb",
    "latvia_company_activity_duckdb",
    "latvia_address_buildings_duckdb",
    "latvia_address_cities_duckdb",
    "latvia_address_municipalities_duckdb",
    "latvia_ur_clickhouse_companies",
}
```

- [ ] **Step 2: Update design doc**

Add a section to `latvia_ur-design.md`:

```markdown
## VZD address enrichment

Latvia company addresses are enriched from the VZD State Address Register open dataset:

- `AW_EKA.CSV` -> `latvia_address_buildings_duckdb`
- `AW_PILSETA.CSV` -> `latvia_address_cities_duckdb`
- `AW_NOVADS.CSV` -> `latvia_address_municipalities_duckdb`

The ClickHouse export joins UR `address_id`, `city_code`, and `atvk_code` against these
reference tables to populate `vzd_address_text`, `vzd_address_postal_code`,
`address_city_name`, `address_municipality_name`, `address_latitude`, and
`address_longitude` in `corpscout.lv_companies`.
```

- [ ] **Step 3: Run docs/job tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py::test_schedules_registered_and_jobs_cover_full_chains -q
```

Expected: pass.

## Task 6: Full Verification

**Files:**

- No code changes unless verification finds a real bug.

- [ ] **Step 1: Run Latvia tests**

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_latvia_ur_assets.py tests/test_latvia_ur_resources.py tests/test_latvia_ur_tables.py -q
```

Expected: all pass.

- [ ] **Step 2: Run lint**

```bash
cd companycollect/corpscout/dagster_v3
uv run ruff check src/dagster_v3/defs/latvia_ur tests/test_latvia_ur_assets.py tests/test_latvia_ur_tables.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Run Dagster definitions check**

```bash
cd companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 4: Run whitespace check**

```bash
cd companycollect
git diff --check
```

Expected: no output.

## Self-Review

- Spec coverage: the plan adds all three requested VZD DuckDB assets, connects them to `latvia_ur_clickhouse_companies`, adds ClickHouse columns, and updates job coverage/docs.
- Placeholder scan: no TBD/TODO/fill-later steps remain.
- Type consistency: asset names are exactly `latvia_address_buildings_duckdb`, `latvia_address_cities_duckdb`, and `latvia_address_municipalities_duckdb`; table names are `address_buildings`, `address_cities`, and `address_municipalities`.

