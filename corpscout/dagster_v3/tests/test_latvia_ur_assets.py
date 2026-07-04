from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.latvia_ur import assets, resources, tables
from dagster_v3.defs.latvia_ur import clickhouse as latvia_ur_clickhouse

SAMPLE_CSV = (
    "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
    "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
    "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    "40103550818;LV95ZZZ40103550818;\"SIA \"\"Psihologs\"\"\";SIA;Psihologs;\"\";0;"
    "K;Komercreģistrs;SIA;Sabiedrība ar ierobežotu atbildību;2012-05-31;;;"
    "\"Valmiera\";4201;103045133;100015821;0;0885162;\n"
    "41202013815;LV53ZZZ41202013815;\"IK \"\"KRASTNIEKI\"\"\";IK;KRASTNIEKI;\"\";0;"
    "K;Komercreģistrs;IK;Individuālais komersants;1998-02-26;2014-04-10;L;"
    "\"Dundaga\";3275;103045134;100015821;0;0885162;\n"
)
ACTIVITY_CSV = (
    '"legal_entity_registration_number";"name";"legal_form_code";'
    '"legal_form_code_text";"area_of_activity"\n'
    '"40103550818";"SIA ""Psihologs""";"LIMITED_LIABILITY_COMPANY_SIA";'
    '"Sabiedrība ar ierobežotu atbildību";"psiholoģiskie pakalpojumi"\n'
)
VZD_BUILDINGS_CSV = (
    "KODS,TIPS_CD,STATUSS,APSTIPR,APST_PAK,VKUR_CD,VKUR_TIPS,NOSAUKUMS,SORT_NOS,"
    "ATRIB,PNOD_CD,DAT_SAK,DAT_MOD,DAT_BEIG,FOR_BUILD,PLAN_ADR,STD,KOORD_X,KOORD_Y,DD_N,DD_E\n"
    "103045133,108,EKS,Y,101,0885162,113,Building 1,Building 1,LV-4201,,2020.01.01,"
    "2026.01.01,,N,N,\"Valmiera, Building 1\",1,2,57.5380,25.4260\n"
)
VZD_CITIES_CSV = (
    "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,"
    "DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD\n"
    "100015821,104,Valmiera,0885162,113,Y,101,EKS,Valmiera,2020.01.01,2026.01.01,,0885162,Valmiera\n"
)
VZD_MUNICIPALITIES_CSV = (
    "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,"
    "DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD\n"
    "0885162,113,Valmieras novads,100000000,100,Y,101,EKS,Valmieras novads,"
    "2020.01.01,2026.01.01,,0885162,Valmieras novads\n"
)


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.content = body

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self._body


class _FakeSession:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def get(self, url: str, *, timeout: int, stream: bool = False) -> _FakeResponse:
        return _FakeResponse(self._body)


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        if "system.tables" in sql:
            return [(tables.LV_COMPANIES_TABLE,)]
        if isinstance(params, (list, tuple)):
            self.inserts.append((sql, list(params)))
            return None
        self.statements.append(sql)
        return None


def _load_latvia_address_fixtures(conn) -> None:
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


def test_schedules_registered_and_jobs_cover_full_chains():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    reg = repo.get_schedule_def("latvia_ur_register_schedule")
    fin = repo.get_schedule_def("latvia_financials_schedule")
    assert reg.cron_schedule == "30 4 * * *"  # daily, staggered
    assert reg.job.name == "latvia_ur_register_job"
    assert fin.cron_schedule == "0 5 * * 1"  # weekly full refresh
    assert fin.job.name == "latvia_financials_job"

    register_keys = {
        k.path[-1]
        for k in repo.get_job("latvia_ur_register_job").asset_layer.executable_asset_keys
    }
    assert register_keys == {
        "latvia_ur_entities_duckdb",
        "latvia_company_activity_duckdb",
        "latvia_address_buildings_duckdb",
        "latvia_address_cities_duckdb",
        "latvia_address_municipalities_duckdb",
        "latvia_ur_clickhouse_companies",
        # The translation loader runs at the end of every register refresh so
        # newly landed texts are enqueued to the translator service.
        "latvia_ur_translation_load",
        # The NACE classifier runs at the end of every register refresh so
        # newly landed activity texts are classified.
        "latvia_ur_nace_classification",
    }

    # full transitive chain: 4 raw multi-asset outputs + pivot + metrics + usd + 2 exports = 9
    financials_keys = {
        k.path[-1]
        for k in repo.get_job("latvia_financials_job").asset_layer.executable_asset_keys
    }
    assert len(financials_keys) == 9
    assert "latvia_financial_statements_raw_duckdb" in financials_keys
    assert "latvia_financial_metrics_usd_duckdb" in financials_keys
    assert "latvia_ur_financial_statements_raw_duckdb" not in financials_keys

    asset_graph = repo.asset_graph
    assert asset_graph.get(
        next(
            k
            for k in asset_graph.get_all_asset_keys()
            if k.path[-1] == assets.ENTITIES_ASSET_KEY
        )
    ).group_name == "latvia"
    assert asset_graph.get(
        next(
            k
            for k in asset_graph.get_all_asset_keys()
            if k.path[-1] == "latvia_financial_statements_raw_duckdb"
        )
    ).group_name == "latvia_financial"


def test_nace_classification_asset_deps_and_group():
    from dagster_v3.defs.latvia_ur import classification as latvia_classification

    import dagster as dg

    asset = latvia_classification.latvia_ur_nace_classification
    spec = asset.specs_by_key[asset.key]
    assert dg.AssetKey("latvia_ur_clickhouse_companies") in {
        dep.asset_key for dep in spec.deps
    }
    assert spec.group_name == "latvia_ur"
    # GPU-endpoint dependencies must be visible to operators: the asset only
    # works while the embedder and LLM boxes are up.
    assert spec.tags.get("requires_llm") == "true"
    assert spec.tags.get("requires_embedder") == "true"


def test_nace_classification_config_defaults_to_env():
    from dagster_v3.defs.latvia_ur.classification import NaceClassificationConfig

    config = NaceClassificationConfig()
    assert config.embed_base_url is None
    assert config.llm_base_url is None
    assert config.llm_model is None


def test_pipeline_loads_register_rows_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    pipelines_dir = tmp_path / "dlt"

    load_info = assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=pipelines_dir,
    )
    assert load_info is not None

    qualified = f"{resources.DLT_DATASET_NAME}.{resources.ENTITIES_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        count = conn.execute(f"select count(*) from {qualified}").fetchone()[0]
        active = conn.execute(
            f"select count(*) from {qualified} where is_active"
        ).fetchone()[0]
        sample = conn.execute(
            f"select vat_id, status, legal_form_description_en from {qualified} "
            "where regcode = '40103550818'"
        ).fetchone()
    assert count == 2
    assert active == 1
    assert sample == ("LV40103550818", "active", "Private limited company")


def test_duckdb_table_count_helper(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=tmp_path / "dlt",
    )
    qualified = f"{resources.DLT_DATASET_NAME}.{resources.ENTITIES_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert assets._duckdb_table_count(duckdb_connection=conn, table_name=qualified) == 2


def test_load_company_activity_rows_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        rows = assets.load_latvia_company_activity_csv(
            duckdb_connection=conn,
            session=_FakeSession(ACTIVITY_CSV.encode("utf-8")),
        )

    assert rows == 1
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            f"select regcode, activity_text_original "
            f"from {tables.DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE}"
        ).fetchone()
    assert row == ("40103550818", "psiholoģiskie pakalpojumi")


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
        building = conn.execute(
            f"select address_code, full_address, latitude, longitude "
            f"from {tables.DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}"
        ).fetchone()
        city = conn.execute(
            f"select address_code, name, parent_address_code "
            f"from {tables.DLT_DATASET_NAME}.{tables.ADDRESS_CITIES_TABLE}"
        ).fetchone()
        municipality = conn.execute(
            f"select address_code, name, atvk_code "
            f"from {tables.DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}"
        ).fetchone()
    assert building == ("103045133", "Valmiera, Building 1", 57.5380, 25.4260)
    assert city == ("100015821", "Valmiera", "0885162")
    assert municipality == ("0885162", "Valmieras novads", "0885162")


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


def test_lv_companies_columns_match_entities_schema_and_migration():
    # Published companies are register entities plus enriched activity text.
    assert tables.LV_COMPANIES_COLUMNS == (
        *tuple(tables.LATVIA_UR_ENTITIES_COLUMNS),
        "activity_text_original",
        *tables.LATVIA_VZD_ADDRESS_COLUMNS,
    )
    # ...and register columns must exist in the 000015 migration that owns the base schema.
    migration = (
        Path(__file__).resolve().parents[2]
        / "clickhouse"
        / "migrations"
        / "000015_corpscout_lv_companies.up.sql"
    ).read_text()
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LV_COMPANIES_TABLE}" in migration
    for column in tables.LATVIA_UR_ENTITIES_COLUMNS:
        assert f"    {column} " in migration

    activity_migration = (
        Path(__file__).resolve().parents[2]
        / "clickhouse"
        / "migrations"
        / "000081_corpscout_lv_companies_activity_translation.up.sql"
    ).read_text()
    assert "ADD COLUMN IF NOT EXISTS activity_text_original Nullable(String)" in activity_migration
    assert "CREATE OR REPLACE VIEW corpscout.lv_companies_translated" in activity_migration


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


def test_export_companies_replaces_clickhouse_table(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=tmp_path / "dlt",
    )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(db_path)) as conn:
        assets.load_latvia_company_activity_csv(
            duckdb_connection=conn,
            session=_FakeSession(ACTIVITY_CSV.encode("utf-8")),
        )
        _load_latvia_address_fixtures(conn)
        rows = latvia_ur_clickhouse.export_latvia_ur_clickhouse_companies(
            duckdb_connection=conn,
            clickhouse=ClickhouseResource(host="localhost"),
        )

    assert rows == 2
    # the staged table is created and atomically exchanged into corpscout.lv_companies
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    assert fake.inserts, "expected a batched INSERT of the register rows"
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows) == 2
    assert len(inserted_rows[0]) == len(tables.LV_COMPANIES_EXPORT_COLUMNS)


def test_export_companies_includes_activity_text(tmp_path: Path, monkeypatch):
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
        _load_latvia_address_fixtures(conn)

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
    activity_index = tables.LV_COMPANIES_EXPORT_COLUMNS.index("activity_text_original")
    assert inserted_rows[0][activity_index] == "psiholoģiskie pakalpojumi"


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
        _load_latvia_address_fixtures(conn)

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
