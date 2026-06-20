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
    assert assets._duckdb_table_count(database_path=db_path, table_name=qualified) == 2


def test_lv_companies_columns_match_entities_schema_and_migration():
    # The ClickHouse export column order must match the DuckDB entities table...
    assert tables.LV_COMPANIES_COLUMNS == tuple(tables.LATVIA_UR_ENTITIES_COLUMNS)
    # ...and every column must exist in the 000015 migration that owns the schema.
    migration = (
        Path(__file__).resolve().parents[2]
        / "clickhouse"
        / "migrations"
        / "000015_corpscout_lv_companies.up.sql"
    ).read_text()
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LV_COMPANIES_TABLE}" in migration
    for column in tables.LV_COMPANIES_COLUMNS:
        assert f"    {column} " in migration


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

    rows = latvia_ur_clickhouse.export_latvia_ur_clickhouse_companies(
        database_path=db_path,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert rows == 2
    # the staged table is created and atomically exchanged into corpscout.lv_companies
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    assert fake.inserts, "expected a batched INSERT of the register rows"
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows) == 2
    assert len(inserted_rows[0]) == len(tables.LV_COMPANIES_EXPORT_COLUMNS)
