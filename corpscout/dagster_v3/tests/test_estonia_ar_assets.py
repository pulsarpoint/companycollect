import io
import zipfile
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.estonia_ar import assets, resources, tables
from dagster_v3.defs.estonia_ar import clickhouse as ee_clickhouse
from tests.test_estonia_ar_resources import SAMPLE_CSV


def _zip_bytes(csv_text: str) -> bytes:
    # leading BOM exercises the utf-8-sig handling on the register CSV.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "ettevotja_rekvisiidid__lihtandmed.csv",
            ("﻿" + csv_text).encode("utf-8"),
        )
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.content = body
        self.headers = {"Content-Length": str(len(body))}

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
            return [(tables.EE_COMPANIES_TABLE,)]
        if isinstance(params, (list, tuple)):
            self.inserts.append((sql, list(params)))
            return None
        self.statements.append(sql)
        return None


def test_pipeline_unzips_and_loads_register_rows_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    load_info = assets.run_estonia_ar_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(_zip_bytes(SAMPLE_CSV)),
        pipelines_dir=tmp_path / "dlt",
    )
    assert load_info is not None

    qualified = f"{resources.DLT_DATASET_NAME}.{resources.ENTITIES_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        count = conn.execute(f"select count(*) from {qualified}").fetchone()[0]
        active = conn.execute(
            f"select count(*) from {qualified} where is_active"
        ).fetchone()[0]
        sample = conn.execute(
            "select name, legal_form_en, status_en, first_entry_date "
            f"from {qualified} where reg_code = '16752073'"
        ).fetchone()
    assert count == 2
    assert active == 1
    assert sample == ("007 Agent OÜ", "Private limited company", "Registered", date(2023, 6, 5))


def test_export_companies_replaces_clickhouse_table(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    assets.run_estonia_ar_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(_zip_bytes(SAMPLE_CSV)),
        pipelines_dir=tmp_path / "dlt",
    )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    rows = ee_clickhouse.export_estonia_ar_clickhouse_companies(
        database_path=db_path,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert rows == 2
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    assert fake.inserts, "expected a batched INSERT of the register rows"
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows) == 2
    assert len(inserted_rows[0]) == len(tables.EE_COMPANIES_EXPORT_COLUMNS)
