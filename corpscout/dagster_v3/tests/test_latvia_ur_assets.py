from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import assets, resources

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
