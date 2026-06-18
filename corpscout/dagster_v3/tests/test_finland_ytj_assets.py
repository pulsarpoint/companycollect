import json
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import duckdb

import dagster_v3.defs.finland_ytj.assets as ytj_assets
from dagster_v3.definitions import defs as load_project_defs

DLT_COMPANIES_TABLE = "all_companies"
DLT_DATASET_NAME = "finland_prhytj"


class FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpSession:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[str, dict | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict | None = None, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, params, timeout))
        return FakeResponse(self.content)


def _zip_json(name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, json.dumps(payload))
    return buffer.getvalue()


def _session(payload: dict) -> FakeHttpSession:
    session = FakeHttpSession(_zip_json("companies.json", payload))
    return session


def test_prh_code_constants_drive_classification() -> None:
    assert ytj_assets.PRH_NAME_TYPE_PRIMARY == "1"
    assert ytj_assets.PRH_TRADE_REGISTER_STATUS_CEASED == "3"
    assert ytj_assets.DEFAULT_DUCKDB_PATH == "data/finland_ytj.duckdb"


def test_ytj_api_is_modeled_as_dlt_source_not_dagster_resource() -> None:
    repository = load_project_defs().get_repository_def()
    resource_keys = repository.get_top_level_resources().keys()
    asset_keys = repository.asset_graph.get_all_asset_keys()

    assert "ytj_api" not in resource_keys
    assert "dlt" in resource_keys
    assert "finland_ytj_source" in ytj_assets.__dict__
    assert "YtjApiResource" not in ytj_assets.__dict__
    assert "finland_ytj_all_companies_json" not in {key.path[-1] for key in asset_keys}
    assert "dlt" in ytj_assets.finland_ytj_all_companies_duckdb_asset.required_resource_keys


def test_ytj_dlt_source_downloads_zip_and_yields_company_rows() -> None:
    session = _session(
        {
            "companies": [
                {"businessId": {"value": "old"}, "registrationDate": "2023-12-31"},
                {"businessId": {"value": "latest"}, "registrationDate": "2026-06-15"},
            ]
        }
    )

    source = ytj_assets.finland_ytj_source(session=session, run_id="test-run")
    rows = list(source.resources[DLT_COMPANIES_TABLE])

    assert [row["business_id"] for row in rows] == ["old", "latest"]
    assert rows[0]["source_run_id"] == "test-run"
    assert session.calls == [
        ("https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies", None, 120)
    ]


def test_dlt_company_rows_keep_raw_payload_and_extract_eligibility_fields() -> None:
    rows = ytj_assets.build_dlt_company_rows(
        [
            {
                "businessId": {"value": "active-1"},
                "registrationDate": "2024-01-01",
                "tradeRegisterStatus": "2",
                "status": "2",
                "endDate": None,
                "website": {"url": "Example.fi/path", "registrationDate": "2024-01-02"},
                "names": [{"name": "Active One Oy", "type": "1", "endDate": None}],
            },
            {
                "businessId": {"value": "ceased-1"},
                "registrationDate": "2020-01-01",
                "tradeRegisterStatus": "3",
                "status": "2",
                "endDate": "2025-01-01",
                "names": [{"name": "Ceased One Oy", "type": "1", "endDate": None}],
            },
        ],
        run_id="test-run",
    )

    assert rows[0]["business_id"] == "active-1"
    assert rows[0]["is_active"] is True
    assert rows[0]["website_url"] == "Example.fi/path"
    assert rows[0]["website_normalized_url"] == "https://example.fi/path"
    assert rows[0]["primary_name"] == "Active One Oy"
    assert json.loads(rows[0]["raw_company"])["businessId"]["value"] == "active-1"
    assert rows[1]["business_id"] == "ceased-1"
    assert rows[1]["is_active"] is False
    assert rows[1]["website_normalized_url"] == ""


def test_dlt_duckdb_asset_loads_all_companies_table(tmp_path: Path) -> None:
    session = _session(
        {
            "companies": [
                {
                    "businessId": {"value": "active-1"},
                    "registrationDate": "2024-01-01",
                    "tradeRegisterStatus": "2",
                    "status": "2",
                    "endDate": None,
                    "names": [{"name": "Active One Oy", "type": "1", "endDate": None}],
                    "addresses": [
                        {
                            "type": 2,
                            "street": "Example Street 1",
                            "postCode": "00100",
                            "registrationDate": "2024-01-01",
                            "postOffices": [
                                {
                                    "languageCode": "3",
                                    "city": "Helsinki",
                                    "municipalityCode": "091",
                                }
                            ],
                        }
                    ],
                },
                {
                    "businessId": {"value": "ceased-1"},
                    "registrationDate": "2020-01-01",
                    "tradeRegisterStatus": "3",
                    "status": "2",
                    "endDate": "2025-01-01",
                    "names": [{"name": "Ceased One Oy", "type": "1", "endDate": None}],
                },
            ],
        }
    )
    database_path = tmp_path / "finland_ytj.duckdb"

    load_info = ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id="test-run",
        session=session,
    )

    assert load_info

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select business_id, lifecycle_status, primary_name, website_normalized_url
            from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}
            order by business_id
            """
        ).fetchall()

    assert rows == [
        ("active-1", "active", "Active One Oy", ""),
        ("ceased-1", "ceased", "Ceased One Oy", ""),
    ]
