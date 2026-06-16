import gzip
import json
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.definitions import defs as load_project_defs
import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.defs.norway_brreg.resources import NorwayDuckDBResource


class FakeResponse:
    def __init__(self, content: bytes = b"", status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpSession:
    def __init__(self, content: bytes = b"") -> None:
        self.content = content
        self.calls: list[tuple[str, dict[str, Any] | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: int = 120,
    ) -> FakeResponse:
        self.calls.append((url, params, timeout))
        return FakeResponse(content=self.content)


def _gzip_json_array(records: list[dict[str, Any]]) -> bytes:
    return gzip.compress(json.dumps(records).encode("utf-8"))


def _entity_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "organisasjonsnummer": "923609016",
        "navn": "EQUINOR ASA",
        "organisasjonsform": {"kode": "ASA", "beskrivelse": "Allmennaksjeselskap"},
        "hjemmeside": "www.equinor.com",
        "registreringsdatoEnhetsregisteret": "1995-03-12",
        "registrertIMvaregisteret": True,
        "naeringskode1": {"kode": "06.100", "beskrivelse": "Utvinning av raolje"},
        "naeringskode2": {"kode": "06.200", "beskrivelse": "Utvinning av naturgass"},
        "antallAnsatte": 21467,
        "harRegistrertAntallAnsatte": True,
        "forretningsadresse": {
            "adresse": ["Forusbeen 50"],
            "postnummer": "4035",
            "poststed": "STAVANGER",
            "kommune": "STAVANGER",
            "kommunenummer": "1103",
            "landkode": "NO",
        },
        "stiftelsesdato": "1972-09-18",
        "registrertIForetaksregisteret": True,
        "sisteInnsendteAarsregnskap": "2024",
        "konkurs": False,
        "underAvvikling": False,
        "underTvangsavviklingEllerTvangsopplosning": False,
        "erIKonsern": True,
        "overordnetEnhet": "000000000",
        "_links": {
            "self": {
                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
            }
        },
    }
    record.update(overrides)
    return record


def test_streaming_json_dependency_is_available() -> None:
    import ijson

    assert ijson


def test_norway_duckdb_resource_defaults_to_country_database() -> None:
    resource = NorwayDuckDBResource()

    assert resource.path() == Path("data/norway_brreg.duckdb")


def test_norway_duckdb_resource_connects_to_configured_path(tmp_path: Path) -> None:
    resource = NorwayDuckDBResource(database_path=str(tmp_path / "norway.duckdb"))

    with resource.connect() as connection:
        connection.execute("create table smoke(id integer)")
        connection.execute("insert into smoke values (1)")

    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        assert connection.execute("select id from smoke").fetchone() == (1,)


def test_entity_rows_extract_company_spine_fields() -> None:
    rows = brreg_assets.build_entity_rows([_entity_record()], run_id="test-run")

    assert rows[0]["country_iso2"] == "NO"
    assert rows[0]["source_slug"] == "norway_brregenhet"
    assert rows[0]["source_run_id"] == "test-run"
    assert rows[0]["source_line_number"] == 1
    assert rows[0]["source_record_id"] == "923609016"
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["vat_id"] == "NO923609016MVA"
    assert rows[0]["legal_name"] == "EQUINOR ASA"
    assert rows[0]["legal_form_code"] == "ASA"
    assert rows[0]["legal_form_description_original"] == "Allmennaksjeselskap"
    assert rows[0]["nace1_code"] == "06.100"
    assert rows[0]["nace1_description_original"] == "Utvinning av raolje"
    assert rows[0]["employee_count"] == 21467
    assert rows[0]["status"] == "active"
    assert rows[0]["is_active"] is True
    assert rows[0]["last_submitted_accounts_year"] == "2024"
    assert json.loads(rows[0]["raw_entity"])["organisasjonsnummer"] == "923609016"


def test_entity_status_derivation_handles_liquidation_and_bankruptcy() -> None:
    rows = brreg_assets.build_entity_rows(
        [
            _entity_record(organisasjonsnummer="1", konkurs=True),
            _entity_record(organisasjonsnummer="2", underAvvikling=True),
            _entity_record(
                organisasjonsnummer="3",
                underTvangsavviklingEllerTvangsopplosning=True,
            ),
        ],
        run_id="test-run",
    )

    assert [row["status"] for row in rows] == [
        "bankrupt",
        "liquidation",
        "compulsory_liquidation",
    ]
    assert [row["is_active"] for row in rows] == [False, False, False]


def test_brreg_entity_source_downloads_gzip_and_yields_rows() -> None:
    session = FakeHttpSession(
        _gzip_json_array(
            [
                _entity_record(),
                _entity_record(organisasjonsnummer="999999999"),
            ]
        )
    )

    source = brreg_assets.norway_brreg_entities_source(session=session, run_id="test-run")
    rows = list(source.resources[brreg_assets.ENTITIES_TABLE])

    assert [row["org_number"] for row in rows] == ["923609016", "999999999"]
    assert session.calls == [
        ("https://data.brreg.no/enhetsregisteret/api/enheter/lastned", None, 120)
    ]
    assert session.headers["User-Agent"] == brreg_assets.DEFAULT_USER_AGENT


def test_entity_dlt_pipeline_loads_entities_table(tmp_path: Path) -> None:
    session = FakeHttpSession(_gzip_json_array([_entity_record()]))

    load_info = brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=tmp_path / "norway.duckdb",
        run_id="test-run",
        session=session,
    )

    assert load_info
    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            "select org_number, legal_name, status from norway_brreg.entities"
        ).fetchall()

    assert rows == [("923609016", "EQUINOR ASA", "active")]


def test_norway_entity_asset_is_registered() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert "norway_brreg_entities_duckdb" in {
        key.path[-1] for key in asset_graph.get_all_asset_keys()
    }
