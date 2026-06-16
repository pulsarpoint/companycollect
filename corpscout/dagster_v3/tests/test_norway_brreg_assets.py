import gzip
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import get_type_hints

import duckdb
from dagster_clickhouse import ClickhouseResource

import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.norway_brreg import tables as brreg_tables
from dagster_v3.defs.norway_brreg.clickhouse import (
    prepare_norway_brreg_clickhouse_tables,
)


class FakeResponse:
    def __init__(self, content: bytes = b"", status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code
        try:
            self.text = content.decode("utf-8") if content else ""
        except UnicodeDecodeError:
            self.text = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return json.loads(self.text)


class FakeHttpSession:
    def __init__(self, content: bytes = b"", json_by_url: dict[str, Any] | None = None) -> None:
        self.content = content
        self.json_by_url = json_by_url or {}
        self.calls: list[tuple[str, dict[str, Any] | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: int = 120,
    ) -> FakeResponse:
        self.calls.append((url, params, timeout))
        if url in self.json_by_url:
            return FakeResponse(content=json.dumps(self.json_by_url[url]).encode("utf-8"))
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
        "naeringskode3": {
            "kode": "19.200",
            "beskrivelse": "Produksjon av raffinerte petroleumsprodukter",
        },
        "vedtektsfestetFormaal": [
            "Aa utvikle, produsere og markedsfoere energi",
            "samt annen virksomhet",
        ],
        "aktivitet": [
            "Selv, eller gjennom andre selskaper aa utvikle energi",
            "og avledede produkter og tjenester",
        ],
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


def _financial_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": True,
        },
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": False,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": False, "fravalgRevisjon": False},
        "regnkapsprinsipper": {
            "smaaForetak": False,
            "regnskapsregler": "forenkletAnvendelseIFRS",
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 109150000000,
            "egenkapital": {"sumEgenkapital": 41090000000},
            "gjeldOversikt": {
                "sumGjeld": 68060000000,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000},
            },
        },
        "eiendeler": {
            "sumEiendeler": 109150000000,
            "omloepsmidler": {"sumOmloepsmidler": 45079000000},
            "anleggsmidler": {"sumAnleggsmidler": 64071000000},
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 8168000000,
            "aarsresultat": 8141000000,
            "finansresultat": {"nettoFinans": -2179000000},
            "driftsresultat": {
                "driftsresultat": 10347000000,
                "driftsinntekter": {"sumDriftsinntekter": 72543000000},
                "driftskostnad": {"sumDriftskostnad": 62196000000},
            },
        },
    }
    record.update(overrides)
    return record


class FakeUsdRate:
    currency = "NOK"
    rate_date = "2024-12-31"
    rate = Decimal("0.10")
    source = "test-fx"

    def convert(self, amount: Decimal) -> Decimal:
        return amount * self.rate


class FakeExchangeRates:
    def usd_rate(self, *, currency: str, rate_date: str) -> FakeUsdRate:
        assert currency == "NOK"
        assert rate_date == "2024-12-31"
        return FakeUsdRate()


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[Any, ...]], tuple[str, ...]]] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def insert(
        self,
        table: str,
        data: list[tuple[Any, ...]],
        *,
        column_names: tuple[str, ...],
    ) -> None:
        self.inserts.append((table, data, column_names))


def test_streaming_json_dependency_is_available() -> None:
    import ijson

    assert ijson


def test_norway_duckdb_path_is_a_source_constant_not_custom_resource() -> None:
    assert brreg_assets.NORWAY_BRREG_DUCKDB_PATH == Path("data/norway_brreg.duckdb")
    assert "NorwayDuckDBResource" not in brreg_assets.__dict__


def test_entity_resource_declares_explicit_table_schema() -> None:
    source = brreg_assets.norway_brreg_entities_source(
        session=FakeHttpSession(),
        run_id="test-run",
    )
    schema = source.resources[brreg_assets.ENTITIES_TABLE].compute_table_schema()
    columns = schema["columns"]
    row = brreg_assets.build_entity_rows([_entity_record()], run_id="test-run")[0]

    assert set(columns) == set(row)
    assert columns["org_number"]["data_type"] == "text"
    assert columns["source_line_number"]["data_type"] == "bigint"
    assert columns["employee_count"]["data_type"] == "bigint"
    assert columns["is_active"]["data_type"] == "bool"
    assert columns["company_description_en"]["data_type"] == "text"
    assert columns["raw_entity"]["data_type"] == "text"


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
    assert rows[0]["legal_form_description_en"] == ""
    assert rows[0]["nace1_code"] == "06.100"
    assert rows[0]["nace1_description_original"] == "Utvinning av raolje"
    assert rows[0]["nace1_description_en"] == ""
    assert rows[0]["nace2_code"] == "06.200"
    assert rows[0]["nace2_description_original"] == "Utvinning av naturgass"
    assert rows[0]["nace2_description_en"] == ""
    assert rows[0]["nace3_code"] == "19.200"
    assert (
        rows[0]["nace3_description_original"]
        == "Produksjon av raffinerte petroleumsprodukter"
    )
    assert rows[0]["nace3_description_en"] == ""
    assert (
        rows[0]["articles_purpose_original"]
        == "Aa utvikle, produsere og markedsfoere energi\nsamt annen virksomhet"
    )
    assert rows[0]["articles_purpose_en"] == ""
    assert (
        rows[0]["activity_text_original"]
        == "Selv, eller gjennom andre selskaper aa utvikle energi\n"
        "og avledede produkter og tjenester"
    )
    assert rows[0]["activity_text_en"] == ""
    assert rows[0]["company_description_original"] == rows[0]["activity_text_original"]
    assert rows[0]["company_description_en"] == ""
    assert rows[0]["employee_count"] == 21467
    assert rows[0]["status"] == "active"
    assert rows[0]["is_active"] is True
    assert rows[0]["last_submitted_accounts_year"] == "2024"
    assert json.loads(rows[0]["raw_entity"])["organisasjonsnummer"] == "923609016"


def test_entity_rows_serialize_decimal_values_from_streaming_parser() -> None:
    rows = brreg_assets.build_entity_rows(
        [_entity_record(sourceDecimal=Decimal("1.25"))],
        run_id="test-run",
    )

    assert len(rows[0]["source_payload_hash"]) == 64
    assert json.loads(rows[0]["raw_entity"])["sourceDecimal"] == 1.25


def test_financial_orgs_resource_filters_entities_from_duckdb(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=database_path,
        run_id="entity-run",
        session=FakeHttpSession(
            _gzip_json_array(
                [
                    _entity_record(),
                    _entity_record(organisasjonsnummer="inactive", konkurs=True),
                    _entity_record(organisasjonsnummer="no-website", hjemmeside=""),
                    _entity_record(
                        organisasjonsnummer="no-accounts",
                        sisteInnsendteAarsregnskap="",
                    ),
                ]
            )
        ),
    )

    rows = list(
        brreg_assets.norway_brreg_financial_orgs_resource(database_path=database_path)
    )

    assert rows == [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        }
    ]


def test_financial_statement_resource_declares_explicit_table_schema(tmp_path: Path) -> None:
    source = brreg_assets.norway_brreg_financial_statements_source(
        database_path=tmp_path / "norway.duckdb",
        session=FakeHttpSession(),
        exchange_rates=FakeExchangeRates(),
        run_id="financial-run",
    )
    schema = source.resources[brreg_assets.FINANCIAL_STATEMENTS_TABLE].compute_table_schema()
    columns = schema["columns"]
    row = brreg_assets.build_financial_statement_rows(
        [_financial_record()],
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        exchange_rates=FakeExchangeRates(),
        run_id="financial-run",
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
    )[0]

    assert set(columns) == set(row)
    assert columns["org_number"]["data_type"] == "text"
    assert columns["filing_id"]["data_type"] == "bigint"
    assert columns["period_end_date"]["data_type"] == "date"
    assert columns["operating_revenue_amount_original"]["data_type"] == "decimal"
    assert columns["operating_revenue_amount_usd"]["data_type"] == "decimal"


def test_financial_records_are_normalized_with_usd_amounts() -> None:
    rows = brreg_assets.build_financial_statement_rows(
        [_financial_record()],
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        exchange_rates=FakeExchangeRates(),
        run_id="financial-run",
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
    )

    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["accounts_type"] == "SELSKAP"
    assert rows[0]["period_end_date"] == "2024-12-31"
    assert rows[0]["currency"] == "NOK"
    assert rows[0]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.00")
    assert rows[0]["fx_rate_to_usd"] == Decimal("0.10")
    assert rows[0]["fx_rate_date"] == "2024-12-31"
    assert rows[0]["fx_source"] == "test-fx"
    assert json.loads(rows[0]["raw_financial_record"])["id"] == 5667197


def test_financial_rows_serialize_decimal_values_from_streaming_parser() -> None:
    rows = brreg_assets.build_financial_statement_rows(
        [_financial_record(sourceDecimal=Decimal("2.50"))],
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        exchange_rates=FakeExchangeRates(),
        run_id="financial-run",
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
    )

    assert len(rows[0]["source_payload_hash"]) == 64
    assert json.loads(rows[0]["raw_financial_record"])["sourceDecimal"] == 2.5


def test_financial_dlt_pipeline_loads_statements_table(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=database_path,
        run_id="entity-run",
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    session = FakeHttpSession(
        json_by_url={
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                _financial_record()
            ]
        }
    )

    load_info = brreg_assets.run_norway_brreg_financial_statements_dlt_pipeline(
        database_path=database_path,
        run_id="financial-run",
        session=session,
        exchange_rates=FakeExchangeRates(),
    )

    assert load_info
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select org_number, period_end_date, accounts_type,
                   operating_revenue_amount_original, operating_revenue_amount_usd
            from norway_brreg.financial_statements
            """
        ).fetchall()

    assert rows == [
        (
            "923609016",
            date(2024, 12, 31),
            "SELSKAP",
            Decimal("72543000000.000"),
            Decimal("7254300000.000"),
        )
    ]


def test_norway_brreg_clickhouse_schema_contract() -> None:
    assert brreg_tables.NORWAY_BRREG_DATABASE == "norway_brreg"
    assert brreg_tables.QUALIFIED_COMPANIES_TABLE == "norway_brreg.companies"
    assert (
        brreg_tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE
        == "norway_brreg.financial_statements"
    )
    assert "org_number" in brreg_tables.COMPANIES_COLUMNS
    assert "company_description_original" in brreg_tables.COMPANIES_COLUMNS
    assert "company_description_en" in brreg_tables.COMPANIES_COLUMNS
    assert "operating_revenue_amount_original" in (
        brreg_tables.FINANCIAL_STATEMENTS_COLUMNS
    )
    assert "operating_revenue_amount_usd" in brreg_tables.FINANCIAL_STATEMENTS_COLUMNS
    assert "fx_rate_to_usd" in brreg_tables.FINANCIAL_STATEMENTS_COLUMNS
    assert (
        "CREATE TABLE IF NOT EXISTS norway_brreg.companies"
        in brreg_tables.COMPANIES_DDL
    )
    assert (
        "CREATE TABLE IF NOT EXISTS norway_brreg.financial_statements"
        in brreg_tables.FINANCIAL_STATEMENTS_DDL
    )


def test_prepare_norway_brreg_clickhouse_tables_is_typed_for_official_resource() -> None:
    annotations = get_type_hints(prepare_norway_brreg_clickhouse_tables)

    assert annotations["clickhouse"] is ClickhouseResource


def test_prepare_norway_brreg_clickhouse_tables_uses_official_resource_connection(
    monkeypatch,
) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()
    connection_calls: list[ClickhouseResource] = []

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        connection_calls.append(self)
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_norway_brreg_clickhouse_tables(resource)

    assert connection_calls == [resource]
    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS norway_brreg",
        brreg_tables.COMPANIES_DDL.strip(),
        brreg_tables.FINANCIAL_STATEMENTS_DDL.strip(),
        "TRUNCATE TABLE norway_brreg.companies",
        "TRUNCATE TABLE norway_brreg.financial_statements",
    ]


def test_export_norway_brreg_clickhouse_tables_reads_duckdb_and_inserts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "norway.duckdb"
    brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=database_path,
        run_id="entity-run",
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    brreg_assets.run_norway_brreg_financial_statements_dlt_pipeline(
        database_path=database_path,
        run_id="financial-run",
        session=FakeHttpSession(
            json_by_url={
                "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                    _financial_record()
                ]
            }
        ),
        exchange_rates=FakeExchangeRates(),
    )
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()
    log_messages: list[str] = []

    def capture_log(message: str, *args: Any) -> None:
        log_messages.append(message % args if args else message)

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    result = brreg_assets.export_norway_brreg_clickhouse_tables(
        database_path=database_path,
        clickhouse=resource,
        log=capture_log,
    )

    assert result == {"companies": 1, "financial_statements": 1}
    assert [insert[0] for insert in client.inserts] == [
        "norway_brreg.companies",
        "norway_brreg.financial_statements",
    ]
    assert client.inserts[0][2] == brreg_tables.COMPANIES_COLUMNS
    assert client.inserts[1][2] == brreg_tables.FINANCIAL_STATEMENTS_COLUMNS

    company_row = dict(zip(brreg_tables.COMPANIES_COLUMNS, client.inserts[0][1][0]))
    financial_row = dict(
        zip(brreg_tables.FINANCIAL_STATEMENTS_COLUMNS, client.inserts[1][1][0])
    )
    assert company_row["org_number"] == "923609016"
    assert company_row["legal_name"] == "EQUINOR ASA"
    assert company_row["company_description_en"] == ""
    assert financial_row["org_number"] == "923609016"
    assert financial_row["period_end_date"] == date(2024, 12, 31)
    assert financial_row["operating_revenue_amount_original"] == Decimal("72543000000.000")
    assert financial_row["operating_revenue_amount_usd"] == Decimal("7254300000.000")
    assert log_messages == [
        (
            "Preparing Norway Brreg ClickHouse tables: database=norway_brreg, "
            "companies_table=norway_brreg.companies, "
            "financial_statements_table=norway_brreg.financial_statements"
        ),
        f"Opening Norway Brreg DuckDB staging database: path={database_path}",
        "Reading Norway Brreg company rows from DuckDB: table=norway_brreg.entities",
        "Read Norway Brreg company rows from DuckDB: rows=1",
        (
            "Reading Norway Brreg financial statement rows from DuckDB: "
            "table=norway_brreg.financial_statements"
        ),
        "Read Norway Brreg financial statement rows from DuckDB: rows=1",
        (
            "Inserting Norway Brreg company rows into ClickHouse: "
            "table=norway_brreg.companies, rows=1"
        ),
        (
            "Inserting Norway Brreg financial statement rows into ClickHouse: "
            "table=norway_brreg.financial_statements, rows=1"
        ),
        "Finished Norway Brreg ClickHouse export: companies=1, financial_statements=1",
    ]


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
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    asset_names = {key.path[-1] for key in asset_graph.get_all_asset_keys()}

    assert "norway_brreg_entities_duckdb" in asset_names
    assert "norway_brreg_financial_statements_duckdb" in asset_names
    assert "norway_brreg_clickhouse_tables" in asset_names
    assert "norway_duckdb" not in repository.get_top_level_resources().keys()
