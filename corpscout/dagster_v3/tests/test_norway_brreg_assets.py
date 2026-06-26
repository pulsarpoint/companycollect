import gzip
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import get_type_hints

import dlt
import duckdb
from dagster_clickhouse import ClickhouseResource

import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.defs.norway_brreg import resources as brreg_resources
from dagster_v3.defs.norway_brreg import tables as brreg_tables
from dagster_v3.defs.norway_brreg.financial_fetches import (
    run_brreg_financial_statement_fetches,
)
from dagster_v3.defs.norway_brreg.clickhouse import (
    prepare_norway_brreg_clickhouse_companies_table,
    prepare_norway_brreg_clickhouse_financial_statements_table,
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

    def iter_content(self, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]


class FakeHttpSession:
    def __init__(
        self,
        content: bytes = b"",
        json_by_url: dict[str, Any] | None = None,
        status_by_url: dict[str, int] | None = None,
    ) -> None:
        self.content = content
        self.json_by_url = json_by_url or {}
        self.status_by_url = status_by_url or {}
        self.calls: list[tuple[str, dict[str, Any] | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: int = 120,
        stream: bool = False,
    ) -> FakeResponse:
        self.calls.append((url, params, timeout))
        status_code = self.status_by_url.get(url, 200)
        if url in self.json_by_url:
            return FakeResponse(
                content=json.dumps(self.json_by_url[url]).encode("utf-8"),
                status_code=status_code,
            )
        return FakeResponse(content=self.content, status_code=status_code)


def _gzip_json_array(records: list[dict[str, Any]]) -> bytes:
    return gzip.compress(json.dumps(records).encode("utf-8"))


def _run_entities_dlt_pipeline_for_test(
    *,
    database_path: str | Path,
    session: brreg_resources.HttpSession,
) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="test_norway_brreg_entities",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=brreg_resources.DLT_DATASET_NAME,
        dev_mode=False,
    ).run(brreg_resources.norway_brreg_entities_source(session=session))


def _run_financial_fetches_for_test(
    *,
    database_path: str | Path,
    client: Any,
) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_file)) as connection:
        return run_brreg_financial_statement_fetches(
            duckdb_connection=connection,
            source_run_id="test-run",
            client=client,
            commit_every_rows=1,
        )


def _normalize_financial_statements_for_test(
    *,
    database_path: str | Path,
    exchange_rates: Any,
) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        return brreg_assets.normalize_norway_brreg_financial_statements_duckdb(
            duckdb_connection=connection,
            exchange_rates=exchange_rates,
        )


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
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        self.requests.extend((request.currency, request.rate_date) for request in requests)
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }

    def usd_rate(self, *, currency: str, rate_date: str) -> FakeUsdRate:
        assert currency == "NOK"
        assert rate_date == "2024-12-31"
        return FakeUsdRate()


class FakeExchangeRatesWithMissing(FakeExchangeRates):
    def usd_rates(self, requests):
        if any(request.currency == "USN" for request in requests):
            raise LookupError("No USD exchange rate for USN on, before, or after 2024-12-31")
        return super().usd_rates(requests)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.insert_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(
        self,
        sql: str,
        params: list[tuple[Any, ...]] | None = None,
    ) -> None:
        if params is None:
            self.statements.append(sql)
        else:
            self.insert_calls.append((sql, params))


def test_streaming_json_dependency_is_available() -> None:
    import ijson

    assert ijson


def test_norway_duckdb_path_is_a_source_constant_not_custom_resource() -> None:
    assert brreg_assets.NORWAY_BRREG_DUCKDB_PATH == Path("data/norway_brreg_source.duckdb")
    assert brreg_assets.NORWAY_BRREG_DUCKDB_PATH.stem != brreg_assets.DLT_DATASET_NAME
    assert "NorwayDuckDBResource" not in brreg_assets.__dict__


def test_entity_resource_declares_explicit_table_schema() -> None:
    columns = brreg_resources.BRREG_ENTITIES_COLUMNS
    row = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")[0]

    assert brreg_resources.BRREG_ENTITIES_COLUMNS is brreg_tables.BRREG_ENTITIES_COLUMNS
    assert set(columns) == set(row)
    assert columns["org_number"]["data_type"] == "text"
    assert columns["source_line_number"]["data_type"] == "bigint"
    assert columns["employee_count"]["data_type"] == "bigint"
    assert columns["is_active"]["data_type"] == "bool"
    assert columns["company_description_en"]["data_type"] == "text"
    assert columns["raw_entity"]["data_type"] == "text"


def test_norway_table_schema_constants_are_not_mutated_by_dlt(tmp_path: Path) -> None:
    assert "name" not in brreg_tables.BRREG_ENTITIES_COLUMNS["org_number"]

    _run_entities_dlt_pipeline_for_test(
        database_path=tmp_path / "norway.duckdb",
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )

    assert "name" not in brreg_tables.BRREG_ENTITIES_COLUMNS["org_number"]


def test_norway_brreg_entities_uses_resources_module_for_dlt_source() -> None:
    assert "norway_brreg_entities_source" not in brreg_assets.__dict__
    assert "_entities_resource" not in brreg_assets.__dict__
    assert "norway_brreg_pipeline" not in brreg_assets.__dict__


def test_norway_brreg_entity_dlt_source_is_defined_in_resources_module() -> None:
    assert "norway_brreg_entities_source" in brreg_resources.__dict__
    assert "_entities_resource" in brreg_resources.__dict__
    assert "norway_brreg_financial_fetches_source" not in brreg_resources.__dict__
    assert "_financial_fetches_resource" not in brreg_resources.__dict__

    entity_source = brreg_resources.norway_brreg_entities_source(
        session=FakeHttpSession(_gzip_json_array([_entity_record()]))
    )

    assert entity_source.name == "norway_brreg_entities"
    assert brreg_resources.ENTITIES_TABLE in entity_source.resources


def test_norway_brreg_assets_do_not_expose_pipeline_helpers() -> None:
    assert "run_norway_brreg_entities_dlt_pipeline" not in brreg_assets.__dict__
    assert "run_norway_brreg_financial_fetches_dlt_pipeline" not in brreg_assets.__dict__
    assert "run_norway_brreg_entities_dlt_pipeline" not in brreg_resources.__dict__
    assert "run_norway_brreg_financial_fetches_dlt_pipeline" not in brreg_resources.__dict__


def test_entity_rows_extract_company_spine_fields() -> None:
    rows = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")

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
    assert rows[0]["legal_form_description_en"] == "Public limited company"
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


def test_norway_legal_form_description_en_uses_deterministic_mapping() -> None:
    rows = brreg_resources.build_entity_rows(
        [
            _entity_record(
                organisasjonsform={
                    "kode": "ENK",
                    "beskrivelse": "Enkeltpersonforetak",
                }
            )
        ],
        run_id="test-run",
    )

    assert rows[0]["legal_form_description_en"] == "Sole proprietorship"


def test_unknown_norway_legal_form_description_en_is_empty() -> None:
    rows = brreg_resources.build_entity_rows(
        [
            _entity_record(
                organisasjonsform={
                    "kode": "UNKNOWN",
                    "beskrivelse": "Ukjent",
                }
            )
        ],
        run_id="test-run",
    )

    assert rows[0]["legal_form_description_en"] == ""


def test_entity_rows_serialize_decimal_values_from_streaming_parser() -> None:
    rows = brreg_resources.build_entity_rows(
        [_entity_record(sourceDecimal=Decimal("1.25"))],
        run_id="test-run",
    )

    assert len(rows[0]["source_payload_hash"]) == 64
    assert json.loads(rows[0]["raw_entity"])["sourceDecimal"] == 1.25


def test_financial_statement_schema_matches_normalized_rows() -> None:
    columns = brreg_assets.BRREG_FINANCIAL_STATEMENTS_COLUMNS
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

    assert (
        brreg_assets.BRREG_FINANCIAL_STATEMENTS_COLUMNS
        is brreg_tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS
    )
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


def test_financial_fetch_and_normalize_pipeline_loads_statements_table(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    client = FakeHttpSession(
        json_by_url={
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                _financial_record()
            ]
        }
    )

    load_info = _run_financial_fetches_for_test(
        database_path=database_path,
        client=client,
    )
    counts = _normalize_financial_statements_for_test(
        database_path=database_path,
        exchange_rates=FakeExchangeRates(),
    )

    assert load_info
    assert counts == {
        "financial_fetches": 1,
        "financial_statements": 1,
        "successful_fetches": 1,
        "failed_fetches": 0,
    }
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


def test_financial_normalize_persists_missing_fx_as_null_dates(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    client = FakeHttpSession(
        json_by_url={
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                _financial_record(valuta="USN")
            ]
        }
    )

    _run_financial_fetches_for_test(
        database_path=database_path,
        client=client,
    )
    counts = _normalize_financial_statements_for_test(
        database_path=database_path,
        exchange_rates=FakeExchangeRatesWithMissing(),
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select currency, fx_rate_to_usd, fx_rate_date, operating_revenue_amount_usd
            from norway_brreg.financial_statements
            """
        ).fetchall()

    assert counts["financial_statements"] == 1
    assert rows == [("USN", None, None, None)]


def test_financial_fetch_pipeline_persists_not_found_and_server_errors(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(
            _gzip_json_array(
                [
                    _entity_record(organisasjonsnummer="811685852"),
                    _entity_record(organisasjonsnummer="814115232"),
                    _entity_record(organisasjonsnummer="923609016"),
                ]
            )
        ),
    )
    client = FakeHttpSession(
        json_by_url={
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                _financial_record()
            ]
        },
        status_by_url={
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": 404,
            "https://data.brreg.no/regnskapsregisteret/regnskap/814115232": 500,
        },
    )

    _run_financial_fetches_for_test(
        database_path=database_path,
        client=client,
    )
    counts = _normalize_financial_statements_for_test(
        database_path=database_path,
        exchange_rates=FakeExchangeRates(),
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        fetch_rows = connection.execute(
            """
            select org_number, fetch_status, http_status
            from norway_brreg.financial_fetches
            order by org_number
            """
        ).fetchall()
        statement_rows = connection.execute(
            "select org_number from norway_brreg.financial_statements order by org_number"
        ).fetchall()

    assert fetch_rows == [
        ("811685852", "not_found", 404),
        ("814115232", "server_error", 500),
        ("923609016", "success", 200),
    ]
    assert statement_rows == [("923609016",)]
    assert counts == {
        "financial_fetches": 3,
        "financial_statements": 1,
        "successful_fetches": 1,
        "failed_fetches": 2,
    }


def test_norway_brreg_clickhouse_schema_contract() -> None:
    assert brreg_tables.NORWAY_BRREG_DATABASE == "corpscout"
    assert brreg_tables.QUALIFIED_COMPANIES_TABLE == "corpscout.companies"
    assert (
        brreg_tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE
        == "corpscout.financial_statements"
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
        "CREATE TABLE IF NOT EXISTS corpscout.companies"
        in brreg_tables.COMPANIES_DDL
    )
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.financial_statements"
        in brreg_tables.FINANCIAL_STATEMENTS_DDL
    )
    assert "ENGINE = ReplacingMergeTree" in brreg_tables.COMPANIES_DDL
    assert "ENGINE = ReplacingMergeTree" in brreg_tables.FINANCIAL_STATEMENTS_DDL
    assert "ENGINE = MergeTree" not in brreg_tables.COMPANIES_DDL
    assert "ENGINE = MergeTree" not in brreg_tables.FINANCIAL_STATEMENTS_DDL


def test_prepare_norway_brreg_clickhouse_tables_are_typed_for_official_resource() -> None:
    companies_annotations = get_type_hints(
        prepare_norway_brreg_clickhouse_companies_table
    )
    financials_annotations = get_type_hints(
        prepare_norway_brreg_clickhouse_financial_statements_table
    )

    assert companies_annotations["clickhouse"] is ClickhouseResource
    assert financials_annotations["clickhouse"] is ClickhouseResource


def test_prepare_norway_brreg_clickhouse_companies_uses_official_resource_connection(
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

    prepare_norway_brreg_clickhouse_companies_table(resource)

    assert connection_calls == [resource]
    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS corpscout",
        brreg_tables.COMPANIES_DDL.strip(),
    ]


def test_prepare_norway_brreg_clickhouse_financials_uses_official_resource_connection(
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

    prepare_norway_brreg_clickhouse_financial_statements_table(resource)

    assert connection_calls == [resource]
    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS corpscout",
        brreg_tables.FINANCIAL_STATEMENTS_DDL.strip(),
    ]


def test_entity_status_derivation_handles_liquidation_and_bankruptcy() -> None:
    rows = brreg_resources.build_entity_rows(
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


def test_iter_brreg_entity_rows_downloads_gzip_and_yields_rows() -> None:
    session = FakeHttpSession(
        _gzip_json_array(
            [
                _entity_record(),
                _entity_record(organisasjonsnummer="999999999"),
            ]
        )
    )

    rows = list(brreg_resources.iter_brreg_entity_rows(session=session, run_id="test-run"))

    assert [row["org_number"] for row in rows] == ["923609016", "999999999"]
    assert session.calls == [
        ("https://data.brreg.no/enhetsregisteret/api/enheter/lastned", None, 120)
    ]
    assert session.headers["User-Agent"] == brreg_resources.DEFAULT_USER_AGENT


def test_iter_brreg_entity_rows_logs_every_1000_rows() -> None:
    records = [
        _entity_record(organisasjonsnummer=str(900000000 + index))
        for index in range(2001)
    ]
    messages: list[str] = []

    rows = list(
        brreg_resources.iter_brreg_entity_rows(
            session=FakeHttpSession(_gzip_json_array(records)),
            log=lambda message, *args: messages.append(message % args),
        )
    )

    assert len(rows) == 2001
    assert messages == [
        "Processed Norway Brreg entity rows: rows=1000",
        "Processed Norway Brreg entity rows: rows=2000",
    ]


def test_download_bytes_logs_progress_by_byte_threshold() -> None:
    messages: list[str] = []

    body = brreg_resources._download_bytes(
        url="https://data.brreg.no/enhetsregisteret/api/enheter/lastned",
        timeout_seconds=120,
        user_agent="test-agent",
        session=FakeHttpSession(b"abcdefghij"),
        log=lambda message, *args: messages.append(message % args),
        progress_every_bytes=4,
    )

    assert body == b"abcdefghij"
    assert messages == [
        "Downloaded Norway Brreg entity archive: downloaded_bytes=4 downloaded_mb=0.0",
        "Downloaded Norway Brreg entity archive: downloaded_bytes=8 downloaded_mb=0.0",
    ]


def test_entity_dlt_pipeline_loads_entities_table(tmp_path: Path) -> None:
    session = FakeHttpSession(_gzip_json_array([_entity_record()]))

    load_info = _run_entities_dlt_pipeline_for_test(
        database_path=tmp_path / "norway.duckdb",
        session=session,
    )

    assert load_info
    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            "select org_number, legal_name, status from norway_brreg.entities"
        ).fetchall()

    assert rows == [("923609016", "EQUINOR ASA", "active")]
