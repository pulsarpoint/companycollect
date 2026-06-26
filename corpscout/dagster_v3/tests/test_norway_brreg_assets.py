import gzip
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import get_type_hints

import dagster as dg
import dlt
import duckdb
import pyarrow as pa
import pytest
from temporalio.client import WorkflowExecutionStatus
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource


def test_norway_refresh_schedule_and_jobs_respect_translation_flow():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sched = repo.get_schedule_def("norway_brreg_refresh_schedule")
    assert sched.cron_schedule == "0 6 7 * *"  # monthly, staggered
    assert sched.job.name == "norway_brreg_refresh_job"

    # refresh kicks off translation + financials, sharing one entities load; it
    # must NOT include translations_applied / clickhouse_companies (sensor-driven).
    refresh = {
        k.path[-1]
        for k in repo.get_job("norway_brreg_refresh_job").asset_layer.executable_asset_keys
    }
    assert refresh == {
        "norway_brreg_entities_duckdb",
        "norway_brreg_translation_queue",
        "norway_brreg_financial_fetches_duckdb",
        "norway_brreg_financial_statements_duckdb",
        "norway_brreg_clickhouse_financial_statements",
    }
    # the sensor's completion job now also exports companies to ClickHouse.
    completion = {
        k.path[-1]
        for k in repo.get_job(
            "norway_brreg_translation_completion_job"
        ).asset_layer.executable_asset_keys
    }
    assert completion == {
        "norway_brreg_translations_applied",
        "norway_brreg_clickhouse_companies",
    }

import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.norway_brreg import resources as brreg_resources
from dagster_v3.defs.norway_brreg import sensors as brreg_sensors
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


def _seed_translation_queue_for_test(
    *,
    source_duckdb_path: str | Path,
    queue_duckdb_path: str | Path,
    log: Any | None = None,
    refresh_existing_queue: bool = False,
) -> dict[str, Any]:
    queue_file = Path(queue_duckdb_path)
    queue_file.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(queue_file)) as queue_connection:
        return brreg_assets.seed_norway_brreg_translation_queue(
            source_duckdb_path=source_duckdb_path,
            queue_duckdb_connection=queue_connection,
            queue_duckdb_path=queue_file,
            log=log,
            refresh_existing_queue=refresh_existing_queue,
        )


def _apply_translation_queue_results_for_test(
    *,
    source_duckdb_path: str | Path,
    queue_duckdb_path: str | Path,
) -> dict[str, int]:
    with duckdb.connect(str(source_duckdb_path)) as source_connection:
        return brreg_assets.apply_norway_brreg_translation_queue_results(
            source_duckdb_connection=source_connection,
            queue_duckdb_path=queue_duckdb_path,
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


class FakeTemporalWorkflowDescription:
    def __init__(self, *, run_id: str, status: WorkflowExecutionStatus) -> None:
        self.run_id = run_id
        self.status = status


class FakeTemporalWorkflowHandle:
    id = "translation-norway-brreg"
    result_run_id = "temporal-run-123"

    def __init__(self, description: FakeTemporalWorkflowDescription) -> None:
        self.description = description

    async def describe(self) -> FakeTemporalWorkflowDescription:
        return self.description


class FakeTemporalClient:
    def __init__(self, description: FakeTemporalWorkflowDescription) -> None:
        self.description = description

    async def start_workflow(self, *args: Any, **kwargs: Any) -> FakeTemporalWorkflowHandle:
        return FakeTemporalWorkflowHandle(self.description)

    def get_workflow_handle(self, workflow_id: str) -> FakeTemporalWorkflowHandle:
        assert workflow_id == "translation-norway-brreg"
        return FakeTemporalWorkflowHandle(self.description)


class MissingWorkflowTemporalClient:
    def get_workflow_handle(self, workflow_id: str) -> FakeTemporalWorkflowHandle:
        assert workflow_id == "translation-norway-brreg"
        raise RuntimeError("workflow not found")


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


def test_norway_definitions_wire_official_duckdb_resources() -> None:
    resources = load_project_defs().get_repository_def().get_top_level_resources()

    assert resources["norway_brreg_duckdb"].configurable_resource_cls is DuckDBResource
    assert (
        resources["norway_brreg_translation_queue_duckdb"].configurable_resource_cls
        is DuckDBResource
    )


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


def test_norway_brreg_translation_sensor_is_defined_in_sensors_module() -> None:
    assert "norway_brreg_translation_completion_sensor" not in brreg_assets.__dict__
    assert "build_norway_brreg_translation_completion_sensor_result" not in brreg_assets.__dict__
    assert brreg_sensors.norway_brreg_translation_completion_sensor.name == (
        "norway_brreg_translation_completion_sensor"
    )
    assert brreg_sensors.norway_brreg_translation_completion_sensor.default_status == (
        dg.DefaultSensorStatus.RUNNING
    )


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


def test_build_norway_brreg_translation_queue_items_excludes_reference_fields() -> None:
    row = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")[0]

    items = brreg_assets.build_norway_brreg_translation_queue_items(
        [row],
        source_duckdb_path="data/norway_brreg_source.duckdb",
    )

    assert {item.source_field for item in items} == {
        "articles_purpose_original",
        "activity_text_original",
        "company_description_original",
    }
    assert "legal_form_description_original" not in {item.source_field for item in items}
    assert "nace1_description_original" not in {item.source_field for item in items}
    assert all(item.source_table == "norway_brreg.entities" for item in items)
    assert all(item.source_pk == "923609016" for item in items)
    assert all(item.target_language == "en" for item in items)


def test_build_norway_brreg_translation_queue_items_skips_existing_translations() -> None:
    row = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")[0]
    row["activity_text_en"] = "Already translated"

    items = brreg_assets.build_norway_brreg_translation_queue_items(
        [row],
        source_duckdb_path="data/norway_brreg_source.duckdb",
    )

    assert {item.source_field for item in items} == {
        "articles_purpose_original",
        "company_description_original",
    }


def test_seed_norway_brreg_translation_queue_reads_entities_duckdb(tmp_path: Path) -> None:
    source_duckdb_path = tmp_path / "norway_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    rows = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")

    with duckdb.connect(str(source_duckdb_path)) as connection:
        connection.execute("create schema norway_brreg")
        connection.register("entity_rows", pa.Table.from_pylist(rows))
        connection.execute("create table norway_brreg.entities as select * from entity_rows")

    counts = _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert counts["source_rows"] == 1
    assert counts["candidate_items"] == 3
    assert counts["queue_total_items"] == 2
    assert counts["queue_location_items"] == 3


def test_seed_norway_brreg_translation_queue_only_needs_translation_columns(
    tmp_path: Path,
) -> None:
    source_duckdb_path = tmp_path / "norway_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    log_messages: list[str] = []

    with duckdb.connect(str(source_duckdb_path)) as connection:
        connection.execute("create schema norway_brreg")
        connection.execute(
            """
            create table norway_brreg.entities (
              org_number text,
              articles_purpose_original text,
              articles_purpose_en text,
              activity_text_original text,
              activity_text_en text,
              company_description_original text,
              company_description_en text
            )
            """
        )
        connection.execute(
            """
            insert into norway_brreg.entities values
              ('923609016', 'Formaal', '', 'Aktivitet', '', 'Beskrivelse', ''),
              ('999999999', 'Ferdig', 'Done', '', '', null, '')
            """
        )

    counts = _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
        log=lambda message, *args: log_messages.append(message % args),
    )

    assert counts["source_rows"] == 2
    assert counts["candidate_items"] == 3
    assert counts["queue_total_items"] == 3
    assert counts["queue_location_items"] == 3
    assert any("Counting Norway Brreg translation candidates" in message for message in log_messages)
    assert any("Inserted Norway Brreg translation queue candidates" in message for message in log_messages)


def test_seed_norway_brreg_translation_queue_skips_seeded_queue_source_scan(
    tmp_path: Path,
) -> None:
    source_duckdb_path = tmp_path / "missing_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    queue = brreg_assets.TranslationQueue(queue_duckdb_path)
    queue.initialize()
    queue.enqueue_items(
        [
            brreg_assets.TranslationQueueItem(
                source_duckdb_path=str(source_duckdb_path),
                source_table="norway_brreg.entities",
                source_pk="923609016",
                source_field="activity_text_original",
                source_text="Aktivitet",
                target_language="en",
            )
        ]
    )

    counts = _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert counts["source_scan_skipped"] is True
    assert counts["source_rows"] == 0
    assert counts["candidate_items"] == 0
    assert counts["inserted_items"] == 0
    assert counts["inserted_locations"] == 0
    assert counts["queue_total_items"] == 1
    assert counts["queue_location_items"] == 1
    assert counts["queue_pending_items"] == 1


def test_seed_norway_brreg_translation_queue_does_not_initialize_seeded_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_duckdb_path = tmp_path / "missing_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    queue = brreg_assets.TranslationQueue(queue_duckdb_path)
    queue.initialize()
    queue.enqueue_items(
        [
            brreg_assets.TranslationQueueItem(
                source_duckdb_path=str(source_duckdb_path),
                source_table="norway_brreg.entities",
                source_pk="923609016",
                source_field="activity_text_original",
                source_text="Aktivitet",
                target_language="en",
            )
        ]
    )

    def fail_initialize(self: object) -> None:
        raise AssertionError("seeded queue fast path must not initialize queue tables")

    monkeypatch.setattr(brreg_assets.TranslationQueue, "initialize", fail_initialize)

    counts = _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert counts["source_scan_skipped"] is True
    assert counts["queue_total_items"] == 1
    assert counts["queue_location_items"] == 1


def test_seed_norway_brreg_translation_queue_refreshes_seeded_queue_when_configured(
    tmp_path: Path,
) -> None:
    source_duckdb_path = tmp_path / "norway_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    queue = brreg_assets.TranslationQueue(queue_duckdb_path)
    queue.initialize()
    queue.enqueue_items(
        [
            brreg_assets.TranslationQueueItem(
                source_duckdb_path=str(source_duckdb_path),
                source_table="norway_brreg.entities",
                source_pk="existing",
                source_field="activity_text_original",
                source_text="Already queued",
                target_language="en",
            )
        ]
    )
    rows = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")
    with duckdb.connect(str(source_duckdb_path)) as connection:
        connection.execute("create schema norway_brreg")
        connection.register("entity_rows", pa.Table.from_pylist(rows))
        connection.execute("create table norway_brreg.entities as select * from entity_rows")

    counts = _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
        refresh_existing_queue=True,
    )

    assert counts["source_scan_skipped"] is False
    assert counts["source_rows"] == 1
    assert counts["candidate_items"] == 3
    assert counts["inserted_items"] == 2
    assert counts["inserted_locations"] == 3
    assert counts["queue_total_items"] == 3
    assert counts["queue_location_items"] == 4


def test_apply_norway_brreg_translation_queue_results_updates_free_text_fields(
    tmp_path: Path,
) -> None:
    source_duckdb_path, queue_duckdb_path = _source_and_completed_translation_queue(tmp_path)

    counts = _apply_translation_queue_results_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert counts["completed_translations"] == 3
    assert counts["rows_updated"] == 1
    assert counts["fields_updated"] == 3
    with duckdb.connect(str(source_duckdb_path), read_only=True) as connection:
        row = connection.execute(
            """
            select
              legal_form_description_en,
              nace1_description_en,
              articles_purpose_en,
              activity_text_en,
              company_description_en
            from norway_brreg.entities
            where org_number = '923609016'
            """
        ).fetchone()

    assert row == (
        "Public limited company",
        "",
        "Purpose translated",
        "Activity translated",
        "Activity translated",
    )


def test_apply_norway_brreg_translation_queue_results_is_idempotent(tmp_path: Path) -> None:
    source_duckdb_path, queue_duckdb_path = _source_and_completed_translation_queue(tmp_path)

    first = _apply_translation_queue_results_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )
    second = _apply_translation_queue_results_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert first["fields_updated"] == 3
    assert second["fields_updated"] == 0
    assert second["rows_updated"] == 0


def test_apply_norway_brreg_translation_queue_results_ignores_reference_fields(
    tmp_path: Path,
) -> None:
    source_duckdb_path, queue_duckdb_path = _source_and_completed_translation_queue(tmp_path)
    brreg_assets._insert_completed_translation_for_test(
        queue_duckdb_path=queue_duckdb_path,
        source_duckdb_path=source_duckdb_path,
        source_pk="923609016",
        source_field="nace1_description_original",
        source_text="Utvinning av raolje",
        translated_text="Extraction of crude petroleum",
    )

    counts = _apply_translation_queue_results_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )

    assert counts["completed_translations"] == 4
    assert counts["skipped_non_free_text"] == 1
    with duckdb.connect(str(source_duckdb_path), read_only=True) as connection:
        nace_en = connection.execute(
            """
            select nace1_description_en
            from norway_brreg.entities
            where org_number = '923609016'
            """
        ).fetchone()[0]
    assert nace_en == ""


def test_entity_rows_serialize_decimal_values_from_streaming_parser() -> None:
    rows = brreg_resources.build_entity_rows(
        [_entity_record(sourceDecimal=Decimal("1.25"))],
        run_id="test-run",
    )

    assert len(rows[0]["source_payload_hash"]) == 64
    assert json.loads(rows[0]["raw_entity"])["sourceDecimal"] == 1.25


def _source_and_completed_translation_queue(tmp_path: Path) -> tuple[Path, Path]:
    source_duckdb_path = tmp_path / "norway_source.duckdb"
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    rows = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")
    with duckdb.connect(str(source_duckdb_path)) as connection:
        connection.execute("create schema norway_brreg")
        connection.register("entity_rows", pa.Table.from_pylist(rows))
        connection.execute("create table norway_brreg.entities as select * from entity_rows")

    _seed_translation_queue_for_test(
        source_duckdb_path=source_duckdb_path,
        queue_duckdb_path=queue_duckdb_path,
    )
    brreg_assets._complete_all_translation_queue_items_for_test(
        queue_duckdb_path=queue_duckdb_path,
        translations_by_field={
            "articles_purpose_original": "Purpose translated",
            "activity_text_original": "Activity translated",
            "company_description_original": "Company description translated",
        },
    )
    return source_duckdb_path, queue_duckdb_path


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


def test_export_norway_brreg_clickhouse_tables_reads_duckdb_and_inserts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    _run_financial_fetches_for_test(
        database_path=database_path,
        client=FakeHttpSession(
            json_by_url={
                "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                    _financial_record()
                ]
            }
        ),
    )
    _normalize_financial_statements_for_test(
        database_path=database_path,
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

    with duckdb.connect(str(database_path), read_only=True) as connection:
        result = brreg_assets.export_norway_brreg_clickhouse_tables(
            duckdb_connection=connection,
            clickhouse=resource,
            log=capture_log,
        )

    assert result == {"companies": 1, "financial_statements": 1}
    assert [insert[0].split(" (", 1)[0] for insert in client.insert_calls] == [
        "INSERT INTO `corpscout`.`companies`",
        "INSERT INTO `corpscout`.`financial_statements`",
    ]

    company_row = dict(zip(brreg_tables.COMPANIES_EXPORT_COLUMNS, client.insert_calls[0][1][0]))
    financial_row = dict(
        zip(brreg_tables.FINANCIAL_STATEMENTS_EXPORT_COLUMNS, client.insert_calls[1][1][0])
    )
    assert company_row["org_number"] == "923609016"
    assert company_row["legal_name"] == "EQUINOR ASA"
    assert financial_row["org_number"] == "923609016"
    assert financial_row["period_end_date"] == date(2024, 12, 31)
    assert financial_row["operating_revenue_amount_original"] == Decimal("72543000000.000")
    assert financial_row["operating_revenue_amount_usd"] == Decimal("7254300000.000")
    assert (
        (
            "Preparing Norway Brreg companies ClickHouse table: database=corpscout, "
            "table=corpscout.companies"
        )
        in log_messages
    )
    assert (
        (
            "Preparing Norway Brreg financial statements ClickHouse table: "
            "database=corpscout, table=corpscout.financial_statements"
        )
        in log_messages
    )


def test_export_norway_brreg_clickhouse_companies_touches_only_company_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = brreg_assets.export_norway_brreg_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS corpscout",
        brreg_tables.COMPANIES_DDL.strip(),
    ]
    assert [insert[0].split(" (", 1)[0] for insert in client.insert_calls] == [
        "INSERT INTO `corpscout`.`companies`"
    ]
    company_row = dict(zip(brreg_tables.COMPANIES_EXPORT_COLUMNS, client.insert_calls[0][1][0]))
    assert company_row["org_number"] == "923609016"


def test_export_norway_brreg_clickhouse_financials_touches_only_financial_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "norway.duckdb"
    _run_entities_dlt_pipeline_for_test(
        database_path=database_path,
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )
    _run_financial_fetches_for_test(
        database_path=database_path,
        client=FakeHttpSession(
            json_by_url={
                "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [
                    _financial_record()
                ]
            }
        ),
    )
    _normalize_financial_statements_for_test(
        database_path=database_path,
        exchange_rates=FakeExchangeRates(),
    )
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = brreg_assets.export_norway_brreg_clickhouse_financial_statements(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS corpscout",
        brreg_tables.FINANCIAL_STATEMENTS_DDL.strip(),
    ]
    assert [insert[0].split(" (", 1)[0] for insert in client.insert_calls] == [
        "INSERT INTO `corpscout`.`financial_statements`"
    ]
    financial_row = dict(
        zip(brreg_tables.FINANCIAL_STATEMENTS_EXPORT_COLUMNS, client.insert_calls[0][1][0])
    )
    assert financial_row["org_number"] == "923609016"


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


def test_norway_entity_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    asset_names = {key.path[-1] for key in asset_graph.get_all_asset_keys()}

    assert "norway_brreg_entities_duckdb" in asset_names
    assert "norway_brreg_financial_fetches_duckdb" in asset_names
    assert "norway_brreg_financial_statements_duckdb" in asset_names
    assert "norway_brreg_financial_statements_duckdb_asset" not in asset_names
    assert "norway_brreg_translation_queue" in asset_names
    assert "norway_brreg_translation_queue_seeded" not in asset_names
    assert "norway_brreg_translation_workflow_started" not in asset_names
    assert "norway_brreg_translation_workflow_status" in asset_names
    assert "norway_brreg_translation_workflow_completed" not in asset_names
    assert "norway_brreg_translations_applied" in asset_names
    assert "norway_brreg_clickhouse_tables" not in asset_names
    assert "norway_brreg_clickhouse_companies" in asset_names
    assert "norway_brreg_clickhouse_financial_statements" in asset_names
    queue_node = asset_graph.get(brreg_assets.norway_brreg_translation_queue.key)
    applied_node = asset_graph.get(brreg_assets.norway_brreg_translations_applied.key)
    workflow_status_node = asset_graph.get(
        brreg_assets.NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY
    )
    assert queue_node.group_name == "norway_brreg"
    assert workflow_status_node.tags["system"] == "temporal"
    assert workflow_status_node.tags["temporal"] == "true"
    assert workflow_status_node.tags["dagster/kind/temporal"] == ""
    assert "temporal" in workflow_status_node.to_asset_spec().kinds
    assert {key.path[-1] for key in queue_node.parent_keys} == {"norway_brreg_entities_duckdb"}
    assert {key.path[-1] for key in applied_node.parent_keys} == {
        "norway_brreg_translation_queue",
        "norway_brreg_translation_workflow_status",
    }
    fetches_node = asset_graph.get(brreg_assets.norway_brreg_financial_fetches_duckdb_asset.key)
    statements_node = asset_graph.get(
        brreg_assets.norway_brreg_financial_statements_duckdb_asset.key
    )
    assert fetches_node.to_asset_spec().kinds == {"python", "duckdb"}
    assert {key.path[-1] for key in fetches_node.parent_keys} == {
        "norway_brreg_entities_duckdb"
    }
    assert {key.path[-1] for key in statements_node.parent_keys} == {
        "norway_brreg_financial_fetches_duckdb"
    }
    assert brreg_assets.norway_brreg_entities_duckdb_asset.op.pool == "norway_brreg_duckdb"
    assert (
        brreg_assets.norway_brreg_financial_fetches_duckdb_asset.op.pool
        == "norway_brreg_duckdb"
    )
    assert (
        brreg_assets.norway_brreg_financial_statements_duckdb_asset.op.pool
        == "norway_brreg_duckdb"
    )
    assert brreg_assets.norway_brreg_translation_queue.op.pool == "norway_brreg_duckdb"
    assert brreg_assets.norway_brreg_translations_applied.op.pool == "norway_brreg_duckdb"
    clickhouse_companies_node = asset_graph.get(
        dg.AssetKey("norway_brreg_clickhouse_companies")
    )
    clickhouse_financial_node = asset_graph.get(
        dg.AssetKey("norway_brreg_clickhouse_financial_statements")
    )
    assert brreg_assets.norway_brreg_clickhouse_companies.op.pool == "norway_brreg_duckdb"
    assert (
        brreg_assets.norway_brreg_clickhouse_financial_statements.op.pool
        == "norway_brreg_duckdb"
    )
    assert {key.path[-1] for key in clickhouse_companies_node.parent_keys} == {
        "norway_brreg_entities_duckdb",
    }
    assert {key.path[-1] for key in clickhouse_financial_node.parent_keys} == {
        "norway_brreg_financial_statements_duckdb",
    }
    assert "norway_brreg_translation_completion_job" in repository.job_names
    assert "norway_brreg_apply_translations_job" not in repository.job_names
    resources = repository.get_top_level_resources()
    assert "norway_duckdb" not in resources.keys()
    assert resources["norway_brreg_duckdb"].configurable_resource_cls is DuckDBResource
    assert (
        resources["norway_brreg_translation_queue_duckdb"].configurable_resource_cls
        is DuckDBResource
    )


def test_norway_translation_config_exposes_operator_tunable_defaults() -> None:
    config = brreg_assets.NorwayBrregTranslationConfig()

    assert config.batch_size == 50
    assert config.timeout_seconds == 120
    assert config.max_batch_failures == 0
    assert config.worker_id == "translation-temporal-worker"
    assert config.max_tokens == 4096
    assert config.extra_body_json == '{"chat_template_kwargs":{"enable_thinking":false}}'
    assert config.initialize_timeout_seconds == 300
    assert config.batch_timeout_buffer_seconds == 600
    assert config.summarize_timeout_seconds == 30
    assert config.activity_maximum_attempts == 1
    assert config.lease_timeout_seconds == 1800
    assert config.temporal_address == ""
    assert "norway_brreg_translation_start_config" not in brreg_assets.__dict__


def _queue_status_metadata_for_test(**overrides: Any) -> dict[str, Any]:
    metadata = {
        "queue_available": True,
        "queue_duckdb_path": "test-translation-queue.duckdb",
        "queue_total_items": 10,
        "queue_location_items": 14,
        "queue_remaining_items": 7,
        "queue_pending_items": 6,
        "queue_leased_items": 1,
        "queue_completed_items": 3,
        "queue_failed_retryable_items": 0,
        "queue_result_items": 3,
        "queue_batch_attempts": 5,
        "queue_successful_batches": 5,
        "queue_failed_batches": 0,
        "queue_completed_percent": 30.0,
        "queue_error": "",
    }
    metadata.update(overrides)
    return metadata


def test_norway_translation_completion_sensor_skips_running_workflow(monkeypatch) -> None:
    monkeypatch.setattr(
        brreg_sensors,
        "norway_brreg_translation_queue_status_metadata",
        _queue_status_metadata_for_test,
    )
    context = dg.build_sensor_context()
    result = brreg_sensors.build_norway_brreg_translation_completion_sensor_result(
        context,
        temporal_client=FakeTemporalClient(
            FakeTemporalWorkflowDescription(
                run_id="temporal-run-123",
                status=WorkflowExecutionStatus.RUNNING,
            )
        ),
    )

    assert result.run_requests == []
    assert "not complete" in result.skip_reason.skip_message
    assert len(result.asset_events) == 1
    assert (
        result.asset_events[0].asset_key
        == brreg_assets.NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY
    )
    assert result.asset_events[0].metadata["workflow_status"].value == "RUNNING"
    assert result.asset_events[0].metadata["workflow_complete"].value is False
    assert result.asset_events[0].metadata["queue_remaining_items"].value == 7
    assert result.asset_events[0].metadata["queue_batch_attempts"].value == 5


def test_norway_translation_completion_sensor_skips_missing_workflow(monkeypatch) -> None:
    monkeypatch.setattr(
        brreg_sensors,
        "norway_brreg_translation_queue_status_metadata",
        _queue_status_metadata_for_test,
    )
    result = brreg_sensors.build_norway_brreg_translation_completion_sensor_result(
        dg.build_sensor_context(),
        temporal_client=MissingWorkflowTemporalClient(),
    )

    assert result.run_requests == []
    assert "not available" in result.skip_reason.skip_message
    assert len(result.asset_events) == 1
    assert (
        result.asset_events[0].asset_key
        == brreg_assets.NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY
    )
    assert result.asset_events[0].metadata["workflow_status"].value == "unavailable"
    assert result.asset_events[0].metadata["workflow_available"].value is False
    assert result.asset_events[0].metadata["queue_completed_items"].value == 3


def test_norway_translations_applied_skips_when_workflow_is_running(monkeypatch) -> None:
    def fail_if_called(**kwargs: Any) -> dict[str, int]:
        raise AssertionError("translation queue results should not be applied")

    monkeypatch.setattr(
        brreg_assets,
        "describe_norway_brreg_translation_workflow",
        lambda: {
            "workflow_id": "translation-norway-brreg",
            "workflow_run_id": "temporal-run-123",
            "workflow_status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        brreg_assets,
        "apply_norway_brreg_translation_queue_results",
        fail_if_called,
    )

    result = brreg_assets.norway_brreg_translations_applied(
        dg.build_asset_context(),
        DuckDBResource(database=":memory:"),
        DuckDBResource(database=":memory:"),
    )

    assert result.metadata["applied"] is False
    assert result.metadata["workflow_status"] == "RUNNING"
    assert result.metadata["workflow_run_id"] == "temporal-run-123"


def test_norway_translations_applied_reports_unavailable_workflow(monkeypatch) -> None:
    def fail_if_called(**kwargs: Any) -> dict[str, int]:
        raise AssertionError("translation queue results should not be applied")

    def raise_workflow_error() -> dict[str, str]:
        raise RuntimeError("workflow not found")

    monkeypatch.setattr(
        brreg_assets,
        "describe_norway_brreg_translation_workflow",
        raise_workflow_error,
    )
    monkeypatch.setattr(
        brreg_assets,
        "apply_norway_brreg_translation_queue_results",
        fail_if_called,
    )

    result = brreg_assets.norway_brreg_translations_applied(
        dg.build_asset_context(),
        DuckDBResource(database=":memory:"),
        DuckDBResource(database=":memory:"),
    )

    assert result.metadata["applied"] is False
    assert result.metadata["workflow_status"] == "unavailable"
    assert result.metadata["workflow_error"] == "workflow not found"


def test_norway_translations_applied_applies_when_workflow_is_completed(
    monkeypatch, tmp_path
) -> None:
    applied_calls: list[dict[str, Any]] = []

    def apply_results(**kwargs: Any) -> dict[str, int]:
        applied_calls.append(kwargs)
        return {
            "completed_translations": 3,
            "rows_updated": 1,
            "fields_updated": 3,
            "skipped_non_norway": 0,
            "skipped_non_free_text": 0,
            "skipped_missing_source_row": 0,
            "skipped_already_applied": 0,
        }

    monkeypatch.setattr(
        brreg_assets,
        "describe_norway_brreg_translation_workflow",
        lambda: {
            "workflow_id": "translation-norway-brreg",
            "workflow_run_id": "temporal-run-123",
            "workflow_status": "COMPLETED",
        },
    )
    monkeypatch.setattr(
        brreg_assets,
        "apply_norway_brreg_translation_queue_results",
        apply_results,
    )

    queue_path = tmp_path / "translation_queue.duckdb"

    result = brreg_assets.norway_brreg_translations_applied(
        dg.build_asset_context(),
        DuckDBResource(database=":memory:"),
        DuckDBResource(database=str(queue_path)),
    )

    assert len(applied_calls) == 1
    assert applied_calls[0]["queue_duckdb_path"] == queue_path
    assert result.metadata["applied"] is True
    assert result.metadata["workflow_status"] == "COMPLETED"
    assert result.metadata["fields_updated"] == 3


def test_norway_translation_completion_sensor_launches_apply_once(monkeypatch) -> None:
    monkeypatch.setattr(
        brreg_sensors,
        "norway_brreg_translation_queue_status_metadata",
        _queue_status_metadata_for_test,
    )
    context = dg.build_sensor_context()
    result = brreg_sensors.build_norway_brreg_translation_completion_sensor_result(
        context,
        temporal_client=FakeTemporalClient(
            FakeTemporalWorkflowDescription(
                run_id="temporal-run-123",
                status=WorkflowExecutionStatus.COMPLETED,
            )
        ),
    )

    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key == "translation-norway-brreg:temporal-run-123"
    assert result.cursor == "translation-norway-brreg:temporal-run-123"
    assert len(result.asset_events) == 1
    assert result.asset_events[0].metadata["workflow_status"].value == "COMPLETED"
    assert result.asset_events[0].metadata["queue_completed_percent"].value == 30.0

    duplicate = brreg_sensors.build_norway_brreg_translation_completion_sensor_result(
        dg.build_sensor_context(cursor=result.cursor),
        temporal_client=FakeTemporalClient(
            FakeTemporalWorkflowDescription(
                run_id="temporal-run-123",
                status=WorkflowExecutionStatus.COMPLETED,
            )
        ),
    )

    assert duplicate.run_requests == []
    assert "already triggered" in duplicate.skip_reason.skip_message
    assert len(duplicate.asset_events) == 1
    assert duplicate.asset_events[0].metadata["workflow_status"].value == "COMPLETED"
    assert duplicate.asset_events[0].metadata["queue_remaining_items"].value == 7


def test_norway_translation_workflow_status_observe_result(monkeypatch) -> None:
    monkeypatch.setattr(
        brreg_assets,
        "describe_norway_brreg_translation_workflow",
        lambda: {
            "workflow_id": "translation-norway-brreg",
            "workflow_run_id": "temporal-run-123",
            "workflow_status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        brreg_assets,
        "norway_brreg_translation_queue_status_metadata",
        _queue_status_metadata_for_test,
    )

    result = brreg_assets.norway_brreg_translation_workflow_status.observe_fn(
        dg.build_asset_context(),
        DuckDBResource(database=":memory:"),
    )

    assert result.metadata["workflow_status"] == "RUNNING"
    assert result.metadata["workflow_run_id"] == "temporal-run-123"
    assert result.metadata["workflow_complete"] is False
    assert result.metadata["queue_remaining_items"] == 7
    assert result.metadata["queue_completed_percent"] == 30.0


def test_norway_translation_queue_status_metadata_reports_progress(tmp_path: Path) -> None:
    queue_duckdb_path = tmp_path / "translation_queue.duckdb"
    queue = brreg_assets.TranslationQueue(queue_duckdb_path)
    queue.initialize()
    queue.enqueue_items(
        [
            brreg_assets.TranslationQueueItem(
                source_duckdb_path="source.duckdb",
                source_table="source.entities",
                source_pk="1",
                source_field="activity_text_original",
                source_text="Konsulenttjenester",
                target_language="en",
            ),
            brreg_assets.TranslationQueueItem(
                source_duckdb_path="source.duckdb",
                source_table="source.entities",
                source_pk="1-duplicate",
                source_field="company_description_original",
                source_text="Konsulenttjenester",
                target_language="en",
            ),
            brreg_assets.TranslationQueueItem(
                source_duckdb_path="source.duckdb",
                source_table="source.entities",
                source_pk="2",
                source_field="activity_text_original",
                source_text="Utvikling av programvare",
                target_language="en",
            ),
        ]
    )
    claimed = queue.claim_batch(limit=1, worker_id="test-worker")
    queue.complete_batch(
        claimed,
        [
            brreg_assets.SmokeTranslationResult(
                item_id=claimed[0].item_id,
                translated_text="Consulting services",
            )
        ],
        provider="test-provider",
        model="qwen3:6b",
        duration_seconds=0.1,
    )

    metadata = brreg_assets.norway_brreg_translation_queue_status_metadata(
        queue_duckdb_path=queue_duckdb_path,
    )

    assert metadata["queue_available"] is True
    assert metadata["queue_total_items"] == 2
    assert metadata["queue_location_items"] == 3
    assert metadata["queue_remaining_items"] == 1
    assert metadata["queue_pending_items"] == 1
    assert metadata["queue_completed_items"] == 1
    assert metadata["queue_result_items"] == 1
    assert metadata["queue_batch_attempts"] == 1
    assert metadata["queue_successful_batches"] == 1
    assert metadata["queue_completed_percent"] == 50.0
