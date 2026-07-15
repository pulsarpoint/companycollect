import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_v3.defs.denmark_cvr.assets import (
    DENMARK_CVR_BUCKET,
    DENMARK_CVR_SEARCH_PARTITIONS,
    DENMARK_CVR_SEARCH_TERMS,
    denmark_cvr_search_results_s3,
    invalid_page_object_key,
    manifest_object_key,
    page_object_key,
    write_denmark_cvr_search_partition,
)
from dagster_v3.defs.denmark_cvr.models import (
    CompanySearchResult,
    PersonSearchResult,
    ProductionUnitSearchResult,
    SearchResponse,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DenmarkCvrRequestError,
    DenmarkCvrSearchPage,
    DenmarkCvrSearchResource,
    DenmarkCvrValidationError,
    build_search_payload,
    search_results_url,
)


def _company() -> dict[str, Any]:
    return {
        "beliggenhedsadresse": "Testvej 1",
        "by": "Testby",
        "coNavn": None,
        "cvr": "12345678",
        "email": "company@example.test",
        "enhedsnummer": "4000000001",
        "enhedstype": "virksomhed",
        "harPseudoCvr": False,
        "highlightBinavn": False,
        "highlightHistoriskBinavn": False,
        "highlightHistoriskHovednavn": False,
        "hovedbranche": "Test industry",
        "ophoersDato": "",
        "postnummer": "1000",
        "reg": None,
        "reklameBeskyttet": False,
        "senesteNavn": "Example Company ApS",
        "startDato": "2020-01-02",
        "status": "NORMAL",
        "telefonnummer": "+45 00000000",
        "virksomhedsform": "Anpartsselskab",
        "visNavnPostfix": False,
    }


def _person() -> dict[str, Any]:
    return {
        "aktiveTilknytninger": [{"rolle": "DIREKTION"}],
        "beliggenhedsadresse": "Testvej 2",
        "by": None,
        "coNavn": None,
        "enhedsnummer": "4000000002",
        "enhedstype": "person",
        "harAktiveRelationer": True,
        "personType": "PERSON",
        "postnummer": None,
        "senesteNavn": "Example Person",
        "tilknytning": [{"cvr": "12345678"}],
    }


def _production_unit() -> dict[str, Any]:
    return {
        "beliggenhedsadresse": "Testvej 3",
        "by": "Testby",
        "coNavn": None,
        "email": None,
        "enhedstype": "produktionsenhed",
        "hovedbranche": "Test industry",
        "ophoersDato": "",
        "pNummer": "1000000001",
        "postnummer": "1000",
        "reklameBeskyttet": False,
        "senesteNavn": "Example Production Unit",
        "startDato": "2021-02-03",
        "status": "NORMAL",
        "telefonnummer": None,
    }


def _response_body(
    units: list[dict[str, Any]],
    *,
    total: int | None = None,
) -> str:
    company_count = sum(unit["enhedstype"] == "virksomhed" for unit in units)
    person_count = sum(unit["enhedstype"] == "person" for unit in units)
    production_unit_count = sum(
        unit["enhedstype"] == "produktionsenhed" for unit in units
    )
    return json.dumps(
        {
            "enheder": units,
            "pEnhedTotal": production_unit_count,
            "personTotal": person_count,
            "total": len(units) if total is None else total,
            "virksomhedTotal": company_count,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _search_page(page_index: int, raw_body: str) -> DenmarkCvrSearchPage:
    return DenmarkCvrSearchPage(
        page_index=page_index,
        raw_body=raw_body,
        response=SearchResponse.model_validate_json(raw_body),
        status=200,
        response_headers={"content-type": "application/json"},
    )


class FakePage:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = list(results)
        self.goto_calls: list[tuple[str, str]] = []
        self.evaluate_calls: list[dict[str, Any]] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    def evaluate(
        self,
        _script: str,
        argument: dict[str, Any],
    ) -> dict[str, Any]:
        self.evaluate_calls.append(argument)
        return self.results.pop(0)


class FakeBrowser:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.closed = False

    def new_page(self) -> FakePage:
        return self.page

    def close(self) -> None:
        self.closed = True


class FakeObjectStore:
    def __init__(self) -> None:
        self.ensured_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}
        self.write_order: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.ensured_buckets.append(bucket)

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body
        self.write_order.append((bucket, key))

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body.encode("utf-8")
        self.write_order.append((bucket, key))


class FakeSearchResource:
    search_base_url = "https://datacvr.virk.dk"

    def __init__(self, pages: list[DenmarkCvrSearchPage]) -> None:
        self.pages = pages
        self.search_terms: list[str] = []

    def iter_search_pages(self, search_term: str):
        self.search_terms.append(search_term)
        yield from self.pages


class InvalidSearchResource:
    search_base_url = "https://datacvr.virk.dk"

    def iter_search_pages(self, _search_term: str):
        raise DenmarkCvrValidationError(
            page_index=1,
            raw_body='{"private":"invalid"}',
        )
        yield


def test_search_response_models_select_all_entity_types() -> None:
    response = SearchResponse.model_validate_json(
        _response_body([_company(), _person(), _production_unit()])
    )

    assert isinstance(response.enheder[0], CompanySearchResult)
    assert isinstance(response.enheder[1], PersonSearchResult)
    assert isinstance(response.enheder[2], ProductionUnitSearchResult)
    assert response.enheder[0].co_navn is None
    assert response.enheder[0].ophoers_dato is None
    assert response.enheder[2].p_nummer == "1000000001"
    assert response.enheder[2].ophoers_dato is None


def test_company_search_result_accepts_null_optional_fields() -> None:
    company = _company()
    company["by"] = None
    company["hovedbranche"] = None
    company["postnummer"] = None

    response = SearchResponse.model_validate_json(_response_body([company]))

    assert isinstance(response.enheder[0], CompanySearchResult)
    assert response.enheder[0].by is None
    assert response.enheder[0].hovedbranche is None
    assert response.enheder[0].postnummer is None


def test_production_unit_search_result_accepts_null_location_fields() -> None:
    production_unit = _production_unit()
    production_unit["by"] = None
    production_unit["postnummer"] = None

    response = SearchResponse.model_validate_json(_response_body([production_unit]))

    assert isinstance(response.enheder[0], ProductionUnitSearchResult)
    assert response.enheder[0].by is None
    assert response.enheder[0].postnummer is None


def test_search_response_rejects_negative_totals_and_unknown_entity_types() -> None:
    negative_total = json.loads(_response_body([]))
    negative_total["total"] = -1
    with pytest.raises(ValidationError):
        SearchResponse.model_validate(negative_total)

    unknown_type = _company()
    unknown_type["enhedstype"] = "unknown"
    with pytest.raises(ValidationError):
        SearchResponse.model_validate_json(_response_body([unknown_type]))


def test_search_payload_and_results_url_match_datacvr_contract() -> None:
    payload = build_search_payload("æ", page_index=2, size=100)

    assert payload["fritekstCommand"]["soegOrd"] == "æ"
    assert payload["fritekstCommand"]["sideIndex"] == "2"
    assert payload["fritekstCommand"]["size"] == 100
    assert payload["fritekstCommand"]["sortering"] == ""
    assert search_results_url(
        "https://datacvr.virk.dk",
        "æ",
        page_index=2,
        size=100,
    ) == (
        "https://datacvr.virk.dk/soegeresultater?fritekst=%C3%A6&sideIndex=2&size=100"
    )


@pytest.mark.parametrize(
    ("search_term", "page_index", "size"),
    [("", 0, 100), ("a", -1, 100), ("a", 0, 0)],
)
def test_search_payload_rejects_invalid_inputs(
    search_term: str,
    page_index: int,
    size: int,
) -> None:
    with pytest.raises(ValueError):
        build_search_payload(search_term, page_index=page_index, size=size)


def test_search_resource_paginates_and_closes_browser() -> None:
    first_body = _response_body([_company(), _person()], total=3)
    second_body = _response_body([_production_unit()], total=3)
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json", "set-cookie": "secret"},
                "body": first_body,
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": second_body,
            },
        ]
    )
    browser = FakeBrowser(page)
    delays: list[float] = []
    resource = DenmarkCvrSearchResource(
        page_size=2,
        min_delay_ms=0,
        max_delay_ms=0,
    )

    pages = list(
        resource.iter_search_pages(
            "a",
            launcher=lambda: browser,
            sleep=delays.append,
        )
    )

    assert [result.raw_body for result in pages] == [first_body, second_body]
    assert [result.page_index for result in pages] == [0, 1]
    assert pages[0].response_headers == {"content-type": "application/json"}
    assert page.goto_calls == [
        (
            "https://datacvr.virk.dk/soegeresultater?fritekst=a&sideIndex=0&size=2",
            "networkidle",
        )
    ]
    assert [
        call["payload"]["fritekstCommand"]["sideIndex"] for call in page.evaluate_calls
    ] == ["0", "2"]
    assert delays == [0.0]
    assert browser.closed is True


def test_search_resource_uses_fixed_page_size_until_empty() -> None:
    body = _response_body([_company()], total=3_574)
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": body,
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": _response_body([], total=3_574),
            },
        ]
    )
    browser = FakeBrowser(page)
    resource = DenmarkCvrSearchResource(min_delay_ms=0, max_delay_ms=0)

    pages = list(
        resource.iter_search_pages(
            "0",
            launcher=lambda: browser,
        )
    )

    assert len(pages) == 1
    assert len(page.evaluate_calls) == 2
    assert {
        call["payload"]["fritekstCommand"]["size"] for call in page.evaluate_calls
    } == {1_000}
    assert page.goto_calls == [
        (
            "https://datacvr.virk.dk/soegeresultater?fritekst=0&sideIndex=0&size=1000",
            "networkidle",
        )
    ]
    assert browser.closed is True


def test_search_resource_closes_browser_and_hides_failed_response_body() -> None:
    private_body = '{"private":"do not log"}'
    page = FakePage(
        [
            {
                "ok": False,
                "status": 503,
                "headers": {},
                "body": private_body,
            }
        ]
    )
    browser = FakeBrowser(page)

    with pytest.raises(DenmarkCvrRequestError) as exc_info:
        list(
            DenmarkCvrSearchResource(page_size=2).iter_search_pages(
                "a",
                launcher=lambda: browser,
            )
        )

    assert private_body not in str(exc_info.value)
    assert browser.closed is True


def test_search_resource_reports_browser_startup_failure_safely() -> None:
    browser_state = "private browser launch state"

    def fail_to_launch() -> None:
        raise RuntimeError(browser_state)

    with pytest.raises(
        DenmarkCvrRequestError,
        match="failed to start.*runtime dependencies",
    ) as exc_info:
        list(
            DenmarkCvrSearchResource(page_size=2).iter_search_pages(
                "a",
                launcher=fail_to_launch,
            )
        )

    assert browser_state not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_search_resource_retains_invalid_body_without_exposing_it() -> None:
    invalid_body = '{"private":"invalid"}'
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {},
                "body": invalid_body,
            }
        ]
    )

    with pytest.raises(DenmarkCvrValidationError) as exc_info:
        list(
            DenmarkCvrSearchResource(page_size=2).iter_search_pages(
                "a",
                launcher=lambda: FakeBrowser(page),
            )
        )

    assert exc_info.value.raw_body == invalid_body
    assert exc_info.value.page_index == 0
    assert len(exc_info.value.schema_issues) == 5
    assert (("enheder",), "missing") in exc_info.value.schema_issues
    assert "enheder:missing" in str(exc_info.value)
    assert invalid_body not in str(exc_info.value)


def test_search_resource_stops_only_after_empty_page() -> None:
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {},
                "body": _response_body([_company(), _person()], total=3),
            },
            {
                "ok": True,
                "status": 200,
                "headers": {},
                "body": _response_body([], total=3),
            },
        ]
    )

    pages = list(
        DenmarkCvrSearchResource(
            page_size=2,
            min_delay_ms=0,
            max_delay_ms=0,
        ).iter_search_pages(
            "a",
            launcher=lambda: FakeBrowser(page),
        )
    )

    assert len(pages) == 1
    assert pages[0].response.total == 3
    assert len(pages[0].response.enheder) == 2


def test_denmark_cvr_object_keys_are_search_term_scoped() -> None:
    assert page_object_key("æ", "test-run", 0) == (
        "denmark_cvr/search/search_term=æ/run_id=test-run/page=000000.json"
    )
    assert invalid_page_object_key("æ", "test-run", 0) == (
        "denmark_cvr/search/search_term=æ/run_id=test-run/page=000000.invalid.json"
    )
    assert manifest_object_key("æ", "test-run") == (
        "denmark_cvr/search/search_term=æ/run_id=test-run/manifest.json"
    )

    for invalid_term in ("", "/", "aa"):
        with pytest.raises(ValueError):
            manifest_object_key(invalid_term, "test-run")
    with pytest.raises(ValueError):
        manifest_object_key("a", "bad/run")
    with pytest.raises(ValueError):
        page_object_key("a", "test-run", -1)


def test_partition_storage_preserves_raw_pages_and_writes_manifest_last() -> None:
    first_body = _response_body([_company(), _person()], total=3)
    second_body = _response_body([_production_unit()], total=3)
    search = FakeSearchResource(
        [_search_page(0, first_body), _search_page(1, second_body)]
    )
    object_store = FakeObjectStore()
    logs: list[tuple[Any, ...]] = []

    summary = write_denmark_cvr_search_partition(
        object_store=object_store,
        search=search,
        search_term="a",
        run_id="test-run",
        retrieved_at=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        log_info=lambda *args: logs.append(args),
    )

    first_key = page_object_key("a", "test-run", 0)
    second_key = page_object_key("a", "test-run", 1)
    manifest_key = manifest_object_key("a", "test-run")
    manifest = json.loads(object_store.objects[(DENMARK_CVR_BUCKET, manifest_key)])

    assert search.search_terms == ["a"]
    assert object_store.ensured_buckets == [DENMARK_CVR_BUCKET]
    assert object_store.objects[(DENMARK_CVR_BUCKET, first_key)] == first_body.encode()
    assert (
        object_store.objects[(DENMARK_CVR_BUCKET, second_key)] == second_body.encode()
    )
    assert object_store.write_order[-1] == (DENMARK_CVR_BUCKET, manifest_key)
    assert manifest == {
        "advertised_total": 3,
        "bucket": DENMARK_CVR_BUCKET,
        "company_count": 1,
        "entity_count": 3,
        "is_truncated": False,
        "page_count": 2,
        "page_keys": [first_key, second_key],
        "person_count": 1,
        "production_unit_count": 1,
        "retrieved_at": "2026-07-15T12:00:00+00:00",
        "run_id": "test-run",
        "search_term": "a",
        "source": "denmark_cvr",
        "source_url": "https://datacvr.virk.dk",
        "total_size_bytes": len(first_body.encode()) + len(second_body.encode()),
    }
    assert summary.manifest_key == manifest_key
    assert summary.advertised_entity_count == 3
    assert summary.downloaded_entity_count == 3
    assert summary.is_truncated is False
    assert summary.company_count == 1
    assert summary.person_count == 1
    assert summary.production_unit_count == 1
    assert summary.downloaded_file_count == 2
    assert summary.stored_file_count == 3
    assert summary.downloaded_size_bytes == (
        len(first_body.encode()) + len(second_body.encode())
    )
    assert summary.manifest_size_bytes == len(
        object_store.objects[(DENMARK_CVR_BUCKET, manifest_key)]
    )
    assert summary.stored_size_bytes == (
        summary.downloaded_size_bytes + summary.manifest_size_bytes
    )
    assert [entry[0] for entry in logs] == [
        "Starting DataCVR download: search_term=%s bucket=%s prefix=%s",
        (
            "DataCVR download progress: search_term=%s page=%s object_key=%s "
            "downloaded_files=%s advertised_entities=%s downloaded_entities=%s "
            "companies=%s persons=%s production_units=%s downloaded_bytes=%s"
        ),
        (
            "DataCVR download progress: search_term=%s page=%s object_key=%s "
            "downloaded_files=%s advertised_entities=%s downloaded_entities=%s "
            "companies=%s persons=%s production_units=%s downloaded_bytes=%s"
        ),
        (
            "DataCVR download complete: search_term=%s downloaded_files=%s "
            "stored_files=%s advertised_entities=%s downloaded_entities=%s "
            "truncated=%s companies=%s persons=%s production_units=%s downloaded_bytes=%s "
            "stored_bytes=%s manifest_key=%s"
        ),
    ]
    assert logs[-1][1:] == (
        "a",
        2,
        3,
        3,
        3,
        False,
        1,
        1,
        1,
        summary.downloaded_size_bytes,
        summary.stored_size_bytes,
        manifest_key,
    )
    assert all("Example" not in str(entry) for entry in logs)


def test_partition_summary_flags_incomplete_capture() -> None:
    body = _response_body([_company()], total=2)
    search = FakeSearchResource([_search_page(0, body)])
    object_store = FakeObjectStore()

    summary = write_denmark_cvr_search_partition(
        object_store=object_store,
        search=search,
        search_term="a",
        run_id="capped-run",
        retrieved_at=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
    )

    manifest = json.loads(
        object_store.objects[
            (DENMARK_CVR_BUCKET, manifest_object_key("a", "capped-run"))
        ]
    )
    assert summary.advertised_entity_count == 2
    assert summary.downloaded_entity_count == 1
    assert summary.is_truncated is True
    assert manifest["is_truncated"] is True


def test_partition_storage_persists_invalid_body_without_completion_manifest() -> None:
    object_store = FakeObjectStore()
    logs: list[tuple[Any, ...]] = []

    with pytest.raises(DenmarkCvrValidationError):
        write_denmark_cvr_search_partition(
            object_store=object_store,
            search=InvalidSearchResource(),
            search_term="a",
            run_id="test-run",
            retrieved_at=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
            log_info=lambda *args: logs.append(args),
        )

    invalid_key = invalid_page_object_key("a", "test-run", 1)
    assert object_store.objects[(DENMARK_CVR_BUCKET, invalid_key)] == (
        b'{"private":"invalid"}'
    )
    assert (
        DENMARK_CVR_BUCKET,
        manifest_object_key("a", "test-run"),
    ) not in object_store.objects
    assert logs[-1] == (
        "DataCVR download stopped after invalid response: search_term=%s page=%s "
        "invalid_object_key=%s downloaded_files=%s downloaded_entities=%s "
        "downloaded_bytes=%s",
        "a",
        1,
        invalid_key,
        0,
        0,
        0,
    )


def test_asset_reports_download_and_storage_statistics() -> None:
    body = _response_body([_company(), _person(), _production_unit()])
    search = FakeSearchResource([_search_page(0, body)])
    object_store = FakeObjectStore()
    context = SimpleNamespace(
        partition_key="a",
        run=SimpleNamespace(run_id="stats-run"),
        log=SimpleNamespace(info=lambda *_args: None),
    )

    result = denmark_cvr_search_results_s3.node_def.compute_fn.decorated_fn(
        context,
        search,
        object_store,
    )

    run_id = "stats-run"
    manifest_key = manifest_object_key("a", run_id)
    manifest_size = len(object_store.objects[(DENMARK_CVR_BUCKET, manifest_key)])
    assert result.metadata == {
        "s3_bucket": DENMARK_CVR_BUCKET,
        "s3_prefix": f"denmark_cvr/search/search_term=a/run_id={run_id}/",
        "manifest_key": manifest_key,
        "source_url": "https://datacvr.virk.dk",
        "search_term": "a",
        "advertised_entity_count": 3,
        "downloaded_entity_count": 3,
        "is_truncated": False,
        "downloaded_file_count": 1,
        "stored_file_count": 2,
        "company_count": 1,
        "person_count": 1,
        "production_unit_count": 1,
        "downloaded_size_bytes": len(body.encode()),
        "manifest_size_bytes": manifest_size,
        "stored_size_bytes": len(body.encode()) + manifest_size,
    }


def test_denmark_cvr_asset_uses_static_search_term_partitions() -> None:
    partition_keys = DENMARK_CVR_SEARCH_PARTITIONS.get_partition_keys()

    assert isinstance(DENMARK_CVR_SEARCH_PARTITIONS, dg.StaticPartitionsDefinition)
    assert partition_keys == list(DENMARK_CVR_SEARCH_TERMS)
    assert partition_keys == list("0123456789abcdefghijklmnopqrstuvwxyzæøå")
    assert denmark_cvr_search_results_s3.partitions_def is DENMARK_CVR_SEARCH_PARTITIONS
    assert denmark_cvr_search_results_s3.backfill_policy.max_partitions_per_run == 1
    assert denmark_cvr_search_results_s3.op.pool == "denmark_cvr_search"

    spec = denmark_cvr_search_results_s3.get_asset_spec()
    assert spec.group_name == "denmark_cvr"
    assert spec.tags["country"] == "denmark"
    assert spec.tags["source"] == "cvr"
    assert spec.tags["source_name"] == "denmark_cvr"
    assert spec.tags["layer"] == "raw"
    for kind in ("python", "browser", "json", "s3"):
        assert spec.tags[f"dagster/kind/{kind}"] == ""


def test_denmark_cvr_definitions_register_one_asset_and_no_schedule() -> None:
    from dagster_v3.defs.denmark_cvr.assets import defs
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()

    assert dg.AssetKey("denmark_cvr_search_results_s3") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert len(defs.assets) == 1
    assert defs.schedules is None
    assert set(defs.resources) == {"denmark_cvr_search"}
