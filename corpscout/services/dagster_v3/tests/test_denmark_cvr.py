import json
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_v3.defs.denmark_cvr.assets import (
    DENMARK_CVR_BUCKET,
    active_invalid_response_object_key,
    active_result_object_key,
    backfill_invalid_response_object_key,
    backfill_result_object_key,
    denmark_cvr_active_s3,
    denmark_cvr_backfill_s3,
    write_denmark_cvr_active_date,
    write_denmark_cvr_backfill_month,
)
from dagster_v3.defs.denmark_cvr.filters import (
    DATACVR_MUNICIPALITIES,
    DenmarkCvrQueryFilter,
)
from dagster_v3.defs.denmark_cvr.models import (
    CompanySearchResult,
    PersonSearchResult,
    ProductionUnitSearchResult,
    SearchResponse,
)
from dagster_v3.defs.denmark_cvr.partitions import (
    DENMARK_CVR_ACTIVE_PARTITIONS,
    DENMARK_CVR_BACKFILL_PARTITIONS,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_COMPANY_ENTITY_TYPE,
    DATACVR_PERSON_ENTITY_TYPE,
    DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
    DenmarkCvrDateRangeDownload,
    DenmarkCvrEntityType,
    DenmarkCvrQueryDownload,
    DenmarkCvrRequestError,
    DenmarkCvrSearchResource,
    DenmarkCvrValidationError,
    build_search_payload,
    search_results_url,
)


def _company(
    *,
    cvr: str = "12345678",
    company_form: str | None = "Anpartsselskab",
) -> dict[str, Any]:
    return {
        "beliggenhedsadresse": "Testvej 1",
        "by": "Testby",
        "coNavn": None,
        "cvr": cvr,
        "email": "company@example.test",
        "enhedsnummer": f"4{cvr}",
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
        "virksomhedsform": company_form,
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
    company_total: int | None = None,
) -> str:
    actual_company_count = sum(unit["enhedstype"] == "virksomhed" for unit in units)
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
            "virksomhedTotal": (
                actual_company_count if company_total is None else company_total
            ),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _query_filter(
    *,
    region: str = "",
    municipality: str = "",
) -> DenmarkCvrQueryFilter:
    return DenmarkCvrQueryFilter(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        region=region,
        municipality=municipality,
    )


def _browser_result(body: str, *, ok: bool = True, status: int = 200) -> dict[str, Any]:
    return {
        "ok": ok,
        "status": status,
        "headers": {"content-type": "application/json", "set-cookie": "secret"},
        "body": body,
    }


class FakePage:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = list(results)
        self.goto_calls: list[tuple[str, str]] = []
        self.evaluate_calls: list[dict[str, Any]] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    def evaluate(self, _script: str, argument: dict[str, Any]) -> dict[str, Any]:
        self.evaluate_calls.append(argument)
        return self.results.pop(0)


class LargeDateRangePage:
    def __init__(self) -> None:
        self.goto_calls: list[tuple[str, str]] = []
        self.evaluate_calls: list[dict[str, Any]] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    def evaluate(self, _script: str, argument: dict[str, Any]) -> dict[str, Any]:
        self.evaluate_calls.append(argument)
        command = argument["payload"]["fritekstCommand"]
        if command["kommune"] == []:
            return _browser_result(
                _response_body([_company()], total=3_001, company_total=3_001)
            )
        if command["kommune"] == ["101"]:
            return _browser_result(_response_body([_company()]))
        return _browser_result(_response_body([]))


class FakeBrowser:
    def __init__(self, page: FakePage | LargeDateRangePage) -> None:
        self.page = page
        self.closed = False

    def new_page(self) -> FakePage | LargeDateRangePage:
        return self.page

    def close(self) -> None:
        self.closed = True


class FakeObjectStore:
    def __init__(self) -> None:
        self.ensured_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}
        self.write_order: list[tuple[str, str]] = []
        self.exists_calls: list[tuple[str, str]] = []

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

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        self.exists_calls.append((bucket, key))
        return (bucket, key) in self.objects


class FakeSearchResource:
    search_base_url = "https://datacvr.virk.dk"

    def __init__(self, download: DenmarkCvrDateRangeDownload) -> None:
        self.download = download
        self.date_ranges: list[tuple[date, date]] = []
        self.entity_types: list[DenmarkCvrEntityType] = []

    def download_date_range(
        self,
        *,
        start_date: date,
        end_date: date,
        entity_type: DenmarkCvrEntityType = DATACVR_COMPANY_ENTITY_TYPE,
        log_info: Any = None,
    ) -> DenmarkCvrDateRangeDownload:
        self.date_ranges.append((start_date, end_date))
        self.entity_types.append(entity_type)
        return self.download


class InvalidSearchResource:
    search_base_url = "https://datacvr.virk.dk"

    def download_date_range(
        self,
        *,
        start_date: date,
        end_date: date,
        entity_type: DenmarkCvrEntityType = DATACVR_COMPANY_ENTITY_TYPE,
        log_info: Any = None,
    ) -> DenmarkCvrDateRangeDownload:
        raise DenmarkCvrValidationError(
            filter_id="all-companies",
            page_index=1,
            raw_body='{"private":"invalid"}',
        )


def _query_download(
    *,
    entities: tuple[dict[str, Any], ...],
    advertised_count: int,
    region: str = "",
    municipality: str = "",
) -> DenmarkCvrQueryDownload:
    return DenmarkCvrQueryDownload(
        query_filter=_query_filter(region=region, municipality=municipality),
        advertised_count=advertised_count,
        entities=entities,
        page_count=1,
        downloaded_size_bytes=123,
    )


def _date_range_download(
    *,
    generic_advertised_count: int,
    query_downloads: tuple[DenmarkCvrQueryDownload, ...],
) -> DenmarkCvrDateRangeDownload:
    return DenmarkCvrDateRangeDownload(
        generic_advertised_count=generic_advertised_count,
        query_downloads=query_downloads,
    )


def test_search_response_models_select_all_entity_types() -> None:
    response = SearchResponse.model_validate_json(
        _response_body([_company(), _person(), _production_unit()])
    )

    assert isinstance(response.enheder[0], CompanySearchResult)
    assert isinstance(response.enheder[1], PersonSearchResult)
    assert isinstance(response.enheder[2], ProductionUnitSearchResult)
    assert response.enheder[0].ophoers_dato is None
    assert response.enheder[2].ophoers_dato is None


def test_company_and_production_unit_accept_null_location_fields() -> None:
    company = _company()
    company.update({"by": None, "hovedbranche": None, "postnummer": None})
    production_unit = _production_unit()
    production_unit.update({"by": None, "postnummer": None})

    response = SearchResponse.model_validate_json(
        _response_body([company, production_unit])
    )

    assert response.enheder[0].by is None
    assert response.enheder[0].hovedbranche is None
    assert response.enheder[1].postnummer is None


def test_company_accepts_null_company_form_from_datacvr() -> None:
    response = SearchResponse.model_validate_json(
        _response_body([_company(company_form=None)])
    )

    assert response.enheder[0].virksomhedsform is None


def test_company_accepts_null_latest_name_from_datacvr() -> None:
    company = _company()
    company["senesteNavn"] = None

    response = SearchResponse.model_validate_json(_response_body([company]))

    assert response.enheder[0].seneste_navn is None


def test_search_response_rejects_negative_totals_and_unknown_entity_types() -> None:
    negative_total = json.loads(_response_body([]))
    negative_total["total"] = -1
    with pytest.raises(ValidationError):
        SearchResponse.model_validate(negative_total)

    unknown_type = _company()
    unknown_type["enhedstype"] = "unknown"
    with pytest.raises(ValidationError):
        SearchResponse.model_validate_json(_response_body([unknown_type]))


def test_search_payload_contains_month_region_and_municipality_filters() -> None:
    query_filter = _query_filter(region="29190623", municipality="101")

    command = build_search_payload(query_filter, page_index=2, size=1_000)[
        "fritekstCommand"
    ]

    assert command == {
        "soegOrd": "",
        "sideIndex": "2",
        "enhedstype": "virksomhed",
        "kommune": ["101"],
        "region": ["29190623"],
        "antalAnsatte": [],
        "virksomhedsform": [],
        "virksomhedsstatus": [],
        "virksomhedsmarkering": [],
        "personrolle": [],
        "startdatoFra": "2025-01-01",
        "startdatoTil": "2025-01-31",
        "ophoersdatoFra": "",
        "ophoersdatoTil": "",
        "branchekode": "",
        "size": 1_000,
        "sortering": "",
    }
    assert search_results_url(
        "https://datacvr.virk.dk",
        query_filter,
        page_index=2,
        size=1_000,
    ) == (
        "https://datacvr.virk.dk/soegeresultater?sideIndex=2"
        "&enhedstype=virksomhed&startdatoFra=2025-01-01&startdatoTil=2025-01-31"
        "&region=29190623&kommune=101&size=1000"
    )


@pytest.mark.parametrize(
    ("entity_type", "expected_query_value"),
    [
        (DATACVR_COMPANY_ENTITY_TYPE, "virksomhed"),
        (DATACVR_PRODUCTION_UNIT_ENTITY_TYPE, "produktionsenhed"),
        (DATACVR_PERSON_ENTITY_TYPE, "person"),
    ],
)
def test_search_request_selects_one_entity_type(
    entity_type: DenmarkCvrEntityType,
    expected_query_value: str,
) -> None:
    query_filter = _query_filter()

    command = build_search_payload(
        query_filter,
        page_index=0,
        size=1,
        entity_type=entity_type,
    )["fritekstCommand"]
    url = search_results_url(
        "https://datacvr.virk.dk",
        query_filter,
        page_index=0,
        size=1,
        entity_type=entity_type,
    )

    assert command["enhedstype"] == expected_query_value
    assert f"enhedstype={expected_query_value}" in url


@pytest.mark.parametrize(
    ("entity_type", "entity"),
    [
        (DATACVR_PRODUCTION_UNIT_ENTITY_TYPE, _production_unit()),
        (DATACVR_PERSON_ENTITY_TYPE, _person()),
    ],
)
def test_search_resource_downloads_non_company_entity_type(
    entity_type: DenmarkCvrEntityType,
    entity: dict[str, Any],
) -> None:
    page = FakePage(
        [
            _browser_result(_response_body([entity])),
            _browser_result(_response_body([entity])),
        ]
    )

    download = DenmarkCvrSearchResource(
        min_delay_ms=0,
        max_delay_ms=0,
    ).download_date_range(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        entity_type=entity_type,
        launcher=lambda: FakeBrowser(page),
        sleep=lambda _seconds: None,
    )

    assert download.generic_advertised_count == 1
    assert download.entities == (entity,)
    assert download.is_complete is True
    assert {
        call["payload"]["fritekstCommand"]["enhedstype"] for call in page.evaluate_calls
    } == {entity_type}


def test_search_resource_counts_then_downloads_all_pages_with_one_browser() -> None:
    count_body = _response_body([_company()], total=3, company_total=3)
    first_body = _response_body(
        [_company(cvr="12345678"), _company(cvr="12345679")],
        total=3,
        company_total=3,
    )
    second_body = _response_body([_company(cvr="12345680")], total=3, company_total=3)
    page = FakePage(
        [
            _browser_result(count_body),
            _browser_result(first_body),
            _browser_result(second_body),
        ]
    )
    browser = FakeBrowser(page)
    delays: list[float] = []
    logs: list[tuple[Any, ...]] = []

    download = DenmarkCvrSearchResource(
        page_size=2,
        min_delay_ms=0,
        max_delay_ms=0,
    ).download_date_range(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        log_info=lambda *args: logs.append(args),
        launcher=lambda: browser,
        sleep=delays.append,
    )

    assert download.generic_advertised_count == 3
    assert download.downloaded_entity_count == 3
    assert download.is_complete is True
    assert [entity["cvr"] for entity in download.entities] == [
        "12345678",
        "12345679",
        "12345680",
    ]
    assert [
        call["payload"]["fritekstCommand"]["size"] for call in page.evaluate_calls
    ] == [1, 2, 2]
    assert [
        call["payload"]["fritekstCommand"]["sideIndex"] for call in page.evaluate_calls
    ] == ["0", "0", "1"]
    assert page.goto_calls == [
        (
            "https://datacvr.virk.dk/soegeresultater?sideIndex=0"
            "&enhedstype=virksomhed&startdatoFra=2025-01-01"
            "&startdatoTil=2025-01-31&size=1",
            "networkidle",
        )
    ]
    assert browser.closed is True
    assert delays == [0.0]
    assert [entry[0] for entry in logs] == [
        "DataCVR %s filters selected: start_date=%s "
        "end_date=%s generic_advertised=%s query_count=%s",
        "DataCVR %s progress: filter=%s query=%s/%s "
        "advertised=%s downloaded=%s pages=%s "
        "total_downloaded=%s total_pages=%s downloaded_bytes=%s",
    ]
    assert [entry[1] for entry in logs] == ["company", "company"]


def test_search_resource_uses_fixed_filters_for_large_date_range() -> None:
    page = LargeDateRangePage()
    browser = FakeBrowser(page)

    download = DenmarkCvrSearchResource(
        min_delay_ms=0,
        max_delay_ms=0,
    ).download_date_range(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        launcher=lambda: browser,
        sleep=lambda _seconds: None,
    )

    assert download.generic_advertised_count == 3_001
    assert len(download.query_downloads) == len(DATACVR_MUNICIPALITIES) == 105
    assert download.filtered_advertised_count == 1
    assert download.downloaded_entity_count == 1
    assert download.is_complete is False
    assert len(page.evaluate_calls) == 106
    assert browser.closed is True


def test_search_resource_rejects_results_outside_requested_entity_type() -> None:
    page = FakePage(
        [
            _browser_result(_response_body([_company()])),
            _browser_result(_response_body([_person()])),
        ]
    )

    with pytest.raises(
        DenmarkCvrRequestError, match="outside the requested entity type"
    ):
        DenmarkCvrSearchResource().download_date_range(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            launcher=lambda: FakeBrowser(page),
        )


def test_search_resource_closes_browser_and_hides_failed_response_body() -> None:
    private_body = '{"private":"do not log"}'
    page = FakePage([_browser_result(private_body, ok=False, status=503)])
    browser = FakeBrowser(page)

    with pytest.raises(DenmarkCvrRequestError) as exc_info:
        DenmarkCvrSearchResource().download_date_range(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            launcher=lambda: browser,
        )

    assert private_body not in str(exc_info.value)
    assert browser.closed is True


def test_search_resource_retains_invalid_body_without_exposing_it() -> None:
    invalid_body = '{"private":"invalid"}'
    page = FakePage([_browser_result(invalid_body)])

    with pytest.raises(DenmarkCvrValidationError) as exc_info:
        DenmarkCvrSearchResource().download_date_range(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            launcher=lambda: FakeBrowser(page),
        )

    assert exc_info.value.raw_body == invalid_body
    assert exc_info.value.filter_id == "all-companies"
    assert exc_info.value.page_index == 0
    assert invalid_body not in str(exc_info.value)


def test_backfill_object_keys_are_month_scoped_and_run_independent() -> None:
    assert backfill_result_object_key("2025-01", is_complete=True) == (
        "denmark_cvr/backfill/month=2025-01/companies.json"
    )
    assert backfill_result_object_key("2025-01", is_complete=False) == (
        "denmark_cvr/backfill/month=2025-01/companies_incomplete.json"
    )
    assert backfill_invalid_response_object_key(
        "2025-01",
        "all-companies",
        1,
    ).endswith("/invalid/filter=all-companies/page=000001.invalid.json")

    with pytest.raises(ValueError):
        backfill_result_object_key("2014-12", is_complete=True)


def test_active_object_keys_are_date_scoped_and_run_independent() -> None:
    assert active_result_object_key("2026-07-01", is_complete=True) == (
        "denmark_cvr/active/date=2026-07-01/companies.json"
    )
    assert active_result_object_key("2026-07-01", is_complete=False) == (
        "denmark_cvr/active/date=2026-07-01/companies_incomplete.json"
    )
    assert active_invalid_response_object_key(
        "2026-07-01",
        "all-companies",
        1,
    ).endswith("/invalid/filter=all-companies/page=000001.invalid.json")

    with pytest.raises(ValueError):
        active_result_object_key("2026-06-30", is_complete=True)


@pytest.mark.parametrize(
    ("entity_type", "filename"),
    [
        (DATACVR_COMPANY_ENTITY_TYPE, "companies.json"),
        (DATACVR_PRODUCTION_UNIT_ENTITY_TYPE, "production_units.json"),
        (DATACVR_PERSON_ENTITY_TYPE, "persons.json"),
    ],
)
def test_each_entity_type_has_an_independent_result_object(
    entity_type: DenmarkCvrEntityType,
    filename: str,
) -> None:
    assert (
        backfill_result_object_key(
            "2025-01",
            is_complete=True,
            entity_type=entity_type,
        )
        == f"denmark_cvr/backfill/month=2025-01/{filename}"
    )
    assert (
        active_result_object_key(
            "2026-07-01",
            is_complete=True,
            entity_type=entity_type,
        )
        == f"denmark_cvr/active/date=2026-07-01/{filename}"
    )


def test_non_company_invalid_responses_have_independent_object_scopes() -> None:
    assert backfill_invalid_response_object_key(
        "2025-01",
        "all-production-units",
        1,
        entity_type=DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
    ).endswith(
        "/invalid/entity=produktionsenhed/filter=all-production-units/"
        "page=000001.invalid.json"
    )
    assert active_invalid_response_object_key(
        "2026-07-01",
        "all-persons",
        1,
        entity_type=DATACVR_PERSON_ENTITY_TYPE,
    ).endswith("/invalid/entity=person/filter=all-persons/page=000001.invalid.json")


@pytest.mark.parametrize(
    ("entity_type", "entity", "filename"),
    [
        (
            DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
            _production_unit(),
            "production_units.json",
        ),
        (DATACVR_PERSON_ENTITY_TYPE, _person(), "persons.json"),
    ],
)
def test_non_company_backfill_storage_uses_its_own_object(
    entity_type: DenmarkCvrEntityType,
    entity: dict[str, Any],
    filename: str,
) -> None:
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=1,
            query_downloads=(_query_download(entities=(entity,), advertised_count=1),),
        )
    )
    object_store = FakeObjectStore()

    summary = write_denmark_cvr_backfill_month(
        object_store=object_store,
        search=search,
        entity_type=entity_type,
        partition_key="2025-01",
        run_id="entity-run",
        retrieved_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )

    key = f"denmark_cvr/backfill/month=2025-01/{filename}"
    stored = json.loads(object_store.objects[(DENMARK_CVR_BUCKET, key)])
    assert summary.result_key == key
    assert stored["entity_type"] == entity_type
    assert stored["enheder"] == [entity]
    assert search.entity_types == [entity_type]


def test_month_storage_merges_raw_entities_into_one_complete_json() -> None:
    first_company = _company(cvr="12345678")
    second_company = _company(cvr="12345679")
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=2,
            query_downloads=(
                _query_download(
                    entities=(first_company, second_company),
                    advertised_count=2,
                ),
            ),
        )
    )
    object_store = FakeObjectStore()

    summary = write_denmark_cvr_backfill_month(
        object_store=object_store,
        search=search,
        partition_key="2025-01",
        run_id="complete-run",
        retrieved_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )

    key = backfill_result_object_key("2025-01", is_complete=True)
    stored = json.loads(object_store.objects[(DENMARK_CVR_BUCKET, key)])
    assert object_store.write_order == [(DENMARK_CVR_BUCKET, key)]
    assert search.date_ranges == [(date(2025, 1, 1), date(2025, 1, 31))]
    assert stored["is_complete"] is True
    assert stored["generic_advertised_count"] == 2
    assert stored["filtered_advertised_count"] == 2
    assert stored["downloaded_entity_count"] == 2
    assert stored["enheder"] == [first_company, second_company]
    assert stored["enheder"][0]["ophoersDato"] == ""
    assert summary.result_key == key
    assert summary.stored_file_count == 1
    assert summary.is_skipped is False


def test_active_date_storage_downloads_exact_day_into_one_complete_json() -> None:
    company = _company(cvr="87654321")
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=1,
            query_downloads=(_query_download(entities=(company,), advertised_count=1),),
        )
    )
    object_store = FakeObjectStore()

    summary = write_denmark_cvr_active_date(
        object_store=object_store,
        search=search,
        partition_key="2026-07-01",
        run_id="daily-run",
        retrieved_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
    )

    key = active_result_object_key("2026-07-01", is_complete=True)
    stored = json.loads(object_store.objects[(DENMARK_CVR_BUCKET, key)])
    assert search.date_ranges == [(date(2026, 7, 1), date(2026, 7, 1))]
    assert stored["partition_key"] == "2026-07-01"
    assert stored["start_date"] == "2026-07-01"
    assert stored["end_date"] == "2026-07-01"
    assert stored["enheder"] == [company]
    assert summary.result_key == key
    assert summary.is_complete is True


@pytest.mark.parametrize("is_complete", [True, False])
def test_active_date_skips_when_result_json_already_exists(
    is_complete: bool,
) -> None:
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=1,
            query_downloads=(
                _query_download(entities=(_company(),), advertised_count=1),
            ),
        )
    )
    object_store = FakeObjectStore()
    existing_key = active_result_object_key(
        "2026-07-01",
        is_complete=is_complete,
    )
    object_store.objects[(DENMARK_CVR_BUCKET, existing_key)] = b"already-loaded"

    summary = write_denmark_cvr_active_date(
        object_store=object_store,
        search=search,
        partition_key="2026-07-01",
        run_id="daily-retry",
        retrieved_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
    )

    assert summary.result_key == existing_key
    assert summary.is_complete is is_complete
    assert summary.is_skipped is True
    assert search.date_ranges == []
    assert object_store.write_order == []


@pytest.mark.parametrize("is_complete", [True, False])
def test_backfill_skips_partition_when_result_json_already_exists(
    is_complete: bool,
) -> None:
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=1,
            query_downloads=(
                _query_download(entities=(_company(),), advertised_count=1),
            ),
        )
    )
    object_store = FakeObjectStore()
    existing_key = backfill_result_object_key(
        "2025-01",
        is_complete=is_complete,
    )
    object_store.objects[(DENMARK_CVR_BUCKET, existing_key)] = b"already-loaded"

    summary = write_denmark_cvr_backfill_month(
        object_store=object_store,
        search=search,
        partition_key="2025-01",
        run_id="retry-run",
        retrieved_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )

    assert summary.result_key == existing_key
    assert summary.is_complete is is_complete
    assert summary.is_skipped is True
    assert summary.stored_file_count == 0
    assert search.date_ranges == []
    assert object_store.write_order == []


def test_incomplete_month_is_stored_logged_and_processed() -> None:
    warnings: list[tuple[Any, ...]] = []
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=3,
            query_downloads=(
                _query_download(entities=(_company(),), advertised_count=1),
            ),
        )
    )
    object_store = FakeObjectStore()

    summary = write_denmark_cvr_backfill_month(
        object_store=object_store,
        search=search,
        partition_key="2025-01",
        run_id="incomplete-run",
        retrieved_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        log_warning=lambda *args: warnings.append(args),
    )

    key = backfill_result_object_key("2025-01", is_complete=False)
    stored = json.loads(object_store.objects[(DENMARK_CVR_BUCKET, key)])
    assert summary.is_complete is False
    assert stored["is_complete"] is False
    assert stored["generic_advertised_count"] == 3
    assert stored["filtered_advertised_count"] == 1
    assert stored["downloaded_entity_count"] == 1
    assert stored["missing_entity_count"] == 2
    assert len(warnings) == 1
    assert "incomplete" in warnings[0][0].lower()


def test_invalid_response_is_preserved_without_result_json() -> None:
    object_store = FakeObjectStore()

    with pytest.raises(DenmarkCvrValidationError):
        write_denmark_cvr_backfill_month(
            object_store=object_store,
            search=InvalidSearchResource(),
            partition_key="2025-01",
            run_id="invalid-run",
            retrieved_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        )

    invalid_key = backfill_invalid_response_object_key(
        "2025-01",
        "all-companies",
        1,
    )
    assert object_store.objects[(DENMARK_CVR_BUCKET, invalid_key)] == (
        b'{"private":"invalid"}'
    )
    assert len(object_store.objects) == 1


def test_asset_reports_monthly_download_statistics() -> None:
    search = FakeSearchResource(
        _date_range_download(
            generic_advertised_count=1,
            query_downloads=(
                _query_download(entities=(_company(),), advertised_count=1),
            ),
        )
    )
    object_store = FakeObjectStore()
    context = SimpleNamespace(
        partition_key="2025-01",
        run=SimpleNamespace(run_id="stats-run"),
        log=SimpleNamespace(
            info=lambda *_args: None,
            warning=lambda *_args: None,
        ),
    )

    result = denmark_cvr_backfill_s3.node_def.compute_fn.decorated_fn(
        context,
        search,
        object_store,
    )

    assert result.metadata["partition_key"] == "2025-01"
    assert result.metadata["is_complete"] is True
    assert result.metadata["generic_advertised_count"] == 1
    assert result.metadata["filtered_advertised_count"] == 1
    assert result.metadata["downloaded_entity_count"] == 1
    assert result.metadata["stored_file_count"] == 1
    assert result.metadata["is_skipped"] is False


def test_denmark_cvr_backfill_asset_uses_bounded_monthly_partitions() -> None:
    assert denmark_cvr_backfill_s3.partitions_def is DENMARK_CVR_BACKFILL_PARTITIONS
    assert denmark_cvr_backfill_s3.backfill_policy.max_partitions_per_run == 1
    assert denmark_cvr_backfill_s3.op.pool == "denmark_cvr_search"

    spec = denmark_cvr_backfill_s3.get_asset_spec()
    assert spec.group_name == "denmark_cvr"
    assert spec.tags["country"] == "denmark"
    assert spec.tags["source"] == "cvr"
    assert spec.tags["layer"] == "raw"


def test_denmark_cvr_active_asset_uses_daily_partitions() -> None:
    assert denmark_cvr_active_s3.partitions_def is DENMARK_CVR_ACTIVE_PARTITIONS
    assert denmark_cvr_active_s3.backfill_policy.max_partitions_per_run == 1
    assert denmark_cvr_active_s3.op.pool == "denmark_cvr_search"

    spec = denmark_cvr_active_s3.get_asset_spec()
    assert spec.group_name == "denmark_cvr"
    assert spec.tags["country"] == "denmark"
    assert spec.tags["source"] == "cvr"
    assert spec.tags["layer"] == "raw"


def test_denmark_cvr_definitions_register_supported_search_assets() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.denmark_cvr.assets import defs

    repository = load_defs().get_repository_def()

    assert dg.AssetKey("denmark_cvr_backfill_s3") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_active_s3") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_production_units_backfill_s3") not in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_production_units_active_s3") not in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_persons_backfill_s3") not in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_persons_active_s3") not in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_company_detail_person_ids_duckdb") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_person_details_s3") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_search_results_s3") not in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert len(defs.assets) == 2
    assert defs.schedules is None
    assert set(defs.resources) == {"denmark_cvr_search"}
