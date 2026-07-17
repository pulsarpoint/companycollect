import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from math import ceil
from random import randint
from typing import Any, Literal, Self
from urllib.parse import urlencode

import dagster as dg
from cloakbrowser import launch
from pydantic import ValidationError, model_validator

from dagster_v3.defs.denmark_cvr.filters import (
    DATACVR_RESULT_LIMIT,
    DenmarkCvrQueryFilter,
    filters_for_date_range,
)
from dagster_v3.defs.denmark_cvr.models import (
    CompanySearchResult,
    PersonSearchResult,
    ProductionUnitSearchResult,
    SearchResponse,
)

DATACVR_BASE_URL = "https://datacvr.virk.dk"
DATACVR_COMPANY_ENTITY_TYPE = "virksomhed"
DATACVR_PRODUCTION_UNIT_ENTITY_TYPE = "produktionsenhed"
DATACVR_PERSON_ENTITY_TYPE = "person"
DATACVR_PAGE_SIZE = 1_000

type DenmarkCvrEntityType = Literal["virksomhed", "produktionsenhed", "person"]

_ENTITY_LABELS: dict[DenmarkCvrEntityType, str] = {
    DATACVR_COMPANY_ENTITY_TYPE: "company",
    DATACVR_PRODUCTION_UNIT_ENTITY_TYPE: "production unit",
    DATACVR_PERSON_ENTITY_TYPE: "person",
}
_ENTITY_MODELS = {
    DATACVR_COMPANY_ENTITY_TYPE: CompanySearchResult,
    DATACVR_PRODUCTION_UNIT_ENTITY_TYPE: ProductionUnitSearchResult,
    DATACVR_PERSON_ENTITY_TYPE: PersonSearchResult,
}
SAFE_RESPONSE_HEADERS = frozenset(
    {
        "content-type",
        "date",
        "etag",
        "last-modified",
        "retry-after",
        "x-request-id",
    }
)
DATACVR_SEARCH_SCRIPT = """
async ({ payload }) => {
  const response = await fetch(
    "/gateway/soeg/fritekst",
    {
      method: "POST",
      headers: {
        Accept: "application/json, text/plain, */*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        Pragma: "no-cache",
        "X-Requested-With": "XMLHttpRequest",
      },
      credentials: "include",
      body: JSON.stringify(payload),
    },
  );

  return {
    ok: response.ok,
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    body: await response.text(),
  };
}
"""


@dataclass(frozen=True)
class DenmarkCvrSearchPage:
    page_index: int
    raw_body: str
    response: SearchResponse
    status: int
    response_headers: dict[str, str]


@dataclass(frozen=True)
class DenmarkCvrQueryDownload:
    query_filter: DenmarkCvrQueryFilter
    advertised_count: int
    entities: tuple[dict[str, Any], ...]
    page_count: int
    downloaded_size_bytes: int
    downloaded_entity_count: int = field(init=False)
    is_complete: bool = field(init=False)

    def __post_init__(self) -> None:
        if self.advertised_count < 0:
            raise ValueError("DataCVR advertised count must not be negative")
        if self.page_count <= 0:
            raise ValueError("DataCVR query download must contain at least one page")
        if self.downloaded_size_bytes < 0:
            raise ValueError("DataCVR downloaded size must not be negative")
        downloaded_entity_count = len(self.entities)
        object.__setattr__(self, "downloaded_entity_count", downloaded_entity_count)
        object.__setattr__(
            self,
            "is_complete",
            self.advertised_count <= DATACVR_RESULT_LIMIT
            and downloaded_entity_count == self.advertised_count,
        )


@dataclass(frozen=True)
class DenmarkCvrDateRangeDownload:
    generic_advertised_count: int
    query_downloads: tuple[DenmarkCvrQueryDownload, ...]
    filtered_advertised_count: int = field(init=False)
    downloaded_entity_count: int = field(init=False)
    page_count: int = field(init=False)
    downloaded_size_bytes: int = field(init=False)
    entities: tuple[dict[str, Any], ...] = field(init=False)
    is_complete: bool = field(init=False)

    def __post_init__(self) -> None:
        if self.generic_advertised_count < 0:
            raise ValueError("DataCVR generic advertised count must not be negative")
        if not self.query_downloads:
            raise ValueError(
                "DataCVR date range download must contain at least one query"
            )
        filtered_advertised_count = sum(
            query.advertised_count for query in self.query_downloads
        )
        entities = tuple(
            entity for query in self.query_downloads for entity in query.entities
        )
        downloaded_entity_count = len(entities)
        object.__setattr__(
            self,
            "filtered_advertised_count",
            filtered_advertised_count,
        )
        object.__setattr__(self, "downloaded_entity_count", downloaded_entity_count)
        object.__setattr__(
            self,
            "page_count",
            sum(query.page_count for query in self.query_downloads),
        )
        object.__setattr__(
            self,
            "downloaded_size_bytes",
            sum(query.downloaded_size_bytes for query in self.query_downloads),
        )
        object.__setattr__(self, "entities", entities)
        object.__setattr__(
            self,
            "is_complete",
            self.generic_advertised_count
            == filtered_advertised_count
            == downloaded_entity_count
            and all(query.is_complete for query in self.query_downloads),
        )


class DenmarkCvrRequestError(RuntimeError):
    pass


class DenmarkCvrValidationError(ValueError):
    def __init__(
        self,
        *,
        filter_id: str,
        page_index: int,
        raw_body: str,
        schema_issues: tuple[tuple[tuple[str | int, ...], str], ...] = (),
    ) -> None:
        issue_summary = ", ".join(
            f"{'.'.join(str(part) for part in location)}:{error_type}"
            for location, error_type in schema_issues[:5]
        )
        message = (
            f"DataCVR returned an invalid response for filter {filter_id} "
            f"on page {page_index}"
        )
        if issue_summary:
            message += f"; schema_errors={len(schema_issues)} issues={issue_summary}"
        super().__init__(message)
        self.filter_id = filter_id
        self.page_index = page_index
        self.raw_body = raw_body
        self.schema_issues = schema_issues


def build_search_payload(
    query_filter: DenmarkCvrQueryFilter,
    *,
    page_index: int,
    size: int,
    entity_type: DenmarkCvrEntityType = DATACVR_COMPANY_ENTITY_TYPE,
) -> dict[str, dict[str, object]]:
    _validate_search_request(
        query_filter,
        page_index=page_index,
        size=size,
        entity_type=entity_type,
    )
    return {
        "fritekstCommand": {
            "soegOrd": "",
            "sideIndex": str(page_index),
            "enhedstype": entity_type,
            "kommune": (
                [query_filter.municipality] if query_filter.municipality else []
            ),
            "region": [query_filter.region] if query_filter.region else [],
            "antalAnsatte": [],
            "virksomhedsform": [],
            "virksomhedsstatus": [],
            "virksomhedsmarkering": [],
            "personrolle": [],
            "startdatoFra": query_filter.start_date.isoformat(),
            "startdatoTil": query_filter.end_date.isoformat(),
            "ophoersdatoFra": "",
            "ophoersdatoTil": "",
            "branchekode": "",
            "size": size,
            "sortering": "",
        }
    }


def search_results_url(
    base_url: str,
    query_filter: DenmarkCvrQueryFilter,
    *,
    page_index: int,
    size: int,
    entity_type: DenmarkCvrEntityType = DATACVR_COMPANY_ENTITY_TYPE,
) -> str:
    _validate_search_request(
        query_filter,
        page_index=page_index,
        size=size,
        entity_type=entity_type,
    )
    if base_url.strip() == "":
        raise ValueError("DataCVR base URL must not be blank")
    parameters: list[tuple[str, str | int]] = [
        ("sideIndex", page_index),
        ("enhedstype", entity_type),
        ("startdatoFra", query_filter.start_date.isoformat()),
        ("startdatoTil", query_filter.end_date.isoformat()),
    ]
    if query_filter.region:
        parameters.append(("region", query_filter.region))
    if query_filter.municipality:
        parameters.append(("kommune", query_filter.municipality))
    parameters.append(("size", size))
    return f"{base_url.rstrip('/')}/soegeresultater?{urlencode(parameters)}"


class DenmarkCvrSearchResource(dg.ConfigurableResource):
    search_base_url: str = DATACVR_BASE_URL
    page_size: int = DATACVR_PAGE_SIZE
    min_delay_ms: int = 100
    max_delay_ms: int = 800

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        if self.search_base_url.strip() == "":
            raise ValueError("search_base_url must not be blank")
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")
        if self.page_size > DATACVR_PAGE_SIZE:
            raise ValueError("page_size must not exceed 1,000")
        if self.min_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("request delays must not be negative")
        if self.min_delay_ms > self.max_delay_ms:
            raise ValueError("min_delay_ms must not exceed max_delay_ms")
        return self

    def download_date_range(
        self,
        *,
        start_date: date,
        end_date: date,
        entity_type: DenmarkCvrEntityType = DATACVR_COMPANY_ENTITY_TYPE,
        log_info: Callable[..., object] | None = None,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> DenmarkCvrDateRangeDownload:
        _validate_entity_type(entity_type)
        selected_entity_label = entity_label(entity_type)
        root_filter = DenmarkCvrQueryFilter(
            start_date=start_date,
            end_date=end_date,
            region="",
            municipality="",
        )
        try:
            browser = launcher()
        except Exception:
            raise DenmarkCvrRequestError(
                "DataCVR browser failed to start; verify Chromium runtime dependencies"
            ) from None
        try:
            page = browser.new_page()
            page.goto(
                search_results_url(
                    self.search_base_url,
                    root_filter,
                    page_index=0,
                    size=1,
                    entity_type=entity_type,
                ),
                wait_until="networkidle",
            )
            count_page = self._request_page(
                page,
                query_filter=root_filter,
                page_index=0,
                size=1,
                entity_type=entity_type,
            )
            _validate_entity_only_response(
                count_page.response,
                filter_id=entity_filter_id(root_filter, entity_type),
                entity_type=entity_type,
            )
            query_filters = filters_for_date_range(
                start_date=start_date,
                end_date=end_date,
                advertised_count=count_page.response.total,
            )
            if log_info is not None:
                log_info(
                    "DataCVR %s filters selected: start_date=%s "
                    "end_date=%s generic_advertised=%s query_count=%s",
                    selected_entity_label,
                    start_date,
                    end_date,
                    count_page.response.total,
                    len(query_filters),
                )
            query_downloads: list[DenmarkCvrQueryDownload] = []
            downloaded_entity_count = 0
            downloaded_page_count = 0
            downloaded_size_bytes = 0
            for filter_index, query_filter in enumerate(query_filters):
                if filter_index > 0:
                    sleep(self._request_delay_seconds())
                query_download = self._download_query(
                    page,
                    query_filter=query_filter,
                    entity_type=entity_type,
                    sleep=sleep,
                )
                query_downloads.append(query_download)
                downloaded_entity_count += query_download.downloaded_entity_count
                downloaded_page_count += query_download.page_count
                downloaded_size_bytes += query_download.downloaded_size_bytes
                if log_info is not None:
                    log_info(
                        "DataCVR %s progress: filter=%s query=%s/%s "
                        "advertised=%s downloaded=%s pages=%s "
                        "total_downloaded=%s total_pages=%s downloaded_bytes=%s",
                        selected_entity_label,
                        entity_filter_id(query_filter, entity_type),
                        filter_index + 1,
                        len(query_filters),
                        query_download.advertised_count,
                        query_download.downloaded_entity_count,
                        query_download.page_count,
                        downloaded_entity_count,
                        downloaded_page_count,
                        downloaded_size_bytes,
                    )
            return DenmarkCvrDateRangeDownload(
                generic_advertised_count=count_page.response.total,
                query_downloads=tuple(query_downloads),
            )
        finally:
            browser.close()

    def _download_query(
        self,
        page: Any,
        *,
        query_filter: DenmarkCvrQueryFilter,
        entity_type: DenmarkCvrEntityType,
        sleep: Callable[[float], None],
    ) -> DenmarkCvrQueryDownload:
        search_pages = [
            self._request_page(
                page,
                query_filter=query_filter,
                page_index=0,
                size=self.page_size,
                entity_type=entity_type,
            )
        ]
        _validate_entity_only_response(
            search_pages[0].response,
            filter_id=entity_filter_id(query_filter, entity_type),
            entity_type=entity_type,
        )
        page_count = max(
            1,
            min(
                ceil(search_pages[0].response.total / self.page_size),
                ceil(DATACVR_RESULT_LIMIT / self.page_size),
            ),
        )
        for page_index in range(1, page_count):
            sleep(self._request_delay_seconds())
            search_page = self._request_page(
                page,
                query_filter=query_filter,
                page_index=page_index,
                size=self.page_size,
                entity_type=entity_type,
            )
            _validate_entity_only_response(
                search_page.response,
                filter_id=entity_filter_id(query_filter, entity_type),
                entity_type=entity_type,
            )
            search_pages.append(search_page)
            if not search_page.response.enheder:
                break

        return DenmarkCvrQueryDownload(
            query_filter=query_filter,
            advertised_count=search_pages[0].response.total,
            entities=tuple(
                entity
                for search_page in search_pages
                for entity in _raw_entities(search_page.raw_body)
            ),
            page_count=len(search_pages),
            downloaded_size_bytes=sum(
                len(search_page.raw_body.encode("utf-8"))
                for search_page in search_pages
            ),
        )

    def _request_page(
        self,
        page: Any,
        *,
        query_filter: DenmarkCvrQueryFilter,
        page_index: int,
        size: int,
        entity_type: DenmarkCvrEntityType,
    ) -> DenmarkCvrSearchPage:
        result = page.evaluate(
            DATACVR_SEARCH_SCRIPT,
            {
                "payload": build_search_payload(
                    query_filter,
                    page_index=page_index,
                    size=size,
                    entity_type=entity_type,
                )
            },
        )
        return _validated_search_page(
            result,
            filter_id=entity_filter_id(query_filter, entity_type),
            page_index=page_index,
        )

    def _request_delay_seconds(self) -> float:
        return randint(self.min_delay_ms, self.max_delay_ms) / 1_000


def _validated_search_page(
    result: object,
    *,
    filter_id: str,
    page_index: int,
) -> DenmarkCvrSearchPage:
    if not isinstance(result, dict):
        raise DenmarkCvrRequestError(
            f"DataCVR returned an invalid browser result on page {page_index}"
        )
    status = result.get("status")
    if not isinstance(status, int):
        raise DenmarkCvrRequestError(
            f"DataCVR omitted the response status on page {page_index}"
        )
    if result.get("ok") is not True:
        raise DenmarkCvrRequestError(
            f"DataCVR search request failed on page {page_index} with status {status}"
        )
    raw_body = result.get("body")
    if not isinstance(raw_body, str):
        raise DenmarkCvrRequestError(
            f"DataCVR omitted the response body on page {page_index}"
        )
    try:
        response = SearchResponse.model_validate_json(raw_body)
    except ValidationError as exc:
        raise DenmarkCvrValidationError(
            filter_id=filter_id,
            page_index=page_index,
            raw_body=raw_body,
            schema_issues=tuple(
                (tuple(error["loc"]), str(error["type"]))
                for error in exc.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                )
            ),
        ) from None
    return DenmarkCvrSearchPage(
        page_index=page_index,
        raw_body=raw_body,
        response=response,
        status=status,
        response_headers=_safe_response_headers(result.get("headers")),
    )


def _raw_entities(raw_body: str) -> tuple[dict[str, Any], ...]:
    raw_response = json.loads(raw_body)
    entities = raw_response["enheder"]
    if not isinstance(entities, list) or any(
        not isinstance(entity, dict) for entity in entities
    ):
        raise DenmarkCvrRequestError("DataCVR raw entities are invalid")
    return tuple(entities)


def _safe_response_headers(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(name).lower(): str(header_value)
        for name, header_value in value.items()
        if str(name).lower() in SAFE_RESPONSE_HEADERS
    }


def _validate_search_request(
    query_filter: DenmarkCvrQueryFilter,
    *,
    page_index: int,
    size: int,
    entity_type: DenmarkCvrEntityType,
) -> None:
    _validate_entity_type(entity_type)
    if page_index < 0:
        raise ValueError("DataCVR page index must not be negative")
    if size <= 0:
        raise ValueError("DataCVR page size must be greater than zero")
    if size > DATACVR_RESULT_LIMIT:
        raise ValueError("DataCVR page size must not exceed the result limit")
    if query_filter.start_date > query_filter.end_date:
        raise ValueError("DataCVR start date must not exceed its end date")


def _validate_entity_only_response(
    response: SearchResponse,
    *,
    filter_id: str,
    entity_type: DenmarkCvrEntityType,
) -> None:
    counts = {
        DATACVR_COMPANY_ENTITY_TYPE: response.virksomhed_total,
        DATACVR_PRODUCTION_UNIT_ENTITY_TYPE: response.p_enhed_total,
        DATACVR_PERSON_ENTITY_TYPE: response.person_total,
    }
    selected_count = counts[entity_type]
    other_count = sum(
        count for current_type, count in counts.items() if current_type != entity_type
    )
    expected_model = _ENTITY_MODELS[entity_type]
    if (
        response.total != selected_count
        or other_count != 0
        or any(
            not isinstance(search_result, expected_model)
            for search_result in response.enheder
        )
    ):
        raise DenmarkCvrRequestError(
            f"DataCVR filter {filter_id} returned results outside the requested "
            f"entity type {entity_type}"
        )


def _validate_entity_type(entity_type: str) -> None:
    if entity_type not in _ENTITY_LABELS:
        raise ValueError(f"Unsupported DataCVR entity type: {entity_type}")


def entity_label(entity_type: DenmarkCvrEntityType) -> str:
    _validate_entity_type(entity_type)
    return _ENTITY_LABELS[entity_type]


def entity_filter_id(
    query_filter: DenmarkCvrQueryFilter,
    entity_type: DenmarkCvrEntityType,
) -> str:
    if query_filter.municipality != "":
        return query_filter.filter_id
    return {
        DATACVR_COMPANY_ENTITY_TYPE: "all-companies",
        DATACVR_PRODUCTION_UNIT_ENTITY_TYPE: "all-production-units",
        DATACVR_PERSON_ENTITY_TYPE: "all-persons",
    }[entity_type]
