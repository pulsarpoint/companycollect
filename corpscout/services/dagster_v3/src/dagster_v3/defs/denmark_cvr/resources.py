import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from random import randint
from typing import Any, Self
from urllib.parse import urlencode

import dagster as dg
from cloakbrowser import launch
from pydantic import ValidationError, model_validator

from dagster_v3.defs.denmark_cvr.models import SearchResponse

DATACVR_BASE_URL = "https://datacvr.virk.dk"
DATACVR_PAGE_SIZE = 1_000
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


class DenmarkCvrRequestError(RuntimeError):
    pass


class DenmarkCvrValidationError(ValueError):
    def __init__(
        self,
        *,
        page_index: int,
        raw_body: str,
        schema_issues: tuple[tuple[tuple[str | int, ...], str], ...] = (),
    ) -> None:
        issue_summary = ", ".join(
            f"{'.'.join(str(part) for part in location)}:{error_type}"
            for location, error_type in schema_issues[:5]
        )
        message = f"DataCVR returned an invalid response on page {page_index}"
        if issue_summary:
            message += f"; schema_errors={len(schema_issues)} issues={issue_summary}"
        super().__init__(message)
        self.page_index = page_index
        self.raw_body = raw_body
        self.schema_issues = schema_issues


def build_search_payload(
    search_term: str,
    *,
    page_index: int,
    size: int,
) -> dict[str, dict[str, object]]:
    _validate_search_request(search_term, page_index=page_index, size=size)
    return {
        "fritekstCommand": {
            "soegOrd": search_term,
            "sideIndex": str(page_index),
            "enhedstype": "",
            "kommune": [],
            "region": [],
            "antalAnsatte": [],
            "virksomhedsform": [],
            "virksomhedsstatus": [],
            "virksomhedsmarkering": [],
            "personrolle": [],
            "startdatoFra": "",
            "startdatoTil": "",
            "ophoersdatoFra": "",
            "ophoersdatoTil": "",
            "branchekode": "",
            "size": size,
            "sortering": "",
        }
    }


def search_results_url(
    base_url: str,
    search_term: str,
    *,
    page_index: int,
    size: int,
) -> str:
    _validate_search_request(search_term, page_index=page_index, size=size)
    if base_url.strip() == "":
        raise ValueError("DataCVR base URL must not be blank")
    query = urlencode(
        {
            "fritekst": search_term,
            "sideIndex": page_index,
            "size": size,
        }
    )
    return f"{base_url.rstrip('/')}/soegeresultater?{query}"


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
        if self.min_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("request delays must not be negative")
        if self.min_delay_ms > self.max_delay_ms:
            raise ValueError("min_delay_ms must not exceed max_delay_ms")
        return self

    def iter_search_pages(
        self,
        search_term: str,
        *,
        start_page_index: int = 0,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[DenmarkCvrSearchPage]:
        _validate_search_request(
            search_term,
            page_index=start_page_index,
            size=self.page_size,
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
                    search_term,
                    page_index=start_page_index,
                    size=self.page_size,
                ),
                wait_until="networkidle",
            )
            yield from self._iter_page_responses(
                page,
                search_term=search_term,
                start_page_index=start_page_index,
                sleep=sleep,
            )
        finally:
            browser.close()

    def _iter_page_responses(
        self,
        page: Any,
        *,
        search_term: str,
        start_page_index: int,
        sleep: Callable[[float], None],
    ) -> Iterator[DenmarkCvrSearchPage]:
        expected_total: int | None = None
        downloaded_count = 0
        page_index = 0
        offset = start_page_index
        while True:
            result = page.evaluate(
                DATACVR_SEARCH_SCRIPT,
                {
                    "payload": build_search_payload(
                        search_term,
                        page_index=offset,
                        size=self.page_size,
                    )
                },
            )
            search_page = _validated_search_page(result, page_index=page_index)
            if expected_total is None:
                expected_total = search_page.response.total

            if not search_page.response.enheder:
                return

            yield search_page
            downloaded_count += len(search_page.response.enheder)
            if downloaded_count >= expected_total:
                return

            sleep(randint(self.min_delay_ms, self.max_delay_ms) / 1_000)
            offset += len(search_page.response.enheder)
            page_index += 1


def _validated_search_page(
    result: object,
    *,
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


def _safe_response_headers(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(name).lower(): str(header_value)
        for name, header_value in value.items()
        if str(name).lower() in SAFE_RESPONSE_HEADERS
    }


def _validate_search_request(
    search_term: str,
    *,
    page_index: int,
    size: int,
) -> None:
    if search_term.strip() == "":
        raise ValueError("DataCVR search term must not be blank")
    if page_index < 0:
        raise ValueError("DataCVR page index must not be negative")
    if size <= 0:
        raise ValueError("DataCVR page size must be greater than zero")
