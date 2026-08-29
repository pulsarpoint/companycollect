import gzip
import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import Any

import dagster as dg
import pytest

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.common.resources import ObjectStoreObject
from dagster_v3.defs.sweden_ratsit.assets import (
    RATSIT_BUCKET_COUNT,
    RATSIT_CLICKHOUSE_DATABASE,
    RATSIT_MAX_COMPANIES,
    RATSIT_PARTITIONS,
    RATSIT_PARSER_VERSION,
    RATSIT_RESULT_COLUMNS,
    RATSIT_RESULT_TABLE,
    RATSIT_S3_BUCKET,
    RATSIT_SCHEMA_VERSION,
    RATSIT_SUCCESS_FRESHNESS,
    RatsitScanProgress,
    StoredRatsitReport,
    load_active_ratsit_company_ids,
    load_fresh_ratsit_company_ids_from_clickhouse,
    load_fresh_ratsit_company_ids_from_s3,
    persist_ratsit_scan,
    ratsit_bucket_key,
    ratsit_result_object_key,
    select_ratsit_companies_for_scan,
    se_ratsit_scan_dispatch,
    write_ratsit_scan,
)
from dagster_v3.defs.sweden_ratsit.parser import parse_company_page
from dagster_v3.defs.sweden_ratsit.resources import (
    RATSIT_BROWSER_WORKER_COUNT,
    RatsitCompanyFailure,
    RatsitCompanyNotFound,
    RatsitCompanyReport,
    SwedenRatsitBrowserResource,
    ratsit_company_url,
    ratsit_round_robin_assignments,
)

COMPANY_ID = "5560004615"
COMPANY_URL = f"https://www.ratsit.se/{COMPANY_ID}-Skanska_AB"
FETCHED_AT = datetime(2026, 8, 28, 10, 30, tzinfo=UTC)
TEST_PROXY_URLS = (
    "http://proxy-user:proxy-password@proxy1.example:8080",
    "http://proxy-user:proxy-password@proxy2.example:8080",
    "http://proxy-user:proxy-password@proxy3.example:8080",
)


def ignore_scan_progress(_progress: RatsitScanProgress) -> None:
    pass


ROUND_ROBIN_COMPANY_IDS = (
    "5560004615",
    "5560125790",
    "5560160680",
    "5560094178",
    "5560073495",
)

COMPANY_HTML = """
<html>
  <head>
    <script type="application/ld+json">
      {
        "@type": "Organization",
        "name": "Skanska AB",
        "address": {
          "streetAddress": "Warfvinges väg 25",
          "postalCode": "112 74",
          "addressLocality": "Stockholm"
        }
      }
    </script>
  </head>
  <body>
    <main><div class="main-inner">
      <div id="foretaget">
        <h3>Juridisk Person</h3>
        <div><div>Organisationsnummer:</div><div>556000-4615</div></div>
        <div><div>Bolagsform:</div><div>Aktiebolag</div></div>
        <div><div>Status:</div><div>Aktiv</div></div>
        <h3>Verksamhetsbeskrivning</h3><p>Byggverksamhet.</p>
      </div>
      <table>
        <thead><tr><th>Resultaträkning (MSEK)</th><th>2025</th></tr></thead>
        <tbody>
          <tr><td>Rörelsens omsättning</td><td>177&nbsp;034,0</td></tr>
          <tr><td>Årets resultat</td><td>5&nbsp;772,0</td></tr>
        </tbody>
      </table>
    </div></main>
  </body>
</html>
"""


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status
        self.ok = 200 <= status < 400


class FakeLocator:
    def __init__(self) -> None:
        self.wait_calls: list[dict[str, Any]] = []

    @property
    def first(self) -> "FakeLocator":
        return self

    def wait_for(self, **kwargs: Any) -> None:
        self.wait_calls.append(kwargs)


class FakePage:
    def __init__(
        self,
        pages: list[tuple[int, str, str]],
        *,
        clock: FakeClock,
    ) -> None:
        self._pages = list(pages)
        self._current_html = ""
        self._url = "about:blank"
        self.clock = clock
        self.request_starts: list[float] = []
        self.goto_calls: list[dict[str, Any]] = []
        self._locator = FakeLocator()
        self.closed = False

    @property
    def url(self) -> str:
        return self._url

    def goto(self, url: str, **kwargs: Any) -> FakeResponse:
        self.request_starts.append(self.clock.monotonic())
        self.goto_calls.append({"url": url, **kwargs})
        status, self._url, self._current_html = self._pages.pop(0)
        return FakeResponse(status)

    def locator(self, selector: str) -> FakeLocator:
        assert selector == "main .main-inner"
        return self._locator

    def content(self) -> str:
        return self._current_html

    def close(self) -> None:
        self.closed = True


class FakeBrowser:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.new_page_count = 0
        self.closed = False

    def new_page(self) -> FakePage:
        self.new_page_count += 1
        return self.page

    def close(self) -> None:
        self.closed = True


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.last_modified: dict[str, datetime] = {}
        self.ensured_buckets: list[str] = []
        self.listed_prefixes: list[str] = []
        self.read_keys: list[str] = []

    def client(self) -> "FakeObjectStore":
        return self

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.ensured_buckets.append(bucket)

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        assert bucket == RATSIT_S3_BUCKET
        self.objects[key] = body.encode()
        self.last_modified[key] = FETCHED_AT

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket == RATSIT_S3_BUCKET
        self.objects[key] = body
        self.last_modified[key] = FETCHED_AT

    def list_objects(
        self,
        prefix: str,
        bucket: str | None = None,
    ) -> list[ObjectStoreObject]:
        assert bucket == RATSIT_S3_BUCKET
        self.listed_prefixes.append(prefix)
        return [
            ObjectStoreObject(
                key=key,
                last_modified=self.last_modified[key],
            )
            for key in self.objects
            if key.startswith(prefix)
        ]

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == RATSIT_S3_BUCKET
        self.read_keys.append(key)
        return self.objects[key]


class FakeRatsitResource:
    def __init__(
        self,
        results: list[
            RatsitCompanyReport | RatsitCompanyFailure | RatsitCompanyNotFound
        ],
    ) -> None:
        self.results = results
        self.requested_ids: tuple[str, ...] | None = None
        self.headless = True
        self.request_interval_seconds = 2.0

    def iter_company_reports(self, company_ids: tuple[str, ...]):
        self.requested_ids = company_ids
        yield from self.results


class FakeClickHouseClient:
    def __init__(
        self,
        *,
        active_company_rows: list[tuple[str]] | None = None,
        fresh_company_rows: list[tuple[str]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self.active_company_rows = active_company_rows or []
        self.fresh_company_rows = fresh_company_rows or []

    def execute(self, sql: str, parameters: object | None = None):
        self.calls.append((sql, parameters))
        if "FROM system.tables" in sql:
            assert isinstance(parameters, dict)
            return [(table,) for table in parameters["tables"]]
        if "FROM corpscout.se_companies FINAL" in sql:
            return self.active_company_rows
        if (
            "FROM corpscout.se_company_ratsit FINAL" in sql
            and "fetched_at >= %(freshness_cutoff)s" in sql
        ):
            return self.fresh_company_rows
        return []


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeDagsterInstance:
    def __init__(self) -> None:
        self.added_run_tags: list[tuple[str, dict[str, str]]] = []

    def add_run_tags(self, run_id: str, tags: dict[str, str]) -> None:
        self.added_run_tags.append((run_id, tags))


class FakeDagsterLog:
    def info(self, _message: str, *_args: object) -> None:
        pass


class FakeDagsterRun:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id


class FakeRatsitAssetContext:
    def __init__(self, *, partition_key: str, run_id: str) -> None:
        self.partition_key = partition_key
        self.run = FakeDagsterRun(run_id)
        self.instance = FakeDagsterInstance()
        self.log = FakeDagsterLog()


def _stored_report_json(company_id: str) -> bytes:
    return json.dumps(
        {
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "company_id": company_id,
            "requested_url": f"https://www.ratsit.se/{company_id[-10:]}",
            "source_url": (f"https://www.ratsit.se/{company_id[-10:]}-Test_Company"),
            "report": {
                "company": {
                    "name": "Test Company AB",
                    "organization_number": (f"{company_id[-10:-4]}-{company_id[-4:]}"),
                }
            },
        }
    ).encode()


def _ratsit_browser_resource(
    *,
    max_companies: int = RATSIT_MAX_COMPANIES,
) -> SwedenRatsitBrowserResource:
    return SwedenRatsitBrowserResource(
        crawl_proxy1=TEST_PROXY_URLS[0],
        crawl_proxy2=TEST_PROXY_URLS[1],
        crawl_proxy3=TEST_PROXY_URLS[2],
        max_companies=max_companies,
    )


def test_parser_extracts_company_identity_address_description_and_financials() -> None:
    report = parse_company_page(COMPANY_HTML, source_url=COMPANY_URL)

    assert report["company"] == {
        "name": "Skanska AB",
        "organization_number": "556000-4615",
        "legal_form": "Aktiebolag",
        "status": "Aktiv",
        "address": {
            "street": "Warfvinges väg 25",
            "postal_code": "112 74",
            "locality": "Stockholm",
            "county": None,
        },
        "industry_codes": [],
        "business_description": "Byggverksamhet.",
        "summary": [],
    }
    assert report["financials"] == [
        {
            "scope": "company",
            "monetary_unit": "MSEK",
            "periods": [
                {
                    "fiscal_year": 2025,
                    "period_start": None,
                    "period_end": None,
                    "period_months": None,
                    "income_statement": {
                        "revenue": 177034.0,
                        "net_income": 5772.0,
                    },
                    "balance_sheet": {},
                    "key_ratios": {},
                    "dividend": None,
                    "employee_count": None,
                }
            ],
        }
    ]


def test_browser_resource_streams_all_four_round_robin_workers() -> None:
    company_ids = ROUND_ROBIN_COMPANY_IDS
    proxy_urls = (None, *TEST_PROXY_URLS)
    clock = FakeClock()
    browsers: dict[str | None, FakeBrowser] = {}
    for worker_index, proxy_url in enumerate(proxy_urls):
        worker_company_ids = company_ids[worker_index::RATSIT_BROWSER_WORKER_COUNT]
        browsers[proxy_url] = FakeBrowser(
            FakePage(
                [
                    (
                        200,
                        f"https://www.ratsit.se/{company_id}-Test_Company",
                        COMPANY_HTML.replace(
                            "556000-4615",
                            f"{company_id[:6]}-{company_id[6:]}",
                        ),
                    )
                    for company_id in worker_company_ids
                ],
                clock=clock,
            )
        )

    launch_barrier = Barrier(RATSIT_BROWSER_WORKER_COUNT)
    launch_calls: list[dict[str, Any]] = []

    def launcher(**kwargs: Any) -> FakeBrowser:
        launch_calls.append(kwargs)
        launch_barrier.wait(timeout=5)
        return browsers[kwargs["proxy"]]

    reports = list(
        _ratsit_browser_resource().iter_company_reports(
            company_ids,
            launcher=launcher,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            now=lambda: FETCHED_AT,
        )
    )

    reports_by_company = {result.company_id: result for result in reports}
    assert set(reports_by_company) == set(company_ids)
    assert all(isinstance(result, RatsitCompanyReport) for result in reports)
    assert [
        (
            reports_by_company[company_id].connection_mode,
            reports_by_company[company_id].proxy_name,
        )
        for company_id in company_ids
    ] == [
        ("direct", ""),
        ("proxy", "crawl_proxy1"),
        ("proxy", "crawl_proxy2"),
        ("proxy", "crawl_proxy3"),
        ("direct", ""),
    ]
    assert len(launch_calls) == RATSIT_BROWSER_WORKER_COUNT
    assert {call["proxy"] for call in launch_calls} == set(proxy_urls)
    assert all(call["headless"] is True for call in launch_calls)
    assert ratsit_round_robin_assignments(company_ids) == (
        ("direct", (company_ids[0], company_ids[4])),
        ("crawl_proxy1", (company_ids[1],)),
        ("crawl_proxy2", (company_ids[2],)),
        ("crawl_proxy3", (company_ids[3],)),
    )
    assert browsers[None].page.request_starts == [0.0, 2.0]
    assert clock.sleeps == [2.0]
    assert all(browser.new_page_count == 1 for browser in browsers.values())
    assert all(browser.page.closed is True for browser in browsers.values())
    assert all(browser.closed is True for browser in browsers.values())


def test_browser_resource_returns_429_without_retrying_another_route() -> None:
    clock = FakeClock()
    launch_calls: list[dict[str, Any]] = []

    def launcher(**kwargs: Any) -> FakeBrowser:
        launch_calls.append(kwargs)
        return FakeBrowser(FakePage([(429, COMPANY_URL, "")], clock=clock))

    results = list(
        _ratsit_browser_resource().iter_company_reports(
            (COMPANY_ID,),
            launcher=launcher,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            now=lambda: FETCHED_AT,
        )
    )

    assert len(results) == 1
    failure = results[0]
    assert isinstance(failure, RatsitCompanyFailure)
    assert failure.http_status == 429
    assert failure.proxy_name == ""
    assert [call["proxy"] for call in launch_calls] == [None]
    assert clock.sleeps == []


def test_browser_resource_rejects_more_than_configured_companies_before_launch() -> (
    None
):
    launch_count = 0

    def launcher(**_: Any) -> FakeBrowser:
        nonlocal launch_count
        launch_count += 1
        raise AssertionError("browser must not start")

    with pytest.raises(ValueError, match="at most 2"):
        list(
            _ratsit_browser_resource(max_companies=2).iter_company_reports(
                ("5560004615", "5560125790", "5560160680"),
                launcher=launcher,
            )
        )

    assert launch_count == 0


def test_browser_resource_keeps_rendered_html_when_required_fields_are_missing() -> (
    None
):
    clock = FakeClock()
    unexpected_html = (
        "<html><body><main><div class='main-inner'>Blocked</div></main></body></html>"
    )
    page = FakePage([(200, COMPANY_URL, unexpected_html)], clock=clock)

    results = list(
        _ratsit_browser_resource().iter_company_reports(
            (COMPANY_ID,),
            launcher=lambda **_: FakeBrowser(page),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            now=lambda: FETCHED_AT,
        )
    )

    assert len(results) == 1
    failure = results[0]
    assert isinstance(failure, RatsitCompanyFailure)
    assert failure.error_type == "parse"
    assert (failure.connection_mode, failure.proxy_name) == ("direct", "")
    assert "company name" in failure.message
    assert failure.diagnostic_html == unexpected_html.encode()
    assert failure.html_sha256 is not None


def test_browser_resource_treats_ratsit_missing_redirect_as_not_found() -> None:
    clock = FakeClock()
    missing_html = "<html><body>Företaget saknas</body></html>"
    page = FakePage(
        [(200, "https://www.ratsit.se/foretag?saknas", missing_html)],
        clock=clock,
    )

    results = list(
        _ratsit_browser_resource().iter_company_reports(
            (COMPANY_ID,),
            launcher=lambda **_: FakeBrowser(page),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            now=lambda: FETCHED_AT,
        )
    )

    assert len(results) == 1
    failure = results[0]
    assert isinstance(failure, RatsitCompanyNotFound)
    assert failure.reason == "ratsit_missing"
    assert failure.http_status == 200
    assert failure.source_url == "https://www.ratsit.se/foretag?saknas"
    assert failure.message == "Ratsit redirected to /foretag?saknas"
    assert failure.diagnostic_html == missing_html.encode()
    assert failure.html_sha256 is not None
    assert page._locator.wait_calls == []


def test_browser_resource_treats_http_404_as_not_found() -> None:
    clock = FakeClock()
    page = FakePage([(404, COMPANY_URL, "not found")], clock=clock)

    results = list(
        _ratsit_browser_resource().iter_company_reports(
            (COMPANY_ID,),
            launcher=lambda **_: FakeBrowser(page),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            now=lambda: FETCHED_AT,
        )
    )

    assert len(results) == 1
    failure = results[0]
    assert isinstance(failure, RatsitCompanyNotFound)
    assert failure.reason == "http_not_found"
    assert failure.http_status == 404
    assert failure.diagnostic_html is None


def test_ratsit_partition_definition_has_128_canonical_bucket_keys() -> None:
    partition_keys = RATSIT_PARTITIONS.get_partition_keys()

    assert RATSIT_BUCKET_COUNT == 128
    assert len(partition_keys) == RATSIT_BUCKET_COUNT
    assert partition_keys[0] == "bucket_000"
    assert partition_keys[-1] == "bucket_127"
    assert RATSIT_MAX_COMPANIES == 50_000


@pytest.mark.parametrize(
    ("company_id", "expected_bucket"),
    (
        ("5560004615", "bucket_081"),
        ("5560125790", "bucket_065"),
        ("191511286237", "bucket_094"),
    ),
)
def test_ratsit_bucket_key_is_stable_for_canonical_company_ids(
    company_id: str,
    expected_bucket: str,
) -> None:
    assert ratsit_bucket_key(company_id) == expected_bucket


def test_active_company_selection_reads_one_crc32_bucket() -> None:
    client = FakeClickHouseClient(active_company_rows=[("191511286237",)])

    company_ids = load_active_ratsit_company_ids(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        "bucket_094",
    )

    assert company_ids == ("191511286237",)
    selection_sql, parameters = next(
        (sql, parameters)
        for sql, parameters in client.calls
        if "FROM corpscout.se_companies FINAL" in sql
    )
    assert "status = 'active'" in selection_sql
    assert "modulo(CRC32(company_id), %(bucket_count)s)" in selection_sql
    assert "ORDER BY company_id" in selection_sql
    assert parameters == {"bucket_count": 128, "bucket_index": 94}


def test_clickhouse_freshness_uses_success_http_200_and_fetched_at() -> None:
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    client = FakeClickHouseClient(fresh_company_rows=[(COMPANY_ID,)])

    fresh_company_ids = load_fresh_ratsit_company_ids_from_clickhouse(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        (COMPANY_ID, "5560125790"),
        freshness_cutoff,
    )

    assert fresh_company_ids == frozenset({COMPANY_ID})
    selection_sql, parameters = next(
        (sql, parameters)
        for sql, parameters in client.calls
        if "fetched_at >= %(freshness_cutoff)s" in sql
    )
    assert "outcome = 'success'" in selection_sql
    assert "http_status = 200" in selection_sql
    assert parameters == {
        "company_ids": (COMPANY_ID, "5560125790"),
        "freshness_cutoff": freshness_cutoff,
    }


def test_scan_selection_checks_s3_only_after_clickhouse_fallback() -> None:
    clickhouse_company_id = COMPANY_ID
    s3_company_id = "5560125790"
    retry_company_id = "5560160680"
    active_company_ids = (
        clickhouse_company_id,
        s3_company_id,
        retry_company_id,
    )
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    client = FakeClickHouseClient(
        fresh_company_rows=[(clickhouse_company_id,)],
    )
    object_store = FakeObjectStore()
    s3_report_key = ratsit_result_object_key(
        s3_company_id,
        "completed-scan",
        "report.json",
    )
    object_store.objects[s3_report_key] = _stored_report_json(s3_company_id)
    object_store.last_modified[s3_report_key] = FETCHED_AT
    retry_error_key = ratsit_result_object_key(
        retry_company_id,
        "rate-limited-scan",
        "error.json",
    )
    object_store.objects[retry_error_key] = json.dumps(
        {"http_status": 429, "outcome": "failure"}
    ).encode()
    object_store.last_modified[retry_error_key] = FETCHED_AT

    selection = select_ratsit_companies_for_scan(
        clickhouse=FakeClickHouseResource(client),  # type: ignore[arg-type]
        object_store=object_store,  # type: ignore[arg-type]
        active_company_ids=active_company_ids,
        freshness_cutoff=freshness_cutoff,
    )

    assert selection.clickhouse_fresh_company_ids == (clickhouse_company_id,)
    assert selection.s3_fresh_company_ids == (s3_company_id,)
    assert selection.selected_company_ids == (retry_company_id,)
    assert selection.freshness_cutoff == freshness_cutoff
    assert set(object_store.listed_prefixes) == {
        f"sweden_ratsit/pilot/company_id={s3_company_id}/",
        f"sweden_ratsit/pilot/company_id={retry_company_id}/",
    }
    assert not any(
        f"company_id={clickhouse_company_id}/" in prefix
        for prefix in object_store.listed_prefixes
    )
    assert object_store.read_keys == [s3_report_key]


def test_s3_freshness_rejects_report_older_than_thirty_days() -> None:
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    object_store = FakeObjectStore()
    report_key = ratsit_result_object_key(
        COMPANY_ID,
        "stale-scan",
        "report.json",
    )
    object_store.objects[report_key] = _stored_report_json(COMPANY_ID)
    object_store.last_modified[report_key] = freshness_cutoff - timedelta(seconds=1)

    fresh_company_ids = load_fresh_ratsit_company_ids_from_s3(
        object_store,  # type: ignore[arg-type]
        (COMPANY_ID,),
        freshness_cutoff,
    )

    assert fresh_company_ids == frozenset()
    assert object_store.read_keys == []


@pytest.mark.parametrize(
    "report_body",
    (
        b"{not-json",
        _stored_report_json("5560125790"),
    ),
)
def test_s3_freshness_rejects_invalid_or_mismatched_report(
    report_body: bytes,
) -> None:
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    object_store = FakeObjectStore()
    report_key = ratsit_result_object_key(
        COMPANY_ID,
        "invalid-scan",
        "report.json",
    )
    object_store.objects[report_key] = report_body
    object_store.last_modified[report_key] = FETCHED_AT

    fresh_company_ids = load_fresh_ratsit_company_ids_from_s3(
        object_store,  # type: ignore[arg-type]
        (COMPANY_ID,),
        freshness_cutoff,
    )

    assert fresh_company_ids == frozenset()


def test_s3_freshness_ignores_retryable_errors_and_not_found_results() -> None:
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    object_store = FakeObjectStore()
    result_keys = (
        ratsit_result_object_key(COMPANY_ID, "rate-limited", "error.json"),
        ratsit_result_object_key(COMPANY_ID, "server-error", "error.json"),
        ratsit_result_object_key(COMPANY_ID, "not-found", "not_found.json"),
    )
    payloads = (
        {"outcome": "failure", "http_status": 429},
        {"outcome": "failure", "http_status": 500},
        {"outcome": "not_found", "http_status": 200},
    )
    for result_key, payload in zip(result_keys, payloads, strict=True):
        object_store.objects[result_key] = json.dumps(payload).encode()
        object_store.last_modified[result_key] = FETCHED_AT

    fresh_company_ids = load_fresh_ratsit_company_ids_from_s3(
        object_store,  # type: ignore[arg-type]
        (COMPANY_ID,),
        freshness_cutoff,
    )

    assert fresh_company_ids == frozenset()
    assert object_store.read_keys == []


def test_fully_fresh_selection_does_not_write_or_index_an_empty_scan() -> None:
    freshness_cutoff = FETCHED_AT - RATSIT_SUCCESS_FRESHNESS
    client = FakeClickHouseClient(fresh_company_rows=[(COMPANY_ID,)])
    clickhouse = FakeClickHouseResource(client)
    object_store = FakeObjectStore()
    selection = select_ratsit_companies_for_scan(
        clickhouse=clickhouse,  # type: ignore[arg-type]
        object_store=object_store,  # type: ignore[arg-type]
        active_company_ids=(COMPANY_ID,),
        freshness_cutoff=freshness_cutoff,
    )
    ratsit = FakeRatsitResource([])

    summary = write_ratsit_scan(
        object_store=object_store,  # type: ignore[arg-type]
        ratsit=ratsit,  # type: ignore[arg-type]
        company_ids=selection.selected_company_ids,
        scan_id="fully-cached-scan",
        on_progress=ignore_scan_progress,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )
    indexed_result_count = persist_ratsit_scan(
        clickhouse,  # type: ignore[arg-type]
        summary,
        recorded_at=FETCHED_AT,
    )

    assert selection.selected_company_ids == ()
    assert ratsit.requested_ids == ()
    assert summary.results == ()
    assert object_store.ensured_buckets == []
    assert object_store.objects == {}
    assert indexed_result_count == 0
    assert not any("INSERT INTO" in sql for sql, _ in client.calls)


def test_browser_preserves_canonical_twelve_digit_id_but_requests_final_ten() -> None:
    company_id = "191511286237"
    page = FakePage(
        [
            (
                200,
                "https://www.ratsit.se/1511286237-Test_Company",
                COMPANY_HTML.replace("556000-4615", "151128-6237"),
            )
        ],
        clock=FakeClock(),
    )

    [result] = list(
        _ratsit_browser_resource().iter_company_reports(
            (company_id,),
            launcher=lambda **_: FakeBrowser(page),
            now=lambda: FETCHED_AT,
        )
    )

    assert isinstance(result, RatsitCompanyReport)
    assert result.company_id == company_id
    assert result.requested_url == "https://www.ratsit.se/1511286237"
    assert page.goto_calls[0]["url"] == result.requested_url
    assert ratsit_company_url("https://www.ratsit.se", company_id) == (
        result.requested_url
    )


def test_ratsit_dispatch_and_normalized_table_assets_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = repository.asset_graph.get_all_asset_keys()

    assert dg.AssetKey("se_ratsit_scan_dispatch") in asset_keys
    scan_node = repository.asset_graph.get(dg.AssetKey("se_ratsit_scan_dispatch"))
    assert scan_node.parent_keys == {dg.AssetKey("sweden_company_companies_clickhouse")}
    assert scan_node.partitions_def == RATSIT_PARTITIONS
    for asset_name in (
        "se_ratsit_company",
        "se_ratsit_company_industry_codes",
        "se_ratsit_company_summaries",
        "se_ratsit_responsible_people",
        "se_ratsit_establishments",
        "se_ratsit_financial_reports",
        "se_ratsit_financial_periods",
    ):
        assert dg.AssetKey(asset_name) in asset_keys
        assert repository.asset_graph.get(dg.AssetKey(asset_name)).parent_keys == {
            dg.AssetKey("se_ratsit_scan_dispatch"),
            dg.AssetKey("nace_categories_clickhouse"),
        }
        assert (
            repository.asset_graph.get(dg.AssetKey(asset_name)).partitions_def
            == RATSIT_PARTITIONS
        )
    assert dg.AssetKey("se_ratsit_people_at_address") not in asset_keys
    assert dg.AssetKey("se_ratsit_scan_coverage") not in asset_keys
    assert dg.AssetKey("se_ratsit_pilot_reports_s3") not in asset_keys
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "se_ratsit_scan_dispatch_job" in job_names
    assert "se_ratsit_normalize_job" in job_names
    assert "se_ratsit_scan_coverage_job" not in job_names
    assert "se_ratsit_scan_job" not in job_names
    assert "se_ratsit_pilot_reports_job" not in job_names


def test_dispatch_persists_every_result_before_failing_a_rate_limited_bucket() -> None:
    scan_id = "rate-limited-bucket"
    rate_limited_company_id = "5560094178"
    successful_company_id = "5560073495"
    partition_key = ratsit_bucket_key(rate_limited_company_id)
    assert ratsit_bucket_key(successful_company_id) == partition_key
    context = FakeRatsitAssetContext(partition_key=partition_key, run_id=scan_id)
    clickhouse_client = FakeClickHouseClient(
        active_company_rows=[
            (rate_limited_company_id,),
            (successful_company_id,),
        ]
    )
    clickhouse = FakeClickHouseResource(clickhouse_client)
    object_store = FakeObjectStore()
    rate_limited = RatsitCompanyFailure(
        company_id=rate_limited_company_id,
        connection_mode="direct",
        proxy_name="",
        requested_url=f"https://www.ratsit.se/{rate_limited_company_id}",
        source_url=f"https://www.ratsit.se/{rate_limited_company_id}",
        fetched_at=FETCHED_AT,
        error_type="http",
        message="Ratsit returned HTTP status 429",
        http_status=429,
        html_sha256=None,
        diagnostic_html=None,
    )
    successful = RatsitCompanyReport(
        company_id=successful_company_id,
        connection_mode="proxy",
        proxy_name="crawl_proxy1",
        requested_url=f"https://www.ratsit.se/{successful_company_id}",
        source_url=f"https://www.ratsit.se/{successful_company_id}-Test_AB",
        fetched_at=FETCHED_AT,
        html_sha256="a" * 64,
        report={
            "company": {
                "name": "Test AB",
                "organization_number": "556007-3495",
            }
        },
    )

    with pytest.raises(dg.Failure, match="completed with 1 HTTP 429") as exc_info:
        se_ratsit_scan_dispatch.node_def.compute_fn.decorated_fn(
            context,
            clickhouse,
            FakeRatsitResource([rate_limited, successful]),
            object_store,
        )

    error_key = ratsit_result_object_key(
        rate_limited_company_id,
        scan_id,
        "error.json",
    )
    report_key = ratsit_result_object_key(
        successful_company_id,
        scan_id,
        "report.json",
    )
    assert error_key in object_store.objects
    assert report_key in object_store.objects
    insert_calls = [
        (sql, parameters)
        for sql, parameters in clickhouse_client.calls
        if sql.lstrip().startswith("INSERT INTO corpscout.se_company_ratsit")
    ]
    assert len(insert_calls) == 1
    assert len(insert_calls[0][1]) == 2
    assert {row[8] for row in insert_calls[0][1]} == {200, 429}
    assert context.instance.added_run_tags == [(scan_id, {"dagster/max_retries": "0"})]
    failure = exc_info.value
    assert failure.allow_retries is False
    assert failure.metadata["http_429_count"].value == 1
    assert failure.metadata["indexed_result_count"].value == 2
    assert failure.metadata["bucket_status"].value == "failed_http_429"


def test_scan_writes_run_scoped_results_and_keeps_diagnostic_html() -> None:
    report = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="direct",
        proxy_name="",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url=COMPANY_URL,
        fetched_at=FETCHED_AT,
        html_sha256="a" * 64,
        report={"company": {"name": "Skanska AB"}},
    )
    parse_failure = RatsitCompanyFailure(
        company_id="5560125790",
        connection_mode="proxy",
        proxy_name="crawl_proxy1",
        requested_url="https://www.ratsit.se/5560125790",
        source_url="https://www.ratsit.se/5560125790-AB_Volvo",
        fetched_at=FETCHED_AT,
        error_type="parse",
        message="Ratsit company page did not contain an organization number",
        http_status=200,
        html_sha256="b" * 64,
        diagnostic_html=b"<html>unexpected page</html>",
    )
    object_store = FakeObjectStore()
    ratsit = FakeRatsitResource([parse_failure, report])
    progress: list[RatsitScanProgress] = []

    summary = write_ratsit_scan(
        object_store=object_store,  # type: ignore[arg-type]
        ratsit=ratsit,  # type: ignore[arg-type]
        company_ids=(COMPANY_ID, "5560125790"),
        scan_id="scan-run-1",
        on_progress=progress.append,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )

    report_key = ratsit_result_object_key(COMPANY_ID, "scan-run-1", "report.json")
    error_key = ratsit_result_object_key("5560125790", "scan-run-1", "error.json")
    html_key = ratsit_result_object_key(
        "5560125790", "scan-run-1", "diagnostic.html.gz"
    )
    assert set(object_store.objects) == {report_key, error_key, html_key}
    report_payload = json.loads(object_store.objects[report_key])
    assert report_payload["report"]["company"] == {"name": "Skanska AB"}
    assert "scan_id" not in report_payload
    assert "fetched_at" not in report_payload
    error_payload = json.loads(object_store.objects[error_key])
    assert error_payload["error_type"] == "parse"
    assert error_payload["scan_id"] == "scan-run-1"
    assert gzip.decompress(object_store.objects[html_key]) == (
        b"<html>unexpected page</html>"
    )
    assert object_store.ensured_buckets == [RATSIT_S3_BUCKET]
    assert ratsit.requested_ids == (COMPANY_ID, "5560125790")
    assert summary.success_count == 1
    assert summary.failure_count == 1
    assert summary.diagnostic_html_count == 1
    assert summary.written_object_count == 3
    assert summary.reused_report_count == 0
    assert summary.result_object_keys == (report_key, error_key)
    assert (summary.results[0].outcome, summary.results[0].failure_type) == (
        "success",
        "",
    )
    assert (summary.results[1].outcome, summary.results[1].failure_type) == (
        "failure",
        "parse",
    )
    assert (
        summary.results[0].connection_mode,
        summary.results[0].proxy_name,
    ) == ("direct", "")
    assert (
        summary.results[1].connection_mode,
        summary.results[1].proxy_name,
    ) == ("proxy", "crawl_proxy1")
    assert summary.results[0].result_size_bytes == len(object_store.objects[report_key])
    assert len(summary.results[0].result_sha256) == 64
    assert [event.completed_count for event in progress] == [1, 2]
    assert [event.latest_result.company_id for event in progress] == [
        "5560125790",
        COMPANY_ID,
    ]
    assert progress[-1].total_count == 2
    assert progress[-1].success_count == 1
    assert progress[-1].failure_count == 1


def test_scan_keeps_missing_company_redirect_html() -> None:
    missing_html = "<html><body>Företaget saknas</body></html>".encode()
    missing_company = RatsitCompanyNotFound(
        company_id=COMPANY_ID,
        connection_mode="direct",
        proxy_name="",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url="https://www.ratsit.se/foretag?saknas",
        fetched_at=FETCHED_AT,
        reason="ratsit_missing",
        message="Ratsit redirected to /foretag?saknas",
        http_status=200,
        html_sha256="c" * 64,
        diagnostic_html=missing_html,
    )
    object_store = FakeObjectStore()

    summary = write_ratsit_scan(
        object_store=object_store,  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([missing_company]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="missing-company-scan",
        on_progress=ignore_scan_progress,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )

    not_found_key = ratsit_result_object_key(
        COMPANY_ID,
        "missing-company-scan",
        "not_found.json",
    )
    html_key = ratsit_result_object_key(
        COMPANY_ID,
        "missing-company-scan",
        "diagnostic.html.gz",
    )
    assert set(object_store.objects) == {not_found_key, html_key}
    not_found_payload = json.loads(object_store.objects[not_found_key])
    assert not_found_payload["outcome"] == "not_found"
    assert not_found_payload["reason"] == "ratsit_missing"
    assert gzip.decompress(object_store.objects[html_key]) == missing_html
    assert summary.results[0].outcome == "not_found"
    assert summary.results[0].failure_type == ""
    assert summary.results[0].error_message == ""
    assert summary.results[0].diagnostic_object_key == html_key
    assert summary.success_count == 0
    assert summary.not_found_count == 1
    assert summary.failure_count == 0
    assert summary.diagnostic_html_count == 1


def test_identical_report_hash_reuses_old_s3_path_for_new_scan() -> None:
    report = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="direct",
        proxy_name="",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url=COMPANY_URL,
        fetched_at=FETCHED_AT,
        html_sha256="a" * 64,
        report={"company": {"name": "Skanska AB"}},
    )
    first_store = FakeObjectStore()
    first = write_ratsit_scan(
        object_store=first_store,  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([report]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="scan-run-1",
        on_progress=ignore_scan_progress,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )
    first_result = first.results[0]
    reusable = StoredRatsitReport(
        company_id=COMPANY_ID,
        result_sha256=first_result.result_sha256,
        result_bucket=first_result.result_bucket,
        result_object_key=first_result.result_object_key,
        result_size_bytes=first_result.result_size_bytes,
    )
    second_store = FakeObjectStore()
    proxied_report = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="proxy",
        proxy_name="crawl_proxy1",
        requested_url=report.requested_url,
        source_url=report.source_url,
        fetched_at=report.fetched_at,
        html_sha256=report.html_sha256,
        report=report.report,
    )

    second = write_ratsit_scan(
        object_store=second_store,  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([proxied_report]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="scan-run-2",
        on_progress=ignore_scan_progress,
        reusable_reports={(COMPANY_ID, reusable.result_sha256): reusable},
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )

    assert second_store.objects == {}
    assert second.reused_report_count == 1
    assert second.written_object_count == 0
    assert second.results[0].scan_id == "scan-run-2"
    assert second.results[0].report_reused is True
    assert (
        second.results[0].connection_mode,
        second.results[0].proxy_name,
    ) == ("proxy", "crawl_proxy1")
    assert second.results[0].result_object_key == first_result.result_object_key
    assert "scan-run-1_report.json" in second.results[0].result_object_key


def test_changed_report_writes_new_scan_id_object() -> None:
    original = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="direct",
        proxy_name="",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url=COMPANY_URL,
        fetched_at=FETCHED_AT,
        html_sha256="a" * 64,
        report={"company": {"name": "Skanska AB"}},
    )
    changed = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="proxy",
        proxy_name="crawl_proxy2",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url=COMPANY_URL,
        fetched_at=FETCHED_AT,
        html_sha256="b" * 64,
        report={"company": {"name": "Skanska AB", "status": "Aktiv"}},
    )
    first = write_ratsit_scan(
        object_store=FakeObjectStore(),  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([original]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="scan-run-1",
        on_progress=ignore_scan_progress,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )
    first_result = first.results[0]
    reusable = StoredRatsitReport(
        company_id=COMPANY_ID,
        result_sha256=first_result.result_sha256,
        result_bucket=first_result.result_bucket,
        result_object_key=first_result.result_object_key,
        result_size_bytes=first_result.result_size_bytes,
    )
    second_store = FakeObjectStore()

    second = write_ratsit_scan(
        object_store=second_store,  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([changed]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="scan-run-2",
        on_progress=ignore_scan_progress,
        reusable_reports={(COMPANY_ID, reusable.result_sha256): reusable},
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )

    expected_key = ratsit_result_object_key(COMPANY_ID, "scan-run-2", "report.json")
    assert set(second_store.objects) == {expected_key}
    assert second.results[0].result_object_key == expected_key
    assert second.results[0].report_reused is False
    assert second.results[0].result_sha256 != first_result.result_sha256


def test_completed_scan_persists_company_result_rows() -> None:
    report = RatsitCompanyReport(
        company_id=COMPANY_ID,
        connection_mode="proxy",
        proxy_name="crawl_proxy3",
        requested_url=f"https://www.ratsit.se/{COMPANY_ID}",
        source_url=COMPANY_URL,
        fetched_at=FETCHED_AT,
        html_sha256="a" * 64,
        report={"company": {"name": "Skanska AB"}},
    )
    summary = write_ratsit_scan(
        object_store=FakeObjectStore(),  # type: ignore[arg-type]
        ratsit=FakeRatsitResource([report]),  # type: ignore[arg-type]
        company_ids=(COMPANY_ID,),
        scan_id="scan-run-1",
        on_progress=ignore_scan_progress,
        started_at=FETCHED_AT,
        completed_at=FETCHED_AT,
    )
    client = FakeClickHouseClient()

    indexed = persist_ratsit_scan(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        summary,
        recorded_at=datetime(2026, 8, 28, 10, 31, tzinfo=UTC),
    )

    assert indexed == 1
    assert len(client.calls) == 2
    result_sql, result_rows = client.calls[1]
    assert (
        f"INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}" in result_sql
    )
    assert ", ".join(RATSIT_RESULT_COLUMNS) in result_sql
    assert isinstance(result_rows, list)
    [result_row] = result_rows
    assert result_row[0:6] == (
        "scan-run-1",
        COMPANY_ID,
        "success",
        "",
        "proxy",
        "crawl_proxy3",
    )
    assert result_row[10] == ratsit_result_object_key(
        COMPANY_ID, "scan-run-1", "report.json"
    )
