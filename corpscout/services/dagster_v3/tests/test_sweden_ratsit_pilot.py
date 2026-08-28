import gzip
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from threading import Barrier
from typing import Any

import dagster as dg
import pytest

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.sweden_ratsit.assets import (
    RATSIT_CLICKHOUSE_DATABASE,
    RATSIT_COMPANY_IDS,
    RATSIT_MAX_COMPANIES,
    RATSIT_RESULT_COLUMNS,
    RATSIT_RESULT_TABLE,
    RATSIT_S3_BUCKET,
    StoredRatsitReport,
    persist_ratsit_scan,
    ratsit_result_object_key,
    write_ratsit_scan,
)
from dagster_v3.defs.sweden_ratsit.parser import parse_company_page
from dagster_v3.defs.sweden_ratsit.resources import (
    RATSIT_BROWSER_WORKER_COUNT,
    RatsitCompanyFailure,
    RatsitCompanyReport,
    SwedenRatsitBrowserResource,
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
        self.ensured_buckets: list[str] = []

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

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket == RATSIT_S3_BUCKET
        self.objects[key] = body


class FakeRatsitResource:
    def __init__(
        self,
        results: list[RatsitCompanyReport | RatsitCompanyFailure],
    ) -> None:
        self.results = results
        self.requested_ids: tuple[str, ...] | None = None

    def iter_company_reports(self, company_ids: tuple[str, ...]):
        self.requested_ids = company_ids
        yield from self.results


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, sql: str, parameters: object | None = None):
        self.calls.append((sql, parameters))
        if "FROM system.tables" in sql:
            return [(RATSIT_RESULT_TABLE,)]
        return []


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def _ratsit_browser_resource() -> SwedenRatsitBrowserResource:
    return SwedenRatsitBrowserResource(
        crawl_proxy1=TEST_PROXY_URLS[0],
        crawl_proxy2=TEST_PROXY_URLS[1],
        crawl_proxy3=TEST_PROXY_URLS[2],
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


def test_browser_resource_runs_four_round_robin_workers_and_preserves_order() -> None:
    company_ids = RATSIT_COMPANY_IDS[:5]
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

    assert [result.company_id for result in reports] == list(company_ids)
    assert all(isinstance(result, RatsitCompanyReport) for result in reports)
    assert [
        (result.connection_mode, result.proxy_name) for result in reports
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


def test_browser_resource_rejects_more_than_one_hundred_companies_before_launch() -> (
    None
):
    launch_count = 0

    def launcher(**_: Any) -> FakeBrowser:
        nonlocal launch_count
        launch_count += 1
        raise AssertionError("browser must not start")

    with pytest.raises(ValueError, match="at most 100"):
        list(
            _ratsit_browser_resource().iter_company_reports(
                tuple(f"{i:010d}" for i in range(101)),
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


def test_scan_defaults_are_exactly_one_hundred_unique_companies() -> None:
    assert RATSIT_MAX_COMPANIES == 100
    assert len(RATSIT_COMPANY_IDS) == RATSIT_MAX_COMPANIES
    assert len(set(RATSIT_COMPANY_IDS)) == RATSIT_MAX_COMPANIES
    assert all(
        len(company_id) == 10 and company_id.isdigit()
        for company_id in RATSIT_COMPANY_IDS
    )
    assert RATSIT_COMPANY_IDS[:2] == ("5560004615", "5560125790")
    assert tuple(
        len(company_ids)
        for _, company_ids in ratsit_round_robin_assignments(RATSIT_COMPANY_IDS)
    ) == (25, 25, 25, 25)


def test_only_dispatch_asset_and_its_existing_job_name_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = repository.asset_graph.get_all_asset_keys()

    assert dg.AssetKey("se_ratsit_scan_dispatch") in asset_keys
    assert dg.AssetKey("se_ratsit_scan_coverage") not in asset_keys
    assert dg.AssetKey("se_ratsit_pilot_reports_s3") not in asset_keys
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "se_ratsit_scan_dispatch_job" in job_names
    assert "se_ratsit_scan_coverage_job" not in job_names
    assert "se_ratsit_scan_job" not in job_names
    assert "se_ratsit_pilot_reports_job" not in job_names


def test_scan_writes_run_scoped_results_and_only_parse_failures_keep_html() -> None:
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
    ratsit = FakeRatsitResource([report, parse_failure])

    summary = write_ratsit_scan(
        object_store=object_store,  # type: ignore[arg-type]
        ratsit=ratsit,  # type: ignore[arg-type]
        company_ids=(COMPANY_ID, "5560125790"),
        scan_id="scan-run-1",
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
