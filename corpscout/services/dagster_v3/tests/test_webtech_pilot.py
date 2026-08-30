import asyncio
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import dagster as dg
import pytest
from playwright.async_api import Error as PlaywrightError
from pydantic import ValidationError

from dagster_v3.defs.webtech import scanner as webtech_scanner
from dagster_v3.defs.webtech.assets import (
    WEBTECH_BROWSER_POOL,
    WEBTECH_PILOT_PARTITION_KEY,
    WEBTECH_PARTITIONS,
    WebtechScanConfig,
    commoncrawl_webtech_scan_job,
    commoncrawl_webtech_scan_results,
    load_webtech_candidates,
)
from dagster_v3.defs.webtech.definitions import defs as webtech_defs
from dagster_v3.defs.webtech.models import (
    ExtensionReport,
    WebtechCandidate,
    WebtechDomainResult,
)
from dagster_v3.defs.webtech.scanner import (
    ExtensionReportRouter,
    WebtechScannerSettings,
    _navigate_page,
    scan_webtech_candidates,
)
from dagster_v3.defs.webtech.storage import (
    WEBTECH_RESULT_COLUMNS,
    WebtechS3Destination,
    parse_webtech_s3_path,
    persist_webtech_results,
    webtech_result_object_key,
)

CRAWL_ID = "CC-MAIN-2026-apr-may-jun"
SCANNED_AT = datetime(2026, 8, 29, 8, 45, tzinfo=UTC)


class FakeClickhouseClient:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> list[tuple[Any, ...]]:
        self.calls.append((sql, parameters))
        return self.rows


class FakeClickhouse:
    def __init__(self, client: FakeClickhouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeObjectStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.objects: dict[tuple[str, str], str] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == "webtech"
        self.events.append("s3:ensure")

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        assert bucket == "webtech"
        self.events.append("s3:write")
        self.objects[(bucket, key)] = body


def _report(*, page_token: str, url: str) -> ExtensionReport:
    return ExtensionReport.model_validate(
        {
            "schema_version": 2,
            "analysis_complete": True,
            "extension_version": "1.3.0",
            "page_token": page_token,
            "url": url,
            "technologies": [
                {
                    "name": "React",
                    "slug": "react",
                    "categories": [
                        {"id": 12, "name": "JavaScript frameworks", "slug": "javascript-frameworks"}
                    ],
                    "confidence": 100,
                    "version": "18.3.1",
                }
            ],
        }
    )


def test_webtech_pilot_has_one_top_1000_partition() -> None:
    assert WEBTECH_PILOT_PARTITION_KEY == "harmonic_top_1000"
    assert WEBTECH_PARTITIONS.get_partition_keys() == [WEBTECH_PILOT_PARTITION_KEY]


def test_pilot_config_is_hard_limited_to_harmonic_top_1000() -> None:
    config = WebtechScanConfig(crawl_id=CRAWL_ID)

    assert config.max_harmonic_rank == 1_000
    assert config.page_worker_count == 10
    assert config.report_timeout_seconds == 120.0
    assert config.force_rescan is False

    with pytest.raises(ValidationError, match="less than or equal to 1000"):
        WebtechScanConfig(crawl_id=CRAWL_ID, max_harmonic_rank=1_001)

    with pytest.raises(ValidationError, match="valid Common Crawl ID"):
        WebtechScanConfig(crawl_id="latest")


def test_candidate_query_uses_requested_crawl_and_rank_without_hash_filter() -> None:
    client = FakeClickhouseClient(
        rows=[
            ("example.com", 1),
            ("test39.com", 2),
        ]
    )

    candidates = load_webtech_candidates(
        FakeClickhouse(client),
        partition_key=WEBTECH_PILOT_PARTITION_KEY,
        crawl_id=CRAWL_ID,
        max_harmonic_rank=1_000,
        force_rescan=False,
    )

    assert candidates == (
        WebtechCandidate(root_domain="example.com", harmonic_rank=1),
        WebtechCandidate(root_domain="test39.com", harmonic_rank=2),
    )
    query, parameters = client.calls[-1]
    assert "commoncrawl_domain_graph_signals FINAL" in query
    assert "cc_harmonic_rank BETWEEN 1 AND %(max_harmonic_rank)s" in query
    assert "CRC32" not in query
    assert "webtech_domain_scan_results FINAL" in query
    assert "ORDER BY cc_harmonic_rank, root_domain" in query
    assert parameters == {
        "crawl_id": CRAWL_ID,
        "detector_version": "mywappalyzer-1.3.0",
        "max_harmonic_rank": 1_000,
    }


def test_extension_report_requires_the_final_schema_v2_message() -> None:
    page_token = "82b2f177-8143-4103-9ebf-3acc16037819"
    report = _report(page_token=page_token, url="https://example.com/")

    assert report.analysis_complete is True
    assert report.page_token == UUID(page_token)
    assert report.technologies[0].name == "React"

    invalid = report.model_dump(mode="json")
    invalid["analysis_complete"] = False
    with pytest.raises(ValidationError, match="analysis_complete"):
        ExtensionReport.model_validate(invalid)


def test_report_router_keeps_identical_redirect_urls_separate_by_page_token() -> None:
    first_token = "82b2f177-8143-4103-9ebf-3acc16037819"
    second_token = "68ee14e1-2054-4546-a4eb-035ad5c70ebd"

    async def route_reports() -> tuple[ExtensionReport, ExtensionReport]:
        router = ExtensionReportRouter(asyncio.get_running_loop())
        first_waiter = router.register(UUID(first_token))
        second_waiter = router.register(UUID(second_token))

        assert router.accept(
            _report(page_token=second_token, url="https://redirect.example/")
        )
        assert router.accept(
            _report(page_token=first_token, url="https://redirect.example/")
        )

        return await asyncio.gather(first_waiter, second_waiter)

    first, second = asyncio.run(route_reports())

    assert first.page_token == UUID(first_token)
    assert second.page_token == UUID(second_token)


def test_report_router_buffers_a_report_that_arrives_before_registration() -> None:
    page_token = "82b2f177-8143-4103-9ebf-3acc16037819"

    async def route_early_report() -> ExtensionReport:
        router = ExtensionReportRouter(asyncio.get_running_loop())
        report = _report(page_token=page_token, url="https://example.com/")

        assert router.accept(report)
        return await router.register(UUID(page_token))

    report = asyncio.run(route_early_report())

    assert report.page_token == UUID(page_token)
    assert report.url == "https://example.com/"


def test_scanner_replaces_the_context_after_each_worker_sized_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts: list[Any] = []
    profile_dirs: list[str] = []
    completed_domains: list[str] = []

    class FakePage:
        def __init__(self) -> None:
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    class FakeContext:
        pages: tuple[FakePage, ...] = ()

        def __init__(self) -> None:
            self.created_pages: list[FakePage] = []
            self.closed = False

        async def new_page(self) -> FakePage:
            page = FakePage()
            self.created_pages.append(page)
            return page

        async def close(self) -> None:
            self.closed = True

    async def launch_context(profile_dir: str, **_: Any) -> FakeContext:
        if contexts:
            assert contexts[-1].closed is True
        context = FakeContext()
        contexts.append(context)
        profile_dirs.append(profile_dir)
        return context

    async def scan_candidate(
        context: FakeContext,
        page: FakePage,
        candidate: WebtechCandidate,
        **_: Any,
    ) -> tuple[WebtechDomainResult, FakePage]:
        del context
        return (
            WebtechDomainResult.failure(
                candidate=candidate,
                outcome="browser_error",
                requested_url=f"https://{candidate.root_domain}",
                final_url="",
                scanned_at=SCANNED_AT,
                duration_ms=1,
                http_fallback_used=False,
                error_message="test result",
            ),
            page,
        )

    monkeypatch.setattr(
        webtech_scanner,
        "launch_persistent_context_async",
        launch_context,
    )
    monkeypatch.setattr(webtech_scanner, "_scan_candidate", scan_candidate)
    candidates = tuple(
        WebtechCandidate(root_domain=f"example{rank}.com", harmonic_rank=rank)
        for rank in range(1, 24)
    )

    results = asyncio.run(
        scan_webtech_candidates(
            candidates,
            settings=WebtechScannerSettings(
                headless=True,
                page_worker_count=10,
                navigation_timeout_seconds=60,
                report_timeout_seconds=120,
            ),
            progress_callback=lambda result: completed_domains.append(
                result.candidate.root_domain
            ),
        )
    )

    assert [len(context.created_pages) for context in contexts] == [10, 10, 3]
    assert all(context.closed for context in contexts)
    assert len(set(profile_dirs)) == 3
    assert tuple(result.candidate for result in results) == candidates
    assert completed_domains == [candidate.root_domain for candidate in candidates]


def test_navigation_accepts_a_redirect_that_settles_after_interruption() -> None:
    class RedirectingPage:
        url = "https://www.example.com/"
        wait_for_load_state_calls: list[dict[str, Any]] = []

        async def goto(self, url: str, **kwargs: Any) -> None:
            raise PlaywrightError(
                f'Navigation to "{url}" is interrupted by another navigation '
                'to "https://www.example.com/"'
            )

        async def wait_for_load_state(self, **kwargs: Any) -> None:
            self.wait_for_load_state_calls.append(kwargs)

    page: Any = RedirectingPage()
    asyncio.run(
        _navigate_page(
            page,
            "https://example.com",
            timeout_seconds=60,
        )
    )

    assert page.wait_for_load_state_calls == [
        {"state": "domcontentloaded", "timeout": 5_000}
    ]


def test_webtech_s3_path_and_result_key_are_explicit() -> None:
    assert parse_webtech_s3_path("s3://webtech/wappalyzer/pilot") == (
        WebtechS3Destination(bucket="webtech", prefix="wappalyzer/pilot")
    )
    assert (
        webtech_result_object_key(
            destination=WebtechS3Destination(
                bucket="webtech",
                prefix="wappalyzer/pilot",
            ),
            crawl_id=CRAWL_ID,
            root_domain="example.com",
        )
        == "wappalyzer/pilot/crawl_id=CC-MAIN-2026-apr-may-jun/root_domain=example.com/report.json"
    )

    with pytest.raises(ValueError, match="s3://bucket/prefix"):
        parse_webtech_s3_path("https://webtech/pilot")


def test_persistence_writes_json_before_clickhouse_index() -> None:
    events: list[str] = []
    object_store = FakeObjectStore(events)

    class OrderedClickhouseClient(FakeClickhouseClient):
        def execute(self, sql: str, parameters: Any = None) -> list[tuple[Any, ...]]:
            events.append("clickhouse:write")
            return super().execute(sql, parameters)

    client = OrderedClickhouseClient()
    report = _report(
        page_token="82b2f177-8143-4103-9ebf-3acc16037819",
        url="https://www.example.com/",
    )
    result = WebtechDomainResult.success(
        candidate=WebtechCandidate(root_domain="example.com", harmonic_rank=1),
        requested_url="https://example.com",
        final_url=report.url,
        report=report,
        scanned_at=SCANNED_AT,
        duration_ms=7_250,
    )

    stored = persist_webtech_results(
        clickhouse=FakeClickhouse(client),
        object_store=object_store,
        destination=WebtechS3Destination(
            bucket="webtech",
            prefix="wappalyzer/pilot",
        ),
        crawl_id=CRAWL_ID,
        partition_key=WEBTECH_PILOT_PARTITION_KEY,
        run_id="pilot-run-1",
        results=(result,),
    )

    assert events == ["s3:ensure", "s3:write", "clickhouse:write"]
    assert stored[0].s3_uri.startswith("s3://webtech/wappalyzer/pilot/")
    document = json.loads(next(iter(object_store.objects.values())))
    assert document["candidate"]["root_domain"] == "example.com"
    assert document["report"]["schema_version"] == 2


def test_asset_uses_one_browser_pool_and_one_pilot_partition() -> None:
    assert commoncrawl_webtech_scan_results.partitions_def is WEBTECH_PARTITIONS
    assert commoncrawl_webtech_scan_results.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert commoncrawl_webtech_scan_results.op.pool == WEBTECH_BROWSER_POOL


def test_webtech_definitions_register_the_asset_and_job() -> None:
    assert commoncrawl_webtech_scan_results in webtech_defs.assets
    assert commoncrawl_webtech_scan_job in webtech_defs.jobs


def test_clickhouse_migration_matches_the_insert_column_contract() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000352_corpscout_webtech_domain_scan_results.up.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_scan_results" in migration
    positions = [migration.index(f"    {column} ") for column in WEBTECH_RESULT_COLUMNS]
    assert positions == sorted(positions)


def test_extension_is_packaged_with_the_dagster_definition() -> None:
    extension_dir = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "webtech"
        / "extension"
    )

    assert (extension_dir / "manifest.json").is_file()
    assert (extension_dir / "runtime-config.json").exists() is False
    assert len(tuple((extension_dir / "technologies").glob("*.json"))) == 27
