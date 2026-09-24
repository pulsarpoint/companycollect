import json
import subprocess
from pathlib import Path

import pytest
from tests.clickhouse_local import clickhouse_local_command
from dagster_v3.defs.webtech.pages import page_identity


@pytest.mark.parametrize(
    ("url", "origin", "page"),
    [
        ("HTTPS://EXAMPLE.com.:443#top", "https://example.com", "https://example.com/"),
        (
            "https://BÜCHER.de:8443/A%2Fb/?x=2&x=1#top",
            "https://xn--bcher-kva.de:8443",
            "https://xn--bcher-kva.de:8443/A%2Fb/?x=2&x=1",
        ),
        ("http://[2001:db8::1]:80", "http://[2001:db8::1]", "http://[2001:db8::1]/"),
        (
            "https://www.example.com/?",
            "https://www.example.com",
            "https://www.example.com/?",
        ),
    ],
)
def test_page_identity(url, origin, page):
    assert page_identity(url) == (origin, page)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "/admin",
        "ftp://example.com/",
        "https://user:pass@example.com",
        "https://example.com:99999/",
        "https://example.com/a\nb",
    ],
)
def test_invalid_page_identity(url):
    with pytest.raises(ValueError):
        page_identity(url)


def test_page_history_in_clickhouse():
    migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
    ddl = (migrations / "000436_corpscout_webtech_pages.up.sql").read_text()
    sql = """
    INSERT INTO corpscout.webtech_domain_scan_results_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,scanned_at,outcome,technology_count)
    VALUES
      ('example.com','https://example.com','https://example.com/','c1','d1','s1',repeat('a',64),'2026-09-01','success',1),
      ('example.com','https://example.com','https://example.com/admin','c1','d1','s1',repeat('b',64),'2026-09-01','success',1);
    INSERT INTO corpscout.webtech_domain_technologies_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,detected_name,version,catalog_match)
    VALUES
      ('example.com','https://example.com','https://example.com/','c1','d1','s1',repeat('a',64),'React','18','exact'),
      ('example.com','https://example.com','https://example.com/admin','c1','d1','s1',repeat('b',64),'React','19','exact');
    SELECT page_url,version FROM corpscout.webtech_domain_technologies_current_v2 ORDER BY page_url FORMAT JSONCompactEachRow;
    INSERT INTO corpscout.webtech_domain_scan_results_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,scanned_at,outcome,technology_count)
    VALUES ('example.com','https://example.com','https://example.com/','c2','d2','s2',repeat('c',64),'2026-09-02','success',0);
    SELECT page_url,version FROM corpscout.webtech_domain_technologies_current_v2 ORDER BY page_url FORMAT JSONCompactEachRow;
    INSERT INTO corpscout.webtech_domain_scan_results_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,scanned_at,outcome,technology_count)
    VALUES ('example.com','https://example.com','https://example.com/admin','c2','d2','s2',repeat('d',64),'2026-09-02','error',0);
    INSERT INTO corpscout.webtech_domain_scan_results_v2 SELECT * FROM corpscout.webtech_domain_scan_results_v2;
    SELECT count() FROM corpscout.webtech_domain_technologies_current_v2 FORMAT JSONCompactEachRow;
    SELECT count() FROM corpscout.webtech_domain_scan_results_v2 FINAL FORMAT JSONCompactEachRow;
    SELECT count() FROM corpscout.webtech_domain_technologies_v2 FINAL FORMAT JSONCompactEachRow;
    """
    result = subprocess.run(
        clickhouse_local_command(),
        input=(ddl + (migrations / "000437_corpscout_webtech_page_current_lookup.up.sql").read_text()) + sql,
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        ["https://example.com/", "18"],
        ["https://example.com/admin", "19"],
        ["https://example.com/admin", "19"],
        [0],
        [4],
        [2],
    ]


def test_conflicting_reports_are_rejected_before_insert():
    from dagster_v3.defs.webtech.writes import PAGE_KEY, validate_report_identities

    class Existing:
        def execute(self, sql, params):
            return [
                (
                    "example.com",
                    "https://example.com",
                    "https://example.com/",
                    "c",
                    "d",
                    "s",
                    "old",
                )
            ]

    with pytest.raises(ValueError, match="Conflicting stored"):
        validate_report_identities(
            Existing(),
            "webtech_domain_scan_results",
            (*PAGE_KEY, "report_sha256"),
            [
                (
                    "example.com",
                    "https://example.com",
                    "https://example.com/",
                    "c",
                    "d",
                    "s",
                    "new",
                )
            ],
        )


def test_dual_write_failure_never_publishes_shadow_marker(monkeypatch):
    from tests.test_webtech_pilot import (
        FakeClickhouseClient,
        FakeClickhouse,
        stored_scan,
        technology_report,
    )
    from dagster_v3.defs.webtech.storage import index_final_results

    monkeypatch.setenv("WEBTECH_WRITE_MODE", "dual")

    class Failing(FakeClickhouseClient):
        def execute(self, sql, parameters=None, *, settings=None):
            if sql.startswith("INSERT INTO"):
                assert settings == {"async_insert": 0}
            if "INSERT INTO corpscout.webtech_domain_technologies_v2 " in sql:
                raise RuntimeError("injected shadow failure")
            return super().execute(sql, parameters, settings=settings)

    client = Failing()
    store, destination, reference = stored_scan(technology_report())
    with pytest.raises(RuntimeError, match="injected"):
        index_final_results(
            clickhouse=FakeClickhouse(client),
            object_store=store,
            destination=destination,
            reference=reference,
            dagster_run_id="test",
        )
    inserts = [sql for sql, _ in client.calls if "INSERT INTO" in sql]
    assert len(inserts) == 2
    assert not any("scan_results_v2" in sql for sql in inserts)
    # Retry repeats the same identities, both destinations must acknowledge.
    healthy = FakeClickhouseClient()
    index_final_results(
        clickhouse=FakeClickhouse(healthy),
        object_store=store,
        destination=destination,
        reference=reference,
        dagster_run_id="retry",
    )
    assert len([sql for sql, _ in healthy.calls if "INSERT INTO" in sql]) == 4


def test_current_partial_unpublished_legacy_and_late_scans():
    migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
    ddl = (migrations / "000436_corpscout_webtech_pages.up.sql").read_text()
    sql = """
    INSERT INTO corpscout.webtech_domain_scan_results_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,scanned_at,recorded_at,outcome,technology_count,final_hostname)
    VALUES
      ('example.com','https://example.com','https://example.com/','c1','d1','',repeat('a',64),'2026-09-01','2026-09-24','success',1,'old.example.com'),
      ('example.com','https://example.com','https://example.com/','c2','d2','',repeat('b',64),'2026-09-02','2026-09-02','partial',1,'new.example.com');
    INSERT INTO corpscout.webtech_domain_technologies_v2
      (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,detected_name,version,catalog_match,analysis_status,analysis_complete,final_hostname)
    VALUES
      ('example.com','https://example.com','https://example.com/','c1','d1','',repeat('a',64),'React','18','exact','complete',1,'old.example.com'),
      ('example.com','https://example.com','https://example.com/','c2','d2','',repeat('b',64),'React','19','exact','partial',0,'new.example.com'),
      ('example.com','https://example.com','https://example.com/','c3','d3','unpublished',repeat('c',64),'React','20','exact','complete',1,'unpublished.example.com');
    SELECT version,analysis_status,analysis_complete FROM corpscout.webtech_domain_technologies_current_v2 FORMAT JSONCompactEachRow;
    SELECT count() FROM corpscout.webtech_domain_technologies_current_v2 WHERE final_hostname='old.example.com' FORMAT JSONCompactEachRow;
    SELECT count() FROM corpscout.webtech_domain_scan_results_v2 FINAL FORMAT JSONCompactEachRow;
    INSERT INTO corpscout.webtech_domain_scan_results_v2
    SELECT * FROM corpscout.webtech_domain_scan_results_v2 WHERE crawl_id='c1';
    SELECT version FROM corpscout.webtech_domain_technologies_current_v2 FORMAT JSONCompactEachRow;
    """
    result = subprocess.run(
        clickhouse_local_command(),
        input=(ddl + (migrations / "000437_corpscout_webtech_page_current_lookup.up.sql").read_text()) + sql,
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        ["19", "partial", 0],
        [0],
        [2],
        ["19"],
    ]


def test_cutover_retry_detects_completed_exchange():
    import runpy

    script = runpy.run_path(
        str(Path(__file__).resolve().parents[1] / "scripts/migrate-webtech-pages.py")
    )

    class Tables:
        def __init__(self):
            self.ids = {"table": "old", "table_v2": "new"}
            self.exchanges = 0

        def execute(self, sql):
            if sql.startswith("EXCHANGE"):
                self.exchanges += 1
                self.ids = {"table": "new", "table_v2": "old"}
            else:
                return list(self.ids.items())

    client = Tables()
    script["exchange_pair"](client, "table", "old", "new")
    # Simulate crash after exchange, before ledger save.
    script["exchange_pair"](client, "table", "old", "new")
    assert client.exchanges == 1
    client.ids["table"] = "unexpected"
    with pytest.raises(RuntimeError, match="Unexpected UUIDs"):
        script["exchange_pair"](client, "table", "old", "new")


def test_migration_has_every_writer_column():
    import re
    from dagster_v3.defs.webtech.storage import WEBTECH_RESULT_COLUMNS
    from dagster_v3.defs.webtech.technologies import WEBTECH_TECHNOLOGY_COLUMNS

    ddl = (
        Path(__file__).resolve().parents[3]
        / "clickhouse/migrations/000436_corpscout_webtech_pages.up.sql"
    ).read_text()
    for table, columns in [
        ("webtech_domain_scan_results_v2", WEBTECH_RESULT_COLUMNS),
        ("webtech_domain_technologies_v2", WEBTECH_TECHNOLOGY_COLUMNS),
    ]:
        body = ddl.split(f"CREATE TABLE IF NOT EXISTS corpscout.{table}", 1)[1].split(
            "ENGINE =", 1
        )[0]
        physical_columns = re.findall(
            r"^    ([a-z_][a-z0-9_]*) (?:String|LowCardinality|UInt\d+|Nullable|Array|FixedString|DateTime64)",
            body,
            re.MULTILINE,
        )
        if table == "webtech_domain_scan_results_v2":
            addition = (Path(__file__).resolve().parents[3] / "clickhouse/migrations/000438_corpscout_webtech_scan_input.up.sql").read_text()
            for column in ("task_id", "input_id"):
                assert f"ADD COLUMN IF NOT EXISTS {column} String" in addition
            physical_columns.extend(["task_id", "input_id"])
        assert set(physical_columns) == set(columns)
