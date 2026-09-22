"""Exercise crawl input migrations against ClickHouse, including unmerged revisions."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command

MIGRATION = "000429_corpscout_website_crawl_requests"
TABLES = (
    "website_full_crawl_requests",
    "website_jobs_crawl_requests",
    "website_site_info_requests",
)
COLUMNS = (
    "domain",
    "website_url",
    "bucket",
    "enabled",
    "priority",
    "page_mode",
    "pages",
    "instructions",
    "headless",
    "proxy_route",
    "save_artifacts",
    "preset_version",
    "config_json",
    "source",
    "created_at",
    "updated_at",
    "revision",
)


@pytest.fixture(scope="module")
def migrations() -> tuple[str, str]:
    directory = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    return (
        (directory / f"{MIGRATION}.up.sql").read_text(encoding="utf-8"),
        (directory / f"{MIGRATION}.down.sql").read_text(encoding="utf-8"),
    )


def run_sql(sql: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        clickhouse_local_command(),
        input=sql,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.mark.parametrize("table", TABLES)
def test_defaults_and_current_schema(migrations: tuple[str, str], table: str) -> None:
    up, _ = migrations
    result = run_sql(
        up
        + f"""
        INSERT INTO corpscout.{table}
            (domain, website_url, created_at, updated_at, revision)
        VALUES ('novelic.com', 'https://novelic.com/',
            '2026-09-20 08:00:00.123456', '2026-09-20 08:00:00.123456', 1);
        SELECT * FROM corpscout.{table}_current FORMAT JSONEachRow;
        SELECT bucket = toUInt16(cityHash64(domain) % 256) AND bucket < 256
            FROM corpscout.{table}_current FORMAT JSONCompactEachRow;
        SELECT engine, sorting_key, partition_key
            FROM system.tables WHERE database = 'corpscout' AND name = '{table}'
            FORMAT JSONCompactEachRow;
    """
    )
    assert result.returncode == 0, result.stderr
    row, bucket_check, storage = [
        json.loads(line) for line in result.stdout.splitlines()
    ]
    assert tuple(row) == COLUMNS
    assert row == {
        "domain": "novelic.com",
        "website_url": "https://novelic.com/",
        "bucket": row["bucket"],
        "enabled": True,
        "priority": 50,
        "page_mode": "discover",
        "pages": [],
        "instructions": "",
        "headless": True,
        "proxy_route": "direct",
        "save_artifacts": True,
        "preset_version": 1,
        "config_json": "{}",
        "source": "domain_inventory",
        "created_at": "2026-09-20 08:00:00.123456",
        "updated_at": "2026-09-20 08:00:00.123456",
        "revision": 1,
    }
    assert bucket_check == [1]
    assert storage == ["ReplacingMergeTree", "bucket, domain", ""]


@pytest.mark.parametrize("table", TABLES)
def test_latest_revision_precedes_mutable_filters(
    migrations: tuple[str, str], table: str
) -> None:
    up, _ = migrations
    result = run_sql(
        up
        + f"""
        SYSTEM STOP MERGES corpscout.{table};
        INSERT INTO corpscout.{table}
            (domain, website_url, enabled, priority, revision)
        VALUES ('novelic.com', 'https://novelic.com/', true, 90, 1);
        INSERT INTO corpscout.{table}
            (domain, website_url, enabled, priority, revision)
        VALUES ('novelic.com', 'https://novelic.com/', true, 10, 2);
        SELECT count() FROM corpscout.{table}_current WHERE priority >= 50
            FORMAT JSONCompactEachRow;
        INSERT INTO corpscout.{table}
            (domain, website_url, enabled, priority, revision)
        VALUES ('novelic.com', 'https://novelic.com/', false, 10, 3);
        -- Late delivery of an older row must not restore enabled or high priority.
        INSERT INTO corpscout.{table}
            (domain, website_url, enabled, priority, revision)
        VALUES ('novelic.com', 'https://novelic.com/', true, 90, 1);
        SELECT count() FROM corpscout.{table} FORMAT JSONCompactEachRow;
        SELECT domain, enabled, priority, revision
            FROM corpscout.{table}_current FORMAT JSONCompactEachRow;
        SELECT count() FROM corpscout.{table}_current WHERE enabled
            FORMAT JSONCompactEachRow;
        SELECT count() FROM corpscout.{table}_current WHERE enabled AND priority >= 50
            FORMAT JSONCompactEachRow;
        INSERT INTO corpscout.{table}
            (domain, website_url, enabled, priority, revision)
        VALUES ('novelic.com', 'https://novelic.com/', true, 70, 4);
        SELECT domain, priority FROM corpscout.{table}_current WHERE enabled
            FORMAT JSONCompactEachRow;
    """
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [0],
        [4],
        ["novelic.com", False, 10, 3],
        [0],
        [0],
        ["novelic.com", 70],
    ]


@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize(
    ("column", "value", "constraint"),
    [
        ("domain", "''", "valid_domain"),
        ("domain", "'Novelic.com'", "valid_domain"),
        ("website_url", "''", "valid_website_url"),
        ("website_url", "'ftp://novelic.com/'", "valid_website_url"),
        ("priority", "101", "valid_priority"),
        ("revision", "0", "valid_revision"),
        ("preset_version", "0", "valid_revision"),
        ("page_mode", "'explicit'", "valid_explicit_pages"),
        ("config_json", "'not-json'", "valid_config"),
        ("config_json", "'[]'", "valid_config"),
        ("config_json", "'{\"refresh_interval_days\": 7}'", "asset_refresh_policy"),
        ("config_json", "'{\"refresh_interval_days\": null}'", "asset_refresh_policy"),
    ],
)
def test_invalid_inputs_are_rejected(
    migrations: tuple[str, str], table: str, column: str, value: str, constraint: str
) -> None:
    up, _ = migrations
    values = {
        "domain": "'novelic.com'",
        "website_url": "'https://novelic.com/'",
        "revision": "1",
    }
    values[column] = value
    result = run_sql(
        up
        + f"""
        INSERT INTO corpscout.{table} ({", ".join(values)})
        VALUES ({", ".join(values.values())});
    """
    )
    assert result.returncode != 0
    assert "VIOLATED_CONSTRAINT" in result.stderr, result.stderr
    assert constraint in result.stderr, result.stderr


@pytest.mark.parametrize("table", TABLES[:2])
def test_discovery_seeds_and_explicit_pages_are_preserved(
    migrations: tuple[str, str], table: str
) -> None:
    up, _ = migrations
    result = run_sql(
        up
        + f"""
        INSERT INTO corpscout.{table}
            (domain, website_url, page_mode, pages, instructions, config_json, priority, revision)
        VALUES
            ('novelic.com', 'https://novelic.com/', 'discover',
                ['https://novelic.com/careers/'], 'Find jobs', '{{"max_pages": 30}}', 0, 1),
            ('melexis.com', 'http://melexis.com/', 'explicit',
                ['https://melexis.com/about', 'https://melexis.com/jobs'], '', '{{}}', 100, 1);
        SELECT domain, page_mode, pages, instructions, config_json, priority
            FROM corpscout.{table}_current ORDER BY domain FORMAT JSONCompactEachRow;
    """
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [
            "melexis.com",
            "explicit",
            ["https://melexis.com/about", "https://melexis.com/jobs"],
            "",
            "{}",
            100,
        ],
        [
            "novelic.com",
            "discover",
            ["https://novelic.com/careers/"],
            "Find jobs",
            '{"max_pages": 30}',
            0,
        ],
    ]


@pytest.mark.parametrize("mode", ["discover", "explicit"])
def test_site_info_rejects_page_collection(
    migrations: tuple[str, str], mode: str
) -> None:
    up, _ = migrations
    result = run_sql(
        up
        + f"""
        INSERT INTO corpscout.website_site_info_requests
            (domain, website_url, page_mode, pages, revision)
        VALUES ('novelic.com', 'https://novelic.com/', '{mode}', ['https://novelic.com/jobs/'], 1);
    """
    )
    assert result.returncode != 0
    assert "VIOLATED_CONSTRAINT" in result.stderr, result.stderr
    assert "site_info_scope" in result.stderr, result.stderr


def test_migration_replay_and_rollback_preserve_unrelated_storage(
    migrations: tuple[str, str],
) -> None:
    up, down = migrations
    result = run_sql(
        up
        + """
        CREATE TABLE corpscout.existing_crawl_results (domain String) ENGINE = Memory;
        INSERT INTO corpscout.existing_crawl_results VALUES ('keep.example');
        INSERT INTO corpscout.website_full_crawl_requests (domain, website_url, revision)
            VALUES ('novelic.com', 'https://novelic.com/', 1);
    """
        + up
        + """
        SELECT count() FROM corpscout.website_full_crawl_requests_current FORMAT JSONCompactEachRow;
    """
        + down
        + down
        + """
        SELECT name FROM system.tables WHERE database = 'corpscout' FORMAT JSONCompactEachRow;
        SELECT domain FROM corpscout.existing_crawl_results FORMAT JSONCompactEachRow;
    """
        + up
        + """
        SELECT count() FROM system.tables WHERE database = 'corpscout' FORMAT JSONCompactEachRow;
    """
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [1],
        ["existing_crawl_results"],
        ["keep.example"],
        [7],
    ]
