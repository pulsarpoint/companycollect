"""Selection SQL and CrawlInputConfig against a disposable ClickHouse server, never production."""

import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from dagster_clickhouse import ClickhouseResource
from pydantic import ValidationError

from dagster_v3.defs.website_crawl.input import (
    INPUT_TABLES,
    TASK_DOMAINS,
    CrawlInputConfig,
)
from dagster_v3.defs.website_crawl.queue_input import (
    CrawlQueueInputConfig,
    load_crawl_draft,
)
from dagster_v3.defs.website_crawl.se_domains import SeDomainFilters
from tests.clickhouse_local import CLICKHOUSE_IMAGE, clickhouse_local_command
from tests.test_processing_store import processing_postgres_url, store  # noqa: F401

CRAWL_TYPES = ("full", "jobs", "site_info")
TARGETS = dict(zip(CRAWL_TYPES, INPUT_TABLES, strict=True))
# The entry-table migrations, in ledger order, applied once per module.
MIGRATIONS = (
    "000429_corpscout_website_crawl_requests.up.sql",
    "000431_corpscout_website_crawl_task_domains.up.sql",
    "000445_corpscout_crawl_draft_queue.up.sql",
    "000447_corpscout_crawl_queue_contract.up.sql",
)


@pytest.fixture(scope="module")
def server() -> Iterator[tuple[Client, ClickhouseResource]]:
    clickhouse_local_command()
    name = "crawl-input-test-" + uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            "127.0.0.1::9000",
            "-e",
            "CLICKHOUSE_USER=test",
            "-e",
            "CLICKHOUSE_PASSWORD=test",
            CLICKHOUSE_IMAGE,
        ],
        check=True,
        capture_output=True,
    )
    try:
        # docker can report the mapping a moment after `run -d` returns.
        port_output = ""
        port_deadline = time.monotonic() + 10
        while ":" not in port_output:
            if time.monotonic() > port_deadline:
                raise RuntimeError(f"no published port for {name}")
            port_output = subprocess.run(
                ["docker", "port", name, "9000"],
                capture_output=True,
                text=True,
            ).stdout
            if ":" not in port_output:
                time.sleep(0.2)
        resource = ClickhouseResource(
            host="127.0.0.1",
            port=int(port_output.strip().splitlines()[0].rsplit(":", 1)[1]),
            user="test",
            password="test",
            database="default",
        )
        # Probe inside the container so startup retries cannot hide query/driver failures.
        deadline = time.monotonic() + 30
        while True:
            probe = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "clickhouse-client",
                    "--user",
                    "test",
                    "--password",
                    "test",
                    "--query",
                    "SELECT 1",
                ],
                check=False,
                capture_output=True,
            )
            if probe.returncode == 0:
                break
            assert time.monotonic() < deadline, probe.stderr.decode()
            time.sleep(0.2)
        with resource.get_connection() as client:
            migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
            for name in MIGRATIONS:
                for statement in (
                    (migrations / name).read_text(encoding="utf-8").split(";")
                ):
                    if statement.strip():
                        client.execute(statement)
            yield client, resource
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


@pytest.fixture
def database(server, store):  # noqa: F811
    client, resource = server
    processing, _ = store
    for table in (*INPUT_TABLES, TASK_DOMAINS):
        client.execute(f"TRUNCATE TABLE {table}")
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_test_source")
    client.execute("""CREATE TABLE corpscout.crawl_test_source (
        company_id String, website Nullable(String), country String, active UInt8, version UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY company_id""")
    return client, resource, processing


def add(database, crawl_type="full", **selection):
    """Import a source selection into the open draft of ``crawl_type``."""
    _, resource, processing = database
    config = CrawlQueueInputConfig(
        crawl_type=crawl_type,
        **{
            "source_relation": "corpscout.crawl_test_source",
            "id_column": "company_id",
            "website_column": "website",
            **selection,
        },
    )
    return load_crawl_draft(config, str(uuid4()), processing, resource)


@pytest.mark.parametrize("crawl_type", CRAWL_TYPES)
def test_ids_and_filters_seed_each_destination_and_preserve_edits(database, crawl_type):
    client, *_ = database
    target = TARGETS[crawl_type]
    client.execute(f"SYSTEM STOP MERGES {target}")
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            ("1", " HTTPS://WWW.NOVELIC.COM./careers#jobs ", "SE", 1, 1),
            ("2", "https://novelic.com/", "SE", 1, 1),
            ("3", "melexis.com", "BE", 1, 1),
            ("4", "frame.work", "SE", 0, 1),
        ],
    )
    result = add(
        database,
        crawl_type,
        ids=["1", "2", "3", "4"],
        filters={"country": ["SE"], "active": ["1"]},
        priority=80,
    )
    assert result["total"] == 1
    assert client.execute(
        f"SELECT domain, website_url, priority, enabled, source, revision, bucket < 256 FROM {target}_current"
    ) == [
        (
            "novelic.com",
            "https://novelic.com/",
            80,
            True,
            "corpscout.crawl_test_source",
            1,
            1,
        )
    ]
    assert client.execute(
        f"SELECT countIf(created_at = updated_at AND created_at > toDateTime64('2026-01-01', 6)) FROM {target}"
    ) == [(1,)]
    assert client.execute(
        f"SELECT crawl_type, domain, website_url FROM {TASK_DOMAINS}"
    ) == [(crawl_type, "novelic.com", "https://novelic.com/")]
    client.execute(f"""INSERT INTO {target}
        SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision, 10 AS priority, 'operator' AS instructions)
        FROM {target}_current""")
    repeated = add(database, crawl_type, ids=["1", "2", "3"], priority=99)
    assert repeated["total"] == 2
    # The existing operator row keeps its settings; membership does not re-enable it.
    assert client.execute(
        f"SELECT domain, priority, enabled, instructions, revision FROM {target}_current ORDER BY domain"
    ) == [
        ("melexis.com", 99, True, "", 1),
        ("novelic.com", 10, False, "operator", 2),
    ]
    assert client.execute(
        f"SELECT count() FROM {target} WHERE domain='novelic.com' AND revision=1"
    ) == [(1,)]
    for other in set(INPUT_TABLES) - {target}:
        assert client.execute(f"SELECT count() FROM {other}") == [(0,)]


def test_filter_only_final_reads_and_parameter_binding(database):
    client, *_ = database
    client.execute("SYSTEM STOP MERGES corpscout.crawl_test_source")
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            ("1", "novelic.com", "SE", 1, 1),
            ("1", "novelic.com", "SE", 0, 2),
            ("2", "melexis.com", "BE", 1, 1),
            ("3", "frame.work", "US", 1, 1),
        ],
    )
    add(
        database,
        "full",
        source_final=True,
        filters={"active": ["1"], "country": ["SE", "BE"]},
    )
    assert client.execute(
        "SELECT domain, priority FROM corpscout.website_full_crawl_requests_current"
    ) == [("melexis.com", 50)]
    add(database, "jobs", ids=["1') OR 1=1 --"])
    add(database, "jobs", filters={"country": ["SE') OR 1=1 --"]})
    assert client.execute(
        "SELECT count() FROM corpscout.website_jobs_crawl_requests"
    ) == [(0,)]


def test_url_normalization_invalid_inputs_and_stable_limit(database):
    client, *_ = database
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            (str(index), website, "SE", 1, 1)
            for index, website in enumerate(
                [
                    "HTTPS://WWW.BÜCHER.DE.:8080/jobs?x=1#top",
                    "//Careers.Example.com/jobs",
                    "example.com",
                    "http://example.com/path",
                    None,
                    "",
                    "not a URL",
                    "https://user:password@example.com",
                    "ftp://bad.example/x",
                    "https://bad.example:99999",
                    "https://bad..example",
                    "https://-bad.example",
                    "https://bad.example:word",
                ]
            )
        ],
    )
    add(database, "full", select_all=True)
    assert client.execute(
        "SELECT domain, website_url FROM corpscout.website_full_crawl_requests_current ORDER BY domain"
    ) == [
        ("careers.example.com", "https://careers.example.com/jobs"),
        ("example.com", "https://example.com"),
        ("xn--bcher-kva.de", "https://www.xn--bcher-kva.de:8080/jobs?x=1"),
    ]
    add(database, "jobs", select_all=True, max_domains=1)
    assert client.execute(
        "SELECT domain FROM corpscout.website_jobs_crawl_requests_current"
    ) == [("careers.example.com",)]
    add(database, "jobs", select_all=True, max_domains=1)
    assert client.execute(
        "SELECT count() FROM corpscout.website_jobs_crawl_requests"
    ) == [(1,)]


def test_missing_source_columns_fail_before_writing(database):
    client, *_ = database
    with pytest.raises(ServerException):
        add(database, "full", filters={"not_a_column": ["value"]})
    assert client.execute(
        "SELECT count() FROM corpscout.website_full_crawl_requests"
    ) == [(0,)]
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS}") == [(0,)]


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"ids": []},
        {"filters": {}},
        {"max_domains": 10},
        {"ids": [""]},
        {"ids": ["1"], "source_relation": "corpscout.domains; DROP TABLE x"},
        {"ids": ["1"], "id_column": "domain) OR 1=1"},
        {"ids": ["1"], "website_column": "domain; SELECT 1"},
        {"filters": {"country)": ["SE"]}},
        {"filters": {"country": []}},
        {"select_all": True, "source_relation": "other.domains"},
        {"select_all": True, "source_relation": INPUT_TABLES[0] + "_current"},
        {"select_all": True, "priority": 101},
        {"select_all": True, "priority": -1},
        {"select_all": True, "max_domains": 0},
    ],
)
def test_invalid_selection_is_rejected(overrides):
    with pytest.raises(ValidationError):
        CrawlInputConfig(**{"source_relation": "corpscout.domains", **overrides})


@pytest.fixture
def se_domains(database):
    client, *_ = database
    client.execute("DROP TABLE IF EXISTS corpscout.se_company_domain")
    client.execute("""CREATE TABLE corpscout.se_company_domain (
        company_id String, root_domain String, sources Array(String), association String,
        active UInt8, confidence Float64, version UInt64
    ) ENGINE=ReplacingMergeTree(version) ORDER BY (company_id, root_domain)""")
    client.execute("SYSTEM STOP MERGES corpscout.se_company_domain")
    client.execute(
        "INSERT INTO corpscout.se_company_domain VALUES",
        [
            ("100", "shared.example", ["brave"], "connected", 1, 0.8, 1),
            ("100", "shared.example", ["brave"], "connected", 0, 0.5, 2),
            ("200", "shared.example", ["wikidata"], "connected", 1, 0.9, 1),
            ("100", "solo.example", ["brave", "brave"], "uncertain", 1, 0.7, 1),
            ("300", "rejected.example", ["esef_filing"], "not_connected", 0, 0.2, 1),
            (
                "400",
                "second.example",
                ["common_crawl_identity"],
                "connected",
                1,
                0.65,
                1,
            ),
        ],
    )
    return database


SE_SOURCE = {
    "source_relation": "corpscout.se_company_domain",
    "source_final": True,
    "id_column": "root_domain",
    "website_column": "root_domain",
}


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"domain": "solo"}, ["solo.example"]),
        ({"company": "100"}, ["shared.example", "solo.example"]),
        ({"source": "brave"}, ["shared.example", "solo.example"]),
        ({"association": "not_connected"}, ["rejected.example"]),
        ({"status": "inactive"}, ["rejected.example", "shared.example"]),
        ({"min_confidence": 0.75}, ["shared.example"]),
        ({"max_confidence": 0.3}, ["rejected.example"]),
        (
            {"min_confidence": 0.6, "max_confidence": 0.8},
            ["second.example", "solo.example"],
        ),
        ({"shared": True}, ["shared.example"]),
        ({"source": "brave", "status": "active"}, ["solo.example"]),
        ({"domain": "' OR 1=1 --"}, []),
        (
            {
                "domain": "shared",
                "company": "200",
                "source": "wikidata",
                "association": "connected",
                "status": "active",
                "min_confidence": 0.85,
                "max_confidence": 0.95,
                "shared": True,
            },
            ["shared.example"],
        ),
    ],
)
def test_se_domain_criteria_match_current_entity(se_domains, filters, expected):
    client, *_ = se_domains
    add(se_domains, "full", **SE_SOURCE, se_domain_filters=filters)
    assert client.execute(
        "SELECT domain FROM corpscout.website_full_crawl_requests_current ORDER BY domain"
    ) == [(domain,) for domain in expected]


def test_se_domain_query_exclusions_apply_to_whole_domain(se_domains):
    client, *_ = se_domains
    add(
        se_domains,
        "jobs",
        **SE_SOURCE,
        select_all=True,
        se_domain_filters={"status": "active"},
        excluded_ids=["shared.example"],
    )
    assert client.execute(
        "SELECT domain FROM corpscout.website_jobs_crawl_requests_current ORDER BY domain"
    ) == [("second.example",), ("solo.example",)]
    add(se_domains, "site_info", **SE_SOURCE, ids=["shared.example", "shared.example"])
    assert client.execute(
        "SELECT domain FROM corpscout.website_site_info_requests_current"
    ) == [("shared.example",)]


@pytest.mark.parametrize(
    "filters",
    [
        {"source": "unknown"},
        {"status": "all"},
        {"association": "bad"},
        {"min_confidence": -1},
        {"max_confidence": 2},
        {"min_confidence": 0.9, "max_confidence": 0.5},
        {"company": "1 OR 1=1"},
    ],
)
def test_invalid_se_domain_criteria_are_rejected(filters):
    with pytest.raises(ValidationError):
        SeDomainFilters(**filters)


def test_se_filters_require_correct_source_identity_and_final():
    for overrides in (
        {"source_final": False},
        {"website_column": "website_url"},
        {"source_relation": "corpscout.domains"},
    ):
        with pytest.raises(ValidationError):
            CrawlInputConfig(
                **{
                    **SE_SOURCE,
                    "se_domain_filters": SeDomainFilters(source="brave"),
                    **overrides,
                }
            )
