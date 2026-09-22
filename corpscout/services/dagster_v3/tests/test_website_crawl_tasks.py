"""Brave-style crawl tasks: frozen selections, whole-task processing and resume.

Runs against disposable ClickHouse and PostgreSQL servers plus the crawler HTTP
fixture from the results tests, never production.
"""

from contextlib import closing
from pathlib import Path
from uuid import uuid4

import dagster as dg
import psycopg2
import pytest
from pydantic import ValidationError

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.assets import website_site_info_requests
from dagster_v3.defs.website_crawl.input import INPUT_TABLES, TASK_DOMAINS
from dagster_v3.defs.website_crawl.results import (
    RESULTS_BY_TYPE,
    SUBMISSIONS,
    CrawlResultsConfig,
)
from dagster_v3.defs.website_crawl.results_assets import website_site_info_results
from tests.test_processing_store import processing_postgres_url, store  # noqa: F401
from tests.test_website_crawl_input_assets import server  # noqa: F401
from tests.test_website_crawl_results import crawler  # noqa: F401

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
SITE_INFO = INPUT_TABLES[2]
SETTINGS = {
    "challenge_agent_model": "deepseek-flash",
    "challenge_agent_max_runs": 3,
    "api": "deepseek",
    "model": "deepseek-flash",
    "max_pages": 1,
    "max_model_calls": 20,
    "page_selection": "basic_info",
    "poll_interval_seconds": 0.01,
}


@pytest.fixture
def db(server, store):  # noqa: F811
    client, resource = server
    _, dsn = store
    for name in (
        "000430_corpscout_website_crawl_type_results.up.sql",
        "000431_corpscout_website_crawl_task_domains.up.sql",
    ):
        for statement in (MIGRATIONS / name).read_text().split(";"):
            if statement.strip():
                client.execute(statement)
    for table in (*INPUT_TABLES, *RESULTS_BY_TYPE.values(), SUBMISSIONS, TASK_DOMAINS):
        client.execute(f"TRUNCATE TABLE {table}")
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_task_source")
    client.execute("""CREATE TABLE corpscout.crawl_task_source (
        company_id String, website String, country String
    ) ENGINE = MergeTree ORDER BY company_id""")
    client.execute(
        "INSERT INTO corpscout.crawl_task_source VALUES",
        [(str(n), f"https://d{n}.example/", "SE") for n in range(1, 6)]
        + [("9", "https://abroad.example/", "NO")],
    )
    return client, resource, ProcessingResource(postgres_url=dsn)


def selection(**extra) -> dict:
    return {
        "source_relation": "corpscout.crawl_task_source",
        "id_column": "company_id",
        "website_column": "website",
        "filters": {"country": ["SE"]},
        **extra,
    }


def materialize(db, assets, ops, instance=None, raise_on_error=True):
    _, resource, processing = db
    return dg.materialize(
        assets,
        instance=instance,
        resources={"clickhouse": resource, "processing": processing},
        run_config={"ops": {name: {"config": config} for name, config in ops.items()}},
        raise_on_error=raise_on_error,
    )


def task_row(processing_url: str, task_id: str) -> dict | None:
    with (
        closing(psycopg2.connect(processing_url)) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT processor, status, total FROM processing.tasks WHERE task_id=%s",
            (task_id,),
        )
        row = cursor.fetchone()
    return (
        dict(zip(("processor", "status", "total"), row, strict=True)) if row else None
    )


def test_input_freezes_the_selection_under_its_task(db):
    client, _, processing = db
    client.execute(f"""INSERT INTO {SITE_INFO} (domain, website_url, enabled, priority, revision)
        VALUES ('d1.example', 'https://d1.example/', false, 10, 7)""")
    task_id = str(uuid4())
    result = materialize(
        db,
        [website_site_info_requests],
        {"website_site_info_requests": selection(task_id=task_id)},
    )
    metadata = result.asset_materializations_for_node("website_site_info_requests")[
        0
    ].metadata
    assert metadata["task_id"].value == task_id
    assert metadata["selected_domains"].value == 5
    assert metadata["inserted_domains"].value == 4
    assert client.execute(
        f"SELECT crawl_type, domain FROM {TASK_DOMAINS} FINAL WHERE task_id=%(t)s ORDER BY domain",
        {"t": task_id},
    ) == [("site_info", f"d{n}.example") for n in range(1, 6)]
    # The existing operator row keeps its settings; membership does not re-enable it.
    assert client.execute(
        f"SELECT enabled, priority, revision FROM {SITE_INFO}_current WHERE domain='d1.example'"
    ) == [(False, 10, 7)]
    assert task_row(processing.postgres_url, task_id) == {
        "processor": "website-crawl-site_info-v1",
        "status": "selected",
        "total": 5,
    }


def test_same_task_is_reused_and_a_changed_selection_is_rejected(db):
    client, _, _ = db
    task_id = str(uuid4())
    ops = {"website_site_info_requests": selection(task_id=task_id)}
    assert materialize(db, [website_site_info_requests], ops).success
    client.execute(
        "INSERT INTO corpscout.crawl_task_source VALUES ('6', 'https://d6.example/', 'SE')"
    )
    # A rerun of the same task keeps its frozen membership, even if the source grew.
    assert materialize(db, [website_site_info_requests], ops).success
    assert client.execute(
        f"SELECT count() FROM {TASK_DOMAINS} FINAL WHERE task_id=%(t)s", {"t": task_id}
    ) == [(5,)]
    changed = {"website_site_info_requests": selection(task_id=task_id, max_domains=2)}
    with pytest.raises(ValueError, match="different selection"):
        materialize(db, [website_site_info_requests], changed)


def test_workflow_processes_the_whole_task_in_batches(db, crawler):  # noqa: F811
    client, _, _ = db
    saved, _, _ = crawler
    client.execute(f"""INSERT INTO {SITE_INFO} (domain, website_url, priority, revision)
        VALUES ('outside.example', 'https://outside.example/', 100, 1)""")
    result = materialize(
        db,
        [website_site_info_requests, website_site_info_results],
        {
            "website_site_info_requests": selection(),
            # batch_size * max_batches is 2, yet the task has 5 domains: task mode
            # works through the whole frozen selection.
            "website_site_info_results": {
                **SETTINGS,
                "batch_size": 2,
                "max_batches": 1,
            },
        },
    )
    task_id = (
        result.asset_materializations_for_node("website_site_info_requests")[0]
        .metadata["task_id"]
        .value
    )
    metadata = result.asset_materializations_for_node("website_site_info_results")[
        0
    ].metadata
    assert metadata["task_id"].value == task_id
    assert metadata["selected_domains"].value == 5
    assert metadata["completed"].value == 5
    assert sorted(payload["url"] for payload in saved.values()) == [
        f"https://d{n}.example/" for n in range(1, 6)
    ]
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL WHERE domain='outside.example'"
    ) == [(0,)]


def test_disabled_members_are_skipped(db, crawler):  # noqa: F811
    client, _, _ = db
    saved, _, _ = crawler
    client.execute(f"""INSERT INTO {SITE_INFO} (domain, website_url, enabled, revision)
        VALUES ('d2.example', 'https://d2.example/', false, 3)""")
    result = materialize(
        db,
        [website_site_info_requests, website_site_info_results],
        {
            "website_site_info_requests": selection(),
            "website_site_info_results": SETTINGS,
        },
    )
    assert result.success
    assert "https://d2.example/" not in {payload["url"] for payload in saved.values()}
    assert len(saved) == 4


def test_resume_by_execution_id_finishes_without_resubmitting(db, crawler):  # noqa: F811
    client, _, _ = db
    saved, _, behavior = crawler
    instance = dg.DagsterInstance.ephemeral()
    behavior["pending"] = True
    first = materialize(
        db,
        [website_site_info_requests, website_site_info_results],
        {
            "website_site_info_requests": selection(),
            "website_site_info_results": {**SETTINGS, "wait_timeout_seconds": 0.05},
        },
        instance=instance,
        raise_on_error=False,
    )
    assert not first.success
    submitted = dict(saved)
    assert 0 < len(submitted) < 5
    behavior["pending"] = False
    resumed = materialize(
        db,
        [website_site_info_results, dg.AssetSpec("website_site_info_requests")],
        {"website_site_info_results": {**SETTINGS, "execution_id": first.run_id}},
        instance=instance,
    )
    metadata = resumed.asset_materializations_for_node("website_site_info_results")[
        0
    ].metadata
    assert metadata["execution_id"].value == first.run_id
    # Every receipt of the first run is recovered, including ones never sent.
    assert metadata["recovered"].value == 5
    # Recovered requests keep their original identity; only the rest are new.
    assert len(saved) == 5
    assert all(
        saved[request_id] == payload for request_id, payload in submitted.items()
    )
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(5,)]


def test_resume_rejects_changed_content_settings(db, crawler):  # noqa: F811
    instance = dg.DagsterInstance.ephemeral()
    first = materialize(
        db,
        [website_site_info_requests, website_site_info_results],
        {
            "website_site_info_requests": selection(),
            "website_site_info_results": SETTINGS,
        },
        instance=instance,
    )
    with pytest.raises(ValueError, match="unchanged"):
        materialize(
            db,
            [website_site_info_results, dg.AssetSpec("website_site_info_requests")],
            {
                "website_site_info_results": {
                    **SETTINGS,
                    "model": "other-model",
                    "execution_id": first.run_id,
                }
            },
            instance=instance,
        )


def test_results_reject_an_unknown_task(db, crawler):  # noqa: F811
    with pytest.raises(ValueError, match="crawl task"):
        materialize(
            db,
            [website_site_info_results, dg.AssetSpec("website_site_info_requests")],
            {"website_site_info_results": {**SETTINGS, "task_id": str(uuid4())}},
        )


def test_task_and_explicit_domains_are_exclusive():
    with pytest.raises(ValidationError, match="task_id"):
        CrawlResultsConfig(**SETTINGS, task_id=str(uuid4()), domains=["a.example"])
