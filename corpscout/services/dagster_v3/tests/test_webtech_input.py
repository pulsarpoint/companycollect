"""Draft imports and immutable replay against real disposable databases."""

from io import BytesIO
from pathlib import Path
from uuid import uuid4

import dagster as dg
import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.input import (
    WebtechInputConfig,
    normalized_target,
    source_query,
    webtech_scan_input,
    load_draft,
)
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION
from tests.test_ip_enrichment_input import server as server
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)


class MemoryS3:
    """Protocol-boundary object store; no remote credentials or files."""

    def __init__(self):
        self.objects = {}

    def create_bucket(self, Bucket):
        pass

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")

    def get_object(self, Bucket, Key):
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body.encode() if isinstance(Body, str) else Body

    def upload_file(self, Filename, Bucket, Key):
        self.objects[(Bucket, Key)] = Path(Filename).read_bytes()

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).write_bytes(self.objects[(Bucket, Key)])


@pytest.fixture
def objects(monkeypatch):
    memory = MemoryS3()
    monkeypatch.setattr(ObjectStoreResource, "client", lambda self: memory)
    return ObjectStoreResource(
        bucket="webtech",
        endpoint_url="http://test",
        access_key="test",
        secret_key="test",
    )


@pytest.fixture
def database(server):
    client, resource = server
    client.execute("DROP TABLE IF EXISTS corpscout.webtech_scan_input")
    client.execute("DROP TABLE IF EXISTS corpscout.webtech_domain_scan_results")
    client.execute(
        "CREATE TABLE corpscout.webtech_domain_scan_results (root_domain String,website_origin String,page_url String,detector_version String,scanned_at DateTime64(3,'UTC'),scan_id String,outcome String,crawl_id String DEFAULT '') ENGINE=ReplacingMergeTree ORDER BY (root_domain,website_origin,page_url,detector_version,scan_id)"
    )
    migration = (
        Path(__file__).parents[3]
        / "clickhouse/migrations/000438_corpscout_webtech_scan_input.up.sql"
    )
    for statement in migration.read_text().split(";"):
        if statement.strip():
            client.execute(statement)
    for name in ("000446_corpscout_webtech_queue_contract.up.sql",):
        path = Path(__file__).parents[3] / "clickhouse/migrations" / name
        for statement in path.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
    return client, resource


def materialize(resource, dsn, objects, **config):
    return dg.materialize(
        [webtech_scan_input],
        resources={
            "clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
            "webtech_object_store": objects,
        },
        run_config={"ops": {"webtech_scan_input": {"config": config}}},
    )


def add(resource, processing, objects, *, submission_id=None, **config):
    return load_draft(
        config=WebtechInputConfig(**config),
        submission_id=submission_id or str(uuid4()),
        run_id=str(uuid4()),
        clickhouse=resource,
        store=processing,
    )


def test_normalization_preserves_pages_and_origins():
    a = normalized_target("HTTPS://SHOP.NOVELIC.COM:443/products?q=1#part")
    assert a[1:] == (
        "novelic.com",
        "https://shop.novelic.com",
        "https://shop.novelic.com/products?q=1",
    )
    assert (
        normalized_target("novelic.com")[0]
        == normalized_target("https://novelic.com/")[0]
    )
    assert (
        normalized_target("novelic.com/a")[0] != normalized_target("novelic.com/b")[0]
    )


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"source_relation": "corpscout.se_domains"},
        {"targets": ["novelic.com"], "source_relation": "corpscout.se_domains"},
        {"source_relation": "corpscout.domains;bad", "select_all": True},
        {
            "source_relation": "corpscout.domains",
            "target_column": "bad`",
            "select_all": True,
        },
    ],
)
def test_invalid_selection(config):
    with pytest.raises(ValidationError):
        WebtechInputConfig(**config)


def test_commoncrawl_rank_is_optional():
    sql, params = source_query(
        WebtechInputConfig(
            source_relation="corpscout.commoncrawl_domain_graph_signals",
            filters={"root_domain": ["novelic.com"]},
        )
    )
    assert "cc_harmonic_rank" not in sql
    assert params["filter_0"] == ("novelic.com",)


def test_asset_appends_sources_and_keeps_recent_pages(database, store, objects):
    client, resource = database
    processing, dsn = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,outcome) VALUES ('novelic.com','https://novelic.com','https://novelic.com/',%(detector)s,now(),'success')",
        {"detector": WEBTECH_DETECTOR_VERSION},
    )
    task = str(uuid4())
    submission_id = str(uuid4())
    assert materialize(
        resource,
        dsn,
        objects,
        task_id=task,
        submission_id=submission_id,
        targets=["novelic.com", "https://novelic.com/"],
        source_name="manual",
    ).success
    assert processing.task(task)["status"] == "draft"
    second = add(
        resource,
        processing,
        objects,
        targets=["novelic.com", "example.com"],
        source_name="another-source",
    )
    assert second["task_id"] == task
    assert second["input_count"] == 2
    assert second["total"] == 2
    assert materialize(
        resource,
        dsn,
        objects,
        task_id=task,
        submission_id=submission_id,
        targets=["https://novelic.com/", "novelic.com"],
        source_name="manual",
    ).success
    assert client.execute("SELECT count() FROM corpscout.webtech_scan_input") == [(2,)]
    with pytest.raises(ValueError, match="different selection"):
        add(
            resource,
            processing,
            objects,
            submission_id=submission_id,
            targets=["different.com"],
        )


def test_retry_replaces_only_its_own_rows_from_the_current_source(
    database, store, objects, monkeypatch
):
    from dagster_v3.defs.webtech import input as module

    client, resource = database
    processing, _ = store
    client.execute("DROP TABLE IF EXISTS corpscout.webtech_test_source")
    client.execute(
        "CREATE TABLE corpscout.webtech_test_source (id String,url String,country String) ENGINE=MergeTree ORDER BY id"
    )
    client.execute(
        "INSERT INTO corpscout.webtech_test_source VALUES ('1','novelic.com','SE'),('2','example.org','DE')"
    )
    other = add(
        resource, processing, objects, targets=["other.se"], source_name="manual"
    )
    submission_id = str(uuid4())
    config = dict(
        submission_id=submission_id,
        source_relation="corpscout.webtech_test_source",
        target_column="url",
        source_record_id_column="id",
        filters={"country": ["SE"]},
    )
    original = module.insert_input_batch

    def crash_after_insert(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("lost acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(module, "insert_input_batch", crash_after_insert)
        with pytest.raises(RuntimeError, match="acknowledgement"):
            add(resource, processing, objects, **config)
    client.execute(
        "INSERT INTO corpscout.webtech_test_source VALUES ('3','example.com','SE')"
    )
    result = add(resource, processing, objects, **config)
    # The retry reselects the source: its own rows are replaced, other submissions kept.
    assert result["input_count"] == 2
    assert sorted(
        client.execute(
            "SELECT root_domain, submission_id != '' FROM corpscout.webtech_scan_input"
        )
    ) == [("example.com", True), ("novelic.com", True), ("other.se", True)]
    assert result["task_id"] == other["task_id"]
    assert not [
        key for (_, key) in objects.client().objects if key.startswith("queue-inputs/")
    ]


def test_se_selection_filters_latest_rows_and_deduplicates_roots(
    database, store, objects
):
    client, resource = database
    processing, _ = store
    client.execute("DROP TABLE IF EXISTS corpscout.se_company_domain")
    client.execute(
        "CREATE TABLE corpscout.se_company_domain (root_domain String,company_id String,sources Array(String),association String,active UInt8,confidence Float64,revision UInt64) ENGINE=ReplacingMergeTree(revision) ORDER BY (root_domain,company_id)"
    )
    client.execute(
        "INSERT INTO corpscout.se_company_domain VALUES",
        [
            ("shared.se", "1", ["brave"], "connected", 1, 0.8, 1),
            ("shared.se", "2", ["brave"], "connected", 1, 0.8, 1),
            ("excluded.se", "1", ["brave"], "connected", 1, 0.8, 1),
            ("excluded.se", "2", ["brave"], "connected", 1, 0.8, 1),
            ("single.se", "1", ["brave"], "connected", 1, 0.8, 1),
            ("inactive.se", "1", ["brave"], "connected", 1, 0.8, 1),
            ("inactive.se", "1", ["brave"], "connected", 0, 0.8, 2),
            ("inactive.se", "2", ["brave"], "connected", 0, 0.8, 1),
        ],
    )
    config = dict(
        source_relation="corpscout.se_company_domain",
        source_final=True,
        select_all=True,
        excluded_targets=["excluded.se"],
        se_domain_filters={
            "domain": ".se",
            "source": "brave",
            "association": "connected",
            "status": "active",
            "shared": True,
            "min_confidence": 0.7,
            "max_confidence": 0.9,
        },
    )
    result = add(resource, processing, objects, **config)
    assert result["total"] == 1
    assert client.execute(
        "SELECT root_domain,page_url FROM corpscout.webtech_scan_input"
    ) == [("shared.se", "https://shared.se/")]
    sql, params = source_query(
        WebtechInputConfig(**config, target_column="root_domain")
    )
    assert "LIMIT" not in sql
    assert "excluded.se" not in sql
    assert params["excluded_targets"] == ("excluded.se",)
    # Company restriction must not change the meaning of shared ownership.
    config["se_domain_filters"]["company"] = "1"
    assert client.execute(*source_query(WebtechInputConfig(**config))) == [
        ("shared.se", "shared.se")
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_relation": "corpscout.domains"},
        {"source_final": False},
        {"target_column": "website_url"},
        {"source_record_id_column": "company_id"},
    ],
)
def test_se_filters_require_the_current_domain_entity(overrides):
    config = dict(
        source_relation="corpscout.se_company_domain",
        source_final=True,
        select_all=True,
        se_domain_filters={},
    )
    with pytest.raises(ValidationError, match="SE domain filters require"):
        WebtechInputConfig(**(config | overrides))


def test_new_selection_options_preserve_old_submission_fingerprints(
    database, store, objects
):
    import hashlib
    import json
    from dagster_v3.defs.common import draft_queue

    _, resource = database
    processing, _ = store
    submission_id = str(uuid4())
    add(
        resource,
        processing,
        objects,
        submission_id=submission_id,
        targets=["example.se"],
    )
    # Exact pre-extension config shape, not inferred from the new model defaults.
    old_selection = {
        "source_relation": None,
        "target_column": "root_domain",
        "source_record_id_column": None,
        "source_name": None,
        "filters": {},
        "source_final": False,
        "select_all": False,
        "max_rows": None,
        "harmonic_rank_limit": None,
        "manual_count": 1,
        "manual_sha256": hashlib.sha256(
            json.dumps(["example.se"]).encode()
        ).hexdigest(),
    }
    expected = hashlib.sha256(
        json.dumps(old_selection, sort_keys=True).encode()
    ).hexdigest()
    assert (
        draft_queue.submission(processing, submission_id)["selection_fingerprint"]
        == expected
    )


def test_entry_table_follows_the_queue_contract(database):
    client, _ = database
    assert client.execute(
        "SELECT partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='webtech_scan_input'"
    ) == [("task_id", "task_id, input_id")]
