"""Archive selection and real typed staging/publication against disposable ClickHouse."""

from pathlib import Path

import pytest

from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_website_crawl_normalization import source as source, payload as payload
from tests.test_website_crawl_normalized_migration import SCHEMAS as EXPECTED_SCHEMAS
from dagster_v3.defs.website_crawl.normalization import tables
from dagster_v3.defs.website_crawl.normalization.load import (
    NormalizationConfig,
    pending_attempts,
    publish_attempt,
    read_archive,
)


@pytest.fixture
def normalized_ch(graph_ch):
    directory = Path(__file__).parents[3] / "clickhouse/migrations"
    for name in (
        "000430_corpscout_website_crawl_type_results",
        "000452_corpscout_website_crawl_normalized",
    ):
        for statement in (directory / (name + ".up.sql")).read_text().split(";"):
            if statement.strip():
                graph_ch.execute(statement)
    for name in tables.COLUMNS:
        graph_ch.execute("TRUNCATE TABLE corpscout.website_crawl_" + name)
    for name in (
        "website_full_crawl_results",
        "website_jobs_crawl_results",
        "website_site_info_results",
    ):
        graph_ch.execute("TRUNCATE TABLE corpscout." + name)
    return graph_ch


def insert_source(client, source):
    row = {k: v for k, v in source.items() if k != "crawl_type"}
    table = {
        "full": "website_full_crawl_results",
        "jobs": "website_jobs_crawl_results",
        "site_info": "website_site_info_results",
    }[source["crawl_type"]]
    client.execute(f"INSERT INTO corpscout.{table} ({','.join(row)}) VALUES", [row])


def test_export_contract_matches_migration():
    assert tables.SCHEMAS == EXPECTED_SCHEMAS


def test_pending_publication_and_native_types(normalized_ch, source, payload):
    client = normalized_ch
    insert_source(client, source)
    config = NormalizationConfig(domains=["example.se"])
    pending = pending_attempts(client, config)
    assert len(pending) == 1 and pending[0]["request_id"] == "request-1"
    counts = publish_attempt(client, pending[0], payload, "test-run")
    assert counts["jobs"] == 1 and counts["business_activities"] == 2
    assert pending_attempts(client, config) == []
    assert (
        len(
            pending_attempts(
                client, NormalizationConfig(domains=["example.se"], force=True)
            )
        )
        == 1
    )
    assert client.execute(
        "SELECT purpose_original FROM corpscout.website_crawl_site_profiles_current"
    ) == [("Present engineering services",)]
    assert client.execute(
        "SELECT value_number_original FROM corpscout.website_crawl_structured_data_current WHERE property_path='/salary'"
    ) == [("12.5",)]
    assert client.execute(
        "SELECT title_original,expires_at FROM corpscout.website_crawl_jobs_current"
    ) == [("Engineer", None)]


def test_interrupted_detail_write_does_not_publish_and_retry_replaces(
    normalized_ch, source, payload
):
    class Interrupted:
        def execute(self, query, *args, **kwargs):
            if query.startswith("INSERT INTO `corpscout`.`website_crawl_pages`"):
                raise RuntimeError("simulated worker interruption")
            return normalized_ch.execute(query, *args, **kwargs)

    with pytest.raises(RuntimeError, match="worker interruption"):
        publish_attempt(Interrupted(), source, payload, "interrupted-run")
    assert normalized_ch.execute(
        "SELECT count() FROM corpscout.website_crawl_scans"
    ) == [(0,)]
    assert normalized_ch.execute(
        "SELECT count() FROM corpscout.website_crawl_site_profiles_published"
    ) == [(0,)]
    publish_attempt(normalized_ch, source, payload, "retry-run")
    assert normalized_ch.execute(
        "SELECT count() FROM corpscout.website_crawl_site_profiles_published"
    ) == [(1,)]
    payload["documents"][0]["input"]["observations"]["contacts"] = []
    publish_attempt(normalized_ch, source, payload, "reparse-run")
    assert normalized_ch.execute(
        "SELECT count() FROM corpscout.website_crawl_contacts_current"
    ) == [(0,)]
    assert normalized_ch.execute(
        "SELECT normalization_revision FROM corpscout.website_crawl_scans FINAL"
    ) == [(2,)]


def test_changed_source_revision_is_pending(normalized_ch, source, payload):
    from datetime import timedelta

    insert_source(normalized_ch, source)
    publish_attempt(normalized_ch, source, payload, "test-run")
    source["ingested_at"] += timedelta(seconds=1)
    insert_source(normalized_ch, source)
    assert len(pending_attempts(normalized_ch, NormalizationConfig())) == 1


def test_exact_archive_path_is_bound_and_identity_checked(source):
    class Archive:
        def execute(self, query, params):
            assert "filename=%(key)s" in query
            assert params["key"] == "request-1/attempts/0001/result.json.gz"
            assert "*" not in params["key"]
            return [('{"schema_version":"company-crawl-error/1.0"}',)]

    assert (
        read_archive(Archive(), source)["schema_version"] == "company-crawl-error/1.0"
    )
    source["s3_path"] = source["s3_path"].replace("request-1", "other-request")
    with pytest.raises(ValueError):
        read_archive(Archive(), source)


@pytest.mark.parametrize(
    "config",
    [
        {"force": True},
        {"attempt": 1},
        {"request_id": "../bad"},
        {"crawl_types": ["unknown"]},
        {"batch_size": 101},
    ],
)
def test_unbounded_force_and_invalid_filters_rejected(config):
    with pytest.raises(ValueError):
        NormalizationConfig(**config)


def test_assets_have_one_output_per_table_and_normalization_only_job():
    from dagster_v3.defs.website_crawl.normalization.assets import (
        website_crawl_normalized,
        defs,
    )
    import dagster as dg

    asset = website_crawl_normalized
    assert asset.keys == {
        dg.AssetKey("website_crawl_" + name) for name in tables.COLUMNS
    }
    assert not asset.can_subset
    assert set(asset.group_names_by_key.values()) == {"website_crawl_normalized"}
    assert asset.op.pool == "website_crawl_normalization"
    assert [job.name for job in defs.jobs] == ["website_crawl_normalized_job"]


def test_missing_uploaded_archive_fails_instead_of_publishing_empty_data(source):
    class MissingArchive:
        def execute(self, *args, **kwargs):
            return []

    with pytest.raises(ValueError, match="exactly one"):
        read_archive(MissingArchive(), source)
