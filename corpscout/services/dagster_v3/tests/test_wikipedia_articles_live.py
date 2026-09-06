"""Opt-in real Wikimedia -> S3 -> ClickHouse smoke; never publishes a limited production snapshot."""

import json
import os
from uuid import uuid4

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikipedia import snapshot
from dagster_v3.defs.wikipedia.source import (
    ARTICLE_TABLE,
    WikipediaClient,
    WikipediaSnapshotConfig,
)


@pytest.mark.skipif(
    os.getenv("WIKIPEDIA_LIVE_SMOKE") != "1",
    reason="requires explicit live smoke opt-in",
)
def test_live_wikimedia_s3_clickhouse_round_trip(monkeypatch):
    suffix = uuid4().hex
    scratch_table = f"wikidata_wikipedia_smoke_{suffix}"
    store = ObjectStoreResource(bucket="source-wikipedia-articles-weekly-smoke")
    store.ensure_bucket()
    original_prefix = snapshot.snapshot_prefix
    monkeypatch.setattr(
        snapshot,
        "snapshot_prefix",
        lambda source_run_id: f"smoke={suffix}/" + original_prefix(source_run_id),
    )
    monkeypatch.setattr(snapshot, "ARTICLE_TABLE", scratch_table)
    config = WikipediaSnapshotConfig(source_run_id="2026-07-20")
    prefix = snapshot.snapshot_prefix(config.source_run_id)
    clickhouse = ClickhouseResource(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() in {"true", "1"},
    )
    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE corpscout.{scratch_table} AS corpscout.{ARTICLE_TABLE}"
        )
        try:
            store.write_bytes(
                prefix + "companies.json",
                json.dumps(
                    {
                        "version": 1,
                        "source_run_id": config.source_run_id,
                        "company_qids": ["Q1421630"],
                    }
                ).encode(),
            )
            manifest = snapshot.build_article_snapshot(
                store, WikipediaClient(config), config.source_run_id, print
            )
            count = snapshot.publish_articles(client, store, manifest, suffix, print)
            rows = client.execute(
                f"SELECT site_id, language_code, wikipedia_revision_id, lengthUTF8(article_lead_text), "
                f"lengthUTF8(article_text), license_name FROM corpscout.{scratch_table} ORDER BY site_id"
            )
            assert count == len(rows) == manifest["row_count"]
            assert {"enwiki", "svwiki"}.issubset({row[0] for row in rows})
            assert all(row[2] > 0 and row[4] > 0 and row[5] for row in rows)
            assert all(row[3] > 0 for row in rows if row[0] in {"enwiki", "svwiki"})
            print(
                "Live Wikipedia smoke:",
                {
                    "rows": count,
                    "objects": len(manifest["objects"]),
                    "languages": manifest["language_counts"],
                },
            )
            replayed = snapshot.build_article_snapshot(
                store, WikipediaClient(config), config.source_run_id, print
            )
            assert replayed == manifest
        finally:
            client.execute(f"DROP TABLE IF EXISTS corpscout.{scratch_table}")
            store.delete_keys(store.list_keys(prefix))
