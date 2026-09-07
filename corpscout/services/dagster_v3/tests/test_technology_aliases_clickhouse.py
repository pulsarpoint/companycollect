"""Run the migration and catalog publisher against an isolated Docker ClickHouse."""

import shutil
import os
import subprocess
import time
import uuid
from pathlib import Path
from unittest.mock import Mock

import dagster as dg
import pytest
from clickhouse_driver.errors import Error as ClickhouseError
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.technology_catalog import assets, tables
from dagster_v3.defs.technology_catalog.catalog import CatalogLayer
from dagster_v3.defs.technology_catalog.icons import IconSyncResult
from tests.test_technology_aliases import alias_entry, write_aliases

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def isolated_clickhouse_service():
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for isolated ClickHouse integration")
    if subprocess.run(["docker", "info"], capture_output=True, timeout=15).returncode:
        pytest.skip("Docker daemon unavailable")
    container = f"technology-alias-test-{uuid.uuid4().hex}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container,
            "--publish",
            "127.0.0.1::9000",
            "--publish",
            "127.0.0.1::8123",
            "--env",
            "CLICKHOUSE_PASSWORD=technology-test-password",
            "clickhouse/clickhouse-server:26.5",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    try:
        binding = subprocess.run(
            ["docker", "port", container, "9000/tcp"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        resource = ClickhouseResource(
            host="127.0.0.1",
            port=int(binding.rsplit(":", 1)[1]),
            user="default",
            password="technology-test-password",
            database="default",
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                with resource.get_connection() as client:
                    client.execute("SELECT 1")
                break
            except ClickhouseError, OSError, EOFError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
        with resource.get_connection() as client:
            for name in [
                "000350_corpscout_technology_catalog.up.sql",
                "000357_corpscout_technology_fingerprints.up.sql",
                "000361_corpscout_technology_catalog_publish_log.up.sql",
                "000388_corpscout_technology_aliases.up.sql",
                "000389_corpscout_technology_proposals.up.sql",
            ]:
                sql = "\n".join(
                    line
                    for line in (migrations / name)
                    .read_text(encoding="utf-8")
                    .splitlines()
                    if not line.lstrip().startswith("--")
                )
                for statement in sql.split(";"):
                    if statement.strip():
                        client.execute(statement)
        http_binding = subprocess.run(
            ["docker", "port", container, "8123/tcp"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        yield resource, f"http://{http_binding}"
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            check=True,
            capture_output=True,
            timeout=30,
        )


@pytest.fixture
def isolated_clickhouse(isolated_clickhouse_service):
    return isolated_clickhouse_service[0]


def test_catalog_asset_publishes_aliases_clears_them_and_rejects_bad_input(
    isolated_clickhouse, isolated_clickhouse_service, monkeypatch, tmp_path
):
    layer = CatalogLayer(
        technologies={
            "Amazon Web Services": {"cats": [1]},
            "git": {"cats": [1]},
        },
        categories={1: {"name": "Development", "groups": [1]}},
        groups={1: "Software"},
        source="extension",
        source_version="test",
    )
    monkeypatch.setattr(assets, "load_extension_layer", lambda path: layer)
    monkeypatch.setattr(assets, "resolve_overlay_commit", lambda: "test-overlay")
    monkeypatch.setattr(assets, "fetch_overlay_layer", lambda sha: layer)
    monkeypatch.setattr(assets, "custom_source_dir", lambda: tmp_path)
    monkeypatch.setattr(
        assets, "sync_icons", lambda *args, **kwargs: IconSyncResult({}, 0, 0, 2, 0)
    )
    monkeypatch.setattr(ObjectStoreResource, "ensure_bucket", lambda self: None)
    monkeypatch.setattr(ObjectStoreResource, "client", lambda self: None)
    monkeypatch.setattr(tables, "MIN_TECHNOLOGY_CATALOG_ROWS", 2)
    monkeypatch.setattr(tables, "MIN_TECHNOLOGY_FINGERPRINT_ROWS", 0)
    for name in ["technologies.json", "categories.json", "fingerprints.json"]:
        (tmp_path / name).write_text("{}", encoding="utf-8")
    store = ObjectStoreResource(
        endpoint_url="http://127.0.0.1:1", access_key="test", secret_key="test"
    )

    write_aliases(tmp_path, [alias_entry()])
    with dg.build_asset_context() as context:
        result = assets.technology_catalog_clickhouse(
            context, isolated_clickhouse, store
        )
    assert result.metadata["alias_rows"] == 1
    with isolated_clickhouse.get_connection() as client:
        assert client.execute(
            "SELECT alias, alias_key, technology, review_status FROM corpscout.technology_aliases"
        ) == [("AWS", "aws", "Amazon Web Services", "accepted")]
        assert client.execute("SELECT count() FROM corpscout.technology_catalog") == [
            (2,)
        ]
        assert client.execute(
            "SELECT count() FROM corpscout.technology_catalog_publish_log"
        ) == [(1,)]

    # Invalid target is rejected before even the catalog snapshot changes.
    write_aliases(tmp_path, [alias_entry(technology="Missing")])
    sync = Mock(
        side_effect=AssertionError("Invalid aliases must fail before icon writes")
    )
    with monkeypatch.context() as patch:
        patch.setattr(assets, "sync_icons", sync)
        with (
            dg.build_asset_context() as context,
            pytest.raises(ValueError, match="absent"),
        ):
            assets.technology_catalog_clickhouse(context, isolated_clickhouse, store)
    with isolated_clickhouse.get_connection() as client:
        assert client.execute("SELECT count() FROM corpscout.technology_aliases") == [
            (1,)
        ]
        assert client.execute(
            "SELECT count() FROM corpscout.technology_catalog_publish_log"
        ) == [(1,)]

    # Removing the last curated alias deliberately publishes zero rows.
    write_aliases(tmp_path, [])
    with dg.build_asset_context() as context:
        result = assets.technology_catalog_clickhouse(
            context, isolated_clickhouse, store
        )
    assert result.metadata["alias_rows"] == 0
    with isolated_clickhouse.get_connection() as client:
        assert client.execute("SELECT count() FROM corpscout.technology_aliases") == [
            (0,)
        ]
        assert client.execute(
            "SELECT count() FROM corpscout.technology_catalog_publish_log"
        ) == [(2,)]
        assert (
            client.execute(
                "SELECT name FROM system.tables WHERE database='corpscout' AND startsWith(name, '_tmp_')"
            )
            == []
        )

    (tmp_path / "technology_aliases.json").unlink()
    with dg.build_asset_context() as context, pytest.raises(FileNotFoundError):
        assets.technology_catalog_clickhouse(context, isolated_clickhouse, store)

    # Use the actual backend submission/review implementation against this
    # disposable DB, then publish its approved input through the actual asset.
    write_aliases(tmp_path, [])
    backoffice = Path(__file__).resolve().parents[2] / "backoffice"
    backend_test = subprocess.run(
        ["npx", "vitest", "run", "tests/technology-proposals.live.test.ts"],
        cwd=backoffice,
        env=os.environ
        | {
            "VITEST_LIVE": "1",
            "TECHNOLOGY_PROPOSAL_ISOLATED_TEST": "1",
            "CLICKHOUSE_URL": isolated_clickhouse_service[1],
            "CLICKHOUSE_USER": "default",
            "CLICKHOUSE_PASSWORD": "technology-test-password",
            "CLICKHOUSE_DATABASE": "corpscout",
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert backend_test.returncode == 0, backend_test.stdout + backend_test.stderr
    for _ in range(2):
        with dg.build_asset_context() as context:
            result = assets.technology_catalog_clickhouse(
                context, isolated_clickhouse, store
            )
        assert result.metadata["reviewed_technology_rows"] == 1
        with isolated_clickhouse.get_connection() as client:
            assert client.execute(
                "SELECT technology, categories, groups, source FROM corpscout.technology_catalog WHERE technology = 'Yocto Project'"
            ) == [("Yocto Project", ["Development"], ["Software"], "admin_review")]
            assert client.execute(
                "SELECT alias, technology, source FROM corpscout.technology_aliases"
            ) == [("Yocto", "Yocto Project", "admin_review")]

    # A later rejection must hide the older approval from the latest-review
    # view (including under WHERE filters), without deleting either audit row.
    with isolated_clickhouse.get_connection() as client:
        client.execute("""
            INSERT INTO corpscout.technology_proposal_reviews
                (review_id, proposal_id, decision, technology, reviewed_by, review_note, reviewed_at)
            SELECT generateUUIDv4(), proposal_id, 'reject', '', 'Integration test administrator',
                   'Synthetic rejection after review', now64(6) + INTERVAL 1 SECOND
            FROM corpscout.technology_proposal_latest_reviews
        """)
        assert client.execute(
            "SELECT count() FROM corpscout.technology_proposal_mappings"
        ) == [(0,)]
        assert client.execute(
            "SELECT count() FROM corpscout.technology_proposal_reviews"
        ) == [(2,)]
        assert client.execute("SELECT count() FROM corpscout.new_tech FINAL") == [(1,)]
    with dg.build_asset_context() as context:
        assets.technology_catalog_clickhouse(context, isolated_clickhouse, store)
    with isolated_clickhouse.get_connection() as client:
        assert client.execute(
            "SELECT count() FROM corpscout.technology_catalog WHERE technology = 'Yocto Project'"
        ) == [(0,)]
        assert client.execute("SELECT count() FROM corpscout.technology_aliases") == [
            (0,)
        ]
