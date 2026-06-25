from datetime import datetime

import dagster as dg
import pytest


def test_brazil_rfb_raw_assets_are_registered_with_single_writer_pool() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_rfb_snapshot_files_duckdb" in keys
    assert "brazil_rfb_raw_files_duckdb" in keys
    assert "brazil_rfb_companies_duckdb" in keys
    assert "brazil_rfb_contact_info_duckdb" in keys
    assert "brazil_rfb_websites_duckdb" in keys
    assert "brazil_rfb_clickhouse_companies" in keys
    assert "brazil_rfb_clickhouse_establishments" in keys
    assert "brazil_rfb_clickhouse_contact_info" in keys
    assert "brazil_rfb_clickhouse_websites" in keys

    snapshot_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_snapshot_files_duckdb")]
    raw_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_raw_files_duckdb")]
    companies_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_companies_duckdb")]
    contact_info_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_contact_info_duckdb")
    ]
    websites_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_websites_duckdb")]
    clickhouse_companies_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_companies")
    ]
    clickhouse_establishments_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_establishments")
    ]
    clickhouse_contact_info_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_contact_info")
    ]
    clickhouse_websites_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_websites")
    ]
    assert snapshot_asset.op.pool == "brazil_rfb_duckdb"
    assert raw_asset.op.pool == "brazil_rfb_duckdb"
    assert companies_asset.op.pool == "brazil_rfb_duckdb"
    assert contact_info_asset.op.pool == "brazil_rfb_duckdb"
    assert websites_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_companies_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_establishments_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_contact_info_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_websites_asset.op.pool == "brazil_rfb_duckdb"


def test_brazil_rfb_assets_are_monthly_partitioned_from_2024() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    expected_partitioned_assets = (
        "brazil_rfb_snapshot_files_duckdb",
        "brazil_rfb_raw_files_duckdb",
        "brazil_rfb_companies_duckdb",
        "brazil_rfb_contact_info_duckdb",
        "brazil_rfb_websites_duckdb",
        "brazil_rfb_clickhouse_companies",
        "brazil_rfb_clickhouse_establishments",
        "brazil_rfb_clickhouse_contact_info",
        "brazil_rfb_clickhouse_websites",
    )

    for asset_name in expected_partitioned_assets:
        node = repo.asset_graph.get(dg.AssetKey(asset_name))
        assert node.partitions_def is not None
        assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"
        partition_keys = node.partitions_def.get_partition_keys(
            current_time=datetime(2024, 3, 15)
        )
        assert partition_keys == ["2024-01", "2024-02"]


def test_brazil_rfb_snapshot_config_documents_partition_key_source() -> None:
    from dagster_v3.defs.brazil_rfb.assets import BrazilRfbConfig

    fields = BrazilRfbConfig.model_fields

    assert "snapshot_year_month" in fields
    assert "snapshot_month" not in fields
    assert fields["snapshot_year_month"].description is not None
    assert "Deprecated" in (fields["snapshot_year_month"].description or "")
    assert "must match the selected Dagster partition key" in (
        fields["snapshot_year_month"].description or ""
    )
    assert fields["snapshot_base_url"].description is not None
    assert "dados_abertos_cnpj" in fields["snapshot_base_url"].description
    assert "Partition key controls the YYYY-MM snapshot" in (
        fields["snapshot_base_url"].description or ""
    )


def test_brazil_rfb_snapshot_asset_uses_partition_key_for_snapshot(monkeypatch) -> None:
    from dagster_v3.defs.brazil_rfb import assets

    calls = {}

    class FakeContext:
        run_id = "run-1"
        partition_key = "2024-02"

    class FakeDlt:
        def run(self, **kwargs):
            calls["run"] = kwargs
            yield "materialization"

    def fake_source(**kwargs):
        calls["source"] = kwargs
        return "dlt-source"

    def fake_pipeline(database_path):
        calls["pipeline_database_path"] = database_path
        return "dlt-pipeline"

    monkeypatch.setattr(assets.source, "brazil_rfb_source", fake_source)
    monkeypatch.setattr(assets.source, "brazil_rfb_pipeline", fake_pipeline)

    result = list(
        assets.brazil_rfb_snapshot_files_duckdb.node_def.compute_fn.decorated_fn(
            FakeContext(),
            assets.BrazilRfbConfig(snapshot_base_url="https://mirror.test/cnpj/"),
            FakeDlt(),
        )
    )

    assert result == ["materialization"]
    assert calls["source"]["source_run_id"] == "run-1"
    assert calls["source"]["snapshot_year_month"] == "2024-02"
    assert calls["source"]["snapshot_base_url"] == "https://mirror.test/cnpj/"
    assert calls["source"]["download_dir"] == assets.BRAZIL_RFB_DOWNLOAD_DIR / "2024-02"
    assert calls["run"]["dlt_source"] == "dlt-source"
    assert calls["run"]["dlt_pipeline"] == "dlt-pipeline"


def test_brazil_rfb_snapshot_asset_accepts_matching_legacy_snapshot_config(
    monkeypatch,
) -> None:
    from dagster_v3.defs.brazil_rfb import assets

    calls = {}

    class FakeContext:
        run_id = "run-1"
        partition_key = "2024-02"

    class FakeDlt:
        def run(self, **kwargs):
            calls["source"] = kwargs["dlt_source"]
            yield "materialization"

    def fake_source(**kwargs):
        return kwargs

    monkeypatch.setattr(assets.source, "brazil_rfb_source", fake_source)
    monkeypatch.setattr(assets.source, "brazil_rfb_pipeline", lambda database_path: None)

    result = list(
        assets.brazil_rfb_snapshot_files_duckdb.node_def.compute_fn.decorated_fn(
            FakeContext(),
            assets.BrazilRfbConfig(snapshot_year_month="2024-02"),
            FakeDlt(),
        )
    )

    assert result == ["materialization"]
    assert calls["source"]["snapshot_year_month"] == "2024-02"


def test_brazil_rfb_snapshot_asset_rejects_mismatched_legacy_snapshot_config() -> None:
    from dagster_v3.defs.brazil_rfb import assets

    class FakeContext:
        run_id = "run-1"
        partition_key = "2024-02"

    with pytest.raises(dg.Failure, match="does not match selected partition"):
        list(
            assets.brazil_rfb_snapshot_files_duckdb.node_def.compute_fn.decorated_fn(
                FakeContext(),
                assets.BrazilRfbConfig(snapshot_year_month="2024-03"),
                object(),
            )
        )


def test_brazil_rfb_raw_asset_depends_on_snapshot_manifest() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(
            dg.AssetKey("brazil_rfb_raw_files_duckdb")
        ).parent_keys
    }

    assert parents == {"brazil_rfb_snapshot_files_duckdb"}


def test_brazil_rfb_companies_asset_depends_on_raw_files() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(
            dg.AssetKey("brazil_rfb_companies_duckdb")
        ).parent_keys
    }

    assert parents == {"brazil_rfb_raw_files_duckdb"}


def test_brazil_rfb_clickhouse_assets_depend_on_normalized_companies() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for asset_name in (
        "brazil_rfb_clickhouse_companies",
        "brazil_rfb_clickhouse_establishments",
    ):
        parents = {
            parent.path[-1]
            for parent in repo.asset_graph.get(dg.AssetKey(asset_name)).parent_keys
        }
        assert parents == {"brazil_rfb_companies_duckdb"}


def test_brazil_rfb_contact_domain_assets_have_ordered_dependencies() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    expected_parents = {
        "brazil_rfb_contact_info_duckdb": {"brazil_rfb_companies_duckdb"},
        "brazil_rfb_websites_duckdb": {"brazil_rfb_contact_info_duckdb"},
        "brazil_rfb_clickhouse_contact_info": {"brazil_rfb_contact_info_duckdb"},
        "brazil_rfb_clickhouse_websites": {"brazil_rfb_websites_duckdb"},
    }

    for asset_name, expected in expected_parents.items():
        parents = {
            parent.path[-1]
            for parent in repo.asset_graph.get(dg.AssetKey(asset_name)).parent_keys
        }
        assert parents == expected


def test_brazil_rfb_resolve_job_covers_brazil_outputs_and_domain_graph() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    assert "brazil_rfb_resolve_job" in set(repo.job_names)
    resolve_job = repo.get_job("brazil_rfb_resolve_job")
    resolve_keys = {
        key.path[-1]
        for key in resolve_job.asset_layer.executable_asset_keys
    }

    assert type(resolve_job.partitions_def).__name__ == "MonthlyPartitionsDefinition"
    assert resolve_job.partitions_def is not None
    partition_keys = resolve_job.partitions_def.get_partition_keys(
        current_time=datetime(2026, 6, 25)
    )
    assert partition_keys[0] == "2024-01"
    assert partition_keys[-1] == "2026-05"
    assert resolve_job.run_config is None
    assert {
        "brazil_rfb_snapshot_files_duckdb",
        "brazil_rfb_raw_files_duckdb",
        "brazil_rfb_companies_duckdb",
        "brazil_rfb_contact_info_duckdb",
        "brazil_rfb_websites_duckdb",
        "brazil_rfb_clickhouse_companies",
        "brazil_rfb_clickhouse_establishments",
        "brazil_rfb_clickhouse_contact_info",
        "brazil_rfb_clickhouse_websites",
        "domains_clickhouse",
    }.issubset(resolve_keys)
    assert "estonia_ar_general_data_duckdb" not in resolve_keys
    assert "norway_resolved_clickhouse" not in resolve_keys
