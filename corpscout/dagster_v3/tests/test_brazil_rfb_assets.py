import dagster as dg


def test_brazil_rfb_raw_assets_are_registered_with_single_writer_pool() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_rfb_snapshot_files_duckdb" in keys
    assert "brazil_rfb_raw_files_duckdb" in keys
    assert "brazil_rfb_companies_duckdb" in keys
    assert "brazil_rfb_clickhouse_companies" in keys
    assert "brazil_rfb_clickhouse_establishments" in keys

    snapshot_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_snapshot_files_duckdb")]
    raw_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_raw_files_duckdb")]
    companies_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_companies_duckdb")]
    clickhouse_companies_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_companies")
    ]
    clickhouse_establishments_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_rfb_clickhouse_establishments")
    ]
    assert snapshot_asset.op.pool == "brazil_rfb_duckdb"
    assert raw_asset.op.pool == "brazil_rfb_duckdb"
    assert companies_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_companies_asset.op.pool == "brazil_rfb_duckdb"
    assert clickhouse_establishments_asset.op.pool == "brazil_rfb_duckdb"


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
