import dagster as dg


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
    resolve_keys = {
        key.path[-1]
        for key in repo.get_job(
            "brazil_rfb_resolve_job"
        ).asset_layer.executable_asset_keys
    }

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
