import dagster as dg


def test_brazil_comp_pgfn_assets_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    asset_graph = repo.asset_graph

    expected_assets = {
        "brazil_comp_pgfn_raw_archives_s3",
        "brazil_comp_pgfn_company_debts_duckdb",
        "brazil_comp_pgfn_company_debts_clickhouse",
    }
    keys = {key.path[-1] for key in asset_graph.get_all_asset_keys()}
    assert expected_assets.issubset(keys)

    for asset_name in expected_assets:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.group_name == "brazil_comp_pgfn"

    raw_node = asset_graph.get(dg.AssetKey("brazil_comp_pgfn_raw_archives_s3"))
    duckdb_node = asset_graph.get(dg.AssetKey("brazil_comp_pgfn_company_debts_duckdb"))
    clickhouse_node = asset_graph.get(
        dg.AssetKey("brazil_comp_pgfn_company_debts_clickhouse")
    )

    assert type(raw_node.partitions_def).__name__ == "StaticPartitionsDefinition"
    assert raw_node.partitions_def.get_first_partition_key() == "2020-Q1"
    assert duckdb_node.partitions_def is raw_node.partitions_def
    assert clickhouse_node.partitions_def is raw_node.partitions_def
    assert raw_node.pools == set()
    assert duckdb_node.pools == {"brazil_comp_pgfn_duckdb"}
    assert clickhouse_node.pools == {"brazil_comp_pgfn_duckdb"}
    assert duckdb_node.parent_keys == {dg.AssetKey("brazil_comp_pgfn_raw_archives_s3")}
    assert clickhouse_node.parent_keys == {
        dg.AssetKey("brazil_comp_pgfn_company_debts_duckdb")
    }


def test_brazil_comp_pgfn_refresh_job_selects_pgfn_group() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    job = repo.get_job("brazil_comp_pgfn_refresh_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert asset_keys == {
        "brazil_comp_pgfn_raw_archives_s3",
        "brazil_comp_pgfn_company_debts_duckdb",
        "brazil_comp_pgfn_company_debts_clickhouse",
    }
