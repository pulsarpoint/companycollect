import dagster as dg


def test_brazil_comp_cgu_assets_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    asset_graph = repo.asset_graph

    expected_assets = {
        "brazil_comp_cgu_raw_archives_s3",
        "brazil_comp_cgu_ceis_company_sanctions_duckdb",
        "brazil_comp_cgu_cnep_company_sanctions_duckdb",
        "brazil_comp_cgu_cepim_blocked_entities_duckdb",
        "brazil_comp_cgu_leniency_agreements_duckdb",
        "brazil_comp_cgu_leniency_agreement_effects_duckdb",
        "brazil_comp_cgu_ceis_company_sanctions_clickhouse",
        "brazil_comp_cgu_cnep_company_sanctions_clickhouse",
        "brazil_comp_cgu_cepim_blocked_entities_clickhouse",
        "brazil_comp_cgu_leniency_agreements_clickhouse",
        "brazil_comp_cgu_leniency_agreement_effects_clickhouse",
    }
    keys = {key.path[-1] for key in asset_graph.get_all_asset_keys()}
    assert expected_assets.issubset(keys)

    for asset_name in expected_assets:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.group_name == "brazil_comp_cgu"
        assert node.partitions_def is None

    for asset_name in expected_assets - {"brazil_comp_cgu_raw_archives_s3"}:
        node = asset_graph.get(dg.AssetKey(asset_name))
        if asset_name.endswith("_duckdb"):
            assert node.parent_keys == {dg.AssetKey("brazil_comp_cgu_raw_archives_s3")}
            assert node.pools == {"brazil_comp_cgu_duckdb"}
        if asset_name.endswith("_clickhouse"):
            assert node.pools == {"brazil_comp_cgu_duckdb"}


def test_brazil_comp_cgu_refresh_job_selects_cgu_group() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    job = repo.get_job("brazil_comp_cgu_refresh_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert asset_keys == {
        "brazil_comp_cgu_raw_archives_s3",
        "brazil_comp_cgu_ceis_company_sanctions_duckdb",
        "brazil_comp_cgu_cnep_company_sanctions_duckdb",
        "brazil_comp_cgu_cepim_blocked_entities_duckdb",
        "brazil_comp_cgu_leniency_agreements_duckdb",
        "brazil_comp_cgu_leniency_agreement_effects_duckdb",
        "brazil_comp_cgu_ceis_company_sanctions_clickhouse",
        "brazil_comp_cgu_cnep_company_sanctions_clickhouse",
        "brazil_comp_cgu_cepim_blocked_entities_clickhouse",
        "brazil_comp_cgu_leniency_agreements_clickhouse",
        "brazil_comp_cgu_leniency_agreement_effects_clickhouse",
    }
