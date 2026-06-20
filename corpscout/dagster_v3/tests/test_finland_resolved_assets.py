from dagster import AssetKey

from dagster_v3.definitions import defs as load_project_defs


def test_dbt_models_and_clickhouse_registered() -> None:
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "finland_ytj_resolved_fi_companies" in keys
    assert "finland_ytj_resolved_fi_names" in keys
    assert "finland_ytj_resolved_fi_websites" in keys
    assert "finland_ytj_resolved_fi_industries" in keys
    assert "finland_ytj_resolved_clickhouse" in keys
    assert "finland_ytj_resolved_duckdb" not in keys


def test_clickhouse_depends_on_dbt_models() -> None:
    repo = load_project_defs().get_repository_def()
    deps = repo.asset_graph.get(AssetKey(["finland_ytj_resolved_clickhouse"])).parent_keys
    dep_names = {k.path[-1] for k in deps}
    assert {
        "finland_ytj_resolved_fi_companies",
        "finland_ytj_resolved_fi_names",
        "finland_ytj_resolved_fi_websites",
        "finland_ytj_resolved_fi_industries",
    } <= dep_names
