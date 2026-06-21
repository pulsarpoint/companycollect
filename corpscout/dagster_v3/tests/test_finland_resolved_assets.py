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


def test_resolved_schedule_covers_register_dbt_and_export() -> None:
    repo = load_project_defs().get_repository_def()
    sched = repo.get_schedule_def("finland_ytj_resolved_schedule")
    assert sched.cron_schedule == "45 4 * * *"  # daily, staggered
    assert sched.job.name == "finland_ytj_resolved_job"
    # .upstream() must cross modules to pull the YTJ register load + dbt + export.
    keys = {
        k.path[-1]
        for k in repo.get_job("finland_ytj_resolved_job").asset_layer.executable_asset_keys
    }
    assert "finland_ytj_all_companies_duckdb" in keys  # register load (finland_ytj module)
    assert "finland_ytj_resolved_clickhouse" in keys  # export (finland_resolved module)
    assert {
        "finland_ytj_resolved_fi_companies",
        "finland_ytj_resolved_fi_names",
        "finland_ytj_resolved_fi_websites",
        "finland_ytj_resolved_fi_industries",
    } <= keys
