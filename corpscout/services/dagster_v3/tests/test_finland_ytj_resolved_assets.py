from dagster import AssetKey

from dagster_v3.definitions import defs as load_project_defs


def test_dbt_models_and_clickhouse_registered() -> None:
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "finland_ytj_resolved_fi_companies" in keys
    assert "finland_ytj_resolved_fi_company_addresses" in keys
    assert "finland_ytj_resolved_fi_names" in keys
    assert "finland_ytj_resolved_fi_websites" in keys
    assert "finland_ytj_resolved_fi_industries" in keys
    assert "finland_ytj_resolved_clickhouse" in keys
    assert "finland_ytj_resolved_duckdb" not in keys
    assert "finland_ytj_clickhouse_canonical_contacts" in keys


def test_clickhouse_depends_on_dbt_models() -> None:
    repo = load_project_defs().get_repository_def()
    deps = repo.asset_graph.get(AssetKey(["finland_ytj_resolved_clickhouse"])).parent_keys
    dep_names = {k.path[-1] for k in deps}
    assert {
        "finland_ytj_resolved_fi_companies",
        "finland_ytj_resolved_fi_company_addresses",
        "finland_ytj_resolved_fi_names",
        "finland_ytj_resolved_fi_websites",
        "finland_ytj_resolved_fi_industries",
    } <= dep_names


def test_canonical_contacts_depends_on_resolved_clickhouse() -> None:
    """finland_ytj_clickhouse_canonical_contacts reshapes corpscout.fi_websites,
    which finland_ytj_resolved_clickhouse writes, so it's the sole dep (mirrors
    the Norway loader/derivation wiring in norway_brreg/assets/translation.py and
    contacts.py)."""
    repo = load_project_defs().get_repository_def()
    deps = repo.asset_graph.get(
        AssetKey(["finland_ytj_clickhouse_canonical_contacts"])
    ).parent_keys
    assert {k.path[-1] for k in deps} == {"finland_ytj_resolved_clickhouse"}


def test_ytj_register_and_resolved_assets_share_dagster_group() -> None:
    repo = load_project_defs().get_repository_def()

    for asset_name in (
        "finland_ytj_all_companies_duckdb",
        "finland_ytj_resolved_fi_companies",
        "finland_ytj_resolved_fi_company_addresses",
        "finland_ytj_resolved_fi_names",
        "finland_ytj_resolved_fi_websites",
        "finland_ytj_resolved_fi_industries",
        "finland_ytj_resolved_clickhouse",
        "finland_ytj_clickhouse_canonical_contacts",
    ):
        assert repo.asset_graph.get(AssetKey([asset_name])).group_name == "finland_ytj"


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
    assert "finland_ytj_resolved_clickhouse" in keys  # export (finland_ytj package)
    # canonical-contacts derivation joined via an explicit union (defs/finland_ytj/contacts.py)
    assert "finland_ytj_clickhouse_canonical_contacts" in keys
    assert {
        "finland_ytj_resolved_fi_companies",
        "finland_ytj_resolved_fi_company_addresses",
        "finland_ytj_resolved_fi_names",
        "finland_ytj_resolved_fi_websites",
        "finland_ytj_resolved_fi_industries",
    } <= keys
