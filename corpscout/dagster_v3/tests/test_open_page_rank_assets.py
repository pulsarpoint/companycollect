from __future__ import annotations

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.open_page_rank.assets import defs


def test_open_page_rank_defs_register_expected_assets_job_and_schedule() -> None:
    resolved = dg.Definitions.merge(
        defs,
        dg.Definitions(
            resources={
                "object_store": ObjectStoreResource(),
                "clickhouse": ClickhouseResource(
                    host="localhost",
                    port=9002,
                    user="default",
                    password="",
                    database="corpscout",
                ),
            }
        ),
    ).get_repository_def()

    asset_keys = {key.path[-1] for key in resolved.asset_graph.get_all_asset_keys()}
    assert "open_page_rank_raw_archive" in asset_keys
    assert "open_page_rank_raw_duckdb" in asset_keys
    assert "open_page_rank_domains_duckdb" in asset_keys
    assert "open_page_rank_domains_clickhouse" in asset_keys
    assert "open_page_rank_raw_retention" in asset_keys
    assert resolved.get_job("open_page_rank_domains_refresh_job").name == (
        "open_page_rank_domains_refresh_job"
    )
    assert resolved.get_schedule_def("open_page_rank_domains_weekly").job.name == (
        "open_page_rank_domains_refresh_job"
    )
