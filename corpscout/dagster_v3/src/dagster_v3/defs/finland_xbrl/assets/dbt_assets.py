from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_v3.defs.finland_xbrl.assets.common import (
    FINLAND_XBRL_DBT_PROJECT_DIR,
    FINLAND_XBRL_DUCKDB_POOL,
)

finland_xbrl_dbt_project = DbtProject(
    project_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
)
finland_xbrl_dbt_project.prepare_if_dev()


class FinlandXbrlDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, props: Mapping[str, Any]) -> dg.AssetKey:
        if props["resource_type"] == "source":
            return super().get_asset_key(props)
        if props["name"] == "eligible_financial_reports":
            return dg.AssetKey("finland_xbrl_eligible_financial_reports")
        return dg.AssetKey(props["name"])  # fi_prh_xbrl_financial_metrics keeps its name

    def get_group_name(self, props: Mapping[str, Any]) -> str:
        return "finland_xbrl"


@dbt_assets(
    manifest=finland_xbrl_dbt_project.manifest_path,
    project=finland_xbrl_dbt_project,
    dagster_dbt_translator=FinlandXbrlDbtTranslator(),
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_dbt_assets(
    context: dg.AssetExecutionContext,
    finland_xbrl_dbt: DbtCliResource,
):
    yield from finland_xbrl_dbt.cli(["build"], context=context).stream()
