import json
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProject, DbtProjectComponent

_ALL_PARTITIONS_SOURCE_KEYS = {
    dg.AssetKey("esef_document_contact_candidates_clickhouse"),
}


class CompanyServingDbtComponent(DbtProjectComponent):
    """Build one country's company-serving projections."""

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: DbtProject | None,
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        if unique_id.startswith("source."):
            # Multiple dbt projects may depend on the same ClickHouse source.
            # Generated source-file references differ by component state path, so
            # leave source ownership metadata to the canonical asset definition.
            return spec.replace_attributes(metadata={})
        return spec.replace_attributes(
            deps=[
                dg.AssetDep(
                    dependency.asset_key,
                    partition_mapping=dg.AllPartitionMapping(),
                    metadata=dependency.metadata,
                )
                if dependency.asset_key in _ALL_PARTITIONS_SOURCE_KEYS
                else dependency
                for dependency in spec.deps
            ]
        )

    @property
    def defs_state_config(self) -> DefsStateConfig:
        base_config = super().defs_state_config
        return DefsStateConfig(
            key=f"{base_config.key}:company-serving",
            management_type=base_config.management_type,
            refresh_if_dev=base_config.refresh_if_dev,
        )

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        return [
            *super().get_cli_args(context),
            "--vars",
            json.dumps(
                {
                    "country_code": context.partition_key,
                    "source_run_id": context.run_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        ]
