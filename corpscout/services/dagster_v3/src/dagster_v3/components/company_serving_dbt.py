import json
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProject, DbtProjectComponent

_ALL_PARTITIONS_SOURCE_KEYS = {
    dg.AssetKey("esef_document_contact_candidates_clickhouse"),
}

_ESEF_ENTITY_REGISTRY_MAP_KEY = dg.AssetKey("esef_entity_registry_map_clickhouse")
# ESEF slice 1 (2026-09-09): sources.yml no longer declares esef_entity_registry_map -- these
# models read a se_esef_* view instead, which resolves company_id by joining the map INSIDE
# ClickHouse, invisible to dbt's ref()/source() graph. Add the map back explicitly so
# company_serving's lineage still reaches it now that no model's SQL names it directly.
_ESEF_MODELS_READING_THE_MAP_THROUGH_A_VIEW = {
    "company_contact_current_build",
    "company_description_current_build",
    "company_management_current_build",
    "company_section_item_source_links_build",
    "company_domains_build",
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
        deps = [
            dg.AssetDep(
                dependency.asset_key,
                partition_mapping=dg.AllPartitionMapping(),
                metadata=dependency.metadata,
            )
            if dependency.asset_key in _ALL_PARTITIONS_SOURCE_KEYS
            else dependency
            for dependency in spec.deps
        ]
        model_name = unique_id.rsplit(".", 1)[-1]
        if (
            model_name in _ESEF_MODELS_READING_THE_MAP_THROUGH_A_VIEW
            and _ESEF_ENTITY_REGISTRY_MAP_KEY
            not in {dependency.asset_key for dependency in deps}
        ):
            deps.append(dg.AssetDep(_ESEF_ENTITY_REGISTRY_MAP_KEY))
        return spec.replace_attributes(deps=deps)

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
