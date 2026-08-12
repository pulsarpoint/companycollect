import json

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProjectComponent as _DbtProjectComponent

from dagster_v3.defs.company_domain_suggestions import tables


class CompanyDomainSuggestionsDbtComponent(_DbtProjectComponent):
    """Pass Dagster partition and run identity into the dbt transformation."""

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        runtime_vars = {
            "chunk_count": 1,
            "chunk_id": 0,
            "country_iso2": context.partition_key,
            "discovery_run_id": context.run_id,
            "max_identifiers_per_domain": tables.MAX_IDENTIFIERS_PER_DOMAIN,
            "max_domains_per_address": tables.MAX_DOMAINS_PER_ADDRESS,
        }
        return [
            *super().get_cli_args(context),
            "--vars",
            json.dumps(runtime_vars, sort_keys=True, separators=(",", ":")),
        ]


class CompanyDomainWebFeaturesDbtComponent(_DbtProjectComponent):
    """Load the global Common Crawl dbt assets independently of country assets."""

    @property
    def defs_state_config(self) -> DefsStateConfig:
        base_config = super().defs_state_config
        return DefsStateConfig(
            key=f"{base_config.key}:web-features",
            management_type=base_config.management_type,
            refresh_if_dev=base_config.refresh_if_dev,
        )
