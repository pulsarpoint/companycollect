"""dbt-duckdb plugin: registers the fi_primary_industry_json UDF on each connection."""

from typing import Any

from dbt.adapters.duckdb.plugins import BasePlugin

from dagster_v3.domains import root_domain
from dagster_v3.defs.finland_ytj.industry import primary_industry_json
from dagster_v3.defs.finland_ytj.registry import legal_form_json, registration_flags_json


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        conn.create_function(
            "fi_primary_industry_json",
            primary_industry_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        conn.create_function(
            "fi_legal_form_json",
            legal_form_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        conn.create_function(
            "fi_registration_flags_json",
            registration_flags_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        conn.create_function(
            "root_domain",
            root_domain,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
