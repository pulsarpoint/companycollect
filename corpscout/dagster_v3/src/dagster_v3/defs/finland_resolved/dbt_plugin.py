"""dbt-duckdb plugin: registers the fi_primary_industry_json UDF on each connection."""

from typing import Any

from dbt.adapters.duckdb.plugins import BasePlugin

from dagster_v3.defs.finland_resolved.industry import primary_industry_json


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        conn.create_function(
            "fi_primary_industry_json",
            primary_industry_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
