from typing import Any

from dbt.adapters.duckdb.plugins import BasePlugin

from dagster_v3.domains import normalized_url, root_domain, website_host, website_path


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        conn.create_function(
            "normalized_url",
            normalized_url,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        conn.create_function(
            "website_host",
            website_host,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        conn.create_function(
            "website_path",
            website_path,
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
