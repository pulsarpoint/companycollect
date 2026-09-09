"""The se_esef_* views: Sweden's slice of each ESEF product (spec 2026-09-09, section 1).

The ESEF products carry no country and no company id. One view per product Sweden reads
joins the product to the register-verified Swedish link of esef_entity_registry_map and
puts the registry id first as company_id. Migration 000395 embeds the rendering;
tests/test_esef_country_views.py pins the two equal so the map's contract cannot drift
from the DDL. A consumer never writes FINAL after a view name: the view already reads a
ReplacingMergeTree product FINAL.
"""

from dagster_v3.defs.esef_filings import tables

VERIFIED_SWEDISH_LINK_SQL = (
    "SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL "
    "WHERE country_iso2 = 'SE' AND link_status = 'register_verified'"
)


def build_se_esef_view_sql(view: tables.SeEsefView) -> str:
    columns = ",\n    ".join(f"t.{column}" for column in view.columns)
    final = " FINAL" if view.final else ""
    return (
        f"CREATE OR REPLACE VIEW corpscout.{view.view} AS\n"
        "SELECT\n"
        "    m.registry_id AS company_id,\n"
        f"    {columns}\n"
        f"FROM corpscout.{view.table} AS t{final}\n"
        f"INNER JOIN ({VERIFIED_SWEDISH_LINK_SQL}) AS m ON m.lei = t.lei"
    )


def render_all_se_esef_views_sql() -> str:
    return ";\n\n".join(build_se_esef_view_sql(view) for view in tables.SE_ESEF_VIEWS) + ";\n"
