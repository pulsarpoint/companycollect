import json
from pathlib import Path

import duckdb

from dagster_v3.defs.finland_resolved import dbt_plugin

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_resolved" / "dbt"
)


def test_plugin_registers_primary_industry_udf() -> None:
    conn = duckdb.connect(":memory:")
    dbt_plugin.Plugin(name="industry", plugin_config={}).configure_connection(conn)
    raw = json.dumps({"mainBusinessLine": {"code": "62010", "codeSet": "NACE_REV_2",
                                            "descriptions": [{"languageCode": "1", "description": "Ohjelmistot"}]}})
    out = conn.execute("select fi_primary_industry_json(?)", [raw]).fetchone()[0]
    parsed = json.loads(out)
    assert parsed["code"] == "62010"
    assert parsed["codeSet"] == "NACE_REV_2"
    assert parsed["description"] == "Ohjelmistot"
    assert parsed["language"] == "fi"
    assert conn.execute("select fi_primary_industry_json(null)").fetchone()[0] is None
