from pathlib import Path

import dagster_v3.defs.finland_xbrl.assets as xbrl_assets


def test_finland_xbrl_does_not_expose_source_local_dbt_project() -> None:
    finland_xbrl_dir = (
        Path(__file__).parents[1] / "src" / "dagster_v3" / "defs" / "finland_xbrl"
    )

    assert not (finland_xbrl_dir / "dbt").exists()
    assert all("dbt" not in name.lower() for name in xbrl_assets.__all__)
