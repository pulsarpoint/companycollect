from pathlib import Path


DESIGN = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "ted_procurement"
    / "docs"
    / "latvia-national-source.md"
)


def test_latvia_follow_up_selects_iub_daily_json_with_explicit_versioning() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "open.iub.gov.lv/data/notice" in text
    assert "DD-MM-YYYY.json" in text
    assert "CC0 1.0" in text
    assert "`clonedFrom`" in text
    assert "latest version" in text


def test_latvia_design_keeps_award_and_execution_grains_and_values_separate() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "one row per (notice, lot, winner)" in text
    assert "one row per (execution notice, contract, winner)" in text
    assert "winner-attributable" in text
    assert "must not be counted as a second award" in text
    assert "EIS" in text
