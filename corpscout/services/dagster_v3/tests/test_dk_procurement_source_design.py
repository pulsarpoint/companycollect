from pathlib import Path


DESIGN = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "ted_procurement"
    / "docs"
    / "denmark-national-source.md"
)


def test_denmark_follow_up_selects_the_official_udbud_api() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "api.udbud.dk/udbud" in text
    assert "/ekstern-data/bekendtgoerelse/v1/fraKilde/{kilde}" in text
    assert "`DKUDBUD`" in text
    assert "`page`, `size`, and `since`" in text
    assert "MU_API_DATASYNK" in text
    assert "MitID Erhverv" in text


def test_denmark_design_keeps_versions_grains_and_source_overlap_explicit() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "base64-encoded eForms XML" in text
    assert "one row per (notice, lot, winner)" in text
    assert "winner-attributable" in text
    assert "noticeVersion" in text
    assert "must not ingest the `TED` API branch" in text
    assert "KFST EU-tender workbook" in text
    assert "CVR ClickHouse spine" in text
