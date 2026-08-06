from pathlib import Path


DESIGN = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "ted_procurement"
    / "docs"
    / "france-slovakia-national-sources.md"
)


def test_france_follow_up_selects_decp_without_mislabeling_contract_value() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "decp-2022-marches-valides/exports/csv" in text
    assert "one row per (contract, holder)" in text
    assert "SIRET" in text and "SIREN" in text
    assert "`montant`" in text
    assert "not winner-attributable spend" in text
    assert "BOAMP" in text


def test_slovakia_follow_up_selects_uvo_and_keeps_crz_research_only() -> None:
    text = DESIGN.read_text(encoding="utf-8")

    assert "ajaxCalendar" in text
    assert "one row per (notice, lot, winner)" in text
    assert "IČO" in text
    assert "does not use a runtime environment gate" in text
    assert "export/rrrr-mm-dd.zip" in text
    assert "research-only" in text
