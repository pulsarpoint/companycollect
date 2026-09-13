"""Financial slice 4a (spec 2026-09-11 section 10): the section-presence model's financials
leg and publish.py's financials reconciliation read the financial entity's ACTIVE rows, keyed
by company, not se_company_financials_latest."""

from pathlib import Path

from dagster_v3.defs.company_serving import publish

DBT_MODELS = Path(publish.__file__).resolve().parent / "dbt" / "models"


def test_the_presence_model_reads_the_entitys_active_rows() -> None:
    text = (DBT_MODELS / "company_section_presence_current_build.sql").read_text(encoding="utf-8")
    # The executable lines only: the model's own comment names the projection it replaced.
    model = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    assert "FROM {{ source('corpscout', 'se_company_financial') }} AS financials FINAL" in model
    assert "WHERE financials.active = 1" in model
    assert "financials.company_id, 'financials', financials.company_id, financials.folded_at" in model
    assert "se_company_financials_latest" not in model
    sources = (DBT_MODELS / "sources.yml").read_text(encoding="utf-8")
    assert "      - name: se_company_financial\n" in sources


def test_the_publish_reconciliation_counts_the_same_rows() -> None:
    text = Path(publish.__file__).read_text(encoding="utf-8")
    assert "FROM corpscout.se_company_financial AS financials FINAL" in text
    assert "WHERE financials.active = 1" in text
    assert "corpscout.se_company_financials_latest AS financials" not in text
