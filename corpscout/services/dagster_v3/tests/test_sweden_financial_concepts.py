from pathlib import Path

import dagster as dg

from dagster_v3.defs.sweden_company.translation import (
    LEGAL_FORM_LABEL_EN_BY_CODE,
    STATUS_REASON_LABEL_EN_BY_CODE,
)
from dagster_v3.defs.sweden_financial.concepts import (
    _INSERT_NEW_CONCEPTS_SQL,
    QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE,
)

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000150_corpscout_se_translations.up.sql"
)


def test_concepts_insert_sql_is_new_only_anti_join() -> None:
    sql = " ".join(_INSERT_NEW_CONCEPTS_SQL.split())
    # Merge semantics: INSERT of the anti-join remainder, never a replace.
    assert sql.startswith(
        f"INSERT INTO {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}"
    )
    assert "SELECT DISTINCT" in sql
    assert "LEFT ANTI JOIN" in sql
    assert "concept_local_name <> ''" in sql
    assert "TRUNCATE" not in sql.upper()
    assert "REPLACE" not in sql.upper()


def test_translation_assets_are_wired() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    graph = repo.asset_graph

    concepts_node = graph.get(dg.AssetKey("se_financial_facts_concepts"))
    assert concepts_node.group_name == "sweden_financial"
    assert concepts_node.parent_keys == {
        dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
        dg.AssetKey("sweden_financial_current_facts_clickhouse"),
    }

    concepts_load_node = graph.get(
        dg.AssetKey("sweden_financial_concepts_translation_load")
    )
    assert concepts_load_node.group_name == "sweden_financial"
    assert concepts_load_node.parent_keys == {
        dg.AssetKey("se_financial_facts_concepts")
    }

    labels_node = graph.get(dg.AssetKey("se_code_labels_clickhouse"))
    assert labels_node.group_name == "sweden_company"

    company_load_node = graph.get(dg.AssetKey("sweden_company_translation_load"))
    assert company_load_node.group_name == "sweden_company"
    assert company_load_node.parent_keys == {
        dg.AssetKey("sweden_company_companies_clickhouse")
    }

    check_keys = {
        (key.asset_key.path[-1], key.name)
        for key in repo.asset_checks_defs_by_key
    }
    assert (
        "sweden_financial_concepts_translation_load",
        "translator_queue_healthy",
    ) in check_keys
    assert (
        "sweden_company_translation_load",
        "translator_queue_healthy",
    ) in check_keys


def test_migration_owns_translation_schema() -> None:
    sql = _MIGRATION_PATH.read_text(encoding="utf-8")
    assert "corpscout.se_financial_facts_concepts" in sql
    assert "corpscout.se_financial_concept_labels" in sql
    assert "corpscout.se_code_labels" in sql
    assert "corpscout.se_companies_translated" in sql
    # The view join shape follows the NO/LV precedent: versioned argMax over
    # text_translations keyed on cityHash64 of the source column.
    assert "argMax(translated_text, version)" in sql
    assert "cityHash64(ifNull(c.activity_description, ''))" in sql
    assert "activity_description_en" in sql
    assert "legal_form_label_en" in sql
    assert "status_reason_label_en" in sql
    assert "label_en" in sql


def test_code_label_dictionaries_are_sane() -> None:
    assert len(LEGAL_FORM_LABEL_EN_BY_CODE) >= 50
    assert len(STATUS_REASON_LABEL_EN_BY_CODE) >= 15
    for mapping in (LEGAL_FORM_LABEL_EN_BY_CODE, STATUS_REASON_LABEL_EN_BY_CODE):
        for code, label in mapping.items():
            assert code == code.strip() and code
            assert label.strip(), code
    # High-volume codes measured on prod (2026-07-20) must be covered.
    for code in ("AB-ORGFO", "E-ORGFO", "HB-ORGFO", "10", "61", "49"):
        assert code in LEGAL_FORM_LABEL_EN_BY_CODE
    for code in ("VERKUPP-AVORG", "OVERK-AVORG", "KKAV-AVORG", "LIAV-AVORG"):
        assert code in STATUS_REASON_LABEL_EN_BY_CODE
