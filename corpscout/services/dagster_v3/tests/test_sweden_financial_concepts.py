from pathlib import Path

import dagster as dg

from dagster_v3.defs.sweden_company.translation import (
    LEGAL_FORM_LABEL_EN_BY_CODE,
    STATUS_REASON_LABEL_EN_BY_CODE,
)
from dagster_v3.defs.sweden_financial.concepts import (
    _INSERT_NEW_CONCEPTS_SQL,
    _official_taxonomy_translation_insert_sql,
    _pending_taxonomy_entrypoints_sql,
    _taxonomy_label,
    QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE,
    QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW,
    TAXONOMY_TRANSLATION_FIELDS,
)

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000150_corpscout_se_translations.up.sql"
)
_READABLE_LABEL_MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000250_corpscout_se_financial_concept_labels_readable.up.sql"
)
_TAXONOMY_TRANSLATION_MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000251_corpscout_se_financial_taxonomy_translations.up.sql"
)


def test_concepts_insert_sql_is_new_only_anti_join() -> None:
    sql = " ".join(_INSERT_NEW_CONCEPTS_SQL.split())
    # Merge semantics: INSERT of the anti-join remainder, never a replace.
    assert sql.startswith(f"INSERT INTO {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}")
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
        dg.AssetKey("sweden_financial_taxonomy_translation_load")
    )
    assert concepts_load_node.group_name == "sweden_financial"
    assert concepts_load_node.parent_keys == {
        dg.AssetKey("se_financial_taxonomy_official_translations")
    }

    official_translations_node = graph.get(
        dg.AssetKey("se_financial_taxonomy_official_translations")
    )
    assert official_translations_node.parent_keys == {
        dg.AssetKey("se_financial_taxonomy_concepts")
    }

    taxonomy_node = graph.get(dg.AssetKey("se_financial_taxonomy_concepts"))
    assert taxonomy_node.group_name == "sweden_financial"
    assert taxonomy_node.parent_keys == {
        dg.AssetKey("se_financial_facts_concepts"),
        dg.AssetKey("sweden_financial_backfill_reports_clickhouse"),
        dg.AssetKey("sweden_financial_current_reports_clickhouse"),
    }

    labels_node = graph.get(dg.AssetKey("se_code_labels_clickhouse"))
    assert labels_node.group_name == "sweden_company"

    company_load_node = graph.get(dg.AssetKey("sweden_company_translation_load"))
    assert company_load_node.group_name == "sweden_company"
    assert company_load_node.parent_keys == {
        dg.AssetKey("sweden_company_companies_clickhouse")
    }

    check_keys = {
        (key.asset_key.path[-1], key.name) for key in repo.asset_checks_defs_by_key
    }
    assert (
        "sweden_financial_taxonomy_translation_load",
        "translations_present",
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


def test_concept_label_view_prefers_taxonomy_and_humanizes_fallbacks() -> None:
    sql = _READABLE_LABEL_MIGRATION_PATH.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_financial_taxonomy_concepts" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_financial_taxonomy_loads" in sql
    assert "CREATE OR REPLACE VIEW corpscout.se_financial_concept_labels" in sql
    assert "description_sv String" in sql
    assert "description_en String" in sql
    assert "argMaxIf(" in sql
    assert "ifNull(o.label_en, '') != ''" in sql
    assert "'taxonomy'" in sql
    assert "'translation'" in sql
    assert "ifNull(t.translated_text, '')" in sql
    assert "'([A-Z]+)([A-Z][a-z])'" in sql
    assert "'([a-z0-9])([A-Z])'" in sql
    assert "'([A-Za-z])([0-9])'" in sql
    assert "'([0-9])([A-Za-z])'" in sql
    assert "'\\\\1 \\\\2'" in sql


def test_taxonomy_translations_are_separate_and_preserve_official_precedence() -> None:
    sql = _TAXONOMY_TRANSLATION_MIGRATION_PATH.read_text(encoding="utf-8")

    assert (
        "CREATE OR REPLACE VIEW corpscout.se_financial_taxonomy_concepts_current" in sql
    )
    assert (
        "CREATE OR REPLACE VIEW corpscout.se_financial_taxonomy_concept_labels" in sql
    )
    assert "source_table = 'corpscout.se_financial_taxonomy_concepts_current'" in sql
    assert "source_column = 'label_sv'" in sql
    assert "source_column = 'description_sv'" in sql
    assert "concepts.label_en != '', concepts.label_en" in sql
    assert "concepts.description_en != '', concepts.description_en" in sql
    assert "'translation'" in sql
    assert "'identifier'" in sql
    assert "source_column = 'concept_local_name'" not in sql


def test_translation_loader_uses_official_swedish_taxonomy_text_only() -> None:
    assert {
        (field.table, field.column, field.extra_where)
        for field in TAXONOMY_TRANSLATION_FIELDS
    } == {
        (
            QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW,
            "label_sv",
            "label_en = ''",
        ),
        (
            QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW,
            "description_sv",
            "description_en = ''",
        ),
    }


def test_official_taxonomy_pairs_are_written_to_the_shared_cache() -> None:
    sql = _official_taxonomy_translation_insert_sql(
        source_column="label_sv",
        target_column="label_en",
    )

    assert "INSERT INTO corpscout.text_translations" in sql
    assert "cityHash64(label_sv)" in sql
    assert "'sv'" in sql
    assert "'en'" in sql
    assert "'taxonomy'" in sql
    assert "'bolagsverket-official-taxonomy'" in sql
    assert "argMax(translated_text, version)" in sql
    assert "current.translated_text != candidates.translated_text" in sql


def test_taxonomy_entrypoint_selector_is_idempotent_by_default() -> None:
    default_sql = _pending_taxonomy_entrypoints_sql(refresh_existing=False)
    refresh_sql = _pending_taxonomy_entrypoints_sql(refresh_existing=True)

    assert "argMax(status, resolved_at)" in default_sql
    assert "ifNull(loads.status, '') != 'success'" in default_sql
    assert "ifNull(loads.status, '') != 'success'" not in refresh_sql
    assert "%(taxonomy_entrypoints)s" in default_sql
    assert "%(max_taxonomies)s" in default_sql


def test_taxonomy_label_normalizes_whitespace_without_inventing_text() -> None:
    class Concept:
        def label(self, **kwargs: object) -> str:
            assert kwargs["lang"] == "en"
            assert kwargs["fallbackToQname"] is False
            return "  Official\n taxonomy   description  "

    assert _taxonomy_label(Concept(), language="en") == (
        "Official taxonomy description"
    )


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
