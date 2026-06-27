from translator.clickhouse import ScannedTerm, build_scan_sql
from translator.registry import get_source_config


def test_build_scan_sql_selects_distinct_untranslated_terms():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[2])  # company_description (dynamic)
    assert "SELECT DISTINCT c.company_description_original AS source_text" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "corpscout.text_translations" in sql
    assert "source_table = {table:String}" in sql
    assert "source_column = {column:String}" in sql
    assert "cityHash64(c.company_description_original)" in sql
    assert "c.company_description_original <> ''" in sql
    assert "t.source_text_hash IS NULL" in sql
    assert "static_key" not in sql


def test_build_scan_sql_per_field_uses_its_original_column():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[0])  # articles_purpose (dynamic)
    assert "c.articles_purpose_original" in sql
    assert "company_description_original" not in sql
    assert "static_key" not in sql


def test_build_scan_sql_for_legal_form_description_includes_key_column():
    config = get_source_config("norway_brreg")
    lf_field = config.fields[3]  # legal_form_description (static)
    sql = build_scan_sql(config, lf_field)
    # source_text column must be present.
    assert "c.legal_form_description_original AS source_text" in sql
    # static_key column from the companion key column must be added.
    assert "c.legal_form_code AS static_key" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "cityHash64(c.legal_form_description_original)" in sql
    assert "c.legal_form_description_original <> ''" in sql
    assert "company_description_original" not in sql


def test_scanned_term_is_frozen_dataclass():
    t = ScannedTerm(source_column="legal_form_description_original", source_text="Aksjeselskap", static_key="AS")
    assert t.source_column == "legal_form_description_original"
    assert t.source_text == "Aksjeselskap"
    assert t.static_key == "AS"
    t_dynamic = ScannedTerm(source_column="company_description_original", source_text="Holding", static_key=None)
    assert t_dynamic.static_key is None


def test_static_map_resolution_known_code():
    """Known legal-form code resolves to the correct English description."""
    config = get_source_config("norway_brreg")
    lf_field = config.fields[3]  # legal_form_description
    mapping = lf_field.static_map_dict()
    assert mapping is not None
    assert mapping["AS"] == "Private limited company"
    assert mapping["ANS"] == "General partnership"
    assert mapping["ENK"] == "Sole proprietorship"


def test_static_map_resolution_unknown_code_returns_empty():
    """An unrecognised code must fall back to empty string (no translation)."""
    config = get_source_config("norway_brreg")
    lf_field = config.fields[3]
    mapping = lf_field.static_map_dict() or {}
    assert mapping.get("UNKNOWN", "") == ""
    assert mapping.get("", "") == ""


def test_static_map_covers_all_register_legal_form_codes():
    """The legal-form dict must cover every code present in the Norway register
    (40 as of 2026-06), including the high-volume ones that were previously missing."""
    config = get_source_config("norway_brreg")
    mapping = config.fields[3].static_map_dict() or {}
    for code in ("FLI", "ESEK", "UTLA", "BRL", "KBO", "SAM", "ANNA", "KF", "FKF", "SÆR", "STAT"):
        assert mapping.get(code), f"{code} must have an English translation"
    assert len(mapping) >= 40
