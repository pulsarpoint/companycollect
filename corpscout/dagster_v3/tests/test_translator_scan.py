from translator.clickhouse import build_scan_sql
from translator.registry import get_source_config


def test_build_scan_sql_selects_distinct_untranslated_terms():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[2])  # company_description
    assert "SELECT DISTINCT c.company_description_original AS source_text" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "corpscout.text_translations" in sql
    assert "field = {field:String}" in sql
    assert "source_slug = {slug:String}" in sql
    assert "cityHash64(c.company_description_original)" in sql
    assert "c.company_description_original <> ''" in sql
    assert "t.source_text_hash IS NULL" in sql


def test_build_scan_sql_per_field_uses_its_original_column():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[0])  # articles_purpose
    assert "c.articles_purpose_original" in sql
    assert "company_description_original" not in sql
