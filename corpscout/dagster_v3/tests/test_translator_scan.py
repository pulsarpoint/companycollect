from translator.clickhouse import ScannedTerm, build_scan_sql, query_arrow
from translator.norway_brreg.config import get_config


def test_build_scan_sql_selects_distinct_untranslated_terms():
    config = get_config()
    sql = build_scan_sql(config, config.fields[1])  # activity_text (dynamic)
    assert "SELECT DISTINCT c.activity_text_original AS source_text" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "corpscout.text_translations" in sql
    assert "source_table = {table:String}" in sql
    assert "source_column = {column:String}" in sql
    assert "cityHash64(ifNull(c.activity_text_original, ''))" in sql
    assert "ifNull(c.activity_text_original, '') <> ''" in sql
    # Correct ClickHouse anti-join — NOT `LEFT JOIN ... WHERE t.hash IS NULL`
    # (which silently returns 0 rows under join_use_nulls=0).
    assert "LEFT ANTI JOIN" in sql
    assert "IS NULL" not in sql
    assert "static_key" not in sql


def test_build_scan_sql_per_field_uses_its_original_column():
    config = get_config()
    sql = build_scan_sql(config, config.fields[0])  # articles_purpose (dynamic)
    assert "c.articles_purpose_original" in sql
    assert "company_description_original" not in sql
    assert "static_key" not in sql


def test_build_scan_sql_for_legal_form_description_includes_key_column():
    config = get_config()
    lf_field = config.fields[2]  # legal_form_description (static)
    sql = build_scan_sql(config, lf_field)
    # source_text column must be present.
    assert "c.legal_form_description_original AS source_text" in sql
    # static_key column from the companion key column must be added.
    assert "c.legal_form_code AS static_key" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "cityHash64(ifNull(c.legal_form_description_original, ''))" in sql
    assert "ifNull(c.legal_form_description_original, '') <> ''" in sql
    assert "company_description_original" not in sql


def test_scanned_term_is_frozen_dataclass():
    t = ScannedTerm(source_column="legal_form_description_original", source_text="Aksjeselskap", static_key="AS")
    assert t.source_column == "legal_form_description_original"
    assert t.source_text == "Aksjeselskap"
    assert t.static_key == "AS"
    t_dynamic = ScannedTerm(source_column="activity_text_original", source_text="Holding", static_key=None)
    assert t_dynamic.static_key is None


def test_static_map_resolution_known_code():
    """Known legal-form code resolves to the correct English description."""
    config = get_config()
    lf_field = config.fields[2]  # legal_form_description
    mapping = lf_field.static_map_dict()
    assert mapping is not None
    assert mapping["AS"] == "Private limited company"
    assert mapping["ANS"] == "General partnership"
    assert mapping["ENK"] == "Sole proprietorship"


def test_static_map_resolution_unknown_code_returns_empty():
    """An unrecognised code must fall back to empty string (no translation)."""
    config = get_config()
    lf_field = config.fields[2]
    mapping = lf_field.static_map_dict() or {}
    assert mapping.get("UNKNOWN", "") == ""
    assert mapping.get("", "") == ""


def test_static_map_covers_all_register_legal_form_codes():
    """The legal-form dict must cover every code present in the Norway register
    (40 as of 2026-06), including the high-volume ones that were previously missing."""
    config = get_config()
    mapping = config.fields[2].static_map_dict() or {}
    for code in ("FLI", "ESEK", "UTLA", "BRL", "KBO", "SAM", "ANNA", "KF", "FKF", "SÆR", "STAT"):
        assert mapping.get(code), f"{code} must have an English translation"
    assert len(mapping) >= 40


class _FakeArrowClient:
    """Minimal fake that records calls and returns a list as a stand-in for a Table."""

    def __init__(self, rows):
        self._rows = rows
        self.calls: list[dict] = []

    def query_arrow(self, sql, *, parameters=None):
        self.calls.append({"sql": sql, "parameters": parameters})
        return self._rows  # stand-in for pa.Table


def test_query_arrow_delegates_to_client_query_arrow():
    client = _FakeArrowClient(["row1", "row2"])
    result = query_arrow(client, "SELECT 1", {"p": "v"})
    assert result == ["row1", "row2"]
    assert len(client.calls) == 1
    assert client.calls[0]["sql"] == "SELECT 1"
    assert client.calls[0]["parameters"] == {"p": "v"}


def test_query_arrow_passes_empty_dict_when_parameters_is_none():
    client = _FakeArrowClient([])
    query_arrow(client, "SELECT 2")
    assert client.calls[0]["parameters"] == {}
