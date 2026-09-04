"""Text pins of the five SQL extractors: each SELECT returns SUGGESTION_SELECT_COLUMNS in
order, binds %(company_ids)s, reads FINAL rows, and maps codes the way the spec says."""

import re

from dagster_v3.defs.se_company.basic_info import bolagsverket, scb
from dagster_v3.defs.se_company.basic_info.extract import SUGGESTION_SELECT_COLUMNS

REGISTER_UID = "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', "


def _aliases(select_sql: str) -> list[str]:
    """The top-level output aliases of a SELECT, from its last projection list."""
    body = select_sql.strip()
    head = body[body.rfind("\nSELECT") + 8 :] if "\nSELECT" in body else body[7:]
    projection = head[: head.index("\nFROM ")]
    return [m.group(1) for m in re.finditer(r"AS (\w+)\s*(?:,|$)", projection, flags=re.M)]


def test_scb_select_matches_the_contract() -> None:
    sql = scb.scb_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    assert "FROM corpscout.se_scb_companies FINAL" in sql
    assert "has_company = 1" in sql and "company_id IN %(company_ids)s" in sql
    assert "'scb' AS source" in sql
    assert REGISTER_UID in sql and "'sweden_scb'" in sql
    assert "multiIf(source_status_code = '1', 'active', source_status_code IN ('0', '9'), 'inactive', NULL) AS status" in sql
    assert "registration_date AS incorporation_date" in sql
    for column in ("lei", "wikidata_id", "description", "description_language", "description_sv"):
        assert f"CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    assert scb.scb_current_sql() == (
        "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL WHERE has_company = 1"
    )


def test_bolagsverket_select_matches_the_contract() -> None:
    sql = bolagsverket.bolagsverket_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    assert "FROM corpscout.se_bolagsverket_companies FINAL" in sql
    assert "'bolagsverket' AS source" in sql
    assert REGISTER_UID in sql and "'sweden_bolagsverket'" in sql
    assert "if(register.deregistration_date IS NULL, 'active', 'inactive') AS status" in sql
    # The English description is the translation pipeline's, keyed the way it keys itself.
    assert "source_table = 'corpscout.se_companies'" in sql
    assert "source_column = 'activity_description'" in sql
    assert "source_lang = 'sv' AND target_lang = 'en'" in sql
    assert "cityHash64(ifNull(register.activity_sv, ''))" in sql
    assert "argMax(translated_text, version) AS translated_text" in sql
    assert "if(ifNull(translation.translated_text, '') != '', translation.translated_text, register.activity_sv) AS description" in sql
    assert "if(ifNull(translation.translated_text, '') != '', 'en', if(register.activity_sv IS NULL, NULL, 'sv')) AS description_language" in sql
    assert "register.activity_sv AS description_sv" in sql
    assert bolagsverket.bolagsverket_current_sql() == (
        "SELECT company_id, observed_at FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 1"
    )
