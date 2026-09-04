"""Text pins of the five SQL extractors: each SELECT returns SUGGESTION_SELECT_COLUMNS in
order, binds %(company_ids)s, reads FINAL rows, and maps codes the way the spec says."""

import re

from dagster_v3.defs.se_company.basic_info import bolagsverket, esef, ratsit, scb, wikidata
from dagster_v3.defs.se_company.basic_info.extract import SUGGESTION_SELECT_COLUMNS
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

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
    # The organisationsform token becomes SCB's juridisk form code, so the entity has one
    # legal-form vocabulary whichever source wins; an unknown token passes through.
    assert (
        "nullIf(transform(trim(ifNull(register.legal_form_code, '')), ['AB-ORGFO', " in sql
        and "trim(ifNull(register.legal_form_code, ''))), '') AS legal_form_code" in sql
    )
    assert bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION == "bolagsverket-v2"
    # The English description is the translation pipeline's, keyed the way it keys itself.
    assert "source_table = 'corpscout.se_companies'" in sql
    assert "source_column = 'activity_description'" in sql
    assert "source_lang = 'sv' AND target_lang = 'en'" in sql
    assert "cityHash64(ifNull(register.activity_sv, ''))" in sql
    # The empty-string hash never enters the translation set, so a company without Swedish
    # text cannot join a translation of some other company's empty description.
    assert "FROM register WHERE activity_sv IS NOT NULL)" in sql
    assert "argMax(translated_text, version) AS translated_text" in sql
    assert "if(ifNull(translation.translated_text, '') != '', translation.translated_text, register.activity_sv) AS description" in sql
    assert "if(ifNull(translation.translated_text, '') != '', 'en', if(register.activity_sv IS NULL, NULL, 'sv')) AS description_language" in sql
    assert "register.activity_sv AS description_sv" in sql
    # The translation is a second input: observed_at is the later of the register row's own
    # stamp and the translation's (text_translations.version, unix seconds), so a company
    # whose text is translated after its last extraction is visited again instead of
    # keeping the Swedish text on an English-facing field forever.
    observed_at = "greatest(register.observed_at, ifNull(translation.translated_at, register.observed_at))"
    assert "toDateTime64(max(version), 3, 'UTC') AS translated_at" in sql
    assert f"    {observed_at} AS observed_at,\n" in sql
    current = bolagsverket.bolagsverket_current_sql()
    # current_sql carries the same CTEs and join, unscoped, so the change scan and the
    # SELECT compute the same observed_at and the scan converges.
    assert current.startswith("WITH register AS (\n")
    assert "FROM corpscout.se_bolagsverket_companies FINAL\n    WHERE has_company = 1\n" in current
    assert "%(company_ids)s" not in current
    assert "toDateTime64(max(version), 3, 'UTC') AS translated_at" in current
    assert current.endswith(
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {observed_at} AS observed_at\n"
        "FROM register\n"
        "LEFT JOIN translations AS translation\n"
        "    ON translation.source_text_hash = cityHash64(ifNull(register.activity_sv, ''))"
    )


def test_esef_select_takes_the_newest_filing_per_company() -> None:
    sql = esef.esef_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    assert "FROM corpscout.esef_document_company_information" in sql
    assert "country_iso2 = 'SE'" in sql and "trim(company_description) != ''" in sql
    assert "company_id IN %(company_ids)s" in sql
    assert "toDateTime64(resolved_at, 3, 'UTC') AS observed_at" in sql
    assert "nullIf(upperUTF8(trim(lei)), '') AS lei" in sql
    assert "if(toString(description_language) = '', 'en', toString(description_language)) AS description_language" in sql
    # source_record_uid is a hash over package_sha256, so it cannot separate two
    # extractions of the same package: prompt_version and model_name make the winner
    # deterministic, the way the old publisher ordered.
    assert sql.rstrip().endswith(
        "ORDER BY resolved_at DESC, fiscal_year DESC, prompt_version DESC, model_name DESC, source_record_uid DESC\n"
        "LIMIT 1 BY company_id"
    )
    current = esef.esef_current_sql()
    assert "max(toDateTime64(resolved_at, 3, 'UTC')) AS observed_at" in current and "GROUP BY company_id" in current


def test_wikidata_select_links_entities_through_orgnr_or_lei() -> None:
    links = wikidata.wikidata_links_cte_sql()
    assert "FROM corpscout.se_scb_companies FINAL WHERE has_company = 1" in links
    assert "FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 1" in links
    assert "UNION DISTINCT" in links
    assert "identifiers.identifier_type = 'se_orgnr'" in links
    assert "replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '')" in links
    assert "issuer_scheme = 'lei' AND identifiers.is_current = 1" in links
    assert "identifiers.identifier_type = 'lei'" in links
    assert "%(company_ids)s" not in links
    sql = wikidata.wikidata_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    assert "'wikidata' AS source" in sql
    assert "concat('wikidata:', entity.wikidata_id) AS source_record_uid" in sql
    assert "entity.resolved_at AS observed_at" in sql
    assert "nullIf(trim(ifNull(entity.official_name, '')), '') AS legal_name" in sql
    assert "if(entity.inception_date > toDate('1970-01-01'), toDate32(entity.inception_date), NULL) AS incorporation_date" in sql
    assert "entity.wikidata_id AS wikidata_id" in sql
    assert "if(entity.company_description IS NULL OR trim(entity.company_description) = '', NULL, 'en') AS description_language" in sql
    assert sql.rstrip().endswith("ORDER BY entity.resolved_at DESC, entity.wikidata_id ASC\nLIMIT 1 BY links.company_id")
    assert "links.company_id IN %(company_ids)s" in sql
    assert "FROM corpscout.se_scb_companies FINAL WHERE has_company = 1 AND company_id IN %(company_ids)s" in sql
    assert "FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 1 AND company_id IN %(company_ids)s" in sql
    assert "%(company_ids)s" not in wikidata.wikidata_current_sql()


def test_ratsit_select_takes_the_newest_report_and_maps_status_text() -> None:
    sql = ratsit.ratsit_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    assert "FROM corpscout.se_ratsit_company FINAL" in sql
    assert "normalizer_version = %(normalizer_version)s" in sql and "company_id IN %(company_ids)s" in sql
    assert "concat('ratsit:', toString(result_sha256)) AS source_record_uid" in sql
    assert "toDateTime64(normalized_at, 3, 'UTC') AS observed_at" in sql
    assert "nullIf(trim(name), '') AS legal_name" in sql
    assert "multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status" in sql
    assert "nullIf(trim(ifNull(business_description, '')), '') AS description" in sql
    assert "if(nullIf(trim(ifNull(business_description, '')), '') IS NULL, NULL, 'sv') AS description_language" in sql
    assert "nullIf(trim(ifNull(business_description, '')), '') AS description_sv" in sql
    assert "CAST(NULL AS Nullable(String)) AS legal_form_code" in sql
    assert sql.rstrip().endswith("ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id")
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    current = ratsit.ratsit_current_sql()
    assert "toDateTime64(max(normalized_at), 3, 'UTC') AS observed_at" in current
    assert "normalizer_version = %(normalizer_version)s" in current and "GROUP BY company_id" in current
