"""Text pins of the five SQL extractors: each SELECT returns SUGGESTION_SELECT_COLUMNS in
order, binds %(company_ids)s, reads FINAL rows, and maps codes the way the spec says."""

import re

from dagster import AssetKey

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
    # Slice 6: the same code as its own field, three values.
    assert "multiIf(source_status_code = '1', 'active', source_status_code = '0', 'never', source_status_code = '9', 'ceased', NULL) AS economic_activity" in sql
    assert scb.SCB_EXTRACTOR_VERSION == "scb-v2"
    assert "registration_date AS incorporation_date" in sql
    for column in ("lei", "wikidata_id", "description", "description_language", "description_sv"):
        assert f"CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    assert scb.scb_current_sql() == (
        "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL WHERE has_company = 1"
    )


def test_only_scb_supplies_economic_activity() -> None:
    for module_sql in (bolagsverket.bolagsverket_select_sql(), esef.esef_select_sql(), ratsit.ratsit_select_sql(), wikidata.wikidata_select_sql()):
        assert "CAST(NULL AS Nullable(String)) AS economic_activity" in module_sql


def test_bolagsverket_select_matches_the_contract() -> None:
    sql = bolagsverket.bolagsverket_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    # The register is read through its _translated view (migration 000390): the English
    # text and its stamp are columns, and no extractor joins text_translations itself.
    assert "FROM corpscout.se_bolagsverket_companies_translated AS register FINAL" in sql
    assert "text_translations" not in sql and "cityHash64" not in sql
    assert "WHERE has_company = 1 AND company_id IN %(company_ids)s" in sql
    assert "'bolagsverket' AS source" in sql
    assert REGISTER_UID in sql and "'sweden_bolagsverket'" in sql
    assert "if(register.deregistration_date IS NULL, 'active', 'inactive') AS status" in sql
    # The organisationsform token becomes SCB's juridisk form code, so the entity has one
    # legal-form vocabulary whichever source wins; an unknown token passes through.
    assert (
        "nullIf(transform(trim(ifNull(register.legal_form_code, '')), ['AB-ORGFO', " in sql
        and "trim(ifNull(register.legal_form_code, ''))), '') AS legal_form_code" in sql
    )
    assert bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION == "bolagsverket-v3"
    swedish = "nullIf(trim(ifNull(register.activity_description, '')), '')"
    assert f"if(register.activity_description_en != '', register.activity_description_en, {swedish}) AS description" in sql
    assert f"if(register.activity_description_en != '', 'en', if({swedish} IS NULL, NULL, 'sv')) AS description_language" in sql
    assert f"{swedish} AS description_sv" in sql
    # observed_at is the later of the register stamp and the translation stamp, so a text
    # translated after the last extraction re-selects the company. The text guards the
    # stamp: under join_use_nulls = 1 an untranslated row's stamp is NULL and a bare
    # greatest would be NULL.
    observed_at = (
        "greatest(register.observed_at, if(register.activity_description_en != '', "
        "ifNull(register.activity_description_translated_at, register.observed_at), register.observed_at))"
    )
    assert f"    {observed_at} AS observed_at,\n" in sql
    # current_sql computes the same observed_at, unscoped, so the change scan converges.
    assert bolagsverket.bolagsverket_current_sql() == (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {observed_at} AS observed_at\n"
        "FROM corpscout.se_bolagsverket_companies_translated AS register FINAL\n"
        "WHERE has_company = 1"
    )


def test_esef_select_takes_the_newest_filing_per_company() -> None:
    sql = esef.esef_select_sql()
    assert _aliases(sql) == list(SUGGESTION_SELECT_COLUMNS)
    # se_esef_document_company_information (migration 000395) already restricts to Sweden
    # and stamps company_id from the register-verified link -- no country_iso2 filter here.
    assert "FROM corpscout.se_esef_document_company_information" in sql
    assert "country_iso2" not in sql
    assert "trim(company_description) != ''" in sql
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
    assert "FROM corpscout.se_esef_document_company_information" in current
    assert "country_iso2" not in current


def test_esef_suggestion_asset_depends_on_the_entity_registry_map() -> None:
    # The view has no asset of its own: se_basic_info_suggestions_esef stays on the product
    # asset that fills esef_document_company_information, and also needs the entity-registry
    # map asset that fills the register-verified link se_esef_document_company_information
    # joins through.
    assert AssetKey("esef_document_company_information_clickhouse") in (
        esef.se_basic_info_suggestions_esef.dependency_keys
    )
    assert AssetKey("esef_entity_registry_map_clickhouse") in (
        esef.se_basic_info_suggestions_esef.dependency_keys
    )


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
    # Read through the _translated view (migration 000390); no text_translations join here.
    assert "FROM corpscout.se_ratsit_company_translated FINAL" in sql
    assert "text_translations" not in sql and "cityHash64" not in sql
    assert "normalizer_version = %(normalizer_version)s" in sql and "company_id IN %(company_ids)s" in sql
    assert "concat('ratsit:', toString(result_sha256)) AS source_record_uid" in sql
    stamp = "toDateTime64(normalized_at, 3, 'UTC')"
    observed_at = (
        f"greatest({stamp}, if(business_description_en != '', "
        f"ifNull(business_description_translated_at, {stamp}), {stamp}))"
    )
    assert f"    {observed_at} AS observed_at,\n" in sql
    assert "nullIf(trim(name), '') AS legal_name" in sql
    assert "multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status" in sql
    swedish = "nullIf(trim(ifNull(business_description, '')), '')"
    assert f"if(business_description_en != '', business_description_en, {swedish}) AS description" in sql
    assert f"if(business_description_en != '', 'en', if({swedish} IS NULL, NULL, 'sv')) AS description_language" in sql
    assert f"{swedish} AS description_sv" in sql
    assert "CAST(NULL AS Nullable(String)) AS legal_form_code" in sql
    assert sql.rstrip().endswith("ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id")
    assert ratsit.RATSIT_EXTRACTOR_VERSION == "ratsit-v2"
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    # current_sql takes the newest report per company and stamps it exactly as the SELECT
    # does, so the change scan converges even when an older report's text is translated
    # later than the newest report.
    assert ratsit.ratsit_current_sql() == (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        company_id AS company_id,\n"
        f"        {observed_at} AS observed_at\n"
        "    FROM corpscout.se_ratsit_company_translated FINAL\n"
        "    WHERE normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "    LIMIT 1 BY company_id\n"
        ")"
    )
