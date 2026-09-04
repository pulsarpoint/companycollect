"""Wikidata entity -> basic-info suggestion for Swedish companies linked by orgnr or LEI."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset

WIKIDATA_EXTRACTOR_VERSION = "wikidata-v1"


def wikidata_links_cte_sql(*, scoped: bool = False) -> str:
    """CTEs `swedish`, `company_leis`, `links` (company_id, wikidata_id): every Swedish
    register company linked to a Wikidata entity directly (identifier_type se_orgnr) or
    through a current LEI. The universe is the union of the two register source tables,
    not the retiring se_companies spine.

    `scoped=True` narrows the `swedish` CTE to `%(company_ids)s` up front, so a page's
    SELECT never rebuilds the whole multi-million-row register universe -- `select_sql`
    uses this; `current_sql`'s per-source scan stays unscoped."""
    company_ids_filter = " AND company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH swedish AS (\n"
        f"    SELECT company_id FROM corpscout.se_scb_companies FINAL WHERE has_company = 1{company_ids_filter}\n"
        "    UNION DISTINCT\n"
        f"    SELECT company_id FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 1{company_ids_filter}\n"
        "),\n"
        "company_leis AS (\n"
        "    SELECT identifiers.company_id AS company_id, upperUTF8(identifiers.issuer_id) AS lei\n"
        "    FROM corpscout.company_identifier AS identifiers\n"
        "    INNER JOIN swedish AS companies ON companies.company_id = identifiers.company_id\n"
        "    WHERE identifiers.country_code = 'SE' AND identifiers.issuer_scheme = 'lei' AND identifiers.is_current = 1\n"
        "    GROUP BY identifiers.company_id, lei\n"
        "),\n"
        "links AS (\n"
        "    SELECT company_id, wikidata_id FROM (\n"
        "        SELECT companies.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN swedish AS companies\n"
        "            ON companies.company_id = replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '')\n"
        "        WHERE identifiers.identifier_type = 'se_orgnr'\n"
        "        UNION ALL\n"
        "        SELECT leis.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN company_leis AS leis ON leis.lei = upperUTF8(identifiers.identifier_value)\n"
        "        WHERE identifiers.identifier_type = 'lei'\n"
        "    )\n"
        "    GROUP BY company_id, wikidata_id\n"
        ")"
    )


def wikidata_current_sql() -> str:
    return (
        f"{wikidata_links_cte_sql()}\n"
        "SELECT links.company_id AS company_id, max(entity.resolved_at) AS observed_at\n"
        "FROM links\n"
        "INNER JOIN corpscout.wikidata_companies AS entity FINAL ON entity.wikidata_id = links.wikidata_id\n"
        "GROUP BY links.company_id"
    )


def wikidata_select_sql() -> str:
    return (
        f"{wikidata_links_cte_sql(scoped=True)}\n"
        "SELECT\n"
        "    links.company_id AS company_id,\n"
        "    'wikidata' AS source,\n"
        "    concat('wikidata:', entity.wikidata_id) AS source_record_uid,\n"
        "    entity.resolved_at AS observed_at,\n"
        "    nullIf(trim(ifNull(entity.official_name, '')), '') AS legal_name,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_form_code,\n"
        "    CAST(NULL AS Nullable(String)) AS status,\n"
        "    if(entity.inception_date > toDate('1970-01-01'), toDate32(entity.inception_date), NULL) AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    entity.wikidata_id AS wikidata_id,\n"
        "    nullIf(trim(ifNull(entity.company_description, '')), '') AS description,\n"
        "    if(entity.company_description IS NULL OR trim(entity.company_description) = '', NULL, 'en') AS description_language,\n"
        "    CAST(NULL AS Nullable(String)) AS description_sv\n"
        "FROM links\n"
        "INNER JOIN corpscout.wikidata_companies AS entity FINAL ON entity.wikidata_id = links.wikidata_id\n"
        "WHERE links.company_id IN %(company_ids)s\n"
        "ORDER BY entity.resolved_at DESC, entity.wikidata_id ASC\n"
        "LIMIT 1 BY links.company_id"
    )


se_basic_info_suggestions_wikidata = define_suggestion_asset(
    source="wikidata",
    extractor_version=WIKIDATA_EXTRACTOR_VERSION,
    current_sql=wikidata_current_sql(),
    select_sql=wikidata_select_sql(),
    deps=[dg.AssetKey("wikidata_companies"), dg.AssetKey("wikidata_company_identifiers"), dg.AssetKey("company_identifier_clickhouse")],
    description=(
        "One wikidata suggestion row per linked Swedish company: the Wikidata id, the official "
        "name, the inception date and the English description of the newest linked entity. "
        "execute=false previews."
    ),
)
