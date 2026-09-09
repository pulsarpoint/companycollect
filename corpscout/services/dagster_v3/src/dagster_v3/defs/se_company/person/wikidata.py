"""Wikidata company-person statements -> raw person suggestions (spec 2026-09-09 sections
3.1 and 6).

The company link mirrors basic_info/wikidata.py: a Swedish company reaches a Wikidata entity
either directly (wikidata_company_identifiers.identifier_type 'se_orgnr', digits stripped)
or through a current LEI in company_identifier. Two differences from that module, both
deliberate:

* The universe is se_company_basic_info rather than the union of the two register tables --
  this entity's universe is the folded company, and reading it once means the page select
  binds %(company_ids)s exactly once on this side.
* The LEI still comes from company_identifier and NOT from se_company_basic_info.lei, which
  is filled for only 392 companies against company_identifier's 115,440 current SE LEIs
  (measured 2026-09-09).

The slot is wikidata_company_people.source_record_id, which is literally
`Q<company>:P<property>:Q<person>` -- spec 3.1.1's "the QID plus the company link id". One
person under two properties for one company is two slots, and the fold unions their roles.

wikidata_persons also holds blank nodes whose name is a `.well-known/genid/...` URL; the
`^Q[0-9]+$` guard keeps them out. It excludes nothing real: all 504 Swedish rows on
2026-09-09 had a Q id and a wikidata_persons row.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "wikidata"
WIKIDATA_PERSON_EXTRACTOR_VERSION = "wikidata-person-v1"

FROM_SQL = (
    "FROM links\n"
    "INNER JOIN corpscout.wikidata_company_people AS link FINAL\n"
    "    ON link.company_wikidata_id = links.wikidata_id\n"
    "INNER JOIN corpscout.wikidata_persons AS person FINAL\n"
    "    ON person.person_wikidata_id = link.person_wikidata_id"
)
WHERE_SQL = "WHERE match(link.person_wikidata_id, '^Q[0-9]+$') AND trim(person.name) != ''"

WIKIDATA_COLUMN_SQL: dict[str, str] = {
    "company_id": "links.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "link.source_record_id",
    "source_record_id": "person.source_record_uid",
    # Wikidata delivers one label; the normalizer splits it.
    "full_name": "nullIf(trim(person.name), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    "birth_year": "person.birth_year",
    "wikidata_id": "nullIf(link.person_wikidata_id, '')",
    "role_original": "nullIf(trim(toString(link.role_label)), '')",
    # The property id is the map key in roles.py (P169, P112, ...).
    "role_key": "nullIf(trim(toString(link.role_property)), '')",
    # Wikidata carries a span, not a fiscal year (spec 4.3: the fold expands the span).
    "fiscal_year": NULL_SQL["fiscal_year"],
    "role_from": "link.start_date",
    "role_to": "link.end_date",
    "document_ref": NULL_SQL["document_ref"],
    # Spec 3.1.2's Wikidata extras, limited to what wikidata_persons actually carries: it
    # has no occupations, nationality or sitelinks columns.
    "data": (
        "toJSONString(map('description', ifNull(person.description, ''), "
        "'image_url', ifNull(person.image_url, ''), "
        "'wikidata_url', ifNull(person.wikidata_url, ''), "
        "'name_normalized', person.name_normalized, "
        "'is_current', toString(link.is_current)))"
    ),
}


def wikidata_links_cte_sql(*, scoped: bool = False) -> str:
    """CTEs `universe`, `company_leis`, `links` (company_id, wikidata_id).

    `scoped=True` narrows `universe` to `%(company_ids)s` up front, so a page never rebuilds
    the whole 3.5M-company link universe -- and binds the ids exactly once for this whole
    side of the select.
    """
    company_ids_filter = " WHERE company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH universe AS (\n"
        f"    SELECT company_id FROM corpscout.se_company_basic_info FINAL{company_ids_filter}\n"
        "),\n"
        "company_leis AS (\n"
        "    SELECT identifiers.company_id AS company_id, upperUTF8(identifiers.issuer_id) AS lei\n"
        "    FROM corpscout.company_identifier AS identifiers\n"
        "    INNER JOIN universe AS companies ON companies.company_id = identifiers.company_id\n"
        "    WHERE identifiers.country_code = 'SE' AND identifiers.issuer_scheme = 'lei' "
        "AND identifiers.is_current = 1\n"
        "    GROUP BY identifiers.company_id, lei\n"
        "),\n"
        "links AS (\n"
        "    SELECT company_id, wikidata_id FROM (\n"
        "        SELECT companies.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN universe AS companies\n"
        "            ON companies.company_id = replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '')\n"
        "        WHERE identifiers.identifier_type = 'se_orgnr'\n"
        "        UNION ALL\n"
        "        SELECT leis.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN company_leis AS leis ON leis.lei = upperUTF8(identifiers.identifier_value)\n"
        "        WHERE identifiers.identifier_type = 'lei'\n"
        "    )\n"
        "    GROUP BY company_id, wikidata_id\n"
        ")\n"
    )


def wikidata_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=WIKIDATA_COLUMN_SQL,
        from_sql=FROM_SQL,
        where_sql=WHERE_SQL,
        with_sql=wikidata_links_cte_sql(scoped=scoped),
    )


def wikidata_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        f"{wikidata_links_cte_sql()}"
        "SELECT links.company_id AS company_id, max(link.resolved_at) AS observed_at\n"
        "FROM links\n"
        "INNER JOIN corpscout.wikidata_company_people AS link FINAL\n"
        "    ON link.company_wikidata_id = links.wikidata_id\n"
        "GROUP BY links.company_id"
    )


def wikidata_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=wikidata_live_sql())


def wikidata_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=wikidata_live_sql(scoped=True))


se_company_person_suggestions_wikidata = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=WIKIDATA_PERSON_EXTRACTOR_VERSION,
    current_sql=wikidata_current_sql(),
    select_sql=wikidata_select_sql(),
    changed_scope_override=wikidata_changed_scope_sql(),
    deps=[
        dg.AssetKey("wikidata_company_people"),
        dg.AssetKey("wikidata_persons"),
        dg.AssetKey("wikidata_company_identifiers"),
        dg.AssetKey("company_identifier_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every Wikidata person statement about a linked Swedish company as a raw person "
        "suggestion in se_company_person_suggestion (slot = Q<company>:P<property>:Q<person>, "
        "with the label, birth year, QID, role span and description); a statement Wikidata no "
        "longer carries is tombstoned. execute=false previews."
    ),
)
