"""ESEF document people -> raw person suggestions (spec 2026-09-09 sections 3.1 and 6).

Reads corpscout.se_esef_document_people, the country-scoped view of the country-agnostic
extraction (migration 000395). A CONSUMER NEVER WRITES FINAL AFTER THE VIEW NAME: the view
already reads its ReplacingMergeTree product FINAL and carries the register-verified Swedish
link inside it, which is also why this extractor needs no LEI join of its own.

The slot is the document id plus `candidate_uid`, the extraction's own per-person id (spec
3.1.1 calls it "the person's index in the extraction"; the table has no index column and
candidate_uid is the id the extraction assigns). candidate_uid is derived from the extracted
item, so a re-extraction under a different model produces new slots and tombstones the old
ones -- which is exactly what a raw observation layer should record. One document can also
carry rows from two extraction models at once (1,722 (document, name) pairs did on
2026-09-09); both are published, and the fold merges them into one person with two members.

When the dedicated per-filing extraction table lands (the ESEF slice-2 plan), this module
changes in one place: the view name in `from_sql`.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "esef"
ESEF_PERSON_EXTRACTOR_VERSION = "esef-person-v1"

# DELIBERATELY UNSCOPED (unlike wikidata.py's universe CTE): the page's company filter is
# applied by the outer person_select_sql, and the reviewer measured no cost difference on
# prod (this universe is a primary-key read). wikidata.py scopes it because the same list
# also feeds its links CTE.
FROM_SQL = (
    "FROM corpscout.se_esef_document_people AS e\n"
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = e.company_id"
)

ESEF_COLUMN_SQL: dict[str, str] = {
    "company_id": "e.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "concat(e.source_document_id, ':', toString(e.candidate_uid))",
    "source_record_id": "toString(e.source_record_uid)",
    # ESEF delivers one name string; the normalizer splits it.
    "full_name": "nullIf(trim(e.name), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    "birth_year": NULL_SQL["birth_year"],
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(e.role), '')",
    # role_category is the map key in roles.py; 'other' is not in its map and falls through
    # to the label, which is the owner's never-bucket rule.
    "role_key": "nullIf(trim(toString(e.role_category)), '')",
    # The document's fiscal year is this row's role year (spec 4.3).
    "fiscal_year": "if(e.fiscal_year BETWEEN 1900 AND 2155, e.fiscal_year, CAST(NULL AS Nullable(UInt16)))",
    # effective_from/to are Nullable(Date32) in the source and Date here; accurateCastOrNull turns a
    # date outside Date's range into NULL instead of wrapping it into a wrong year.
    "role_from": "accurateCastOrNull(e.effective_from, 'Date')",
    "role_to": "accurateCastOrNull(e.effective_to, 'Date')",
    "document_ref": "nullIf(e.source_document_id, '')",
    # Spec 3.1.2's ESEF extras. evidence_ids is the "section it came from" this extraction
    # actually carries, flattened to a comma list so every map value is one String.
    "data": (
        "toJSONString(map('organization', e.organization, 'status', toString(e.status), "
        "'confidence', toString(e.confidence), "
        "'evidence_ids', arrayStringConcat(e.evidence_ids, ','), "
        "'model_provider', toString(e.model_provider), 'model_name', e.model_name, "
        "'prompt_version', e.prompt_version))"
    ),
}


def esef_live_sql(*, scoped: bool = False) -> str:
    where_sql = "WHERE trim(e.name) != ''"
    if scoped:
        where_sql += "\n    AND e.company_id IN %(company_ids)s"
    return live_select_sql(columns=ESEF_COLUMN_SQL, from_sql=FROM_SQL, where_sql=where_sql)


def esef_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT e.company_id AS company_id, max(e.extracted_at) AS observed_at\n"
        f"{FROM_SQL}\n"
        "GROUP BY e.company_id"
    )


def esef_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=esef_live_sql())


def esef_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=esef_live_sql(scoped=True))


se_company_person_suggestions_esef = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=ESEF_PERSON_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    changed_scope_override=esef_changed_scope_sql(),
    deps=[
        dg.AssetKey("esef_document_people_clickhouse"),
        dg.AssetKey("esef_entity_registry_map_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every person the ESEF extraction found in a Swedish filing as a raw person suggestion "
        "in se_company_person_suggestion (slot = document id + candidate uid); a candidate the "
        "extraction no longer produces is tombstoned. execute=false previews."
    ),
)
