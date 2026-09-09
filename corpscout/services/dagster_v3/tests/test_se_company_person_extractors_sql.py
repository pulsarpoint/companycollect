"""Structure and text pins of the person extractors (spec 2026-09-09 sections 3.1 and 6):
every source maps all sixteen raw columns, the slots are the spec's, the select pairs live
rows with per-slot tombstones, the scope is a state hash on both sides, and the INSERT
stamps suggestion_id from the same bound instant as suggested_at. The SQL runs for real in
tests/test_se_company_person_extractors_clickhouse_local.py."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import assets, bolagsverket, tables
from dagster_v3.defs.se_company.person.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.person.suggestions import (
    LIVE_ROW_PREDICATE,
    NULL_SQL,
    PERSON_SELECT_COLUMNS,
    PERSON_STATE_COLUMNS,
    PERSON_TARGET,
    PERSON_TRAILING_SELECT_SQL,
    person_state_sql,
)

EXTRACTORS = {
    "bolagsverket": (
        bolagsverket.BOLAGSVERKET_COLUMN_SQL,
        bolagsverket.bolagsverket_live_sql(scoped=True),
        bolagsverket.bolagsverket_select_sql(),
        bolagsverket.bolagsverket_changed_scope_sql(),
        bolagsverket.se_company_person_suggestions_bolagsverket,
    ),
}


def test_the_target_writes_the_eighteen_suggestion_columns() -> None:
    assert PERSON_SELECT_COLUMNS == (
        "company_id", "source", "slot", "source_record_id", "full_name", "first_name",
        "last_name", "birth_year", "wikidata_id", "role_original", "role_key",
        "fiscal_year", "role_from", "role_to", "document_ref", "data",
    )
    assert PERSON_TARGET.qualified_table == tables.QUALIFIED_SUGGESTION_TABLE
    assert PERSON_TARGET.insert_columns == (*PERSON_SELECT_COLUMNS, "suggestion_id", "suggested_at")
    assert sorted(PERSON_TARGET.insert_columns) == sorted(tables.SUGGESTION_COLUMNS)
    assert PERSON_TARGET.asset_prefix == "se_company_person_suggestions_"
    assert PERSON_TARGET.group_name == assets.GROUP_NAME == "se_company_person"
    assert PERSON_TARGET.scratch_prefix == SCRATCH_SCOPE_PREFIX == "corpscout._tmp_person_scope_"
    # The suggestion table has neither column, so the trailing SQL consumes neither binding.
    assert "source_run_id" not in PERSON_TRAILING_SELECT_SQL
    assert "extractor_version" not in PERSON_TRAILING_SELECT_SQL
    assert PERSON_STATE_COLUMNS == tuple(
        c for c in PERSON_SELECT_COLUMNS if c not in ("company_id", "source")
    )


def test_the_insert_binds_one_stamp_for_the_id_and_the_timestamp() -> None:
    sql = insert_page_sql(select_sql="SELECT 1", target=PERSON_TARGET)
    assert sql.startswith(
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(PERSON_TARGET.insert_columns)})\nWITH (SELECT now64(3, 'UTC')) AS stamp\n"
    )
    assert (
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
        "candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at"
    ) in sql
    assert sql.count("now64(") == 1


def test_every_extractor_maps_all_sixteen_columns_and_names_its_source_once() -> None:
    for source, (columns, live_sql, select_sql, _, _) in EXTRACTORS.items():
        assert set(columns) == set(PERSON_SELECT_COLUMNS), source
        assert columns["source"] == f"'{source}'", source
        for column in PERSON_SELECT_COLUMNS:
            assert f" AS {column}" in live_sql, (source, column)
            # live branch + tombstone branch.
            assert select_sql.count(f" AS {column}") >= 1, (source, column)


def test_every_extractor_binds_company_ids_exactly_twice_in_its_page_select() -> None:
    """jobs.py's page size depends on this: the helper runs every page under
    max_query_size 1 MiB and 20,000 twelve-digit ids render to about 260 KB per binding.
    The `live` CTE is referenced twice but written once, which is what keeps it at two."""
    for source, (_, _, select_sql, scope_sql, _) in EXTRACTORS.items():
        assert select_sql.count("%(company_ids)s") == 2, source
        assert "%(company_ids)s" not in scope_sql, source


def test_every_select_pairs_live_rows_with_tombstones_for_vanished_slots() -> None:
    for source, (_, _, select_sql, _, _) in EXTRACTORS.items():
        assert select_sql.startswith("WITH live AS ("), source
        assert "\nUNION ALL\n" in select_sql, source
        assert (
            "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
            "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
        ) in select_sql, source
        assert f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}" in select_sql, source
        assert "'{}' AS data" in select_sql, source
        for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                       "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                       "document_ref"):
            assert f"    {NULL_SQL[column]} AS {column}" in select_sql, (source, column)


def test_the_changed_scope_compares_one_state_hash_per_company_on_both_sides() -> None:
    state = person_state_sql("live")
    assert state.startswith("lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat(")
    assert "toString(length(ifNull(toString(live.slot), ''))), ':', ifNull(toString(live.slot), '')" in state
    assert "live.data" in state and "live.company_id" not in state
    for source, (_, _, _, scope_sql, _) in EXTRACTORS.items():
        assert scope_sql.count(state) == 2, source
        assert scope_sql.endswith("GROUP BY company_id\nHAVING count() < 2 OR uniqExact(state) > 1"), source
        assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in scope_sql, source
        assert f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}" in scope_sql, source


def test_bolagsverket_slot_is_the_report_record_uid_and_the_signatory_uid() -> None:
    columns = bolagsverket.BOLAGSVERKET_COLUMN_SQL
    assert columns["slot"] == "concat(s.source_record_uid, ':', toString(s.signatory_uid))"
    assert columns["source_record_id"] == "s.source_record_uid"
    assert columns["full_name"] == NULL_SQL["full_name"]
    assert columns["first_name"] == "nullIf(trim(s.first_name), '')"
    assert columns["last_name"] == "nullIf(trim(s.last_name), '')"
    assert columns["role_original"] == "nullIf(trim(s.role_original), '')"
    assert columns["role_key"] == "nullIf(trim(toString(s.role_kind)), '')"
    assert columns["document_ref"] == "nullIf(s.statement_key, '')"
    assert columns["data"] == (
        "toJSONString(map('signatory_kind', toString(s.signatory_kind), "
        "'statement_key', s.statement_key, 'person_seq', toString(s.person_seq)))"
    )
    live = bolagsverket.bolagsverket_live_sql()
    assert "FROM corpscout.se_financial_report_signatories AS s" in live
    assert (
        "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
        "    ON universe.company_id = s.company_id"
    ) in live
    assert "WHERE (trim(s.first_name) != '' OR trim(s.last_name) != '')" in live
    assert "%(company_ids)s" not in live
    assert "%(company_ids)s" in bolagsverket.bolagsverket_live_sql(scoped=True)
    assert bolagsverket.BOLAGSVERKET_PERSON_EXTRACTOR_VERSION == "bolagsverket-person-v1"


def test_the_current_sql_is_only_the_since_escape_hatch() -> None:
    current = bolagsverket.bolagsverket_current_sql()
    assert current.endswith("GROUP BY s.company_id")
    assert "max(s.resolved_at) AS observed_at" in current
    assert "%(company_ids)s" not in current


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("bolagsverket", "esef", "wikidata")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(
        f"se_company_person_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES
    )
    for source, (_, _, _, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_person_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_person"
