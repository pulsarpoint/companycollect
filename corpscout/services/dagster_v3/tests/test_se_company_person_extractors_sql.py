"""Structure and text pins of the person extractors (spec 2026-09-09 sections 3.1 and 6):
every source maps all sixteen raw columns, the slots are the spec's, the select pairs live
rows with per-slot tombstones, the scope is a state hash on both sides, and the INSERT
stamps suggestion_id from the same bound instant as suggested_at. The SQL runs for real in
tests/test_se_company_person_extractors_clickhouse_local.py."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import assets, bolagsverket, esef, ratsit, tables, wikidata
from dagster_v3.defs.se_company.person.suggestions import (
    LIVE_ROW_PREDICATE,
    NULL_SQL,
    PERSON_SELECT_COLUMNS,
    PERSON_STATE_COLUMNS,
    PERSON_TARGET,
    PERSON_TRAILING_SELECT_SQL,
    person_state_sql,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

EXTRACTORS = {
    "bolagsverket": (
        bolagsverket.BOLAGSVERKET_COLUMN_SQL,
        bolagsverket.bolagsverket_live_sql(scoped=True),
        bolagsverket.bolagsverket_select_sql(),
        bolagsverket.bolagsverket_changed_scope_sql(),
        bolagsverket.se_company_person_suggestions_bolagsverket,
    ),
    "esef": (
        esef.ESEF_COLUMN_SQL,
        esef.esef_live_sql(scoped=True),
        esef.esef_select_sql(),
        esef.esef_changed_scope_sql(),
        esef.se_company_person_suggestions_esef,
    ),
    "wikidata": (
        wikidata.WIKIDATA_COLUMN_SQL,
        wikidata.wikidata_live_sql(scoped=True),
        wikidata.wikidata_select_sql(),
        wikidata.wikidata_changed_scope_sql(),
        wikidata.se_company_person_suggestions_wikidata,
    ),
    "ratsit": (
        ratsit.RATSIT_COLUMN_SQL,
        ratsit.ratsit_live_sql(scoped=True),
        ratsit.ratsit_select_sql(),
        ratsit.ratsit_changed_scope_sql(),
        ratsit.se_company_person_suggestions_ratsit,
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
    assert PERSON_TARGET.scratch_prefix == tables.SCRATCH_SCOPE_PREFIX == "corpscout._tmp_person_scope_"
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
    # A non-String column (birth_year is Nullable(UInt16)) must render with the identical
    # ifNull(toString(...), '') wrapper, so a rendering that special-cased one column's type
    # would fail here even though the String-only "slot" pin above would not catch it.
    assert "toString(length(ifNull(toString(live.birth_year), ''))), ':', ifNull(toString(live.birth_year), '')" in state
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


def test_a_company_the_register_deregistered_leaves_the_live_branch() -> None:
    """Spec section 6: `tombstones on has_company = 0`. The flagged companies leave `live`, so
    the shared tombstone branch retires their slots and the state hash reselects them once. A
    company with no register row at all never left the register and keeps its signatures."""
    live = bolagsverket.bolagsverket_live_sql()
    assert (
        "LEFT ANTI JOIN (\n"
        "    SELECT company_id FROM corpscout.se_bolagsverket_companies FINAL WHERE has_company = 0\n"
        ") AS deregistered ON deregistered.company_id = s.company_id"
    ) in live
    # The scope reads the same live text, so both sides of the state hash agree on who is live.
    assert "has_company = 0" in bolagsverket.bolagsverket_changed_scope_sql()
    assert "has_company = 0" in bolagsverket.bolagsverket_select_sql()


def test_the_current_sql_is_only_the_since_escape_hatch() -> None:
    current = bolagsverket.bolagsverket_current_sql()
    assert current.endswith("GROUP BY s.company_id")
    assert "max(s.resolved_at) AS observed_at" in current
    assert "%(company_ids)s" not in current


def test_esef_slot_is_the_document_and_the_extractions_candidate_uid() -> None:
    columns = esef.ESEF_COLUMN_SQL
    assert columns["slot"] == "concat(e.source_document_id, ':', toString(e.candidate_uid))"
    assert columns["source_record_id"] == "toString(e.source_record_uid)"
    assert columns["full_name"] == "nullIf(trim(e.name), '')"
    assert columns["first_name"] == NULL_SQL["first_name"]
    assert columns["last_name"] == NULL_SQL["last_name"]
    assert columns["role_original"] == "nullIf(trim(e.role), '')"
    assert columns["role_key"] == "nullIf(trim(toString(e.role_category)), '')"
    assert columns["role_from"] == "accurateCastOrNull(e.effective_from, 'Date')"
    assert columns["role_to"] == "accurateCastOrNull(e.effective_to, 'Date')"
    assert columns["document_ref"] == "nullIf(e.source_document_id, '')"
    assert "'evidence_ids', arrayStringConcat(e.evidence_ids, ',')" in columns["data"]
    assert columns["data"].startswith("toJSONString(map(")
    live = esef.esef_live_sql()
    assert "FROM corpscout.se_esef_document_people AS e" in live
    # The view already reads its product FINAL (esef_filings/country_views.py): a consumer
    # that adds another FINAL after a view name is a bug.
    assert "se_esef_document_people AS e FINAL" not in live
    assert "WHERE trim(e.name) != ''" in live
    assert esef.ESEF_PERSON_EXTRACTOR_VERSION == "esef-person-v1"


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("bolagsverket", "esef", "wikidata")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(
        f"se_company_person_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES
    )
    for source, (_, _, _, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_person_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_person"


def test_wikidata_slot_is_the_link_record_id_and_the_link_is_orgnr_or_lei() -> None:
    columns = wikidata.WIKIDATA_COLUMN_SQL
    assert columns["slot"] == "link.source_record_id"
    assert columns["source_record_id"] == "person.source_record_uid"
    assert columns["full_name"] == "nullIf(trim(person.name), '')"
    assert columns["birth_year"] == "person.birth_year"
    assert columns["wikidata_id"] == "nullIf(link.person_wikidata_id, '')"
    assert columns["role_original"] == "nullIf(trim(toString(link.role_label)), '')"
    assert columns["role_key"] == "nullIf(trim(toString(link.role_property)), '')"
    assert columns["fiscal_year"] == NULL_SQL["fiscal_year"]
    assert columns["role_from"] == "link.start_date" and columns["role_to"] == "link.end_date"
    assert columns["document_ref"] == NULL_SQL["document_ref"]
    for key in ("'description'", "'image_url'", "'wikidata_url'", "'name_normalized'", "'is_current'"):
        assert key in columns["data"], key

    live = wikidata.wikidata_live_sql()
    assert live.startswith("WITH universe AS (")
    assert "SELECT company_id FROM corpscout.se_company_basic_info FINAL" in live
    assert "identifiers.identifier_type = 'se_orgnr'" in live
    assert "identifiers.issuer_scheme = 'lei' AND identifiers.is_current = 1" in live
    assert "INNER JOIN corpscout.wikidata_company_people AS link FINAL" in live
    assert "INNER JOIN corpscout.wikidata_persons AS person FINAL" in live
    # Wikidata blank nodes (.well-known/genid/...) are not people and carry a URL as a name.
    assert "WHERE match(link.person_wikidata_id, '^Q[0-9]+$') AND trim(person.name) != ''" in live
    assert "%(company_ids)s" not in live
    assert wikidata.wikidata_live_sql(scoped=True).count("%(company_ids)s") == 1
    assert wikidata.WIKIDATA_PERSON_EXTRACTOR_VERSION == "wikidata-person-v1"


def test_ratsit_slot_is_the_profile_token_with_role_and_index_fallbacks() -> None:
    """Spec 2026-09-11 section 4.2. The slot is Ratsit's own person id -- the trailing token
    of profile_url, which the same person carries at every company -- so a re-scan rewrites
    the row in place instead of retiring the slot and inventing a new one. The 31 (company,
    token) pairs that carry two rows (a `Delgivningsbar person` who is also VD) get the role
    appended; a named row without a URL falls back to its person_index."""
    columns = ratsit.RATSIT_COLUMN_SQL
    assert columns["slot"] == (
        "multiIf("
        "r.token = '', concat('idx:', toString(r.person_index)), "
        "count() OVER (PARTITION BY r.company_id, r.token) > 1, "
        "concat(r.token, ':', lowerUTF8(trim(r.role_raw))), "
        "r.token)"
    )
    assert columns["source_record_id"] == (
        "concat('ratsit:', toString(r.result_sha256), ':', toString(r.person_index))"
    )
    # Ratsit delivers one name string; the normalizer splits it.
    assert columns["full_name"] == "nullIf(trim(r.name_raw), '')"
    assert columns["first_name"] == NULL_SQL["first_name"]
    assert columns["last_name"] == NULL_SQL["last_name"]
    # The birth date is the first eight digits of the profile URL's path (277,592 of the
    # 301,536 rows carry it); NULL when the row has no URL.
    assert columns["birth_year"] == (
        "toUInt16OrNull(substring(extract(r.profile_url, "
        "'^https://www\\.ratsit\\.se/(\\d{8})-'), 1, 4))"
    )
    assert columns["wikidata_id"] == NULL_SQL["wikidata_id"]
    assert columns["role_original"] == "nullIf(trim(r.role_raw), '')"
    # Ratsit has no machine role code: roles.py maps the Swedish label instead.
    assert columns["role_key"] == NULL_SQL["role_key"]
    # The role is current at the scan, so the scan date is the role year (the report's
    # normalized_at when Ratsit delivered no source_date_modified).
    assert columns["fiscal_year"] == (
        "toYear(ifNull(r.source_date_modified, toDate32(r.normalized_at)))"
    )
    assert columns["role_from"] == NULL_SQL["role_from"]
    assert columns["role_to"] == NULL_SQL["role_to"]
    assert columns["document_ref"] == NULL_SQL["document_ref"]
    # mapFilter over coalesced String values: a NULL age must leave the key OUT of the
    # object. A bare map() would render "age":null, which is a value no reader expects.
    assert columns["data"].startswith("toJSONString(mapFilter((k, v) -> v != '', map(")
    for key in ("'age'", "'identity_available'", "'profile_url'", "'display_name_raw'",
                "'ratsit_person_id'", "'external'"):
        assert key in columns["data"], key
    assert (
        "if(startsWith(lowerUTF8(trim(r.role_raw)), 'extern'), 'true', 'false')"
    ) in columns["data"]

    live = ratsit.ratsit_live_sql()
    # The current report per company: newest normalized_at, ties by the higher hash.
    assert live.startswith("WITH report AS (")
    assert "FROM corpscout.se_ratsit_company AS c FINAL" in live
    assert (
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id"
    ) in live
    # The same universe as the three siblings: a Ratsit company the entity does not know
    # yields no live row, so it is on neither side of the state hash and never visited.
    assert ratsit.UNIVERSE_JOIN_SQL in live
    assert (
        "    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
        "        ON universe.company_id = c.company_id"
    ) in live
    # People join the report's own key, so a superseded scan's rows never appear.
    assert "FROM corpscout.se_ratsit_responsible_people AS p FINAL" in live
    assert (
        "    INNER JOIN report\n"
        "        ON report.company_id = p.company_id\n"
        "        AND report.result_sha256 = p.result_sha256\n"
        "        AND report.normalizer_version = p.normalizer_version"
    ) in live
    # 23,432 nameless rows are role-only GDPR evidence, not identities.
    assert live.endswith("WHERE trim(r.name_raw) != ''")
    assert "%(company_ids)s" not in live
    assert ratsit.ratsit_live_sql(scoped=True).count("%(company_ids)s") == 1
    assert ratsit.RATSIT_PERSON_EXTRACTOR_VERSION == "ratsit-person-v1"
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    # run_extractor binds select_params into both the scope and every page.
    assert ratsit.ratsit_select_sql().count("%(normalizer_version)s") == 1
    assert ratsit.ratsit_changed_scope_sql().count("%(normalizer_version)s") == 1


def test_the_ratsit_asset_reads_the_ratsit_tables_and_the_basic_info_fold() -> None:
    """Spec 4.1: `se_ratsit_normalized` is the multi-asset's FUNCTION name, not an asset key
    -- a dep on it makes a phantom node. The keys the multi-asset declares are the table
    names, which is what this asset deps on, plus the fold that publishes the universe (the
    key `bolagsverket.py` and `wikidata.py` already carry)."""
    asset = ratsit.se_company_person_suggestions_ratsit
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_responsible_people"),
        dg.AssetKey("se_company_basic_info_fold"),
    }


def test_the_ratsit_current_sql_is_the_reports_own_stamp() -> None:
    """`current_sql` exists only for the `since` escape hatch (the change scan is the state
    hash). It is the newest report's normalized_at, picked exactly as the live branch picks
    the report -- a max() over every report would keep re-selecting companies whose older
    report was written later."""
    current = ratsit.ratsit_current_sql()
    assert "toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at" in current
    assert "    LIMIT 1 BY c.company_id" in current
    # The same universe as the live branch, so `since` and the page agree on who exists
    # (bolagsverket_current_sql carries its UNIVERSE_JOIN_SQL for the same reason).
    assert ratsit.UNIVERSE_JOIN_SQL in current
    assert current.count("%(normalizer_version)s") == 1
    assert "%(company_ids)s" not in current
