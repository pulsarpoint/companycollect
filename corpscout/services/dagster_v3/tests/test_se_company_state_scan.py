"""The shared state-hash scan (financial slice 2 lifted it out of the person entity): the
rendered texts for a three-column toy entity, and the two import-time validations."""

import pytest

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget

TARGET = SuggestionTarget(
    database="corpscout", table="toy_suggestion", insert_columns=("company_id", "source", "k", "v", "stamp"),
    select_columns=("company_id", "source", "k", "v"), trailing_select_sql="now64(3) AS stamp",
    asset_prefix="toy_", group_name="toy", scratch_prefix="corpscout._tmp_toy_",
)
SCAN = state_scan.StateScan(
    target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"),
    key_column="k", live_row_predicate="(v IS NOT NULL)", tombstone_columns=("k",),
    tombstone_values={"company_id": "stored.company_id", "source": "'s'", "k": "stored.k", "v": "CAST(NULL AS Nullable(String))"},
)


def test_state_hash_is_length_prefixed_sorted_and_null_safe() -> None:
    assert state_scan.state_sql(SCAN, "live") == (
        "lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat("
        "toString(length(ifNull(toString(live.k), ''))), ':', ifNull(toString(live.k), ''), '\\n', "
        "toString(length(ifNull(toString(live.v), ''))), ':', ifNull(toString(live.v), '')))), '\\n'))))"
    )


def test_stored_live_rows_filter_the_source_literal_and_the_predicate() -> None:
    assert state_scan.stored_live_sql(SCAN, source="s", columns=("k",), scoped=True) == (
        "SELECT company_id, k\nFROM corpscout.toy_suggestion FINAL\n"
        "WHERE source = 's' AND (v IS NOT NULL)\n    AND company_id IN %(company_ids)s"
    )


def test_changed_scope_unions_both_sides_under_one_alias() -> None:
    sql = state_scan.changed_scope_sql(SCAN, source="s", live_sql="SELECT 1 AS company_id, 'a' AS k, 'b' AS v")
    assert sql.count("AS live\n") == 2 and sql.count("GROUP BY company_id") == 3
    assert sql.endswith("HAVING count() < 2 OR uniqExact(state) > 1")


def test_select_unions_live_rows_with_tombstones_anti_joined_on_the_key() -> None:
    sql = state_scan.select_sql(SCAN, source="s", live_sql="SELECT 1 AS company_id, 's' AS source, 'a' AS k, 'b' AS v")
    assert sql.startswith("WITH live AS (\n")
    assert "SELECT live.company_id AS company_id, live.source AS source, live.k AS k, live.v AS v\nFROM live\nUNION ALL" in sql
    assert "    stored.company_id AS company_id,\n    's' AS source,\n    stored.k AS k,\n    CAST(NULL AS Nullable(String)) AS v" in sql
    assert sql.endswith(
        "LEFT ANTI JOIN (SELECT company_id, k FROM live) AS live_ks\n"
        "    ON live_ks.company_id = stored.company_id AND live_ks.k = stored.k"
    )


def test_live_select_refuses_a_missing_or_extra_column() -> None:
    with pytest.raises(ValueError, match="missing=\\['v'\\]"):
        state_scan.live_select_sql(SCAN, columns={"company_id": "1", "source": "'s'", "k": "'a'"}, from_sql="FROM t", where_sql="WHERE 1")
    text = state_scan.live_select_sql(SCAN, columns={"company_id": "1", "source": "'s'", "k": "'a'", "v": "'b'"}, from_sql="FROM t", where_sql="WHERE 1", with_sql="WITH x AS (SELECT 1)\n")
    assert text == "WITH x AS (SELECT 1)\nSELECT\n    1 AS company_id,\n    's' AS source,\n    'a' AS k,\n    'b' AS v\nFROM t\nWHERE 1"


def test_a_scan_validates_its_tombstone_map_and_key() -> None:
    with pytest.raises(ValueError, match="tombstone_values: missing=\\['v'\\]"):
        state_scan.StateScan(target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"), key_column="k", live_row_predicate="1", tombstone_columns=("k",), tombstone_values={"company_id": "1", "source": "'s'", "k": "1"})
    with pytest.raises(ValueError, match="must include the key column"):
        state_scan.StateScan(target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"), key_column="k", live_row_predicate="1", tombstone_columns=(), tombstone_values={"company_id": "1", "source": "'s'", "k": "1", "v": "1"})
