"""Text pins of the three address extractors (spec section 7): the thirteen raw columns in
order, FINAL reads, id binding, tombstones, and the INSERT that stamps suggestion_id."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets, bolagsverket, ratsit, scb, tables
from dagster_v3.defs.se_company.address.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.address.suggestions import (
    ADDRESS_SELECT_COLUMNS,
    ADDRESS_TARGET,
    ADDRESS_TRAILING_SELECT_SQL,
)
from dagster_v3.defs.se_company.basic_info import bolagsverket as basic_info_bolagsverket
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION


def _aliases(sql: str) -> list[str]:
    """Column names of the top-level projection: the token after the last ' AS ' of each
    top-level comma-separated expression, or the bare column when there is no alias.
    Depth-aware, so commas and FROM inside function calls and subqueries do not count."""
    after_select = sql.split("SELECT", 1)[1]
    depth = 0
    end = len(after_select)
    for index, char in enumerate(after_select):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and after_select.startswith("FROM ", index) and after_select[index - 1] in " \n":
            end = index
            break
    expressions: list[str] = []
    current: list[str] = []
    depth = 0
    for char in after_select[:end]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            expressions.append("".join(current))
            current = []
        else:
            current.append(char)
    expressions.append("".join(current))
    names = []
    for expression in expressions:
        text = expression.strip()
        names.append(text.rsplit(" AS ", 1)[1].strip() if " AS " in text else text.rsplit(".", 1)[-1].strip())
    return names


EXTRACTORS = {
    "scb": (scb.scb_current_sql(), scb.scb_select_sql(), scb.se_company_address_suggestions_scb),
    "bolagsverket": (bolagsverket.bolagsverket_current_sql(), bolagsverket.bolagsverket_select_sql(), bolagsverket.se_company_address_suggestions_bolagsverket),
    "ratsit": (ratsit.ratsit_current_sql(), ratsit.ratsit_select_sql(), ratsit.se_company_address_suggestions_ratsit),
}


def test_the_target_writes_the_raw_table_with_every_column_once() -> None:
    assert ADDRESS_SELECT_COLUMNS == ("company_id", "source", "slot", "source_record_uid", "observed_at", "kind", *tables.RAW_ADDRESS_COLUMNS)
    assert ADDRESS_TARGET.qualified_table == tables.QUALIFIED_SUGGESTION_TABLE
    assert ADDRESS_TARGET.insert_columns == (
        *ADDRESS_SELECT_COLUMNS, "suggestion_id", "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
    )
    assert sorted(ADDRESS_TARGET.insert_columns) == sorted(tables.SUGGESTION_COLUMNS)
    assert ADDRESS_TARGET.asset_prefix == "se_company_address_suggestions_"
    assert ADDRESS_TARGET.group_name == assets.GROUP_NAME == "se_company_address"
    assert ADDRESS_TARGET.scratch_prefix == SCRATCH_SCOPE_PREFIX


def test_the_insert_stamps_suggestion_id_from_the_same_now64_as_suggested_at() -> None:
    sql = insert_page_sql(select_sql="SELECT 1", target=ADDRESS_TARGET)
    assert sql.startswith(
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} ({', '.join(ADDRESS_TARGET.insert_columns)})\n"
        "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
    )
    assert (
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', candidate.slot, '\\n', "
        "toString(stamp))))) AS suggestion_id"
    ) in sql
    assert "CAST(NULL AS Nullable(FixedString(64))) AS replaces_key" in sql
    assert "stamp AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version" in sql
    assert ADDRESS_TRAILING_SELECT_SQL in sql
    assert sql.count("now64(") == 1


def test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page() -> None:
    for source, (current_sql, select_sql, _) in EXTRACTORS.items():
        assert _aliases(select_sql) == list(ADDRESS_SELECT_COLUMNS), source
        assert "%(company_ids)s" in select_sql, source
        assert " FINAL" in select_sql, source
        assert f"'{source}' AS source" in select_sql, source
        assert _aliases(current_sql) == ["company_id", "observed_at"], source
        assert "%(company_ids)s" not in current_sql, source


def test_scb_maps_the_four_delivered_columns_and_tombstones() -> None:
    sql = scb.scb_select_sql()
    assert "'visiting_or_postal' AS kind" in sql and "'' AS slot" in sql
    assert "FROM corpscout.se_scb_companies FINAL" in sql
    assert "has_company = 1" not in scb.scb_current_sql()
    for column in ("care_of", "street_address", "postal_code", "post_town"):
        assert f"if(has_company = 1, nullIf(trim(ifNull({column}, '')), ''), CAST(NULL AS Nullable(String))) AS {column}" in sql, column
    assert "CAST(NULL AS Nullable(String)) AS raw_address" in sql
    assert "CAST(NULL AS Nullable(String)) AS county" in sql
    assert "CAST(NULL AS Nullable(String)) AS country_code" in sql
    assert scb.SCB_ADDRESS_EXTRACTOR_VERSION == "scb-address-v1"


def test_bolagsverket_keeps_the_packed_string_raw_and_tombstones() -> None:
    sql = bolagsverket.bolagsverket_select_sql()
    assert "'postal' AS kind" in sql and "'' AS slot" in sql
    assert "FROM corpscout.se_bolagsverket_companies FINAL" in sql
    assert "if(has_company = 1, nullIf(trim(ifNull(postal_address, '')), ''), CAST(NULL AS Nullable(String))) AS raw_address" in sql
    for column in ("care_of", "street_address", "postal_code", "post_town", "county", "country_code"):
        assert f"CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    assert "text_translations" not in sql and "text_translations" not in bolagsverket.bolagsverket_current_sql()
    assert bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION == "bolagsverket-address-v1"


def test_bolagsverket_record_uid_formula_matches_basic_info() -> None:
    assert bolagsverket.BOLAGSVERKET_ADDRESS_RECORD_UID_SQL == basic_info_bolagsverket.BOLAGSVERKET_RECORD_UID_SQL.replace("register.", "")


def test_ratsit_takes_the_newest_report_into_the_company_slot() -> None:
    sql = ratsit.ratsit_select_sql()
    assert "'postal' AS kind" in sql and "'company' AS slot" in sql
    assert "nullIf(trim(ifNull(address_street, '')), '') AS street_address" in sql
    assert "nullIf(trim(ifNull(address_postal_code, '')), '') AS postal_code" in sql
    assert "nullIf(trim(ifNull(address_locality, '')), '') AS post_town" in sql
    assert "nullIf(trim(ifNull(address_county, '')), '') AS county" in sql
    assert "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s" in sql
    assert sql.endswith("ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id")
    assert ratsit.RATSIT_ADDRESS_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    assert ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION == "ratsit-address-v1"


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("scb", "bolagsverket", "ratsit")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(f"se_company_address_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES)
    for source, (_, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_address_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_address"
    assert "FROM corpscout.se_company_address_suggestion WHERE source = %(source)s" in changed_scope_sql(
        current_sql=scb.scb_current_sql(), target=ADDRESS_TARGET,
    )
