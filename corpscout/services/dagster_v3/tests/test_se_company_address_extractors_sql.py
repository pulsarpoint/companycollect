"""Text pins of the four address extractors (spec section 7): the thirteen raw columns in
order, FINAL reads, id binding, tombstones, and the INSERT that stamps suggestion_id."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets, bolagsverket, esef, ratsit, scb, tables
from dagster_v3.defs.se_company.address.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.address.suggestions import (
    ADDRESS_LIVE_ROW_PREDICATE,
    ADDRESS_SELECT_COLUMNS,
    ADDRESS_TARGET,
    ADDRESS_TOMBSTONE_COLUMNS,
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


def test_ratsit_delivers_the_company_row_and_one_workplace_row_per_establishment() -> None:
    """Spec 2026-09-11 sections 5.1 and 5.2. The company row keeps slot `company` and kind
    `postal`; every establishment of the SAME report with a street and a postcode adds a
    `workplace` row in slot `est:<identifier>`, suffixed with the establishment index when
    one report repeats the identifier (2 rows of the current reports; the raw table's 324
    repeats are mostly one establishment seen in two superseded scans, which the report join
    keeps apart). Both take `post_town` from the SCB register dictionary instead of Ratsit's
    locality, which is the municipality on 260,862 of 928,491 company addresses."""
    sql = ratsit.ratsit_select_sql()

    # (1) the dictionary: the register's most frequent trimmed town per digits-only postcode,
    #     ties by the alphabetically first spelling.
    assert ratsit.TOWNS_SQL == (
        "SELECT\n"
        "    replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(post_town, '')) AS town\n"
        "FROM corpscout.se_scb_companies FINAL\n"
        "WHERE has_company = 1\n"
        "    AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''\n"
        "    AND trim(ifNull(post_town, '')) != ''\n"
        "GROUP BY postal_code_digits, town\n"
        "ORDER BY count() DESC, town\n"
        "LIMIT 1 BY postal_code_digits"
    )
    assert ratsit.TOWNS_SQL in sql
    assert ") AS towns ON towns.postal_code_digits = r.postal_code_digits" in sql
    # A postcode the register does not know -- about 1,000 of the delivered ones today --
    # keeps Ratsit's own locality.
    assert ratsit.POST_TOWN_SQL == (
        "nullIf(if(ifNull(towns.town, '') != '', ifNull(towns.town, ''), r.locality), '')"
    )
    assert f"    {ratsit.POST_TOWN_SQL} AS post_town,\n" in sql

    # (2) the two row kinds and their slots.
    assert "    'company' AS slot,\n" in sql
    assert "    'postal' AS kind,\n" in sql
    assert ratsit.EST_ROWS_SQL == "count() OVER (PARTITION BY est.company_id, est.identifier)"
    assert ratsit.EST_SLOT_SQL == (
        "if(count() OVER (PARTITION BY est.company_id, est.identifier) > 1, "
        "concat('est:', est.identifier, ':', toString(est.establishment_index)), "
        "concat('est:', est.identifier))"
    )
    assert f"    {ratsit.EST_SLOT_SQL} AS slot,\n" in sql
    assert "    'workplace' AS kind,\n" in sql
    assert "workplace" in tables.KINDS
    assert "FROM corpscout.se_ratsit_establishments AS e FINAL" in sql
    assert (
        "WHERE trim(ifNull(e.address_street, '')) != '' "
        "AND trim(ifNull(e.address_postal_code, '')) != ''"
    ) in sql

    # (3) source_record_uid: the report hash, plus the establishment index on a workplace row.
    assert "    concat('ratsit:', toString(report.result_sha256)) AS source_record_uid,\n" in sql
    assert (
        "    concat('ratsit:', toString(est.result_sha256), ':est:', "
        "toString(est.establishment_index)) AS source_record_uid,\n"
    ) in sql

    # (4) observed_at is the report's normalized_at on every row (spec 5.2).
    assert "    toDateTime64(report.normalized_at, 3, 'UTC') AS observed_at,\n" in sql
    assert "    toDateTime64(est.normalized_at, 3, 'UTC') AS observed_at,\n" in sql

    # (5) raw_address and care_of stay NULL: the normalizer PARSES raw_address (it is
    #     Bolagsverket's packed string), and Ratsit glues the care-of onto the street for it.
    assert "    CAST(NULL AS Nullable(String)) AS raw_address,\n" in sql
    assert "    CAST(NULL AS Nullable(String)) AS care_of,\n" in sql
    assert "    CAST(NULL AS Nullable(String)) AS country_code\n" in sql
    assert "    nullIf(r.street_address, '') AS street_address,\n" in sql
    assert "    nullIf(r.postal_code, '') AS postal_code,\n" in sql
    assert "    nullIf(r.county, '') AS county,\n" in sql

    # (6) the report is the newest per company and the establishments join ITS key, so a
    #     superseded scan's workplaces can never appear.
    assert sql.count(
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\nLIMIT 1 BY c.company_id"
    ) == 2
    assert (
        "    ON report.company_id = e.company_id\n"
        "    AND report.result_sha256 = e.result_sha256\n"
        "    AND report.normalizer_version = e.normalizer_version\n"
    ) in sql

    assert ratsit.ADDRESS_SOURCE == "ratsit"
    assert ratsit.RATSIT_ADDRESS_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    assert ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION == "ratsit-address-v2"


def test_ratsit_pairs_live_rows_with_tombstones_for_vanished_slots() -> None:
    """Spec 5.3. One company now has many slots, so a slot the newest report stops
    delivering must be nulled rather than left behind. The shape is
    person/suggestions.py::person_select_sql's -- a `live` CTE written once and read twice,
    LEFT ANTI JOINed against the stored live slots -- plus the one join the address entity
    needs: its change scan IS the observed_at watermark, so a tombstone carries the current
    report's observed_at, never the stored row's, or argMax(observed_at, suggested_at) could
    pick the stale stamp out of the page's tie and re-select the company for ever."""
    sql = ratsit.ratsit_select_sql()
    assert sql.startswith("WITH live AS (\n")
    assert "\nUNION ALL\n" in sql
    # The liveness test names BOTH columns a source can put an address in. A packed source
    # fills only raw_address -- all 2.86M live Bolagsverket rows have a NULL street_address --
    # so `street_address IS NOT NULL` alone would read every one of them as already
    # tombstoned and the branch could never retire one of their slots.
    assert ADDRESS_LIVE_ROW_PREDICATE == "(street_address IS NOT NULL OR raw_address IS NOT NULL)"
    assert ADDRESS_TOMBSTONE_COLUMNS == ("slot", "kind")
    assert (
        f"WHERE source = 'ratsit' AND {ADDRESS_LIVE_ROW_PREDICATE} "
        "AND company_id IN %(company_ids)s"
    ) in sql
    assert (
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    ) in sql
    assert (
        "INNER JOIN (\n"
        "SELECT company_id, max(observed_at) AS observed_at FROM live GROUP BY company_id\n"
        ") AS report ON report.company_id = stored.company_id"
    ) in sql
    # The tombstone keeps its slot and its kind and nulls every address column.
    assert "    stored.company_id AS company_id,\n" in sql
    assert "    stored.slot AS slot,\n" in sql
    assert "    stored.kind AS kind,\n" in sql
    assert "    '' AS source_record_uid,\n" in sql
    assert "    report.observed_at AS observed_at,\n" in sql
    for column in tables.RAW_ADDRESS_COLUMNS:
        assert f"    CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    # The live branch binds the page once per report occurrence, the stored slots once.
    assert sql.count("%(company_ids)s") == 3
    assert sql.count("%(normalizer_version)s") == 2
    assert ratsit.ratsit_live_sql().count("%(company_ids)s") == 0
    assert ratsit.ratsit_live_sql(scoped=True).count("%(company_ids)s") == 2
    # SCB and Bolagsverket keep their own single-slot NULL-row tombstones, untouched.
    assert "UNION ALL" not in scb.scb_select_sql()
    assert "UNION ALL" not in bolagsverket.bolagsverket_select_sql()


def test_the_ratsit_address_asset_reads_the_two_ratsit_tables_and_the_scb_register() -> None:
    """Spec 5.1. `se_ratsit_normalized` is the multi-asset's FUNCTION name, not an asset key
    -- a dep on it makes a phantom node, and that is exactly what the v1 module carried. The
    keys the multi-asset declares are the table names; `sweden_company_scb_companies_clickhouse`
    is the key address/scb.py already uses for the register the dictionary reads. The lineage
    ruling holds: an extractor reads register source tables, never another extractor's output
    nor a fold."""
    asset = ratsit.se_company_address_suggestions_ratsit
    deps = {dep.asset_key for dep in asset.specs_by_key[asset.key].deps}
    assert deps == {
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_establishments"),
        dg.AssetKey("sweden_company_scb_companies_clickhouse"),
    }
    assert dg.AssetKey("se_ratsit_normalized") not in deps
    assert dg.AssetKey("se_company_basic_info_fold") not in deps
    assert dg.AssetKey("se_company_address_fold") not in deps


def test_the_ratsit_address_current_sql_is_the_reports_own_stamp() -> None:
    """Spec 5.2: the address module gets its own `ratsit_current_sql` over se_ratsit_company
    instead of reusing basic info's translation-aware one, whose greatest(normalized_at,
    business_description_translated_at) stamp exceeds the observed_at this select writes and
    re-selects the 213,283 translated companies (as of 2026-09-11) on every address run."""
    current = ratsit.ratsit_current_sql()
    assert current == (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        "WHERE c.normalizer_version = %(normalizer_version)s\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id\n"
        ")"
    )
    assert "se_ratsit_company_translated" not in current
    assert "se_ratsit_company_translated" not in ratsit.ratsit_select_sql()
    assert "%(company_ids)s" not in current


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("scb", "bolagsverket", "ratsit", "esef")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(f"se_company_address_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES)
    for source, (_, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_address_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_address"
    assert "FROM corpscout.se_company_address_suggestion WHERE source = %(source)s" in changed_scope_sql(
        current_sql=scb.scb_current_sql(), target=ADDRESS_TARGET,
    )


def test_esef_takes_the_registered_office_of_the_newest_filing_and_repacks_it() -> None:
    sql = esef.esef_select_sql()
    assert _aliases(sql) == list(ADDRESS_SELECT_COLUMNS)
    assert "FROM corpscout.se_esef_facts AS facts" in sql
    assert "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id" in sql
    assert "FINAL" not in sql
    assert "facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity'" in sql
    assert "'esef' AS source" in sql and "'' AS slot" in sql and "'registered' AS kind" in sql
    assert "'company-source-record-v1\\nfile\\nesef_report_package\\n', lowerUTF8(filings.package_sha256)" in sql
    assert "toDateTime64(filings.processed_at, 3, 'UTC') AS observed_at" in sql
    assert "ORDER BY filings.period_end DESC, filings.processed_at DESC, facts.language DESC, facts.fact_id\nLIMIT 1 BY facts.company_id" in sql
    assert "company_id IN %(company_ids)s" in sql
    # Ruling B (2026-09-12): a filing dated after today() must never win "the newest
    # filing" -- consistent with esef_current_sql()'s own guard, below.
    assert "AND filings.period_end <= today()\n" in sql
    # The packed form the normaliser parses; the unparsed remainder goes to street_address.
    assert esef.ESEF_PACKED_ADDRESS_SQL in sql
    assert "AS raw_address" in sql and "AS street_address" in sql
    assert "CAST(NULL AS Nullable(String)) AS care_of" in sql
    assert "AS post_town" in sql
    assert "[^,]+)$'" in sql
    assert esef.ESEF_ADDRESS_EXTRACTOR_VERSION == "esef-address-v1"
    assert esef.esef_current_sql() == (
        "SELECT facts.company_id AS company_id, argMax(toDateTime64(filings.processed_at, 3, 'UTC'), "
        "(filings.period_end, toDateTime64(filings.processed_at, 3, 'UTC'))) AS observed_at\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity' AND filings.processed_at IS NOT NULL\n"
        "  AND filings.period_end <= today()\n"
        "GROUP BY facts.company_id"
    )
