"""The append path: every stored outcome is attributable, and a re-append cannot go backwards.

The promotion's DuckDB half is exercised against a real in-memory DuckDB (the pattern
tests/test_address_resolution.py already uses for this module), because the thing under
test is a projection over three joined tables, not a Python branch.
"""

from datetime import UTC, datetime

import duckdb
import pytest

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    GEOCODE_STORE_BACKFILL_SQL,
    build_geocode_store_backfill_sql,
    build_store_append_regression_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE,
    SERVING_COLUMNS,
    STORE_COLUMNS,
)

POLICY = "se-address-resolution-policy-v5"


def test_the_backfill_stamps_the_policy_and_promotes_source_md5_to_the_key() -> None:
    sql = build_geocode_store_backfill_sql()
    assert sql.startswith(
        "INSERT INTO corpscout.se_address_geocodes (" + ", ".join(STORE_COLUMNS) + ")"
    )
    assert "%(policy_version)s AS policy_version" in sql
    assert "ifNull(source_md5, '') AS reference_md5" in sql
    assert "FROM corpscout.se_address_geocodes_current" in sql
    # matched_at is COPIED, never restamped: the serving row's own instant is what that
    # outcome claims, and copying it is what makes a second backfill run a no-op in content
    # rather than a version bump on 2.09M rows.
    assert "now64" not in sql and "now(" not in sql
    assert GEOCODE_STORE_BACKFILL_SQL is sql or GEOCODE_STORE_BACKFILL_SQL == sql
    # Every serving column is carried across, none dropped.
    for column in SERVING_COLUMNS:
        assert column in sql


def test_the_append_regression_query_looks_for_rows_that_would_swallow_this_run() -> (
    None
):
    """ReplacingMergeTree keeps the row with the LARGEST matched_at per key. If a row for a
    key this run just appended already carries a newer instant, this run's outcome is
    invisible from the moment it lands -- a silent no-op that no row count would reveal."""
    sql = build_store_append_regression_sql()
    assert "FROM corpscout.se_address_geocodes" in sql
    assert "geocode_run_id = %(geocode_run_id)s" in sql
    assert "matched_at > %(matched_at)s" in sql
    assert "(address_id, policy_version, reference_md5) IN (" in sql


def _promotable(source_md5: str | None = "osm-snapshot-md5") -> duckdb.DuckDBPyConnection:
    """A connection carrying a complete, promotable Sweden shadow run.

    `_create_sweden_shadow_fixture` is the ONE seeding helper for this pipeline and it
    already lives in tests/test_address_resolution.py -- imported rather than re-invented,
    because a second fixture would drift from the five promotion tests that share it. Step 3
    gives it the `source_md5` keyword this file needs; nothing else about it changes.
    """
    from tests.test_address_resolution import _create_sweden_shadow_fixture

    connection = duckdb.connect()
    _create_sweden_shadow_fixture(connection, source_md5=source_md5)
    return connection


def _promote(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    from dagster_v3.defs.sweden_company.address_resolution_promotion import (
        replace_current_geocodes_from_address_resolution_shadow,
    )
    from dagster_v3.defs.sweden_company.address_resolution_shadow import (
        replace_sweden_address_resolution_shadow,
    )

    replace_sweden_address_resolution_shadow(
        connection=connection,
        evaluation_run_id="shadow-run",
        evaluated_at=datetime(2026, 8, 24, tzinfo=UTC),
        log=None,
    )
    return replace_current_geocodes_from_address_resolution_shadow(
        connection=connection,
        geocode_run_id="run-1",
        matched_at=datetime(2026, 8, 24, tzinfo=UTC),
        expected_policy_version=POLICY,
        log=None,
    )


def test_promotion_writes_both_tables_with_their_own_shapes() -> None:
    with _promotable(source_md5="md5-alpha") as connection:
        counts = _promote(connection)

        assert counts["reference_md5"] == "md5-alpha"
        assert counts["appended_rows"] == counts["rows"]
        # DuckDB's `describe` returns (column_name, column_type, ...) -- index 0 is the name.
        serving = [
            row[0]
            for row in connection.execute(
                "describe sweden_company_enrichment.se_address_geocodes_current"
            ).fetchall()
        ]
        appended = [
            row[0]
            for row in connection.execute(
                f"describe {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}"
            ).fetchall()
        ]
        assert serving == list(SERVING_COLUMNS)
        assert appended == list(STORE_COLUMNS)
        [(policies, references)] = connection.execute(
            "select count(distinct policy_version), count(distinct reference_md5)"
            f" from {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}"
        ).fetchall()
        assert int(policies) == 1 and int(references) == 1


def test_promotion_refuses_an_outcome_with_no_reference_identity() -> None:
    """The versioning contract's hard half: an outcome with no reference_md5 is not
    attributable, and the store's sorting key would carry an empty string for ever.

    The message is matched EXACTLY, not on the word "reference". A NULL source_md5 also
    trips the pre-existing provenance invariant, whose message ("missing OSM snapshot
    provenance") contains no such word -- so a loose match would pass whichever raise fired
    and would tell us nothing about the new one. Step 3 puts the reference raise FIRST for
    the same reason: the more specific diagnosis should be the one an operator sees.
    """
    with _promotable(source_md5=None) as connection:
        with pytest.raises(
            ValueError, match="Promoted geocodes are missing the OSM reference identity"
        ):
            _promote(connection)
