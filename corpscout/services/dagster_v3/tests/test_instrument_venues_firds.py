from dagster_v3.defs.instrument_venues.firds import (
    build_firds_instrument_venues_sql,
)

_STAGE = "`corpscout`.`_tmp_instrument_venues_test`"


def test_firds_projection_reads_current_state_not_history() -> None:
    """Layer A is current admission; durable identity is instrument_issuer."""
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "FROM corpscout.firds_instruments_current" in sql
    assert "firds_instrument_events" not in sql


def test_firds_projection_is_not_country_filtered() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "competent_authority_country" not in sql
    assert "XSTO" not in sql


def test_firds_projection_marks_regulator_evidence() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "'esma_firds' AS venue_source" in sql
    assert "'regulator' AS evidence_tier" in sql
    assert "'' AS ticker" in sql
    assert "substring(upperUTF8(trimBoth(f.cfi_code)), 1, 1) AS cfi_category" in sql


def test_firds_projection_requires_both_grain_identifiers() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "WHERE trimBoth(f.isin) != ''" in sql
    assert "AND trimBoth(f.mic) != ''" in sql
