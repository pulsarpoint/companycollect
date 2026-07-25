from dagster_v3.defs.instrument_issuer import tables
from dagster_v3.defs.instrument_issuer.assets import (
    build_firds_instrument_issuer_sql,
)

_STAGE = "`corpscout`.`_tmp_instrument_issuer_test`"


def test_instrument_issuer_column_contract() -> None:
    assert tables.INSTRUMENT_ISSUER_TABLE == "instrument_issuer"
    assert tables.INSTRUMENT_ISSUER_COLUMNS == (
        "isin",
        "issuer_scheme",
        "issuer_id",
        "mapping_source",
        "first_seen_date",
        "last_seen_date",
        "source_run_id",
        "resolved_at",
    )


def test_projection_reads_firds_event_history_not_current_state() -> None:
    """Identity is durable: a delisting must not erase who issued the ISIN."""
    sql = build_firds_instrument_issuer_sql(_STAGE)

    assert "FROM corpscout.firds_instrument_events" in sql
    assert "firds_instruments_current" not in sql


def test_projection_emits_the_lei_scheme() -> None:
    sql = build_firds_instrument_issuer_sql(_STAGE)

    assert "'lei' AS issuer_scheme" in sql
    assert "'esma_firds' AS mapping_source" in sql


def test_projection_carries_no_venue_facts() -> None:
    """Venue and CFI facts belong to instrument_venues."""
    sql = build_firds_instrument_issuer_sql(_STAGE)

    assert "venue_confirmed" not in sql
    assert "cfi_category" not in sql
    assert "mic" not in sql


def test_projection_is_neither_country_nor_cfi_filtered() -> None:
    sql = build_firds_instrument_issuer_sql(_STAGE)

    assert "competent_authority_country" not in sql
    assert "startsWith" not in sql


def test_projection_drops_rows_without_both_identifiers() -> None:
    sql = build_firds_instrument_issuer_sql(_STAGE)

    assert "WHERE trimBoth(e.isin) != ''" in sql
    assert "AND trimBoth(e.issuer_lei) != ''" in sql
