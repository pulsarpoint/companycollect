from dagster_v3.defs.instrument_venues import tables


def test_instrument_venues_column_contract() -> None:
    assert tables.INSTRUMENT_VENUES_TABLE == "instrument_venues"
    assert tables.INSTRUMENT_VENUES_COLUMNS == (
        "isin",
        "mic",
        "venue_source",
        "operating_mic",
        "evidence_tier",
        "cfi_code",
        "cfi_category",
        "instrument_name",
        "instrument_type",
        "ticker",
        "trading_currency",
        "trading_status",
        "is_current",
        "admission_date",
        "first_trade_date",
        "termination_date",
        "first_seen_date",
        "last_seen_date",
        "source_record_id",
        "source_publication_date",
        "source_retrieved_at",
        "source_run_id",
        "resolved_at",
    )
