from dataclasses import replace
from io import BytesIO
from datetime import date
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.esma_firds import state, tables
from dagster_v3.defs.esma_firds.parser import FirdsFileContext, iter_firds_records

FIXTURES = Path(__file__).parent / "fixtures" / "esma_firds"


def _context(file_type: str, publication_date: str) -> FirdsFileContext:
    name = f"{file_type}_{publication_date.replace('-', '')}_01of01.zip"
    return FirdsFileContext(
        source_file_id=f"id-{file_type}-{publication_date}",
        source_file_name=name,
        source_file_type=file_type,
        source_file_checksum="b" * 32,
        source_publication_date=publication_date,
        source_download_url=f"https://example.test/{name}",
        source_object_key=f"raw/{name}",
        source_run_id="run-1",
        source_retrieved_at=f"{publication_date}T10:00:00+00:00",
    )


def _seed(connection: duckdb.DuckDBPyConnection) -> None:
    state.ensure_duckdb_tables(connection)
    full = iter_firds_records(
        BytesIO((FIXTURES / "fulins_sample.xml").read_bytes()),
        context=_context("FULINS", "2026-07-18"),
    )
    delta = iter_firds_records(
        BytesIO((FIXTURES / "dltins_sample.xml").read_bytes()),
        context=_context("DLTINS", "2026-07-20"),
    )
    state.replace_source_file_records(
        connection,
        table=tables.FULL_RECORDS_RAW_TABLE,
        source_file_id="id-FULINS-2026-07-18",
        records=full,
    )
    state.replace_source_file_records(
        connection,
        table=tables.DELTA_EVENTS_RAW_TABLE,
        source_file_id="id-DLTINS-2026-07-20",
        records=delta,
    )
    state.mark_snapshot_set_complete(
        connection,
        file_type="FULINS",
        publication_date="2026-07-18",
        file_count=1,
        source_run_id="run-1",
    )
    state.mark_snapshot_set_complete(
        connection,
        file_type="DLTINS",
        publication_date="2026-07-20",
        file_count=1,
        source_run_id="run-1",
    )


def test_rebuild_keeps_history_and_derives_fresh_current_state() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)

    counts = state.rebuild_derived_tables(
        connection,
        as_of_date="2026-07-20",
    )

    assert counts["event_rows"] == 8
    assert counts["current_rows"] == 4
    current = connection.execute(
        f"""
        select isin, mic, full_name
        from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
        order by isin, mic
        """
    ).fetchall()
    assert current == [
        ("DE0000000001", "XFRA", "Germany Example AG senior bond"),
        ("SE0000000001", "XNGM", "Sweden Example AB ordinary share"),
        ("SE0000000001", "XSTO", "Sweden Example AB class A"),
        ("SE0000000002", "FNSE", "Sweden Growth AB ordinary share"),
    ]

    terminated = connection.execute(
        f"""
        select event_type, is_latest
        from {tables.DUCKDB_SCHEMA}.{tables.EVENTS_EXPORT_TABLE}
        where isin = 'FR0000000001' and mic = 'XPAR'
        order by source_publication_date
        """
    ).fetchall()
    assert terminated == [("BASELINE", False), ("TERMINATED", True)]


def test_rebuild_is_idempotent_and_preserves_column_contracts() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)

    first = state.rebuild_derived_tables(connection, as_of_date="2026-07-20")
    second = state.rebuild_derived_tables(connection, as_of_date="2026-07-20")

    assert second == first
    for table_name, expected_columns in (
        (tables.EVENTS_EXPORT_TABLE, tables.EVENTS_EXPORT_COLUMNS),
        (tables.CURRENT_EXPORT_TABLE, tables.CURRENT_EXPORT_COLUMNS),
    ):
        columns = tuple(
            row[0]
            for row in connection.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = ? and table_name = ?
                order by ordinal_position
                """,
                [tables.DUCKDB_SCHEMA, table_name],
            ).fetchall()
        )
        assert columns == expected_columns


def test_current_state_coverage_guard_rejects_country_collapse() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)
    state.rebuild_event_history(connection)
    state.rebuild_current_state(
        connection,
        as_of_date="2026-07-20",
    )

    with pytest.raises(ValueError, match="countries=2 below 3"):
        state.rebuild_current_state(
            connection,
            as_of_date="2026-07-20",
            minimum_country_count=3,
        )

    current = connection.execute(
        f"""
        select isin, mic
        from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
        order by isin, mic
        """
    ).fetchall()
    assert current == [
        ("DE0000000001", "XFRA"),
        ("SE0000000001", "XNGM"),
        ("SE0000000001", "XSTO"),
        ("SE0000000002", "FNSE"),
    ]


def test_event_history_preserves_same_day_correction_events() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)
    connection.execute(
        f"""
        update {tables.DUCKDB_SCHEMA}.{tables.DELTA_EVENTS_RAW_TABLE}
        set valid_from = '2024-01-02'
        where isin = 'SE0000000001' and mic = 'XSTO'
        """
    )

    state.rebuild_event_history(connection)

    rows = connection.execute(
        f"""
        select event_type, valid_from, valid_to, is_latest
        from {tables.DUCKDB_SCHEMA}.{tables.EVENTS_EXPORT_TABLE}
        where isin = 'SE0000000001' and mic = 'XSTO'
        order by source_publication_date
        """
    ).fetchall()
    assert rows == [
        ("BASELINE", date(2024, 1, 2), date(2024, 1, 2), False),
        ("MODIFIED", date(2024, 1, 2), None, True),
    ]


def test_event_coverage_metadata_reports_source_dimensions() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)
    state.rebuild_event_history(connection)

    metadata = state.event_coverage_metadata(connection)

    assert metadata["event_type_counts"] == {
        "BASELINE": 4,
        "CANCELLED": 1,
        "MODIFIED": 1,
        "NEW": 1,
        "TERMINATED": 1,
    }
    assert metadata["cfi_category_counts"] == {"D": 1, "E": 7}
    assert metadata["issuer_lei_coverage"] == {
        "with_issuer_lei": 8,
        "without_issuer_lei": 0,
        "distinct_issuer_leis": 5,
    }


def test_raw_file_batches_are_committed_and_safely_replaced_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = duckdb.connect(":memory:")
    state.ensure_duckdb_tables(connection)
    monkeypatch.setattr(state, "INSERT_BATCH_SIZE", 2)
    source_file_id = "id-FULINS-2026-07-18"
    sample = next(
        iter_firds_records(
            BytesIO((FIXTURES / "fulins_sample.xml").read_bytes()),
            context=_context("FULINS", "2026-07-18"),
        )
    )

    def interrupted_records():
        for row_number in range(1, 4):
            yield replace(
                sample,
                source_record_id=f"{source_file_id}:{row_number}",
                source_row_number=row_number,
            )
        raise RuntimeError("simulated parser interruption")

    with pytest.raises(RuntimeError, match="parser interruption"):
        state.replace_source_file_records(
            connection,
            table=tables.FULL_RECORDS_RAW_TABLE,
            source_file_id=source_file_id,
            records=interrupted_records(),
        )

    qualified_table = (
        f"{tables.DUCKDB_SCHEMA}.{tables.FULL_RECORDS_RAW_TABLE}"
    )
    assert connection.execute(
        f"select count(*) from {qualified_table}"
    ).fetchone()[0] == 2

    complete_records = [
        replace(
            sample,
            source_record_id=f"{source_file_id}:{row_number}",
            source_row_number=row_number,
        )
        for row_number in range(1, 6)
    ]
    row_count = state.replace_source_file_records(
        connection,
        table=tables.FULL_RECORDS_RAW_TABLE,
        source_file_id=source_file_id,
        records=complete_records,
    )

    assert row_count == 5
    assert connection.execute(
        f"select count(*) from {qualified_table}"
    ).fetchone()[0] == 5
    columns = {
        row[0]
        for row in connection.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            """,
            [tables.DUCKDB_SCHEMA, tables.FULL_RECORDS_RAW_TABLE],
        ).fetchall()
    }
    assert "source_payload_hash" in columns
    assert "raw_record_xml" not in columns


def test_incomplete_delta_set_is_excluded_from_history_and_current_state() -> None:
    connection = duckdb.connect(":memory:")
    _seed(connection)
    state.invalidate_snapshot_set(
        connection,
        file_type="DLTINS",
        publication_date="2026-07-20",
    )

    counts = state.rebuild_derived_tables(
        connection,
        as_of_date="2026-07-20",
    )

    assert counts["event_rows"] == 4
    assert counts["current_rows"] == 4
    assert connection.execute(
        f"""
        select count(*)
        from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
        where isin = 'SE0000000002'
        """
    ).fetchone()[0] == 0
    assert connection.execute(
        f"""
        select count(*)
        from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
        where isin = 'FR0000000001' and mic = 'XPAR'
        """
    ).fetchone()[0] == 1
