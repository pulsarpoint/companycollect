"""Tests for the ESEF filings index and canonical artifact parsing assets.

Index-crawl tests: no network -- EsefFilingsClient is monkeypatched on the
assets module to a stub returning canned EsefFilingRecord instances,
mirroring the class-monkeypatch pattern used for ExchangeRateClient in
tests/test_norway_brreg_financial_statement_assets.py. Each test materializes
into a fresh tmp_path DuckDB file via dg.materialize's resource override.

Fact writer tests exercise the bounded Parquet path reused by the canonical
artifact parser.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg
import duckdb
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.esef_filings import assets
from dagster_v3.defs.esef_filings.client import EsefFilingRecord
from dagster_v3.definitions import defs as load_project_defs

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "esef_filings"


def _record(
    *,
    fxo_id: str,
    country: str = "SE",
    lei: str = "549300CSLHPO6Y1AZN37",
    entity_name: str = "Example AB",
    json_url: str | None = "https://filings.xbrl.org/x/facts.json",
    report_url: str | None = "https://filings.xbrl.org/x/report.html",
    period_end: str | None = "2022-12-31",
    processed_at: str | None = "2023-01-02 00:00:00",
) -> EsefFilingRecord:
    return EsefFilingRecord(
        lei=lei,
        entity_name=entity_name,
        fxo_id=fxo_id,
        country=country,
        period_end=period_end,
        date_added="2023-01-01 00:00:00",
        processed_at=processed_at,
        json_url=json_url,
        package_url="https://filings.xbrl.org/x/package.zip",
        report_url=report_url,
        viewer_url="https://filings.xbrl.org/x/viewer.html",
        package_sha256="deadbeef",
        error_count=0,
        warning_count=0,
        inconsistency_count=0,
    )


class _StubEsefFilingsClient:
    """Stand-in for EsefFilingsClient() -- no session, no network.

    `last_reported_total` defaults to `None` (matching a real client before
    any crawl, or a first page whose `meta` had no usable `count`) so every
    existing test that doesn't pass it stays exempt from the Finding M1
    crawl-completeness guard, which is a no-op when the total is unknown.
    """

    def __init__(
        self,
        records: list[EsefFilingRecord],
        *,
        last_reported_total: int | None = None,
    ) -> None:
        self._records = records
        self.last_reported_total = last_reported_total

    def iter_filings(self, **_: Any) -> Iterator[EsefFilingRecord]:
        yield from self._records


def _patch_client(
    monkeypatch: pytest.MonkeyPatch,
    records: list[EsefFilingRecord],
    *,
    last_reported_total: int | None = None,
) -> None:
    monkeypatch.setattr(
        assets,
        "EsefFilingsClient",
        lambda: _StubEsefFilingsClient(
            records, last_reported_total=last_reported_total
        ),
    )


def _db_resource(tmp_path: Path) -> object:
    # duckdb_resource() caches by (path, connection_config), so calling this
    # again for the same tmp_path returns the same resource -- required for
    # the read-back connection below to share config with the write side
    # (DuckDB refuses a second connection to the same file with a different
    # configuration than an existing one).
    return duckdb_resource(tmp_path / "esef_filings_source.duckdb")


def _resources(tmp_path: Path) -> dict[str, object]:
    return {"esef_filings_duckdb": _db_resource(tmp_path)}


def _seed_filings_index(tmp_path: Path, records: list[EsefFilingRecord]) -> None:
    with _db_resource(tmp_path).get_connection() as connection:
        assets.upsert_esef_filings_index(
            connection=connection,
            records=records,
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="seed-run",
        )


def _materialize_index(tmp_path: Path) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [assets.esef_filings_index_duckdb],
        resources=_resources(tmp_path),
        partition_key="2023-01-01",
    )


def _fetch_filings_index(tmp_path: Path) -> list[tuple]:
    with read_only_duckdb_connection(_db_resource(tmp_path)) as connection:
        return connection.execute(
            "select fxo_id, has_json_facts, source_url, source_run_id "
            "from esef_filings.filings_index order by fxo_id"
        ).fetchall()


def test_three_records_land_one_without_json_facts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [
        _record(fxo_id="A-1"),
        _record(fxo_id="A-2", json_url=None),
        _record(fxo_id="A-3", country="FI"),
    ]
    _patch_client(monkeypatch, records)

    result = _materialize_index(tmp_path)

    assert result.success
    metadata = result.asset_materializations_for_node("esef_filings_index_duckdb")[
        0
    ].metadata
    assert metadata["window_row_count"].value == 3
    assert metadata["index_row_count"].value == 3
    assert metadata["with_json_facts_count"].value == 2
    assert metadata["without_json_facts_count"].value == 1
    assert metadata["distinct_country_count"].value == 2
    assert metadata["country_distribution_top10"].value == {"SE": 2, "FI": 1}
    # No last_reported_total from the (stubbed) client -- nothing to report.
    assert metadata["api_reported_total"].value is None

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2", "A-3"]
    assert [row[1] for row in rows] == [True, False, True]
    assert all(row[2] == assets.ESEF_INDEX_URL for row in rows)
    assert all(row[3] == result.run_id for row in rows)


def test_second_materialization_upserts_without_duplicates_or_erasing_other_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = _materialize_index(tmp_path)
    assert first.success

    _patch_client(monkeypatch, [_record(fxo_id="B-1")])
    second = _materialize_index(tmp_path)
    assert second.success

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2", "B-1"]


def test_empty_processed_window_is_valid_and_preserves_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = _materialize_index(tmp_path)
    assert first.success

    _patch_client(monkeypatch, [])
    empty = _materialize_index(tmp_path)
    assert empty.success

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2"]


def test_upsert_filings_index_arrow_load_preserves_exact_row_shape(
    tmp_path: Path,
) -> None:
    record = dataclass_replace(
        _record(
            fxo_id="EXACT-1",
            json_url=None,
            report_url=None,
            period_end=None,
            processed_at=None,
        ),
        package_url=None,
        viewer_url=None,
        package_sha256=None,
        error_count=1,
        warning_count=2,
        inconsistency_count=3,
    )

    with _db_resource(tmp_path).get_connection() as connection:
        assets.upsert_esef_filings_index(
            connection=connection,
            records=[record],
            source_url="https://example.test/index",
            source_run_id="exact-run",
        )
        result = connection.execute("select * from esef_filings.filings_index")
        columns = tuple(column[0] for column in result.description)
        row = result.fetchone()

    assert columns == assets.tables.ESEF_FILINGS_EXPORT_COLUMNS
    assert row == (
        record.lei,
        record.entity_name,
        "EXACT-1",
        record.country,
        None,
        record.date_added,
        None,
        None,
        None,
        None,
        None,
        None,
        1,
        2,
        3,
        False,
        "https://example.test/index",
        "exact-run",
    )


def test_upsert_filings_index_uses_fxo_id_identity_and_last_record_wins(
    tmp_path: Path,
) -> None:
    first = _record(fxo_id="SAME-1", entity_name="First version")
    last = _record(fxo_id="SAME-1", entity_name="Last version")
    other = _record(fxo_id="OTHER-1")

    with _db_resource(tmp_path).get_connection() as connection:
        first_summary = assets.upsert_esef_filings_index(
            connection=connection,
            records=[first, other, last],
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="first-run",
        )
        second_summary = assets.upsert_esef_filings_index(
            connection=connection,
            records=[first, other, last],
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="second-run",
        )
        rows = connection.execute(
            "select fxo_id, entity_name, source_run_id "
            "from esef_filings.filings_index order by fxo_id"
        ).fetchall()

    assert first_summary["received_count"] == 3
    assert first_summary["duplicate_fxo_id_count"] == 1
    assert first_summary["index_row_count"] == 2
    assert second_summary["index_row_count"] == 2
    assert rows == [
        ("OTHER-1", "Example AB", "second-run"),
        ("SAME-1", "Last version", "second-run"),
    ]


def test_incremental_index_upsert_preserves_other_processed_weeks(
    tmp_path: Path,
) -> None:
    january = _record(fxo_id="JAN-1", processed_at="2023-01-15 12:00:00")
    july_late_filing = _record(
        fxo_id="JUL-1",
        period_end="2021-12-31",
        processed_at="2026-07-21 18:21:29.311103",
    )

    with _db_resource(tmp_path).get_connection() as connection:
        assets.upsert_esef_filings_index(
            connection=connection,
            records=[january],
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="january-run",
        )
        assets.upsert_esef_filings_index(
            connection=connection,
            records=[july_late_filing],
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="july-run",
        )

    assert [row[0] for row in _fetch_filings_index(tmp_path)] == ["JAN-1", "JUL-1"]


# --------------------------------------------------------------------------
# Crawl-completeness guard (Finding M1): a nonzero crawl that's still far
# short of the API's own reported total (`meta.count`) must be refused, the
# same "before touching the existing table" discipline as the index write
# explicit empty-crawl guard.
# --------------------------------------------------------------------------


def test_crawl_completeness_guard_refuses_below_90_percent_and_preserves_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = _materialize_index(tmp_path)
    assert first.success

    # 2 crawled out of a reported 1000 -- nowhere near the 90% floor.
    _patch_client(
        monkeypatch,
        [_record(fxo_id="C-1"), _record(fxo_id="C-2")],
        last_reported_total=1000,
    )
    with pytest.raises(ValueError, match="below 90%"):
        _materialize_index(tmp_path)

    # The prior (complete) crawl's rows must survive untouched.
    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2"]


def test_crawl_completeness_guard_passes_at_exactly_the_90_percent_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 9 crawled out of a reported 10 == exactly 90% -- must NOT refuse
    # (the guard's floor is inclusive: "< 90%" refuses, "90%" passes).
    records = [_record(fxo_id=f"A-{i}") for i in range(9)]
    _patch_client(monkeypatch, records, last_reported_total=10)

    result = _materialize_index(tmp_path)

    assert result.success
    metadata = result.asset_materializations_for_node("esef_filings_index_duckdb")[
        0
    ].metadata
    assert metadata["window_row_count"].value == 9
    assert metadata["api_reported_total"].value == 10


def test_crawl_completeness_guard_is_a_noop_when_api_total_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Only 1 filing crawled, no reported total available at all (the
    # default -- matches a real client whose first page had no meta.count)
    # -- nothing to compare against, so the guard must not fire.
    _patch_client(monkeypatch, [_record(fxo_id="A-1")], last_reported_total=None)

    result = _materialize_index(tmp_path)

    assert result.success


def test_check_crawl_completeness_refuses_below_ratio() -> None:
    with pytest.raises(ValueError, match="below 90%"):
        assets._check_crawl_completeness(crawled_count=89, api_reported_total=100)


def test_check_crawl_completeness_passes_at_ratio() -> None:
    assets._check_crawl_completeness(crawled_count=90, api_reported_total=100)


def test_check_crawl_completeness_noop_when_total_unknown() -> None:
    assets._check_crawl_completeness(crawled_count=0, api_reported_total=None)


def test_filings_index_non_empty_check_passes_on_populated_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1")])

    result = dg.materialize(
        [assets.esef_filings_index_duckdb, assets.filings_index_non_empty],
        resources=_resources(tmp_path),
        partition_key="2023-01-01",
    )

    assert result.success
    checks = result.get_asset_check_evaluations()
    assert len(checks) == 1
    assert checks[0].check_name == "filings_index_non_empty"
    assert checks[0].passed is True
    assert checks[0].metadata["row_count"].value == 1


def test_esef_filings_source_duckdb_path_defaults_under_data_root() -> None:
    assert assets.esef_filings_source_duckdb_path() == Path(
        "data/esef_filings_source.duckdb"
    )
    assert assets.esef_filings_source_duckdb_path(root="custom") == Path(
        "custom/esef_filings_source.duckdb"
    )


# ==========================================================================
# read_only_duckdb_connection: exception-safety hardening.
#
# Every read-only DuckDB usage in this module (the index-non-empty check,
# the facts/report-xhtml partition scope reads, the two ClickHouse export
# assets) goes through this one shared helper
# (dagster_v3.defs.common.duckdb_resources.read_only_duckdb_connection), so
# hardening it once covers all of them. `dagster_duckdb.DuckDBResource
# .get_connection()` is a bare `@contextmanager` (`yield conn; conn.close()`,
# no try/finally) -- an exception raised inside the caller's `with` block is
# thrown straight through the suspended `yield`, so `conn.close()` never
# runs and the connection leaks. This test fakes that exact real behavior
# (a `get_connection()` replacement that only closes on the no-exception
# path) to prove `read_only_duckdb_connection`'s own try/except closes the
# connection anyway.
# ==========================================================================


class _RecordingCloseConnection:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, *_args: object, **_kwargs: object) -> Any:
        raise RuntimeError("scope SELECT failed")

    def close(self) -> None:
        self.closed = True


def test_read_only_duckdb_connection_closes_underlying_connection_on_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_connection = _RecordingCloseConnection()

    @contextmanager
    def fake_get_connection(self: DuckDBResource) -> Iterator[Any]:
        # Mirrors dagster_duckdb's real (buggy) get_connection(): no
        # try/finally, so conn.close() below only runs when the `with`
        # block exits normally -- never on an exception raised inside it.
        yield fake_connection
        fake_connection.close()

    monkeypatch.setattr(DuckDBResource, "get_connection", fake_get_connection)

    resource = duckdb_resource(tmp_path / "esef_filings_source.duckdb")

    with pytest.raises(RuntimeError, match="scope SELECT failed"):
        with read_only_duckdb_connection(resource) as connection:
            connection.execute("select count(*) from esef_filings.filings_index")

    assert fake_connection.closed is True


# ==========================================================================
def _synthetic_fact_row(
    *, fact_id: str, period_end_year: int, source_run_id: str = "test-run"
) -> tuple[Any, ...]:
    """A minimally-valid `esef_filings.facts` row shaped to
    `assets._FACTS_COLUMNS` -- column order/count must match exactly since
    `_replace_facts_for_filings` inserts with positional `?` placeholders."""
    assert len(assets._FACTS_COLUMNS) == 20
    return (
        "5493000000000000TEST",  # lei
        f"fxo-{period_end_year}",  # fxo_id
        f"{period_end_year}-12-31",  # period_end
        period_end_year,  # period_end_year
        fact_id,  # fact_id
        "ifrs-full:Revenue",  # concept_qname
        "ifrs-full",  # concept_namespace
        "Revenue",  # concept_local_name
        None,  # period_start
        None,  # period_instant
        None,  # period_duration_end
        "iso4217:EUR",  # unit
        "EUR",  # currency
        "monetary",  # value_kind
        "100.00",  # raw_value
        "100.00",  # amount_original
        2,  # decimals
        "{}",  # dimensions
        None,  # language
        source_run_id,  # source_run_id
    )


def _fetch_facts_rows(tmp_path: Path) -> list[tuple[Any, ...]]:
    with read_only_duckdb_connection(_db_resource(tmp_path)) as connection:
        return connection.execute(
            "select fxo_id, fact_id, period_end_year, source_run_id "
            "from esef_filings.facts order by fxo_id, fact_id"
        ).fetchall()


def _spool_synthetic_fact_rows(
    target_directory: Path,
    fact_rows: Iterator[tuple[Any, ...]] | list[tuple[Any, ...]],
    *,
    file_prefix: str,
) -> list[tuple[Path, int]]:
    return assets._spool_fact_rows_to_parquet(
        fact_rows,
        target_directory=target_directory,
        file_prefix=file_prefix,
    )


def test_fact_parquet_spool_coalesces_small_filing_submissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(assets, "_FACTS_INSERT_CHUNK_SIZE", 3)
    spool = assets._FactParquetSpool(
        target_directory=tmp_path / "coalesced-fact-batches",
        file_prefix="coalesced",
    )

    first_count = spool.add_fact_rows(
        _synthetic_fact_row(fact_id=f"first-{index}", period_end_year=2022)
        for index in range(2)
    )
    second_count = spool.add_fact_rows(
        _synthetic_fact_row(fact_id=f"second-{index}", period_end_year=2022)
        for index in range(2)
    )
    fact_batches = spool.close()

    assert first_count == 2
    assert second_count == 2
    assert [row_count for _, row_count in fact_batches] == [3, 1]


def test_replace_facts_partition_chunked_insert_lands_all_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """>1 chunk's worth of rows (7 rows / chunk size 3 -> chunks of 3, 3, 1)
    must all land, scoped to the right year, and the per-chunk `log_info`
    calls must show a running total -- proving the insert actually went
    through multiple bounded transactions rather than one giant
    executemany."""
    monkeypatch.setattr(assets, "_FACTS_INSERT_CHUNK_SIZE", 3)
    fact_rows = [
        _synthetic_fact_row(fact_id=f"f-{i}", period_end_year=2022) for i in range(7)
    ]
    log_lines: list[tuple[Any, ...]] = []
    fact_batches = _spool_synthetic_fact_rows(
        tmp_path / "fact-batches",
        fact_rows,
        file_prefix="chunked",
    )

    with _db_resource(tmp_path).get_connection() as connection:
        assets._replace_facts_for_filings(
            connection=connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=fact_batches,
            completed_filing_fact_counts={"fxo-2022": 7},
            source_run_id="test-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: log_lines.append(a),
        )
        exact_result = connection.execute(
            "select * from esef_filings.facts where fact_id = 'f-0'"
        )
        exact_columns = tuple(column[0] for column in exact_result.description)
        exact_row = exact_result.fetchone()

    rows = _fetch_facts_rows(tmp_path)
    assert exact_columns == assets._FACTS_COLUMNS
    assert exact_row == fact_rows[0]
    assert {row[1] for row in rows} == {f"f-{i}" for i in range(7)}
    assert all(row[2] == 2022 for row in rows)

    assert [line[2] for line in log_lines] == [3, 6, 7]  # running "inserted" total
    assert [line[3] for line in log_lines] == [7, 7, 7]  # constant "total" rows


def test_replace_facts_partition_rerun_replaces_not_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Delete-first contract: re-running the partition replace for the same
    year fully replaces its rows rather than appending duplicates alongside
    them. The chunked multi-transaction insert doesn't change this contract
    -- the DELETE still commits, alone, in its own transaction, before any
    insert chunk begins."""
    monkeypatch.setattr(assets, "_FACTS_INSERT_CHUNK_SIZE", 3)
    first_rows = [
        _synthetic_fact_row(fact_id=f"f-{i}", period_end_year=2022) for i in range(5)
    ]
    first_batches = _spool_synthetic_fact_rows(
        tmp_path / "first-fact-batches",
        first_rows,
        file_prefix="first",
    )
    with _db_resource(tmp_path).get_connection() as connection:
        assets._replace_facts_for_filings(
            connection=connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=first_batches,
            completed_filing_fact_counts={"fxo-2022": 5},
            source_run_id="test-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )
    assert len(_fetch_facts_rows(tmp_path)) == 5

    second_rows = [
        _synthetic_fact_row(fact_id=f"g-{i}", period_end_year=2022) for i in range(4)
    ]
    second_batches = _spool_synthetic_fact_rows(
        tmp_path / "second-fact-batches",
        second_rows,
        file_prefix="second",
    )
    with _db_resource(tmp_path).get_connection() as connection:
        assets._replace_facts_for_filings(
            connection=connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=second_batches,
            completed_filing_fact_counts={"fxo-2022": 4},
            source_run_id="test-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )

    rows = _fetch_facts_rows(tmp_path)
    assert {row[1] for row in rows} == {f"g-{i}" for i in range(4)}
    assert len(rows) == 4  # the 5 first-run rows are gone, not appended to


class _CountingFactConnection:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        fail_on_insert: int | None = None,
    ) -> None:
        self.connection = connection
        self.fail_on_insert = fail_on_insert
        self.fact_insert_count = 0
        self.closed = False

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        normalized_statement = " ".join(statement.split()).lower()
        if (
            normalized_statement.startswith("insert into esef_filings.facts ")
            and "read_parquet" in normalized_statement
        ):
            self.fact_insert_count += 1
            if self.fact_insert_count == self.fail_on_insert:
                raise RuntimeError("simulated ESEF fact batch insert failure")
        return self.connection.execute(statement, *args, **kwargs)

    def register(self, name: str, value: object) -> None:
        self.connection.register(name, value)

    def unregister(self, name: str) -> None:
        self.connection.unregister(name)

    def close(self) -> None:
        self.closed = True
        self.connection.close()


def test_replace_facts_spools_and_inserts_500k_rows_in_50k_batches(
    tmp_path: Path,
) -> None:
    fact_batches = _spool_synthetic_fact_rows(
        tmp_path / "bulk-fact-batches",
        (
            _synthetic_fact_row(
                fact_id=f"bulk-{index}",
                period_end_year=2022,
                source_run_id="bulk-run",
            )
            for index in range(500_000)
        ),
        file_prefix="bulk",
    )

    with duckdb.connect(str(tmp_path / "bulk-facts.duckdb")) as connection:
        counting_connection = _CountingFactConnection(connection)
        assets._replace_facts_for_filings(
            connection=counting_connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=fact_batches,
            completed_filing_fact_counts={"fxo-2022": 500_000},
            source_run_id="bulk-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )
        fact_row_count = connection.execute(
            "select count(*) from esef_filings.facts"
        ).fetchone()[0]
        state_row = connection.execute(
            "select fact_count, source_run_id from esef_filings.facts_ingestion_state"
        ).fetchone()

    assert [row_count for _, row_count in fact_batches] == [50_000] * 10
    assert counting_connection.fact_insert_count == 10
    assert fact_row_count == 500_000
    assert state_row == (500_000, "bulk-run")


def test_replace_facts_retry_deletes_a_partially_committed_batch_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(assets, "_FACTS_INSERT_CHUNK_SIZE", 3)
    database_path = tmp_path / "retry-facts.duckdb"
    seed_batches = _spool_synthetic_fact_rows(
        tmp_path / "seed-fact-batches",
        iter([_synthetic_fact_row(fact_id="seed", period_end_year=2022)]),
        file_prefix="seed",
    )
    retry_batches = _spool_synthetic_fact_rows(
        tmp_path / "retry-fact-batches",
        (
            _synthetic_fact_row(fact_id=f"retry-{index}", period_end_year=2022)
            for index in range(7)
        ),
        file_prefix="retry",
    )

    with duckdb.connect(str(database_path)) as connection:
        assets._replace_facts_for_filings(
            connection=connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=seed_batches,
            completed_filing_fact_counts={"fxo-2022": 1},
            source_run_id="seed-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )

    failing_connection = _CountingFactConnection(
        duckdb.connect(str(database_path)),
        fail_on_insert=2,
    )
    with pytest.raises(RuntimeError, match="simulated ESEF fact batch"):
        assets._replace_facts_for_filings(
            connection=failing_connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=retry_batches,
            completed_filing_fact_counts={"fxo-2022": 7},
            source_run_id="retry-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )
    assert failing_connection.closed is True

    with duckdb.connect(str(database_path)) as connection:
        partial_fact_ids = [
            row[0]
            for row in connection.execute(
                "select fact_id from esef_filings.facts order by fact_id"
            ).fetchall()
        ]
        state_count = connection.execute(
            "select count(*) from esef_filings.facts_ingestion_state"
        ).fetchone()[0]
        assets._replace_facts_for_filings(
            connection=connection,
            filing_ids=["fxo-2022"],
            partition_key="2023-01-01",
            fact_batches=retry_batches,
            completed_filing_fact_counts={"fxo-2022": 7},
            source_run_id="retry-run",
            parser_contract=assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
            log_info=lambda *a, **k: None,
        )
        final_fact_ids = [
            row[0]
            for row in connection.execute(
                "select fact_id from esef_filings.facts order by fact_id"
            ).fetchall()
        ]
        final_state = connection.execute(
            "select fact_count, source_run_id from esef_filings.facts_ingestion_state"
        ).fetchone()

    assert partial_fact_ids == ["retry-0", "retry-1", "retry-2"]
    assert state_count == 0
    assert final_fact_ids == [f"retry-{index}" for index in range(7)]
    assert final_state == (7, "retry-run")


# ==========================================================================
# Dead upstream links (production incident, 2026-07-21): filings.xbrl.org's
# index advertises a `json_url` for a filing whose file 404s/410s upstream
# (e.g. several UA filings). A permanently-missing file must be skipped and
# counted (`skipped_upstream_missing`), never starve the whole partition --
# but any OTHER HTTP error (5xx, etc.) must still propagate and fail loudly.
# ==========================================================================


def test_esef_filings_refresh_job_selects_weekly_ingest_evidence_and_exports() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_refresh_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert asset_keys == {
        "esef_filings_index_duckdb",
        "esef_filing_facts_duckdb",
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_entity_registry_map_clickhouse",
        "esef_document_extraction_manifest_s3",
        "esef_document_artifacts_s3",
        "esef_document_contact_candidates_duckdb",
        "esef_document_concept_labels_duckdb",
        "esef_disclosures_duckdb",
        "esef_document_contact_candidates_clickhouse",
        "esef_document_concept_labels_clickhouse",
        "esef_disclosures_clickhouse",
    }


def test_esef_filings_refresh_job_uses_shared_processed_week_partitions() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_refresh_job")

    assert job.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS


def test_esef_official_concept_pairs_are_written_to_text_translations() -> None:
    from dagster_v3.defs.esef_filings.document_publish import (
        _esef_official_translation_insert_sql,
    )

    sql = _esef_official_translation_insert_sql()

    assert "INSERT INTO corpscout.text_translations" in sql
    assert "cityHash64(source.label)" in sql
    assert "source.is_report_language" in sql
    assert "target.language = 'en'" in sql
    assert "target.label_role = source.label_role" in sql
    assert "'taxonomy'" in sql
    assert "'esef-official-taxonomy'" in sql
    assert "current.translated_text != candidates.translated_text" in sql


def test_esef_machine_translation_scan_uses_the_complete_language_pair_key() -> None:
    from dagster_v3.defs.esef_filings.document_publish import (
        _esef_missing_concept_translation_scan_sql,
    )

    sql = _esef_missing_concept_translation_scan_sql()

    assert "source.is_report_language" in sql
    assert "source.language != 'en'" in sql
    assert "source.language = translation.source_lang" in sql
    assert "translation.target_lang = 'en'" in sql
    assert "translation.source_text_hash = cityHash64(source.label)" in sql
    assert "length(source.label) <= 8000" in sql


def test_esef_translation_language_names_support_regional_tags() -> None:
    from dagster_v3.defs.esef_filings.document_publish import _esef_language_name

    assert _esef_language_name("fr") == "French"
    assert _esef_language_name("nl-BE") == "Dutch"
    assert _esef_language_name("sv-SE") == "Swedish"
    assert _esef_language_name("zz") is None


def test_esef_filings_backfill_job_selects_partitioned_assets_only() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_backfill_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert asset_keys == {
        "esef_filings_index_duckdb",
        "esef_filing_facts_duckdb",
        "esef_document_extraction_manifest_s3",
        "esef_document_artifacts_s3",
        "esef_document_contact_candidates_duckdb",
        "esef_document_concept_labels_duckdb",
        "esef_disclosures_duckdb",
        "esef_facts_clickhouse",
        "esef_document_contact_candidates_clickhouse",
        "esef_document_concept_labels_clickhouse",
        "esef_disclosures_clickhouse",
    }
    assert job.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS


def test_esef_filings_refresh_weekly_schedule_registered() -> None:
    repo = load_project_defs().get_repository_def()
    schedule = repo.get_schedule_def("esef_filings_refresh_weekly")

    assert schedule.job_name == "esef_filings_refresh_job"
    assert schedule.cron_schedule == "10 5 * * 0"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_esef_filings_refresh_weekly_schedule_emits_last_closed_week() -> None:
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 7, 5, tzinfo=ZoneInfo("UTC")),
        repository_def=load_project_defs().get_repository_def(),
    )

    execution_data = assets.esef_filings_refresh_weekly.evaluate_tick(context)

    assert [request.partition_key for request in execution_data.run_requests] == [
        "2026-06-28",
    ]
    assert [request.run_key for request in execution_data.run_requests] == [
        "2026-07-05T00:00:00Z:2026-06-28",
    ]


# ==========================================================================
# esef_filings_refresh_job: actually EXECUTING the mixed partitioned/
# unpartitioned selection, not just resolving its static shape (the tests
# above only inspect job.asset_layer/partitions_def -- they never run a
# single step). This is the one test in the whole module that proves the
# weekly schedule's processed-week partition key genuinely drives all
# 7 assets end to end for that partition.
#
# KNOWN GOTCHA (see the facts-download test section's docstring above): a
# ConfigurableResource built with an injected private attribute/instance
# does NOT survive dg.materialize/execute_in_process -- Dagster reconstructs
# it from its resolved pydantic config fields alone, silently dropping a
# fake client and hitting real boto3/clickhouse_driver instead. Monkey-
# patching the underlying methods at the CLASS level (ObjectStoreResource,
# ClickhouseResource), not on an instance, survives that reconstruction: the
# freshly-reconstructed instance still resolves to these patched methods.
# ==========================================================================


class _FakeScheduleEvaluationContext:
    """Duck-typed stand-in for dg.ScheduleEvaluationContext -- the resolver
    only reads `.scheduled_execution_time` (see
    assets._esef_filings_refresh_run_request's docstring)."""

    def __init__(self, scheduled_execution_time: datetime | None) -> None:
        self.scheduled_execution_time = scheduled_execution_time


def test_refresh_run_request_uses_last_closed_utc_week() -> None:
    scheduled_time = datetime(2026, 7, 19, 5, 50, tzinfo=ZoneInfo("UTC"))
    result = list(
        assets._esef_filings_refresh_run_request(
            _FakeScheduleEvaluationContext(scheduled_time)
        )
    )
    assert [request.partition_key for request in result] == ["2026-07-12"]


def test_refresh_run_requests_use_source_utc_clock_at_year_boundary() -> None:
    # It is already January in Belgrade, but still December in the source's
    # UTC processed-time clock.
    scheduled_time = datetime(2025, 12, 31, 23, 30, tzinfo=ZoneInfo("UTC"))
    result = list(
        assets._esef_filings_refresh_run_request(
            _FakeScheduleEvaluationContext(scheduled_time)
        )
    )
    assert [request.partition_key for request in result] == ["2025-12-21"]


def test_refresh_run_request_has_no_manually_maintained_end_year_ceiling() -> None:
    scheduled_time = datetime(2030, 1, 1, tzinfo=ZoneInfo("UTC"))
    result = list(
        assets._esef_filings_refresh_run_request(
            _FakeScheduleEvaluationContext(scheduled_time)
        )
    )
    assert [request.partition_key for request in result] == ["2029-12-23"]


def test_refresh_run_request_falls_back_to_now_when_no_scheduled_time() -> None:
    result = list(
        assets._esef_filings_refresh_run_request(_FakeScheduleEvaluationContext(None))
    )
    now = datetime.now(tz=ZoneInfo("UTC"))
    current_sunday = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    assert [request.partition_key for request in result] == [
        (current_sunday - timedelta(days=7)).strftime("%Y-%m-%d")
    ]
