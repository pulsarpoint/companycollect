"""Tests for the ESEF filings index crawl and processed-week download assets.

Index-crawl tests: no network -- EsefFilingsClient is monkeypatched on the
assets module to a stub returning canned EsefFilingRecord instances,
mirroring the class-monkeypatch pattern used for ExchangeRateClient in
tests/test_norway_brreg_financial_statement_assets.py. Each test materializes
into a fresh tmp_path DuckDB file via dg.materialize's resource override.

Facts-download tests call the raw-S3 and DuckDB runner functions directly
with a duck-typed FakeObjectStore/stub client, rather than through
`dg.materialize` -- see the long comment at the top of that test section for
why (a real, confirmed Dagster resource-reconstruction behavior that drops
an injected fake S3 client).
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg
import duckdb
import pyarrow as pa
import pytest
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
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


def test_replace_filings_index_arrow_load_preserves_exact_row_shape(
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
        assets.replace_esef_filings_index(
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


def test_replace_filings_index_refuses_empty_and_rolls_back_arrow_failure(
    tmp_path: Path,
) -> None:
    original = _record(fxo_id="ORIGINAL-1")
    invalid = dataclass_replace(
        _record(fxo_id="INVALID-1"),
        error_count="not-an-integer",
    )

    with _db_resource(tmp_path).get_connection() as connection:
        assets.replace_esef_filings_index(
            connection=connection,
            records=[original],
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="seed-run",
        )
        with pytest.raises(ValueError, match="refusing to replace"):
            assets.replace_esef_filings_index(
                connection=connection,
                records=[],
                source_url=assets.ESEF_INDEX_URL,
                source_run_id="empty-run",
            )
        with pytest.raises(pa.ArrowInvalid, match="convert"):
            assets.replace_esef_filings_index(
                connection=connection,
                records=[invalid],
                source_url=assets.ESEF_INDEX_URL,
                source_run_id="invalid-run",
            )
        rows = connection.execute(
            "select fxo_id, source_run_id from esef_filings.filings_index"
        ).fetchall()

    assert rows == [("ORIGINAL-1", "seed-run")]


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


class _CountingIndexConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection
        self.insert_count = 0

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        normalized_statement = " ".join(statement.split()).lower()
        if normalized_statement.startswith(
            f"insert into {assets._FILINGS_INDEX_REPLACEMENT_TABLE}"
        ):
            self.insert_count += 1
        return self.connection.execute(statement, *args, **kwargs)

    def register(self, name: str, value: object) -> None:
        self.connection.register(name, value)

    def unregister(self, name: str) -> None:
        self.connection.unregister(name)


def test_replace_filings_index_loads_500k_rows_in_50k_arrow_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [_record(fxo_id="BULK-1")] * 500_000
    batch_sizes: list[int] = []
    original_arrow_table = assets._filings_index_arrow_table

    def tracked_arrow_table(
        batch: list[EsefFilingRecord],
        *,
        source_url: str,
        source_run_id: str,
    ) -> pa.Table:
        batch_sizes.append(len(batch))
        return original_arrow_table(
            batch,
            source_url=source_url,
            source_run_id=source_run_id,
        )

    monkeypatch.setattr(assets, "_filings_index_arrow_table", tracked_arrow_table)

    with duckdb.connect(str(tmp_path / "bulk-index.duckdb")) as connection:
        counting_connection = _CountingIndexConnection(connection)
        summary = assets.replace_esef_filings_index(
            connection=counting_connection,
            records=records,
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="bulk-run",
        )
        row_count = connection.execute(
            "select count(*) from esef_filings.filings_index"
        ).fetchone()[0]

    assert summary["row_count"] == 500_000
    assert row_count == 500_000
    assert batch_sizes == [assets._FILINGS_INDEX_INSERT_BATCH_SIZE] * 10
    assert counting_connection.insert_count == 10


def test_reconciliation_reports_new_and_removed_filing_months(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(fxo_id="KEEP-1", processed_at="2023-01-10 00:00:00"),
            _record(fxo_id="REMOVED-1", processed_at="2023-02-10 00:00:00"),
        ],
    )
    _patch_client(
        monkeypatch,
        [
            _record(fxo_id="KEEP-1", processed_at="2023-01-10 00:00:00"),
            _record(fxo_id="NEW-1", processed_at="2023-03-10 00:00:00"),
        ],
        last_reported_total=2,
    )

    result = dg.materialize(
        [assets.esef_filings_index_reconciliation_duckdb],
        resources=_resources(tmp_path),
    )

    assert result.success
    metadata = result.asset_materializations_for_node(
        "esef_filings_index_reconciliation_duckdb"
    )[0].metadata
    assert metadata["new_since_local_count"].value == 1
    assert metadata["removed_upstream_count"].value == 1
    assert metadata["affected_processed_months"].value == ["2023-02", "2023-03"]
    assert [row[0] for row in _fetch_filings_index(tmp_path)] == ["KEEP-1", "NEW-1"]


# --------------------------------------------------------------------------
# Crawl-completeness guard (Finding M1): a nonzero crawl that's still far
# short of the API's own reported total (`meta.count`) must be refused, the
# same "before touching the existing table" discipline as reconciliation's
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
# esef_filing_facts_json_s3 + esef_filing_facts_duckdb
#
# `run_esef_filing_facts_partition` (the plain function the asset delegates
# to) is called DIRECTLY here rather than through `dg.materialize` -- a
# `ConfigurableResource` built with an injected private attribute (e.g.
# `ObjectStoreResource(s3_client=fake)`) does NOT survive
# `dg.materialize`/`execute_in_process`: Dagster reconstructs the resource
# from its resolved pydantic config fields alone, silently dropping the
# fake client and hitting real boto3/network instead (confirmed by tracing
# `ObjectStoreResource.__init__` -- it's invoked again with `s3_client=None`
# right before the asset body runs). Calling the plain function directly
# mirrors the codebase's established pattern for this exact situation (e.g.
# `extract_sweden_financial_report_xhtml_catalog` in
# tests/test_sweden_financial_resources.py, `source_result_object_keys` in
# tests/test_denmark_cvr_duckdb.py) -- a duck-typed FakeObjectStore, no
# pydantic/Dagster resource machinery involved at all.
# ==========================================================================

_FIXTURE_FACTS_BYTES = (FIXTURES_DIR / "facts_sample.json").read_bytes()  # 6 facts

_SMALL_PAYLOAD_BYTES = json.dumps(
    {
        "facts": {
            "f1": {
                "value": "100.00",
                "decimals": 2,
                "dimensions": {
                    "concept": "ifrs-full:Revenue",
                    "period": "2022-01-01T00:00:00/2022-06-30T00:00:00",
                    "unit": "iso4217:EUR",
                },
            },
            "f2": {
                "value": "A small company.",
                "dimensions": {
                    "concept": "ifrs-full:LegalFormOfEntity",
                    "period": "2022-06-30T00:00:00",
                    "language": "en",
                },
            },
        }
    }
).encode()


class FakeObjectStore:
    """Duck-types the ObjectStoreResource methods the asset calls -- no
    pydantic/boto3 involved (see module-section docstring above)."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_keys: list[tuple[str, str]] = []
        self.downloaded_keys: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self, key: str, source_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.uploaded_keys.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def download_file(
        self, key: str, target_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.downloaded_keys.append((bucket, key))
        Path(target_path).write_bytes(self.objects[(bucket, key)])


def _facts_json_url(fxo_id: str) -> str:
    return f"https://filings.xbrl.org/{fxo_id}/facts.json"


class _FakeHttpErrorResponse:
    """Duck-types `requests.Response` far enough for the asset loop's
    `_is_permanently_missing_upstream_error` guard, which reads only
    `.status_code` off `HTTPError.response`."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def _http_error(status_code: int) -> Exception:
    """Build a `dlt_requests.HTTPError` shaped like what
    `EsefFilingsClient.download_json_facts` actually raises (see
    client.py/session.py: `Session.request()` calls `resp.raise_for_status()`,
    a plain `requests.exceptions.HTTPError` with `.response` set) -- used to
    simulate both a permanently-missing upstream file (404/410, must be
    skipped-and-counted) and any other HTTP error (e.g. 500, must still
    propagate and fail the partition loudly)."""
    return assets.dlt_requests.HTTPError(
        f"{status_code} error", response=_FakeHttpErrorResponse(status_code)
    )


class _StubFactsDownloadClient:
    """Stand-in for EsefFilingsClient() -- serves canned bytes keyed by
    fxo_id (parsed back out of the json_url path `.../<fxo_id>/facts.json`),
    writes malformed text for fxo_ids in `malformed_fxo_ids`, or raises an
    HTTPError with the given status code for fxo_ids in
    `http_error_status_by_fxo_id` (simulating filings.xbrl.org's dead
    upstream links -- 404/410 -- or any other HTTP error)."""

    def __init__(
        self,
        payload_by_fxo_id: dict[str, bytes],
        *,
        malformed_fxo_ids: frozenset[str] = frozenset(),
        http_error_status_by_fxo_id: dict[str, int] | None = None,
    ) -> None:
        self._payload_by_fxo_id = payload_by_fxo_id
        self._malformed_fxo_ids = malformed_fxo_ids
        self._http_error_status_by_fxo_id = http_error_status_by_fxo_id or {}
        self.download_calls: list[str] = []

    def download_json_facts(self, json_url: str, target: Path, **_: Any) -> None:
        self.download_calls.append(json_url)
        fxo_id = json_url.split("/")[-2]
        if fxo_id in self._http_error_status_by_fxo_id:
            raise _http_error(self._http_error_status_by_fxo_id[fxo_id])
        if fxo_id in self._malformed_fxo_ids:
            target.write_text("{this is not valid json")
            return
        target.write_bytes(self._payload_by_fxo_id[fxo_id])


def _seed_filings_index(tmp_path: Path, records: list[EsefFilingRecord]) -> None:
    with _db_resource(tmp_path).get_connection() as connection:
        assets.replace_esef_filings_index(
            connection=connection,
            records=records,
            source_url=assets.ESEF_INDEX_URL,
            source_run_id="seed-run",
        )


def _fetch_facts_rows(tmp_path: Path) -> list[tuple[Any, ...]]:
    with read_only_duckdb_connection(_db_resource(tmp_path)) as connection:
        return connection.execute(
            "select fxo_id, fact_id, period_end_year, source_run_id "
            "from esef_filings.facts order by fxo_id, fact_id"
        ).fetchall()


def _run_facts_partition(
    tmp_path: Path,
    object_store: FakeObjectStore,
    client: _StubFactsDownloadClient,
    *,
    partition_key: str = "2023-01-01",
    source_run_id: str = "run-1",
    log_warning: Any = lambda *a, **k: None,
) -> dict[str, int | str]:
    raw_metadata = assets.run_esef_filing_facts_json_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        client=client,
        partition_key=partition_key,
        log_info=lambda *a, **k: None,
        log_warning=log_warning,
    )
    facts_metadata = assets.run_esef_filing_facts_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        partition_key=partition_key,
        source_run_id=source_run_id,
        log_info=lambda *a, **k: None,
        log_warning=log_warning,
    )
    return {
        **facts_metadata,
        **{f"raw_{key}": value for key, value in raw_metadata.items()},
    }


def test_facts_asset_wiring_partitions_by_processed_week() -> None:
    raw_asset_def = assets.esef_filing_facts_json_s3
    asset_def = assets.esef_filing_facts_duckdb
    assert isinstance(asset_def.partitions_def, dg.TimeWindowPartitionsDefinition)
    assert asset_def.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS
    assert raw_asset_def.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS
    assert asset_def.partitions_def.start == datetime(
        2023, 1, 1, tzinfo=ZoneInfo("UTC")
    )
    raw_dep_keys = {dep.asset_key for spec in raw_asset_def.specs for dep in spec.deps}
    assert dg.AssetKey("esef_filings_index_duckdb") in raw_dep_keys
    dep_keys = {dep.asset_key for spec in asset_def.specs for dep in spec.deps}
    assert dep_keys == {dg.AssetKey("esef_document_artifacts_s3")}
    assert asset_def.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert asset_def.op.pool == assets.ESEF_FILINGS_DUCKDB_POOL


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


def test_facts_partition_downloads_filings_discovered_in_processed_week(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(
                fxo_id="B-1", period_end="2022-06-30", json_url=_facts_json_url("B-1")
            ),
            # Same fiscal-year family, but discovered in a later week.
            _record(
                fxo_id="G-1",
                period_end="2021-12-31",
                processed_at="2023-02-02 00:00:00",
                json_url=_facts_json_url("G-1"),
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"A-1": _FIXTURE_FACTS_BYTES, "B-1": _SMALL_PAYLOAD_BYTES}
    )

    metadata = _run_facts_partition(
        tmp_path,
        object_store,
        client,
        partition_key="2023-01-01",
        source_run_id="run-A",
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["raw_downloaded_count"] == 2
    assert metadata["raw_reused_count"] == 0
    assert metadata["skipped_no_json"] == 0
    assert metadata["partition_key"] == "2023-01-01"
    assert metadata["processed_window_start"] == "2023-01-01 00:00:00"
    assert metadata["processed_window_end"] == "2023-01-08 00:00:00"
    assert metadata["parse_failed_count"] == 0
    assert metadata["fact_row_count"] == 8  # 6 (fixture) + 2 (small payload)
    assert object_store.created_buckets == [assets.ESEF_FILINGS_FACTS_BUCKET]

    # G-1 was discovered in February, so January never attempts it even
    # though fiscal period_end is deliberately unrelated to discovery time.
    assert sorted(client.download_calls) == [
        _facts_json_url("A-1"),
        _facts_json_url("B-1"),
    ]

    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"A-1", "B-1"}
    assert all(row[2] == 2022 for row in rows)
    assert all(row[3] == "run-A" for row in rows)


def test_second_run_reuses_s3_object_and_keeps_row_count_stable(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            )
        ],
    )
    object_store = FakeObjectStore()

    first_metadata = _run_facts_partition(
        tmp_path,
        object_store,
        _StubFactsDownloadClient({"A-1": _FIXTURE_FACTS_BYTES}),
        partition_key="2023-01-01",
        source_run_id="run-1",
    )
    assert first_metadata["raw_downloaded_count"] == 1
    assert first_metadata["raw_reused_count"] == 0
    assert first_metadata["fact_row_count"] == 6

    # New client instance for the second run -- if download_json_facts were
    # called again it would KeyError (no payload registered), proving reuse.
    second_client = _StubFactsDownloadClient({})
    second_metadata = _run_facts_partition(
        tmp_path,
        object_store,
        second_client,
        partition_key="2023-01-01",
        source_run_id="run-2",
    )
    assert second_metadata["raw_downloaded_count"] == 0
    assert second_metadata["raw_reused_count"] == 1
    assert second_metadata["checkpointed_filing_count"] == 1
    assert second_metadata["checkpointed_fact_row_count"] == 6
    assert second_metadata["s3_read_count"] == 0
    assert second_metadata["fact_row_count"] == 6
    assert second_client.download_calls == []

    rows = _fetch_facts_rows(tmp_path)
    assert len(rows) == 6
    assert all(row[3] == "run-1" for row in rows)  # checkpoint reuse, no rewrite


def test_filing_without_json_url_increments_skipped_no_json(tmp_path: Path) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(fxo_id="A-1", period_end="2022-12-31", json_url=None),
            _record(
                fxo_id="B-1", period_end="2022-06-30", json_url=_facts_json_url("B-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient({"B-1": _SMALL_PAYLOAD_BYTES})

    metadata = _run_facts_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["skipped_no_json"] == 1
    assert metadata["raw_downloaded_count"] == 1
    assert metadata["fact_row_count"] == 2
    assert client.download_calls == [_facts_json_url("B-1")]


def test_invalid_period_end_values_are_skipped_without_crashing(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(fxo_id="D-1", period_end=None, json_url=_facts_json_url("D-1")),
            _record(
                fxo_id="E-1", period_end="2022-99-99", json_url=_facts_json_url("E-1")
            ),
            _record(
                fxo_id="F-1", period_end="garbage", json_url=_facts_json_url("F-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {filing_id: _FIXTURE_FACTS_BYTES for filing_id in ("A-1", "D-1", "E-1", "F-1")}
    )

    metadata = _run_facts_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["filings_in_scope"] == 4
    assert metadata["skipped_invalid_period_end"] == 3
    assert metadata["fact_row_count"] == 6
    # The raw archive preserves every source object. Invalid fiscal dates are
    # rejected only at the parsed-DuckDB boundary.
    assert metadata["raw_downloaded_count"] == 4
    assert client.download_calls == [
        _facts_json_url(filing_id) for filing_id in ("A-1", "D-1", "E-1", "F-1")
    ]


def test_malformed_json_increments_parse_failed_count_without_crashing(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(
                fxo_id="F-1", period_end="2022-03-01", json_url=_facts_json_url("F-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"A-1": _FIXTURE_FACTS_BYTES}, malformed_fxo_ids=frozenset({"F-1"})
    )

    metadata = _run_facts_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["parse_failed_count"] == 1
    assert metadata["raw_downloaded_count"] == 2  # raw asset archived both files
    assert metadata["fact_row_count"] == 6  # only A-1's facts landed

    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"A-1"}


def test_partition_scoped_replace_does_not_touch_other_processed_weeks(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(
                fxo_id="G-1",
                period_end="2021-12-31",
                processed_at="2023-02-02 00:00:00",
                json_url=_facts_json_url("G-1"),
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"A-1": _FIXTURE_FACTS_BYTES, "G-1": _SMALL_PAYLOAD_BYTES}
    )

    _run_facts_partition(tmp_path, object_store, client, partition_key="2023-01-29")
    _run_facts_partition(tmp_path, object_store, client, partition_key="2023-01-01")

    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"A-1", "G-1"}
    by_fxo = {row[0]: row[2] for row in rows}
    assert by_fxo["A-1"] == 2022
    assert by_fxo["G-1"] == 2021

    # Re-materializing January with A-1 still in that discovery month but no
    # longer exposing facts deletes A-1's facts only; February's G-1 survives.
    _seed_filings_index(
        tmp_path,
        [
            _record(fxo_id="A-1", period_end="2022-12-31", json_url=None),
            _record(
                fxo_id="G-1",
                period_end="2021-12-31",
                processed_at="2023-02-02 00:00:00",
                json_url=_facts_json_url("G-1"),
            ),
        ],
    )
    empty_metadata = _run_facts_partition(
        tmp_path, object_store, _StubFactsDownloadClient({}), partition_key="2023-01-01"
    )
    assert empty_metadata["fact_row_count"] == 0

    rows_after = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows_after} == {"G-1"}


# ==========================================================================
# _replace_facts_for_filings: bounded Parquet insert path. These tests call
# the writer directly with synthetic rows spooled through the production
# Arrow/Parquet helper, bypassing the already-covered OIM parsing boundary.
# A small monkeypatched batch size exercises multiple Parquet scans and
# transactions without creating a large fixture.
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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
            parser_contract=assets.OIM_FACTS_PARSER_CONTRACT,
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


def test_facts_partition_skips_404_increments_counter_no_s3_no_facts_row(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(
                fxo_id="B-1", period_end="2022-06-30", json_url=_facts_json_url("B-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"B-1": _SMALL_PAYLOAD_BYTES},
        http_error_status_by_fxo_id={"A-1": 404},
    )
    warnings: list[tuple[Any, ...]] = []

    metadata = _run_facts_partition(
        tmp_path,
        object_store,
        client,
        partition_key="2023-01-01",
        log_warning=lambda *a, **k: warnings.append(a),
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["raw_skipped_upstream_missing"] == 1
    assert metadata["raw_downloaded_count"] == 1  # B-1 only
    assert metadata["skipped_missing_raw_object"] == 1
    assert metadata["fact_row_count"] == 2  # only B-1's small payload facts
    # Both filings were attempted (the 404 didn't abort the loop early).
    assert client.download_calls == [
        _facts_json_url("A-1"),
        _facts_json_url("B-1"),
    ]
    # The independent raw and parsed assets each report their own missing
    # boundary: upstream 404 first, then absent S3 input for the parser.
    assert len(warnings) == 2

    # A-1's dead upstream link left no S3 object and no facts row.
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        assets._fact_json_object_key("A-1"),
    ) not in object_store.objects
    assert object_store.uploaded_keys == [
        (assets.ESEF_FILINGS_FACTS_BUCKET, assets._fact_json_object_key("B-1"))
    ]
    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"B-1"}


def test_facts_partition_skips_410_gone_same_as_404(tmp_path: Path) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            )
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient({}, http_error_status_by_fxo_id={"A-1": 410})

    metadata = _run_facts_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["raw_skipped_upstream_missing"] == 1
    assert metadata["raw_downloaded_count"] == 0
    assert metadata["skipped_missing_raw_object"] == 1
    assert metadata["fact_row_count"] == 0
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        assets._fact_json_object_key("A-1"),
    ) not in object_store.objects


def test_facts_partition_5xx_http_error_propagates_and_fails_partition_loudly(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            )
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient({}, http_error_status_by_fxo_id={"A-1": 500})

    with pytest.raises(assets.dlt_requests.HTTPError):
        _run_facts_partition(tmp_path, object_store, client, partition_key="2023-01-01")

    assert client.download_calls == [_facts_json_url("A-1")]
    # No phantom S3 object for the filing whose download 500'd.
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        assets._fact_json_object_key("A-1"),
    ) not in object_store.objects


# ==========================================================================
# Download progress logging (Issue 2, live-operation feedback): a multi-hour
# partition run must log a heartbeat periodically via `log_info`, not just
# print "N filings in scope" up front and go silent until the run's final
# materialization metadata.
# ==========================================================================


def test_facts_partition_logs_progress_every_100_processed_filings(
    tmp_path: Path,
) -> None:
    """105 stub filings -- 95 with archived JSON, 10
    without (skipped_no_json) -- deterministically split the periodic
    100-filing-boundary log from the completion log (105 is not itself a
    multiple of 100, so these are genuinely two distinct log lines), and the
    zero-padded `F-000`..`F-104` fxo_ids sort (and are therefore processed)
    in the same order as the two groups are listed below. Numbers asserted
    below come from the loop's real counters, not hardcoded placeholders.
    """
    downloaded_fxo_ids = [f"F-{i:03d}" for i in range(95)]
    skipped_fxo_ids = [f"F-{i:03d}" for i in range(95, 105)]
    records = [
        _record(
            fxo_id=fxo_id, period_end="2022-12-31", json_url=_facts_json_url(fxo_id)
        )
        for fxo_id in downloaded_fxo_ids
    ] + [
        _record(fxo_id=fxo_id, period_end="2022-12-31", json_url=None)
        for fxo_id in skipped_fxo_ids
    ]
    _seed_filings_index(tmp_path, records)

    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {fxo_id: _SMALL_PAYLOAD_BYTES for fxo_id in downloaded_fxo_ids}
    )
    log_calls: list[tuple[Any, ...]] = []

    assets.run_esef_filing_facts_json_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        client=client,
        partition_key="2023-01-01",
        log_info=lambda *a, **k: None,
        log_warning=lambda *a, **k: None,
    )
    assets.run_esef_filing_facts_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        partition_key="2023-01-01",
        source_run_id="run-progress",
        log_info=lambda *a, **k: log_calls.append(a),
        log_warning=lambda *a, **k: None,
    )

    progress_calls = [call for call in log_calls if "filings processed" in call[0]]
    assert len(progress_calls) == 2

    _, key_1, processed_1, total_1, s3_read_1, checkpointed_1, skipped_1 = (
        progress_calls[0]
    )
    assert (key_1, processed_1, total_1, s3_read_1, checkpointed_1, skipped_1) == (
        "2023-01-01",
        100,
        105,
        95,
        0,
        5,
    )

    _, key_2, processed_2, total_2, s3_read_2, checkpointed_2, skipped_2 = (
        progress_calls[1]
    )
    assert (key_2, processed_2, total_2, s3_read_2, checkpointed_2, skipped_2) == (
        "2023-01-01",
        105,
        105,
        95,
        0,
        10,
    )


# ==========================================================================
# esef_report_xhtml_s3: processed-week report XHTML archive to S3
#
# `run_esef_report_xhtml_partition` (the plain function the asset delegates
# to) is called DIRECTLY here, for the same reason as
# `run_esef_filing_facts_partition` above -- see that section's docstring.
# Unlike the facts asset, this one never writes DuckDB at all: the local
# index is read once, read-only, purely to resolve which filings are in
# scope for the processed-week partition; the archive itself is pure S3 I/O.
# ==========================================================================


def _report_url(fxo_id: str) -> str:
    return f"https://filings.xbrl.org/{fxo_id}/report.html"


class _StubReportXhtmlDownloadClient:
    """Stand-in for EsefFilingsClient() -- serves canned XHTML bytes keyed by
    fxo_id (parsed back out of the report_url path `.../<fxo_id>/report.html`),
    or raises an HTTPError with the given status code for fxo_ids in
    `http_error_status_by_fxo_id` (simulating filings.xbrl.org's dead
    upstream links -- 404/410 -- or any other HTTP error)."""

    def __init__(
        self,
        body_by_fxo_id: dict[str, bytes],
        *,
        http_error_status_by_fxo_id: dict[str, int] | None = None,
    ) -> None:
        self._body_by_fxo_id = body_by_fxo_id
        self._http_error_status_by_fxo_id = http_error_status_by_fxo_id or {}
        self.download_calls: list[str] = []

    def download_json_facts(self, url: str, target: Path, **_: Any) -> None:
        self.download_calls.append(url)
        fxo_id = url.split("/")[-2]
        if fxo_id in self._http_error_status_by_fxo_id:
            raise _http_error(self._http_error_status_by_fxo_id[fxo_id])
        target.write_bytes(self._body_by_fxo_id[fxo_id])


def test_archive_report_xhtml_removes_local_file_after_upload(tmp_path: Path) -> None:
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient({"A-1": b"<html>A-1 report</html>"})

    downloaded = assets._archive_filing_report_xhtml(
        client=client,
        object_store=object_store,
        temp_dir=tmp_path,
        fxo_id="A-1",
        report_url=_report_url("A-1"),
    )

    assert downloaded is True
    assert list(tmp_path.iterdir()) == []
    assert (
        object_store.objects[
            (
                assets.ESEF_FILINGS_FACTS_BUCKET,
                "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
            )
        ]
        == b"<html>A-1 report</html>"
    )


def _run_report_xhtml_partition(
    tmp_path: Path,
    object_store: FakeObjectStore,
    client: _StubReportXhtmlDownloadClient,
    *,
    partition_key: str = "2023-01-01",
    log_warning: Any = lambda *a, **k: None,
) -> dict[str, int | str]:
    return assets.run_esef_report_xhtml_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        client=client,
        partition_key=partition_key,
        log_info=lambda *a, **k: None,
        log_warning=log_warning,
    )


def test_report_xhtml_asset_wiring_partitions_deps_backfill_policy_and_pool() -> None:
    asset_def = assets.esef_report_xhtml_s3
    assert asset_def.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS
    dep_keys = {dep.asset_key for spec in asset_def.specs for dep in spec.deps}
    assert dg.AssetKey("esef_filings_index_duckdb") in dep_keys
    assert asset_def.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert asset_def.op.pool == assets.ESEF_FILINGS_DUCKDB_POOL


def test_report_xhtml_partition_archives_filings_discovered_in_processed_week(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1")
            ),
            _record(
                fxo_id="B-1", period_end="2022-06-30", report_url=_report_url("B-1")
            ),
            # Discovered in a later week, irrespective of its fiscal year.
            _record(
                fxo_id="G-1",
                period_end="2021-12-31",
                processed_at="2023-02-02 00:00:00",
                report_url=_report_url("G-1"),
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {"A-1": b"<html>A-1 report</html>", "B-1": b"<html>B-1 report</html>"}
    )

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata == {
        "filings_in_scope": 2,
        "downloaded_count": 2,
        "reused_count": 0,
        "skipped_no_report_url": 0,
        "skipped_upstream_missing": 0,
        "partition_key": "2023-01-01",
        "processed_window_start": "2023-01-01 00:00:00",
        "processed_window_end": "2023-01-08 00:00:00",
    }
    assert object_store.created_buckets == [assets.ESEF_FILINGS_FACTS_BUCKET]

    # G-1 was discovered in February and is not attempted by January.
    assert sorted(client.download_calls) == [
        _report_url("A-1"),
        _report_url("B-1"),
    ]
    assert (
        object_store.objects[
            (
                assets.ESEF_FILINGS_FACTS_BUCKET,
                "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
            )
        ]
        == b"<html>A-1 report</html>"
    )
    assert (
        object_store.objects[
            (
                assets.ESEF_FILINGS_FACTS_BUCKET,
                "esef_filings/report_xhtml/fxo_id=B-1/report.xhtml",
            )
        ]
        == b"<html>B-1 report</html>"
    )


def test_report_xhtml_second_run_skips_existing_object_without_downloading(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [_record(fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1"))],
    )
    object_store = FakeObjectStore()

    first_metadata = _run_report_xhtml_partition(
        tmp_path,
        object_store,
        _StubReportXhtmlDownloadClient({"A-1": b"<html>A-1 report</html>"}),
        partition_key="2023-01-01",
    )
    assert first_metadata["downloaded_count"] == 1
    assert first_metadata["reused_count"] == 0

    # New client instance for the second run -- if download_json_facts were
    # called again it would KeyError (no payload registered), proving reuse.
    second_client = _StubReportXhtmlDownloadClient({})
    second_metadata = _run_report_xhtml_partition(
        tmp_path, object_store, second_client, partition_key="2023-01-01"
    )
    assert second_metadata["downloaded_count"] == 0
    assert second_metadata["reused_count"] == 1
    assert second_client.download_calls == []


def test_report_xhtml_filing_without_report_url_increments_skipped_no_report_url(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(fxo_id="A-1", period_end="2022-12-31", report_url=None),
            _record(
                fxo_id="B-1", period_end="2022-06-30", report_url=_report_url("B-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient({"B-1": b"<html>B-1 report</html>"})

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["skipped_no_report_url"] == 1
    assert metadata["downloaded_count"] == 1
    assert client.download_calls == [_report_url("B-1")]


def test_report_xhtml_does_not_require_a_valid_fiscal_period_end(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1")
            ),
            _record(fxo_id="D-1", period_end=None, report_url=_report_url("D-1")),
            _record(
                fxo_id="E-1", period_end="2030-01-01", report_url=_report_url("E-1")
            ),
            _record(fxo_id="F-1", period_end="garbage", report_url=_report_url("F-1")),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {
            fxo_id: f"<html>{fxo_id} report</html>".encode()
            for fxo_id in ("A-1", "D-1", "E-1", "F-1")
        }
    )

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["filings_in_scope"] == 4
    assert metadata["downloaded_count"] == 4
    assert client.download_calls == [
        _report_url("A-1"),
        _report_url("D-1"),
        _report_url("E-1"),
        _report_url("F-1"),
    ]


def test_report_xhtml_partition_never_opens_a_writable_duckdb_connection(
    tmp_path: Path,
) -> None:
    """This asset only READS DuckDB -- confirm no `facts`/other table gets
    created as a side effect of running it (a writable `get_connection()`
    call would create the dataset schema)."""
    _seed_filings_index(
        tmp_path,
        [_record(fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1"))],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient({"A-1": b"<html>A-1 report</html>"})

    _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    with read_only_duckdb_connection(_db_resource(tmp_path)) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'esef_filings'"
            ).fetchall()
        }
    assert tables == {"filings_index"}


class _FailingReportXhtmlDownloadClient:
    """Stand-in for EsefFilingsClient() that raises when asked to download a
    specific fxo_id's report -- used to prove a mid-partition download
    failure fails the partition loudly (propagates, no swallowing) and
    leaves no phantom S3 object for the filing that failed."""

    def __init__(
        self, body_by_fxo_id: dict[str, bytes], *, fail_for_fxo_id: str
    ) -> None:
        self._body_by_fxo_id = body_by_fxo_id
        self._fail_for_fxo_id = fail_for_fxo_id
        self.download_calls: list[str] = []

    def download_json_facts(self, url: str, target: Path, **_: Any) -> None:
        self.download_calls.append(url)
        fxo_id = url.split("/")[-2]
        if fxo_id == self._fail_for_fxo_id:
            raise RuntimeError(f"simulated download failure for {fxo_id}")
        target.write_bytes(self._body_by_fxo_id[fxo_id])


def test_report_xhtml_mid_partition_download_failure_propagates_with_no_phantom_object(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1")
            ),
            _record(
                fxo_id="B-1", period_end="2022-06-30", report_url=_report_url("B-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    # Rows are processed in fxo_id order (A-1 then B-1); B-1's download fails.
    client = _FailingReportXhtmlDownloadClient(
        {"A-1": b"<html>A-1 report</html>"}, fail_for_fxo_id="B-1"
    )

    with pytest.raises(RuntimeError, match="simulated download failure for B-1"):
        _run_report_xhtml_partition(
            tmp_path, object_store, client, partition_key="2023-01-01"
        )

    assert client.download_calls == [_report_url("A-1"), _report_url("B-1")]

    # A-1 (processed first) made it into the object store...
    assert (
        object_store.objects[
            (
                assets.ESEF_FILINGS_FACTS_BUCKET,
                "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
            )
        ]
        == b"<html>A-1 report</html>"
    )
    # ...but B-1, whose download raised, left no object at all -- no phantom
    # upload for a filing whose download never completed.
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        "esef_filings/report_xhtml/fxo_id=B-1/report.xhtml",
    ) not in object_store.objects
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        "esef_filings/report_xhtml/fxo_id=B-1/report.xhtml",
    ) not in object_store.uploaded_keys


# --------------------------------------------------------------------------
# Dead upstream links -- same guard as the facts asset above (see that
# section's header comment); mirrored here for esef_report_xhtml_s3.
# --------------------------------------------------------------------------


def test_report_xhtml_partition_skips_404_increments_counter_no_s3_object(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1")
            ),
            _record(
                fxo_id="B-1", period_end="2022-06-30", report_url=_report_url("B-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {"B-1": b"<html>B-1 report</html>"},
        http_error_status_by_fxo_id={"A-1": 404},
    )
    warnings: list[tuple[Any, ...]] = []

    metadata = _run_report_xhtml_partition(
        tmp_path,
        object_store,
        client,
        partition_key="2023-01-01",
        log_warning=lambda *a, **k: warnings.append(a),
    )

    assert metadata == {
        "filings_in_scope": 2,
        "downloaded_count": 1,
        "reused_count": 0,
        "skipped_no_report_url": 0,
        "skipped_upstream_missing": 1,
        "partition_key": "2023-01-01",
        "processed_window_start": "2023-01-01 00:00:00",
        "processed_window_end": "2023-01-08 00:00:00",
    }
    # Both filings were attempted (the 404 didn't abort the loop early).
    assert client.download_calls == [_report_url("A-1"), _report_url("B-1")]
    assert len(warnings) == 1

    # A-1's dead upstream link left no S3 object at all.
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
    ) not in object_store.objects
    assert (
        object_store.objects[
            (
                assets.ESEF_FILINGS_FACTS_BUCKET,
                "esef_filings/report_xhtml/fxo_id=B-1/report.xhtml",
            )
        ]
        == b"<html>B-1 report</html>"
    )


def test_report_xhtml_partition_skips_410_gone_same_as_404(tmp_path: Path) -> None:
    _seed_filings_index(
        tmp_path,
        [_record(fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1"))],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {}, http_error_status_by_fxo_id={"A-1": 410}
    )

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_key="2023-01-01"
    )

    assert metadata["skipped_upstream_missing"] == 1
    assert metadata["downloaded_count"] == 0
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
    ) not in object_store.objects


def test_report_xhtml_partition_5xx_http_error_propagates_and_fails_partition_loudly(
    tmp_path: Path,
) -> None:
    _seed_filings_index(
        tmp_path,
        [_record(fxo_id="A-1", period_end="2022-12-31", report_url=_report_url("A-1"))],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {}, http_error_status_by_fxo_id={"A-1": 500}
    )

    with pytest.raises(assets.dlt_requests.HTTPError):
        _run_report_xhtml_partition(
            tmp_path, object_store, client, partition_key="2023-01-01"
        )

    assert client.download_calls == [_report_url("A-1")]
    assert (
        assets.ESEF_FILINGS_FACTS_BUCKET,
        "esef_filings/report_xhtml/fxo_id=A-1/report.xhtml",
    ) not in object_store.objects


# --- Jobs + schedule (Task 7) ------------------------------------------------
#
# Mirrors the job/schedule contract-test style used elsewhere (e.g.
# tests/test_wikidata_assets.py::test_wikidata_weekly_refresh_job_and_schedule_are_registered,
# tests/test_estonia_ar_assets.py::test_schedules_registered_and_jobs_cover_full_chains):
# load the full project Definitions, resolve jobs/schedules by name through
# the RepositoryDefinition, and assert on their resolved shape (asset
# selection, partitions_def, cron/timezone/status).


def test_esef_filings_refresh_job_selects_weekly_ingest_evidence_and_exports() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_refresh_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert asset_keys == {
        "esef_filings_index_duckdb",
        "esef_filing_facts_duckdb",
        "esef_report_xhtml_s3",
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_entity_registry_map_clickhouse",
        "esef_financial_metrics_clickhouse",
        "esef_document_extraction_manifest_s3",
        "esef_document_artifacts_s3",
        "esef_source_documents_duckdb",
        "esef_document_contact_candidates_duckdb",
        "esef_document_concept_labels_duckdb",
        "esef_fact_disclosure_inputs_s3",
        "esef_fact_disclosure_artifacts_s3",
        "esef_fact_disclosures_duckdb",
        "esef_source_documents_clickhouse",
        "esef_document_contact_candidates_clickhouse",
        "esef_document_concept_labels_clickhouse",
        "esef_document_concept_official_translations_clickhouse",
        "esef_document_concept_translation_load",
        "esef_fact_disclosures_clickhouse",
        "esef_company_source_records_clickhouse",
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

    # Exports excluded from the backfill job -- run the refresh job (or the
    # individual export assets) once after all backfill partitions land.
    assert asset_keys == {
        "esef_filings_index_duckdb",
        "esef_filing_facts_duckdb",
        "esef_report_xhtml_s3",
        "esef_document_extraction_manifest_s3",
        "esef_document_artifacts_s3",
        "esef_source_documents_duckdb",
        "esef_document_contact_candidates_duckdb",
        "esef_document_concept_labels_duckdb",
        "esef_fact_disclosure_inputs_s3",
        "esef_fact_disclosure_artifacts_s3",
        "esef_fact_disclosures_duckdb",
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


def test_esef_filings_reconciliation_job_and_schedule_are_registered() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_reconciliation_job")
    schedule = repo.get_schedule_def("esef_filings_reconciliation_monthly")

    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == {
        "esef_filings_index_reconciliation_duckdb"
    }
    assert job.partitions_def is None
    assert schedule.job_name == "esef_filings_reconciliation_job"
    assert schedule.cron_schedule == "25 5 2 * *"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


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


class _MixedJobEsefFilingsClient:
    """Stand-in for EsefFilingsClient() supporting both `iter_filings()`
    (used by esef_filings_index_duckdb) and `download_json_facts()` (used by
    the report-XHTML archive). The facts asset reads a preseeded Arelle
    artifact in this focused ingest-job test. `_StubEsefFilingsClient` above
    only implements `iter_filings` -- every other test in this file only
    exercises the index asset in isolation."""

    def __init__(self, records: list[EsefFilingRecord]) -> None:
        self._records = records
        self.last_reported_total: int | None = None

    def iter_filings(self, **_: Any) -> Iterator[EsefFilingRecord]:
        yield from self._records

    def download_json_facts(self, url: str, target: Path) -> None:
        target.write_text(
            json.dumps(
                {
                    "facts": {
                        "f1": {
                            "value": "1000",
                            "decimals": -3,
                            "dimensions": {
                                "concept": "ifrs-full:Revenue",
                                "period": "2026-01-01T00:00:00/2026-12-31T00:00:00",
                                "unit": "iso4217:EUR",
                            },
                        }
                    }
                }
            )
        )


class _MixedJobFakeClickHouseClient:
    """Records every statement; answers just enough to drive every
    ClickHouse-touching asset in esef_filings_refresh_job in one run (the
    filings/facts row-batch export, and the entity-map + metrics
    ClickHouse-native stage/INSERT-SELECT/EXCHANGE paths) -- merges the
    statement shapes tests/test_esef_filings_publish.py's and
    tests/test_esef_filings_metrics.py's same-purpose fakes handle
    separately, since this single job run exercises both."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        stripped = sql.strip()
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if isinstance(params, dict) else ()
            return [(table,) for table in requested]
        if stripped.startswith("SELECT count() FROM ("):
            # Pre-stage refuse-on-empty check (publish.py's
            # replace_esef_entity_registry_map_clickhouse still runs this
            # bare "SELECT count() FROM (select_sql)" precheck) -- must stay
            # > 0 so the export doesn't raise refuse-to-replace-on-empty.
            return [(1,)]
        if stripped.startswith("CREATE TABLE"):
            return []
        if stripped.startswith("INSERT INTO") and "SELECT" in stripped:
            return []
        if stripped.startswith("INSERT INTO"):
            return []
        if stripped.startswith("EXCHANGE TABLES"):
            return []
        if stripped.startswith("SELECT count() FROM") and "_tmp_" in stripped:
            # metrics.py's post-stage staged_row_count read (Task 1.2: the
            # SELECT now runs once, via the staged INSERT ... SELECT, and
            # the refuse-on-empty check reads this stage-table count right
            # after -- must stay > 0 so the metrics export doesn't raise
            # refuse-to-replace-on-empty).
            return [(1,)]
        if stripped.startswith("SELECT count() FROM"):
            # Sentinel-exclusion count, plus the shrink guard's existing-
            # table row-count read -- 0 trivially satisfies
            # guard_against_clickhouse_table_shrink (existing_row_count <= 0
            # short-circuits its ratio check, so the staged count of 1 above
            # can never look like a shrink).
            return [(0,)]
        if stripped.startswith("DROP TABLE"):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def test_refresh_job_executes_all_assets_for_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _record(
        fxo_id="A-1",
        period_end="2021-12-31",
        processed_at="2026-07-21 18:21:29.311103",
    )
    monkeypatch.setattr(
        assets, "EsefFilingsClient", lambda: _MixedJobEsefFilingsClient([record])
    )

    def fake_run_esef_artifact_facts_partition(
        *,
        esef_filings_duckdb: DuckDBResource,
        partition_key: str,
        source_run_id: str,
        **_: Any,
    ) -> dict[str, int | str]:
        fact_row = list(
            _synthetic_fact_row(
                fact_id="f1",
                period_end_year=2021,
                source_run_id=source_run_id,
            )
        )
        fact_row[0] = record.lei
        fact_row[1] = record.fxo_id
        with esef_filings_duckdb.get_connection() as connection:
            connection.execute(
                f"create schema if not exists {assets.tables.DLT_DATASET_NAME}"
            )
            connection.execute(
                f"create table if not exists {assets.QUALIFIED_FACTS_TABLE} "
                f"({assets._FACTS_COLUMNS_SQL})"
            )
            placeholders = ", ".join("?" for _value in fact_row)
            connection.execute(
                f"insert into {assets.QUALIFIED_FACTS_TABLE} values "
                f"({placeholders})",
                fact_row,
            )
        return {
            "filings_in_scope": 1,
            "partition_key": partition_key,
            "inserted_fact_row_count": 1,
            "fact_row_count": 1,
        }

    monkeypatch.setattr(
        assets,
        "run_esef_artifact_facts_partition",
        fake_run_esef_artifact_facts_partition,
    )

    ch_client = _MixedJobFakeClickHouseClient()
    stored_objects: dict[tuple[str | None, str], bytes] = {}

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_MixedJobFakeClickHouseClient]:
        yield ch_client

    def fake_ensure_bucket(
        self: ObjectStoreResource, bucket: str | None = None
    ) -> None:
        return None

    def fake_exists(
        self: ObjectStoreResource, key: str, bucket: str | None = None
    ) -> bool:
        return (bucket, key) in stored_objects

    def fake_upload_file(
        self: ObjectStoreResource,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        stored_objects[(bucket, key)] = Path(source_path).read_bytes()

    def fake_download_file(
        self: ObjectStoreResource,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        Path(target_path).write_bytes(stored_objects[(bucket, key)])

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    monkeypatch.setattr(ObjectStoreResource, "ensure_bucket", fake_ensure_bucket)
    monkeypatch.setattr(ObjectStoreResource, "exists", fake_exists)
    monkeypatch.setattr(ObjectStoreResource, "upload_file", fake_upload_file)
    monkeypatch.setattr(ObjectStoreResource, "download_file", fake_download_file)

    object_store = ObjectStoreResource(
        bucket="test-bucket",
        endpoint_url="http://unused",
        access_key="unused",
        secret_key="unused",
    )
    clickhouse = ClickhouseResource(host="unused")

    local_ingest_job = dg.define_asset_job(
        "test_esef_filings_ingest_job",
        selection=assets.ESEF_FILINGS_INGEST_SELECTION,
    )
    local_defs = dg.Definitions(
        assets=[
            assets.esef_filings_index_duckdb,
            assets.esef_filing_facts_json_s3,
            assets.esef_filing_facts_duckdb,
            assets.esef_report_xhtml_s3,
            assets.esef_filings_clickhouse,
            assets.esef_facts_clickhouse,
            assets.esef_entity_registry_map_clickhouse,
            assets.esef_financial_metrics_clickhouse,
        ],
        jobs=[local_ingest_job],
        resources={
            "esef_filings_duckdb": _db_resource(tmp_path),
            "object_store": object_store,
            "clickhouse": clickhouse,
        },
    )
    job = local_defs.get_repository_def().get_job("test_esef_filings_ingest_job")

    result = job.execute_in_process(partition_key="2026-07-19")

    assert result.success
    materialized_keys = {
        event.event_specific_data.materialization.asset_key.path[-1]
        for event in result.all_events
        if event.event_type_value == "ASSET_MATERIALIZATION"
    }
    assert materialized_keys == {
        "esef_filings_index_duckdb",
        "esef_filing_facts_duckdb",
        "esef_report_xhtml_s3",
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_entity_registry_map_clickhouse",
        "esef_financial_metrics_clickhouse",
    }

    # A 2021 fiscal filing discovered in July 2026 proves scoping follows the
    # source processed timestamp rather than fiscal period_end.
    facts_metadata = result.asset_materializations_for_node("esef_filing_facts_duckdb")[
        -1
    ].metadata
    assert facts_metadata["filings_in_scope"].value == 1
    assert facts_metadata["partition_key"].value == "2026-07-19"

    xhtml_metadata = result.asset_materializations_for_node("esef_report_xhtml_s3")[
        -1
    ].metadata
    assert xhtml_metadata["filings_in_scope"].value == 1
    assert xhtml_metadata["partition_key"].value == "2026-07-19"

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1"]


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
