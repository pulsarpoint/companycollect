"""Tests for the ESEF filings index crawl asset and the year-partitioned
fact download/parse asset.

Index-crawl tests: no network -- EsefFilingsClient is monkeypatched on the
assets module to a stub returning canned EsefFilingRecord instances,
mirroring the class-monkeypatch pattern used for ExchangeRateClient in
tests/test_norway_brreg_financial_statement_assets.py. Each test materializes
into a fresh tmp_path DuckDB file via dg.materialize's resource override.

Facts-download tests: call `assets.run_esef_filing_facts_partition` (the
plain function the `esef_filing_facts_duckdb` asset delegates to) directly
with a duck-typed FakeObjectStore/stub client, rather than through
`dg.materialize` -- see the long comment at the top of that test section for
why (a real, confirmed Dagster resource-reconstruction behavior that drops
an injected fake S3 client).
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.esef_filings import assets
from dagster_v3.defs.esef_filings.client import EsefFilingRecord

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
) -> EsefFilingRecord:
    return EsefFilingRecord(
        lei=lei,
        entity_name=entity_name,
        fxo_id=fxo_id,
        country=country,
        period_end=period_end,
        date_added="2023-01-01 00:00:00",
        processed_at="2023-01-02 00:00:00",
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
    """Stand-in for EsefFilingsClient() -- no session, no network."""

    def __init__(self, records: list[EsefFilingRecord]) -> None:
        self._records = records

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
        yield from self._records


def _patch_client(
    monkeypatch: pytest.MonkeyPatch, records: list[EsefFilingRecord]
) -> None:
    monkeypatch.setattr(
        assets, "EsefFilingsClient", lambda: _StubEsefFilingsClient(records)
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

    result = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )

    assert result.success
    metadata = result.asset_materializations_for_node("esef_filings_index_duckdb")[
        0
    ].metadata
    assert metadata["row_count"].value == 3
    assert metadata["with_json_facts_count"].value == 2
    assert metadata["without_json_facts_count"].value == 1
    assert metadata["distinct_country_count"].value == 2
    assert metadata["country_distribution_top10"].value == {"SE": 2, "FI": 1}

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2", "A-3"]
    assert [row[1] for row in rows] == [True, False, True]
    assert all(row[2] == assets.ESEF_INDEX_URL for row in rows)
    assert all(row[3] == result.run_id for row in rows)


def test_second_materialization_full_replaces_no_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert first.success

    _patch_client(monkeypatch, [_record(fxo_id="B-1")])
    second = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert second.success

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["B-1"]


def test_empty_crawl_raises_value_error_and_preserves_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert first.success

    _patch_client(monkeypatch, [])
    with pytest.raises(ValueError, match="0 filings"):
        dg.materialize(
            [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
        )

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2"]


def test_filings_index_non_empty_check_passes_on_populated_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1")])

    result = dg.materialize(
        [assets.esef_filings_index_duckdb, assets.filings_index_non_empty],
        resources=_resources(tmp_path),
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
# esef_filing_facts_duckdb: year-partitioned download + OIM parse
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


class _StubFactsDownloadClient:
    """Stand-in for EsefFilingsClient() -- serves canned bytes keyed by
    fxo_id (parsed back out of the json_url path `.../<fxo_id>/facts.json`),
    or writes malformed text for fxo_ids in `malformed_fxo_ids`."""

    def __init__(
        self,
        payload_by_fxo_id: dict[str, bytes],
        *,
        malformed_fxo_ids: frozenset[str] = frozenset(),
    ) -> None:
        self._payload_by_fxo_id = payload_by_fxo_id
        self._malformed_fxo_ids = malformed_fxo_ids
        self.download_calls: list[str] = []

    def download_json_facts(self, json_url: str, target: Path, **_: Any) -> None:
        self.download_calls.append(json_url)
        fxo_id = json_url.split("/")[-2]
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
    partition_year: int,
    source_run_id: str = "run-1",
) -> dict[str, int]:
    return assets.run_esef_filing_facts_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        client=client,
        partition_year=partition_year,
        source_run_id=source_run_id,
        log_info=lambda *a, **k: None,
        log_warning=lambda *a, **k: None,
    )


def test_facts_asset_wiring_partitions_deps_backfill_policy_and_pool() -> None:
    asset_def = assets.esef_filing_facts_duckdb
    assert asset_def.partitions_def is not None
    assert sorted(asset_def.partitions_def.get_partition_keys()) == [
        str(year) for year in range(2019, 2028)
    ]
    dep_keys = {dep.asset_key for spec in asset_def.specs for dep in spec.deps}
    assert dg.AssetKey("esef_filings_index_duckdb") in dep_keys
    assert asset_def.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert asset_def.op.pool == assets.ESEF_FILINGS_DUCKDB_POOL


def test_facts_partition_downloads_and_parses_filings_in_scope_for_year(
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
            # Different year -- out of scope for the "2022" partition.
            _record(
                fxo_id="G-1", period_end="2021-12-31", json_url=_facts_json_url("G-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"A-1": _FIXTURE_FACTS_BYTES, "B-1": _SMALL_PAYLOAD_BYTES}
    )

    metadata = _run_facts_partition(
        tmp_path, object_store, client, partition_year=2022, source_run_id="run-A"
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["downloaded_count"] == 2
    assert metadata["reused_count"] == 0
    assert metadata["skipped_no_json"] == 0
    assert metadata["skipped_out_of_range"] == 0
    assert metadata["parse_failed_count"] == 0
    assert metadata["fact_row_count"] == 8  # 6 (fixture) + 2 (small payload)
    assert object_store.created_buckets == [assets.ESEF_FILINGS_FACTS_BUCKET]

    # G-1 (period_end 2021) was never even attempted.
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
        partition_year=2022,
        source_run_id="run-1",
    )
    assert first_metadata["downloaded_count"] == 1
    assert first_metadata["reused_count"] == 0
    assert first_metadata["fact_row_count"] == 6

    # New client instance for the second run -- if download_json_facts were
    # called again it would KeyError (no payload registered), proving reuse.
    second_client = _StubFactsDownloadClient({})
    second_metadata = _run_facts_partition(
        tmp_path,
        object_store,
        second_client,
        partition_year=2022,
        source_run_id="run-2",
    )
    assert second_metadata["downloaded_count"] == 0
    assert second_metadata["reused_count"] == 1
    assert second_metadata["fact_row_count"] == 6
    assert second_client.download_calls == []

    rows = _fetch_facts_rows(tmp_path)
    assert len(rows) == 6
    assert all(row[3] == "run-2" for row in rows)  # scoped replace, no dupes


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

    metadata = _run_facts_partition(tmp_path, object_store, client, partition_year=2022)

    assert metadata["filings_in_scope"] == 2
    assert metadata["skipped_no_json"] == 1
    assert metadata["downloaded_count"] == 1
    assert metadata["fact_row_count"] == 2
    assert client.download_calls == [_facts_json_url("B-1")]


def test_null_and_out_of_range_period_end_are_skipped_without_crashing(
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
                fxo_id="E-1", period_end="2030-01-01", json_url=_facts_json_url("E-1")
            ),
            _record(
                fxo_id="F-1", period_end="garbage", json_url=_facts_json_url("F-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient({"A-1": _FIXTURE_FACTS_BYTES})

    metadata = _run_facts_partition(tmp_path, object_store, client, partition_year=2022)

    assert metadata["filings_in_scope"] == 1
    assert metadata["skipped_out_of_range"] == 3
    assert metadata["fact_row_count"] == 6
    # None of the out-of-range filings were ever downloaded.
    assert client.download_calls == [_facts_json_url("A-1")]


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

    metadata = _run_facts_partition(tmp_path, object_store, client, partition_year=2022)

    assert metadata["filings_in_scope"] == 2
    assert metadata["parse_failed_count"] == 1
    assert metadata["downloaded_count"] == 2  # both bytes were fetched/uploaded
    assert metadata["fact_row_count"] == 6  # only A-1's facts landed

    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"A-1"}


def test_partition_scoped_replace_does_not_touch_other_years(tmp_path: Path) -> None:
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="A-1", period_end="2022-12-31", json_url=_facts_json_url("A-1")
            ),
            _record(
                fxo_id="G-1", period_end="2021-12-31", json_url=_facts_json_url("G-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubFactsDownloadClient(
        {"A-1": _FIXTURE_FACTS_BYTES, "G-1": _SMALL_PAYLOAD_BYTES}
    )

    _run_facts_partition(tmp_path, object_store, client, partition_year=2021)
    _run_facts_partition(tmp_path, object_store, client, partition_year=2022)

    rows = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows} == {"A-1", "G-1"}
    by_fxo = {row[0]: row[2] for row in rows}
    assert by_fxo["A-1"] == 2022
    assert by_fxo["G-1"] == 2021

    # Re-materializing "2022" with a filing set that no longer includes A-1
    # (index full-replace can never be empty, so keep G-1) must delete ONLY
    # period_end_year=2022 rows -- 2021's G-1 rows survive.
    _seed_filings_index(
        tmp_path,
        [
            _record(
                fxo_id="G-1", period_end="2021-12-31", json_url=_facts_json_url("G-1")
            )
        ],
    )
    empty_metadata = _run_facts_partition(
        tmp_path, object_store, _StubFactsDownloadClient({}), partition_year=2022
    )
    assert empty_metadata["fact_row_count"] == 0

    rows_after = _fetch_facts_rows(tmp_path)
    assert {row[0] for row in rows_after} == {"G-1"}


# ==========================================================================
# esef_report_xhtml_s3: year-partitioned report XHTML archive to S3
#
# `run_esef_report_xhtml_partition` (the plain function the asset delegates
# to) is called DIRECTLY here, for the same reason as
# `run_esef_filing_facts_partition` above -- see that section's docstring.
# Unlike the facts asset, this one never writes DuckDB at all: the local
# index is read once, read-only, purely to resolve which filings are in
# scope for the partition year; the archive itself is pure S3 I/O.
# ==========================================================================


def _report_url(fxo_id: str) -> str:
    return f"https://filings.xbrl.org/{fxo_id}/report.html"


class _StubReportXhtmlDownloadClient:
    """Stand-in for EsefFilingsClient() -- serves canned XHTML bytes keyed by
    fxo_id (parsed back out of the report_url path `.../<fxo_id>/report.html`).
    """

    def __init__(self, body_by_fxo_id: dict[str, bytes]) -> None:
        self._body_by_fxo_id = body_by_fxo_id
        self.download_calls: list[str] = []

    def download_json_facts(self, url: str, target: Path, **_: Any) -> None:
        self.download_calls.append(url)
        fxo_id = url.split("/")[-2]
        target.write_bytes(self._body_by_fxo_id[fxo_id])


def _run_report_xhtml_partition(
    tmp_path: Path,
    object_store: FakeObjectStore,
    client: _StubReportXhtmlDownloadClient,
    *,
    partition_year: int,
) -> dict[str, int]:
    return assets.run_esef_report_xhtml_partition(
        esef_filings_duckdb=_db_resource(tmp_path),
        object_store=object_store,
        client=client,
        partition_year=partition_year,
        log_info=lambda *a, **k: None,
    )


def test_report_xhtml_asset_wiring_partitions_deps_backfill_policy_and_pool() -> None:
    asset_def = assets.esef_report_xhtml_s3
    assert asset_def.partitions_def is not None
    assert sorted(asset_def.partitions_def.get_partition_keys()) == [
        str(year) for year in range(2019, 2028)
    ]
    dep_keys = {dep.asset_key for spec in asset_def.specs for dep in spec.deps}
    assert dg.AssetKey("esef_filings_index_duckdb") in dep_keys
    assert asset_def.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert asset_def.op.pool == assets.ESEF_FILINGS_DUCKDB_POOL


def test_report_xhtml_partition_archives_filings_in_scope_for_year(
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
            # Different year -- out of scope for the "2022" partition.
            _record(
                fxo_id="G-1", period_end="2021-12-31", report_url=_report_url("G-1")
            ),
        ],
    )
    object_store = FakeObjectStore()
    client = _StubReportXhtmlDownloadClient(
        {"A-1": b"<html>A-1 report</html>", "B-1": b"<html>B-1 report</html>"}
    )

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_year=2022
    )

    assert metadata == {
        "filings_in_scope": 2,
        "downloaded_count": 2,
        "reused_count": 0,
        "skipped_no_report_url": 0,
        "skipped_out_of_range": 0,
    }
    assert object_store.created_buckets == [assets.ESEF_FILINGS_FACTS_BUCKET]

    # G-1 (period_end 2021) was never even attempted.
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
        partition_year=2022,
    )
    assert first_metadata["downloaded_count"] == 1
    assert first_metadata["reused_count"] == 0

    # New client instance for the second run -- if download_json_facts were
    # called again it would KeyError (no payload registered), proving reuse.
    second_client = _StubReportXhtmlDownloadClient({})
    second_metadata = _run_report_xhtml_partition(
        tmp_path, object_store, second_client, partition_year=2022
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
        tmp_path, object_store, client, partition_year=2022
    )

    assert metadata["filings_in_scope"] == 2
    assert metadata["skipped_no_report_url"] == 1
    assert metadata["downloaded_count"] == 1
    assert client.download_calls == [_report_url("B-1")]


def test_report_xhtml_null_and_out_of_range_period_end_are_skipped_without_crashing(
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
    client = _StubReportXhtmlDownloadClient({"A-1": b"<html>A-1 report</html>"})

    metadata = _run_report_xhtml_partition(
        tmp_path, object_store, client, partition_year=2022
    )

    assert metadata["filings_in_scope"] == 1
    assert metadata["skipped_out_of_range"] == 3
    # None of the out-of-range filings were ever downloaded.
    assert client.download_calls == [_report_url("A-1")]


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

    _run_report_xhtml_partition(tmp_path, object_store, client, partition_year=2022)

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
        _run_report_xhtml_partition(tmp_path, object_store, client, partition_year=2022)

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
