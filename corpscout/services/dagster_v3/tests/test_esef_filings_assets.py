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
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS
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

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
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
    # No last_reported_total from the (stubbed) client -- nothing to report.
    assert metadata["api_reported_total"].value is None

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


# --------------------------------------------------------------------------
# Crawl-completeness guard (Finding M1): a nonzero crawl that's still far
# short of the API's own reported total (`meta.count`) must be refused, the
# same "before touching the existing table" discipline as the empty-crawl
# guard above.
# --------------------------------------------------------------------------


def test_crawl_completeness_guard_refuses_below_90_percent_and_preserves_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert first.success

    # 2 crawled out of a reported 1000 -- nowhere near the 90% floor.
    _patch_client(
        monkeypatch,
        [_record(fxo_id="C-1"), _record(fxo_id="C-2")],
        last_reported_total=1000,
    )
    with pytest.raises(ValueError, match="below 90%"):
        dg.materialize(
            [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
        )

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

    result = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )

    assert result.success
    metadata = result.asset_materializations_for_node("esef_filings_index_duckdb")[
        0
    ].metadata
    assert metadata["row_count"].value == 9
    assert metadata["api_reported_total"].value == 10


def test_crawl_completeness_guard_is_a_noop_when_api_total_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Only 1 filing crawled, no reported total available at all (the
    # default -- matches a real client whose first page had no meta.count)
    # -- nothing to compare against, so the guard must not fire.
    _patch_client(monkeypatch, [_record(fxo_id="A-1")], last_reported_total=None)

    result = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )

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


# --- Jobs + schedule (Task 7) ------------------------------------------------
#
# Mirrors the job/schedule contract-test style used elsewhere (e.g.
# tests/test_wikidata_assets.py::test_wikidata_weekly_refresh_job_and_schedule_are_registered,
# tests/test_estonia_ar_assets.py::test_schedules_registered_and_jobs_cover_full_chains):
# load the full project Definitions, resolve jobs/schedules by name through
# the RepositoryDefinition, and assert on their resolved shape (asset
# selection, partitions_def, cron/timezone/status).


def test_esef_filings_refresh_job_selects_index_partitioned_current_year_and_exports() -> (
    None
):
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
    }


def test_esef_filings_refresh_job_partitions_def_is_the_shared_year_partitions() -> (
    None
):
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_refresh_job")

    # Only esef_filing_facts_duckdb + esef_report_xhtml_s3 are partitioned in
    # this selection; the job resolves a single shared partitions_def from
    # them (JobDefinition._get_partitions_def collects only the non-None
    # partitions_defs across the selection) even though the other 5 assets
    # in the job are unpartitioned.
    assert job.partitions_def is not None
    assert set(job.partitions_def.get_partition_keys()) == set(
        assets.ESEF_FILING_FACTS_PARTITIONS.get_partition_keys()
    )


def test_esef_filings_backfill_job_selects_partitioned_assets_only() -> None:
    repo = load_project_defs().get_repository_def()
    job = repo.get_job("esef_filings_backfill_job")
    asset_keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    # Exports excluded from the backfill job -- run the refresh job (or the
    # individual export assets) once after all backfill partitions land.
    assert asset_keys == {"esef_filing_facts_duckdb", "esef_report_xhtml_s3"}
    assert set(job.partitions_def.get_partition_keys()) == set(
        assets.ESEF_FILING_FACTS_PARTITIONS.get_partition_keys()
    )


def test_esef_filings_refresh_weekly_schedule_registered() -> None:
    repo = load_project_defs().get_repository_def()
    schedule = repo.get_schedule_def("esef_filings_refresh_weekly")

    assert schedule.job_name == "esef_filings_refresh_job"
    assert schedule.cron_schedule == "10 5 * * 0"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_esef_filings_jobs_carry_the_heavy_bulk_workload_tag() -> None:
    repo = load_project_defs().get_repository_def()
    for job_name in ("esef_filings_refresh_job", "esef_filings_backfill_job"):
        job = repo.get_job(job_name)
        for key, value in HEAVY_BULK_RUN_TAGS.items():
            assert job.tags.get(key) == value, job_name


# ==========================================================================
# esef_filings_refresh_job: actually EXECUTING the mixed partitioned/
# unpartitioned selection, not just resolving its static shape (the tests
# above only inspect job.asset_layer/partitions_def -- they never run a
# single step). This is the one test in the whole module that proves the
# weekly schedule's RunRequest(partition_key=str(year)) genuinely drives all
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
    both year-partitioned assets, for the fact-JSON and, reused url/content
    agnostic, the report-XHTML download). `_StubEsefFilingsClient` above
    only implements `iter_filings` -- every other test in this file only
    exercises the index asset in isolation."""

    def __init__(self, records: list[EsefFilingRecord]) -> None:
        self._records = records
        self.last_reported_total: int | None = None

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
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
            # Pre-stage refuse-on-empty check (metrics.py / publish.py) --
            # must stay > 0 so neither export raises
            # refuse-to-replace-on-empty.
            return [(1,)]
        if stripped.startswith("CREATE TABLE"):
            return []
        if stripped.startswith("INSERT INTO") and "SELECT" in stripped:
            return []
        if stripped.startswith("INSERT INTO"):
            return []
        if stripped.startswith("EXCHANGE TABLES"):
            return []
        if stripped.startswith("SELECT count() FROM"):
            # Sentinel-exclusion count, plus the shrink guard's staged-vs-
            # existing row-count reads -- 0 on both sides trivially
            # satisfies guard_against_clickhouse_table_shrink
            # (existing_row_count <= 0 short-circuits its ratio check).
            return [(0,)]
        if stripped.startswith("DROP TABLE"):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def test_refresh_job_executes_all_assets_for_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _record(fxo_id="A-1", period_end="2026-06-30")
    monkeypatch.setattr(
        assets, "EsefFilingsClient", lambda: _MixedJobEsefFilingsClient([record])
    )

    ch_client = _MixedJobFakeClickHouseClient()

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
        return False

    def fake_upload_file(
        self: ObjectStoreResource,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        return None

    def fake_download_file(
        self: ObjectStoreResource,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        Path(target_path).write_text("{}")

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

    local_defs = dg.Definitions(
        assets=[
            assets.esef_filings_index_duckdb,
            assets.esef_filing_facts_duckdb,
            assets.esef_report_xhtml_s3,
            assets.esef_filings_clickhouse,
            assets.esef_facts_clickhouse,
            assets.esef_entity_registry_map_clickhouse,
            assets.esef_financial_metrics_clickhouse,
        ],
        jobs=[assets.esef_filings_refresh_job],
        resources={
            "esef_filings_duckdb": _db_resource(tmp_path),
            "object_store": object_store,
            "clickhouse": clickhouse,
        },
    )
    job = local_defs.get_repository_def().get_job("esef_filings_refresh_job")

    result = job.execute_in_process(partition_key="2026")

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

    # Partitioned assets scoped to 2026 -- proves the partition_key genuinely
    # gated the two year-partitioned assets onto the 2026-scoped filing,
    # rather than them silently running unpartitioned/unscoped.
    facts_metadata = result.asset_materializations_for_node("esef_filing_facts_duckdb")[
        -1
    ].metadata
    assert facts_metadata["filings_in_scope"].value == 1
    assert facts_metadata["skipped_out_of_range"].value == 0

    xhtml_metadata = result.asset_materializations_for_node("esef_report_xhtml_s3")[
        -1
    ].metadata
    assert xhtml_metadata["filings_in_scope"].value == 1
    assert xhtml_metadata["skipped_out_of_range"].value == 0

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1"]


class _FakeScheduleEvaluationContext:
    """Duck-typed stand-in for dg.ScheduleEvaluationContext -- the resolver
    only reads `.scheduled_execution_time` (see
    assets._esef_filings_refresh_run_request's docstring)."""

    def __init__(self, scheduled_execution_time: datetime | None) -> None:
        self.scheduled_execution_time = scheduled_execution_time


def test_refresh_run_request_resolves_current_year_partition_from_scheduled_time() -> (
    None
):
    scheduled_time = datetime(2026, 7, 19, 5, 50, tzinfo=ZoneInfo("UTC"))
    result = assets._esef_filings_refresh_run_request(
        _FakeScheduleEvaluationContext(scheduled_time)
    )
    assert isinstance(result, dg.RunRequest)
    # 2026-07-19 UTC 05:50 is still 2026-07-19 in Europe/Belgrade (UTC+2) --
    # same calendar year either way, so this also proves the resolver
    # actually converts to ESEF_FILINGS_TIMEZONE rather than reading the UTC
    # year blindly (a same-year case can't distinguish that on its own, but
    # combined with the boundary test below it does).
    assert result.partition_key == "2026"


def test_refresh_run_request_uses_belgrade_timezone_across_a_utc_year_boundary() -> (
    None
):
    # 2025-12-31 23:30 UTC is already 2026-01-01 00:30 in Europe/Belgrade
    # (UTC+1 in winter) -- proves the resolver converts to
    # ESEF_FILINGS_TIMEZONE before taking `.year`, not the UTC year.
    scheduled_time = datetime(2025, 12, 31, 23, 30, tzinfo=ZoneInfo("UTC"))
    result = assets._esef_filings_refresh_run_request(
        _FakeScheduleEvaluationContext(scheduled_time)
    )
    assert isinstance(result, dg.RunRequest)
    assert result.partition_key == "2026"


def test_refresh_run_request_skips_when_scheduled_year_has_no_partition() -> None:
    scheduled_time = datetime(2030, 1, 1, tzinfo=ZoneInfo("UTC"))
    result = assets._esef_filings_refresh_run_request(
        _FakeScheduleEvaluationContext(scheduled_time)
    )
    assert isinstance(result, dg.SkipReason)


def test_refresh_run_request_falls_back_to_now_when_no_scheduled_time() -> None:
    result = assets._esef_filings_refresh_run_request(
        _FakeScheduleEvaluationContext(None)
    )
    assert isinstance(result, dg.RunRequest)
    expected_year = str(datetime.now(tz=ZoneInfo(assets.ESEF_FILINGS_TIMEZONE)).year)
    assert result.partition_key == expected_year
