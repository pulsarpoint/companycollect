import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
import requests
import whoisit
from pydantic import ValidationError
from whoisit.errors import (
    QueryError,
    RateLimitedError,
    ResourceDoesNotExist,
    UnsupportedError,
)

from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_PARTITIONS,
    commoncrawl_ip_bucket_index,
)
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_CANDIDATES_SQL,
    RDAP_LOOKUP_COLUMNS,
    RDAP_LOOKUP_INSERT_SQL,
    RDAP_NETWORK_COLUMNS,
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_COLUMNS,
    RDAP_SEGMENT_INSERT_SQL,
    CommoncrawlRdapConfig,
    _assert_rdap_storage_exists,
    _enrich_rdap_bucket,
    commoncrawl_ip_rdap_networks,
)
from dagster_v3.defs.commoncrawl_rdap.client import (
    RdapClient,
    RdapClientError,
    ip_resource_from_up_url,
)
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    RdapLookupResponse,
    normalize_rdap_network,
)


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_NAME = "000124_corpscout_rdap_networks"
DICTIONARY_READER_MIGRATION_NAME = "000126_corpscout_rdap_dictionary_reader"
FIXTURES_DIR = Path(__file__).parent / "fixtures" / "rdap"
FETCHED_AT = datetime(2026, 7, 12, 10, 30, tzinfo=UTC)


class FakeRdapReadClient:
    def __init__(self, rows: list[tuple[str, int]]) -> None:
        self.rows = rows
        self.query = ""
        self.params: dict[str, object] = {}
        self.settings: dict[str, object] = {}

    def execute_iter(
        self,
        query: str,
        params: dict[str, object],
        *,
        settings: dict[str, object],
    ):
        self.query = query
        self.params = params
        self.settings = settings
        return iter(self.rows)


class FakeRdapWriteClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, query: str, rows=None) -> None:
        self.inserts.append((query, list(rows or [])))


class FakeRdapClickhouseResource:
    def __init__(
        self,
        read_client: FakeRdapReadClient,
        write_client: FakeRdapWriteClient,
    ) -> None:
        self.clients = iter((read_client, write_client))

    @contextmanager
    def get_connection(self):
        yield next(self.clients)


class FakeStorageClient:
    def __init__(self, responses: list[list[tuple[object, ...]]]) -> None:
        self.responses = iter(responses)

    def execute(self, query: str, params=None):
        return next(self.responses)


class FakeStorageResource:
    def __init__(self, clients: list[FakeStorageClient]) -> None:
        self.clients = iter(clients)

    @contextmanager
    def get_connection(self):
        yield next(self.clients)


class FakeRdapClient:
    def __init__(
        self,
        direct_results: list[RdapLookupResponse | RdapClientError],
        parent_results: list[RdapLookupResponse | RdapClientError] | None = None,
    ) -> None:
        self.direct_results = iter(direct_results)
        self.parent_results = iter(parent_results or [])
        self.direct_queries: list[str] = []
        self.parent_queries: list[tuple[str, str]] = []

    def lookup_ip(self, query: str) -> RdapLookupResponse:
        self.direct_queries.append(query)
        result = next(self.direct_results)
        if isinstance(result, RdapClientError):
            raise result
        return result

    def lookup_up_url(self, up_url: str, *, rir: str) -> RdapLookupResponse:
        self.parent_queries.append((up_url, rir))
        result = next(self.parent_results)
        if isinstance(result, RdapClientError):
            raise result
        return result


def test_rdap_migration_creates_storage_views_and_ip_trie() -> None:
    migration = (MIGRATIONS_DIR / f"{MIGRATION_NAME}.up.sql").read_text(
        encoding="utf-8"
    )

    for table in (
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in migration

    assert "ENGINE = ReplacingMergeTree(fetched_at)" in migration
    assert "ENGINE = ReplacingMergeTree(derived_at)" in migration
    assert "ENGINE = ReplacingMergeTree(queried_at)" in migration
    assert "ORDER BY network_key" in migration
    assert "ORDER BY (network_key, cidr)" in migration
    assert "ORDER BY (bucket, ip_version, ip)" in migration
    assert "raw_response" in migration
    assert "String CODEC(ZSTD(3))" in migration

    assert "CREATE VIEW IF NOT EXISTS corpscout.rdap_networks_current" in migration
    assert (
        "CREATE VIEW IF NOT EXISTS corpscout.rdap_network_segments_current" in migration
    )
    assert (
        "CREATE VIEW IF NOT EXISTS corpscout.rdap_ip_lookup_results_current"
        in migration
    )
    assert migration.count(" FINAL") >= 3
    assert "WHERE segment_role = 'lookup_result'" in migration
    assert "argMax(network_key, tuple(derived_at, network_key))" in migration

    assert "CREATE DICTIONARY IF NOT EXISTS corpscout.rdap_network_trie" in migration
    assert "PRIMARY KEY cidr" in migration
    assert "TABLE 'rdap_network_segments_current'" in migration
    assert "LAYOUT(IP_TRIE())" in migration
    assert "LIFETIME(MIN 300 MAX 600)" in migration


def test_rdap_migration_table_columns_match_contract() -> None:
    migration = (MIGRATIONS_DIR / f"{MIGRATION_NAME}.up.sql").read_text(
        encoding="utf-8"
    )

    assert _create_table_columns(migration, "corpscout.rdap_networks") == (
        "network_key",
        "rir",
        "handle",
        "ip_version",
        "start_address",
        "end_address",
        "name",
        "registration_type",
        "country_code",
        "status",
        "registrant_handles",
        "registrant_names",
        "parent_network_key",
        "parent_handle",
        "self_url",
        "up_url",
        "registration_date",
        "last_changed_at",
        "response_sha256",
        "raw_response",
        "fetched_at",
    )
    assert _create_table_columns(migration, "corpscout.rdap_network_segments") == (
        "network_key",
        "cidr",
        "ip_version",
        "prefix_length",
        "segment_role",
        "response_sha256",
        "derived_at",
    )
    assert _create_table_columns(migration, "corpscout.rdap_ip_lookup_results") == (
        "bucket",
        "ip",
        "ip_version",
        "lookup_status",
        "network_key",
        "error_code",
        "retry_after",
        "queried_at",
    )


def test_rdap_down_migration_drops_objects_in_dependency_order() -> None:
    migration = (MIGRATIONS_DIR / f"{MIGRATION_NAME}.down.sql").read_text(
        encoding="utf-8"
    )

    statements = tuple(
        statement.strip()
        for statement in migration.split(";")
        if statement.strip() != ""
    )

    assert statements[0] == "DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie"
    assert statements[1:4] == (
        "DROP VIEW IF EXISTS corpscout.rdap_ip_lookup_results_current",
        "DROP VIEW IF EXISTS corpscout.rdap_network_segments_current",
        "DROP VIEW IF EXISTS corpscout.rdap_networks_current",
    )
    assert statements[4:] == (
        "DROP TABLE IF EXISTS corpscout.rdap_ip_lookup_results",
        "DROP TABLE IF EXISTS corpscout.rdap_network_segments",
        "DROP TABLE IF EXISTS corpscout.rdap_networks",
    )


def test_rdap_dictionary_reader_migration_uses_a_local_least_privilege_user() -> None:
    migration = (
        MIGRATIONS_DIR / f"{DICTIONARY_READER_MIGRATION_NAME}.up.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE USER IF NOT EXISTS corpscout_rdap_dictionary" in migration
    assert "HOST LOCAL" in migration
    assert "IDENTIFIED WITH no_password" in migration
    assert (
        "GRANT SELECT ON corpscout.rdap_network_segments_current "
        "TO corpscout_rdap_dictionary" in migration
    )
    assert "USER 'corpscout_rdap_dictionary'" in migration
    assert "PASSWORD" not in migration
    assert migration.index("DROP DICTIONARY") < migration.index("CREATE USER")
    assert migration.index("CREATE USER") < migration.index("CREATE DICTIONARY")


def test_rdap_dictionary_reader_down_migration_restores_previous_source() -> None:
    migration = (
        MIGRATIONS_DIR / f"{DICTIONARY_READER_MIGRATION_NAME}.down.sql"
    ).read_text(encoding="utf-8")

    assert migration.index("DROP DICTIONARY") < migration.index("DROP USER")
    assert migration.index("DROP USER") < migration.index("CREATE DICTIONARY")
    restored_dictionary = migration.split("CREATE DICTIONARY", 1)[1]
    assert "USER 'corpscout_rdap_dictionary'" not in restored_dictionary


def test_rdap_asset_rejects_the_pre_reader_dictionary_definition() -> None:
    required_tables = [
        ("commoncrawl_ip_addresses",),
        ("rdap_networks",),
        ("rdap_networks_current",),
        ("rdap_network_segments",),
        ("rdap_network_segments_current",),
        ("rdap_ip_lookup_results",),
        ("rdap_ip_lookup_results_current",),
    ]
    clickhouse = FakeStorageResource(
        [
            FakeStorageClient([required_tables]),
            FakeStorageClient(
                [
                    [("rdap_network_trie",)],
                    [
                        (
                            "CREATE DICTIONARY corpscout.rdap_network_trie "
                            "SOURCE(CLICKHOUSE(DB 'corpscout'))",
                        )
                    ],
                ]
            ),
        ]
    )

    with pytest.raises(ValueError, match="000126"):
        _assert_rdap_storage_exists(clickhouse)


def test_rdap_asset_uses_shared_static_ip_buckets() -> None:
    assert commoncrawl_ip_rdap_networks.partitions_def is COMMONCRAWL_IP_PARTITIONS
    assert commoncrawl_ip_bucket_index("bucket_007") == 7
    assert commoncrawl_ip_rdap_networks.backfill_policy == (
        dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("candidate_scan_limit", 0),
        ("max_requests", 0),
        ("insert_batch_size", 0),
        ("request_delay_seconds", -0.1),
        ("parent_depth", -1),
        ("rate_limit_retry_seconds", 0),
        ("transient_retry_seconds", 0),
    ),
)
def test_rdap_config_rejects_unbounded_or_invalid_values(
    field_name: str,
    invalid_value: int | float,
) -> None:
    with pytest.raises(ValidationError):
        CommoncrawlRdapConfig(**{field_name: invalid_value})


def test_rdap_candidates_use_separate_typed_ipv4_and_ipv6_trie_checks() -> None:
    assert "FROM corpscout.commoncrawl_ip_addresses FINAL" in RDAP_CANDIDATES_SQL
    assert "toIPv4(addresses.ip)" in RDAP_CANDIDATES_SQL
    assert "toIPv6(addresses.ip)" in RDAP_CANDIDATES_SQL
    assert "UNION ALL" in RDAP_CANDIDATES_SQL
    assert "rdap_network_trie" in RDAP_CANDIDATES_SQL
    assert "rdap_ip_lookup_results_current" in RDAP_CANDIDATES_SQL
    assert "retry_after" in RDAP_CANDIDATES_SQL
    assert "%(candidate_scan_limit)s" in RDAP_CANDIDATES_SQL


def test_rdap_insert_column_order_matches_migration() -> None:
    migration = (MIGRATIONS_DIR / f"{MIGRATION_NAME}.up.sql").read_text(
        encoding="utf-8"
    )

    assert _insert_columns(RDAP_NETWORK_INSERT_SQL) == RDAP_NETWORK_COLUMNS
    assert _insert_columns(RDAP_SEGMENT_INSERT_SQL) == RDAP_SEGMENT_COLUMNS
    assert _insert_columns(RDAP_LOOKUP_INSERT_SQL) == RDAP_LOOKUP_COLUMNS
    assert _create_table_columns(migration, "corpscout.rdap_networks") == (
        RDAP_NETWORK_COLUMNS
    )
    assert _create_table_columns(migration, "corpscout.rdap_network_segments") == (
        RDAP_SEGMENT_COLUMNS
    )
    assert _create_table_columns(migration, "corpscout.rdap_ip_lookup_results") == (
        RDAP_LOOKUP_COLUMNS
    )


def test_rdap_bucket_reuses_new_segments_and_caches_non_global_ips() -> None:
    read_client = FakeRdapReadClient([("8.8.8.8", 4), ("8.8.8.9", 4), ("10.0.0.1", 4)])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient([_global_lookup_response()])
    sleeps: list[float] = []
    config = CommoncrawlRdapConfig(
        candidate_scan_limit=10,
        max_requests=5,
        insert_batch_size=10,
        request_delay_seconds=0.25,
        parent_depth=0,
        rate_limit_retry_seconds=3600,
        transient_retry_seconds=900,
    )

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=config,
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=sleeps.append,
        )

    assert read_client.query == RDAP_CANDIDATES_SQL
    assert read_client.params == {"bucket_index": 7, "candidate_scan_limit": 10}
    assert read_client.settings == {"max_block_size": 10}
    assert rdap_client.direct_queries == ["8.8.8.8"]
    assert sleeps == []
    assert result["candidates_scanned"] == 3
    assert result["rdap_requests"] == 1
    assert result["direct_networks"] == 1
    assert result["parent_networks"] == 0
    assert result["segments"] == 1
    assert result["in_run_trie_skips"] == 1
    assert result["non_global_ips"] == 1
    assert result["lookup_rows_written"] == 2
    assert result["rir_counts"] == {"arin": 1}

    assert [query for query, _rows in write_client.inserts] == [
        RDAP_NETWORK_INSERT_SQL,
        RDAP_SEGMENT_INSERT_SQL,
        RDAP_LOOKUP_INSERT_SQL,
    ]
    lookup_rows = write_client.inserts[-1][1]
    status_index = RDAP_LOOKUP_COLUMNS.index("lookup_status")
    assert [row[status_index] for row in lookup_rows] == ["found", "not_global"]


def test_rdap_bucket_follows_one_parent_and_throttles_between_requests() -> None:
    direct = _global_lookup_response(include_up_link=True)
    parent = _parent_lookup_response()
    read_client = FakeRdapReadClient([("8.8.8.8", 4)])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient([direct], [parent])
    sleeps: list[float] = []

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=CommoncrawlRdapConfig(
                candidate_scan_limit=10,
                max_requests=5,
                insert_batch_size=10,
                request_delay_seconds=0.25,
                parent_depth=1,
                rate_limit_retry_seconds=3600,
                transient_retry_seconds=900,
            ),
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=sleeps.append,
        )

    assert result["rdap_requests"] == 2
    assert result["direct_networks"] == 1
    assert result["parent_networks"] == 1
    assert result["segments"] == 2
    assert sleeps == [0.25]
    assert rdap_client.parent_queries == [
        ("https://rdap.arin.net/registry/ip/8.0.0.0/8", "arin")
    ]
    network_rows = write_client.inserts[0][1]
    assert [row[RDAP_NETWORK_COLUMNS.index("network_key")] for row in network_rows] == [
        "arin:NET-8-8-8-0-1",
        "arin:NET-8-0-0-0-1",
    ]
    segment_rows = write_client.inserts[1][1]
    assert [
        row[RDAP_SEGMENT_COLUMNS.index("segment_role")] for row in segment_rows
    ] == [
        "lookup_result",
        "parent",
    ]


def test_rdap_bucket_caches_terminal_and_retryable_failures() -> None:
    read_client = FakeRdapReadClient([("8.8.8.8", 4), ("9.9.9.9", 4)])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient(
        [
            RdapClientError("missing", code="not_found", retryable=False),
            RdapClientError(
                "rate limited",
                code="rate_limited",
                retryable=True,
                status_code=429,
            ),
        ]
    )

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=CommoncrawlRdapConfig(
                candidate_scan_limit=10,
                max_requests=5,
                insert_batch_size=10,
                request_delay_seconds=0,
                parent_depth=1,
                rate_limit_retry_seconds=3600,
                transient_retry_seconds=900,
            ),
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=lambda _seconds: None,
        )

    assert result["terminal_misses"] == 1
    assert result["retryable_errors"] == 1
    lookup_rows = write_client.inserts[-1][1]
    status_index = RDAP_LOOKUP_COLUMNS.index("lookup_status")
    error_index = RDAP_LOOKUP_COLUMNS.index("error_code")
    retry_index = RDAP_LOOKUP_COLUMNS.index("retry_after")
    assert [row[status_index] for row in lookup_rows] == [
        "not_found",
        "retryable_error",
    ]
    assert [row[error_index] for row in lookup_rows] == [
        "not_found",
        "rate_limited",
    ]
    assert lookup_rows[0][retry_index] is None
    assert lookup_rows[1][retry_index] == datetime(2026, 7, 12, 11, 30, tzinfo=UTC)


def test_rdap_bucket_with_no_candidates_makes_no_remote_requests() -> None:
    read_client = FakeRdapReadClient([])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient([])

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=CommoncrawlRdapConfig(),
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=lambda _seconds: None,
        )

    assert result["candidates_scanned"] == 0
    assert result["rdap_requests"] == 0
    assert rdap_client.direct_queries == []
    assert write_client.inserts == []


def test_rdap_bucket_never_exceeds_the_request_limit() -> None:
    read_client = FakeRdapReadClient([("8.8.8.8", 4), ("9.9.9.9", 4)])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient([_global_lookup_response(), _global_lookup_response()])

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=CommoncrawlRdapConfig(
                candidate_scan_limit=10,
                max_requests=1,
                insert_batch_size=10,
                request_delay_seconds=0,
                parent_depth=0,
                rate_limit_retry_seconds=3600,
                transient_retry_seconds=900,
            ),
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=lambda _seconds: None,
        )

    assert result["rdap_requests"] == 1
    assert result["request_limit_reached"] is True
    assert rdap_client.direct_queries == ["8.8.8.8"]


def test_rdap_lookup_rows_flush_in_bounded_batches() -> None:
    read_client = FakeRdapReadClient([("8.8.8.8", 4), ("9.9.9.9", 4)])
    write_client = FakeRdapWriteClient()
    rdap_client = FakeRdapClient(
        [
            RdapClientError("missing", code="not_found", retryable=False),
            RdapClientError("missing", code="not_found", retryable=False),
        ]
    )

    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=CommoncrawlRdapConfig(
                candidate_scan_limit=10,
                max_requests=2,
                insert_batch_size=1,
                request_delay_seconds=0,
                parent_depth=0,
                rate_limit_retry_seconds=3600,
                transient_retry_seconds=900,
            ),
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=lambda _seconds: None,
        )

    assert result["lookup_rows_written"] == 2
    assert [len(rows) for _query, rows in write_client.inserts] == [1, 1]
    assert all(query == RDAP_LOOKUP_INSERT_SQL for query, _rows in write_client.inserts)


@pytest.mark.parametrize(
    ("fixture_name", "expected_cidrs"),
    (
        ("aligned_ipv4.json", ("192.0.2.0/24",)),
        (
            "non_aligned_ipv4.json",
            (
                "198.51.100.10/31",
                "198.51.100.12/30",
                "198.51.100.16/30",
                "198.51.100.20/32",
            ),
        ),
        ("aligned_ipv6.json", ("2001:db8::/120",)),
    ),
)
def test_rdap_exact_address_ranges_are_losslessly_decomposed_into_cidrs(
    fixture_name: str,
    expected_cidrs: tuple[str, ...],
) -> None:
    normalized = normalize_rdap_network(
        _load_lookup_response(fixture_name),
        fetched_at=FETCHED_AT,
        segment_role="lookup_result",
    )

    assert tuple(segment.cidr for segment in normalized.segments) == expected_cidrs
    assert all(
        segment.network_key == normalized.network.network_key
        for segment in normalized.segments
    )
    assert all(
        segment.response_sha256 == normalized.network.response_sha256
        for segment in normalized.segments
    )


def test_rdap_response_normalizes_network_owner_parent_links_and_dates() -> None:
    lookup = _load_lookup_response("aligned_ipv4.json")
    normalized = normalize_rdap_network(
        lookup,
        fetched_at=FETCHED_AT,
        segment_role="lookup_result",
    )
    network = normalized.network

    assert network.network_key == "arin:NET-192-0-2-0-1"
    assert network.rir == "arin"
    assert network.handle == "NET-192-0-2-0-1"
    assert network.ip_version == 4
    assert network.start_address == "192.0.2.0"
    assert network.end_address == "192.0.2.255"
    assert network.name == "TEST-NET-1"
    assert network.registration_type == "DIRECT ALLOCATION"
    assert network.country_code == "US"
    assert network.status == ("active",)
    assert network.registrant_handles == ("EXAMPLE-ORG", "EXAMPLE-CUSTOMER")
    assert network.registrant_names == (
        "Example Corporation",
        "Example Customer Network",
    )
    assert network.parent_network_key == "arin:NET-192-0-0-0-1"
    assert network.parent_handle == "NET-192-0-0-0-1"
    assert network.self_url == ("https://rdap.arin.net/registry/ip/192.0.2.0")
    assert network.up_url == "https://rdap.arin.net/registry/ip/192.0.0.0/8"
    assert network.registration_date == datetime(2010, 1, 1, tzinfo=UTC)
    assert network.last_changed_at == datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
    assert network.fetched_at == FETCHED_AT

    expected_raw = json.dumps(
        lookup.raw_response,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert network.raw_response == expected_raw
    assert (
        network.response_sha256
        == hashlib.sha256(expected_raw.encode("utf-8")).hexdigest()
    )
    assert len(network.clickhouse_values()) == 21
    assert normalized.segments[0].clickhouse_values() == (
        "arin:NET-192-0-2-0-1",
        "192.0.2.0/24",
        4,
        24,
        "lookup_result",
        network.response_sha256,
        FETCHED_AT,
    )


def test_parent_segment_is_kept_but_labeled_outside_lookup_trie_role() -> None:
    normalized = normalize_rdap_network(
        _load_lookup_response("aligned_ipv4.json"),
        fetched_at=FETCHED_AT,
        segment_role="parent",
    )

    assert {segment.segment_role for segment in normalized.segments} == {"parent"}


def test_normalization_rejects_invalid_segment_roles() -> None:
    with pytest.raises(ValueError, match="segment_role"):
        normalize_rdap_network(
            _load_lookup_response("aligned_ipv4.json"),
            fetched_at=FETCHED_AT,
            segment_role="unknown",
        )


def test_rdap_client_bootstraps_once_and_requests_raw_ip_data(monkeypatch) -> None:
    calls: list[tuple[object, ...]] = []
    session = requests.Session()

    monkeypatch.setattr(whoisit, "is_bootstrapped", lambda: False)
    monkeypatch.setattr(whoisit, "bootstrap", lambda: calls.append(("bootstrap",)))

    def fake_ip(query, *, rir, include_raw, session):
        calls.append(("ip", query, rir, include_raw, session))
        fixture = _load_fixture("aligned_ipv4.json")
        return {"rir": fixture["rir"], "raw": fixture["raw"]}

    monkeypatch.setattr(whoisit, "ip", fake_ip)

    client = RdapClient(user_agent="Corpscout RDAP test", session=session)
    first = client.lookup_ip("192.0.2.1")
    second = client.lookup_ip("192.0.2.2")

    assert first.rir == "arin"
    assert first.raw_response["handle"] == "NET-192-0-2-0-1"
    assert second.rir == "arin"
    assert calls == [
        ("bootstrap",),
        ("ip", "192.0.2.1", None, True, session),
        ("ip", "192.0.2.2", None, True, session),
    ]
    assert session.headers["User-Agent"] == "Corpscout RDAP test"


@pytest.mark.parametrize(
    ("library_error", "code", "retryable"),
    (
        (ResourceDoesNotExist("missing", status_code=404), "not_found", False),
        (RateLimitedError("slow down", status_code=429), "rate_limited", True),
        (UnsupportedError("unsupported"), "unsupported", False),
    ),
)
def test_rdap_client_translates_library_errors(
    monkeypatch,
    library_error: Exception,
    code: str,
    retryable: bool,
) -> None:
    monkeypatch.setattr(whoisit, "is_bootstrapped", lambda: True)

    def fail_lookup(*args, **kwargs):
        raise library_error

    monkeypatch.setattr(whoisit, "ip", fail_lookup)
    client = RdapClient(user_agent="Corpscout RDAP test")

    with pytest.raises(RdapClientError) as error:
        client.lookup_ip("192.0.2.1")

    assert error.value.code == code
    assert error.value.retryable is retryable
    assert error.value.status_code == getattr(library_error, "status_code", None)


def test_rdap_client_treats_wrapped_transport_failures_as_retryable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(whoisit, "is_bootstrapped", lambda: True)

    def fail_lookup(*args, **kwargs):
        try:
            raise requests.Timeout("timed out")
        except requests.Timeout as transport_error:
            raise QueryError("request failed") from transport_error

    monkeypatch.setattr(whoisit, "ip", fail_lookup)
    client = RdapClient(user_agent="Corpscout RDAP test")

    with pytest.raises(RdapClientError) as error:
        client.lookup_ip("192.0.2.1")

    assert error.value.code == "query_error"
    assert error.value.retryable is True


def test_rdap_client_can_follow_an_up_link_as_an_rir_scoped_query(
    monkeypatch,
) -> None:
    calls: list[tuple[str, str | None]] = []
    monkeypatch.setattr(whoisit, "is_bootstrapped", lambda: True)

    def fake_ip(query, *, rir, include_raw, session):
        calls.append((query, rir))
        fixture = _load_fixture("aligned_ipv4.json")
        return {"rir": fixture["rir"], "raw": fixture["raw"]}

    monkeypatch.setattr(whoisit, "ip", fake_ip)
    client = RdapClient(user_agent="Corpscout RDAP test")

    client.lookup_up_url(
        "https://rdap.arin.net/registry/ip/192.0.0.0/8",
        rir="arin",
    )

    assert calls == [("192.0.0.0/8", "arin")]
    assert (
        ip_resource_from_up_url("https://rdap.db.ripe.net/ip/2001%3Adb8%3A%3A/32")
        == "2001:db8::/32"
    )


def _load_lookup_response(fixture_name: str) -> RdapLookupResponse:
    fixture = _load_fixture(fixture_name)
    return RdapLookupResponse(
        rir=str(fixture["rir"]),
        raw_response=fixture["raw"],
    )


def _global_lookup_response(*, include_up_link: bool = False) -> RdapLookupResponse:
    fixture = _load_fixture("aligned_ipv4.json")
    raw = dict(fixture["raw"])
    raw.update(
        {
            "handle": "NET-8-8-8-0-1",
            "startAddress": "8.8.8.0",
            "endAddress": "8.8.8.255",
            "name": "EXAMPLE-GLOBAL-NET",
            "parentHandle": "NET-8-0-0-0-1" if include_up_link else None,
        }
    )
    raw["links"] = [
        {
            "rel": "self",
            "href": "https://rdap.arin.net/registry/ip/8.8.8.0",
        }
    ]
    if include_up_link:
        raw["links"].append(
            {
                "rel": "up",
                "href": "https://rdap.arin.net/registry/ip/8.0.0.0/8",
            }
        )
    return RdapLookupResponse(rir="arin", raw_response=raw)


def _parent_lookup_response() -> RdapLookupResponse:
    fixture = _load_fixture("aligned_ipv4.json")
    raw = dict(fixture["raw"])
    raw.update(
        {
            "handle": "NET-8-0-0-0-1",
            "startAddress": "8.0.0.0",
            "endAddress": "8.255.255.255",
            "name": "EXAMPLE-PARENT-NET",
            "parentHandle": None,
            "links": [
                {
                    "rel": "self",
                    "href": "https://rdap.arin.net/registry/ip/8.0.0.0",
                }
            ],
        }
    )
    return RdapLookupResponse(rir="arin", raw_response=raw)


def _load_fixture(fixture_name: str) -> dict[str, object]:
    return json.loads((FIXTURES_DIR / fixture_name).read_text(encoding="utf-8"))


def _insert_columns(sql: str) -> tuple[str, ...]:
    column_block = sql.split("(", 1)[1].split(")\nVALUES", 1)[0]
    return tuple(
        line.strip().removesuffix(",")
        for line in column_block.splitlines()
        if line.strip() != ""
    )


def _create_table_columns(sql: str, table: str) -> tuple[str, ...]:
    table_sql = sql.split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1]
    column_block = table_sql.split("(", 1)[1].split(")\nENGINE", 1)[0]
    return tuple(
        line.strip().split(maxsplit=1)[0]
        for line in column_block.splitlines()
        if line.strip() != "" and not line.lstrip().startswith("--")
    )
