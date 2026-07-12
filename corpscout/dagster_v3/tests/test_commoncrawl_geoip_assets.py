from contextlib import contextmanager
from datetime import UTC, datetime
from ipaddress import ip_address
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_geoip.assets import (
    GEOIP_CANDIDATES_SQL,
    GEOIP_COLUMNS,
    GEOIP_INSERT_SQL,
    _enrich_geoip_bucket,
    commoncrawl_ip_geoip,
)
from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    MaxMindLookup,
    build_geoip_enrichment,
    classify_ip_scope,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_BUCKET_COUNT,
    COMMONCRAWL_IP_PARTITIONS,
    commoncrawl_ip_bucket_index,
)


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"


class FakeReadClient:
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


class FakeWriteClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, query: str, rows: list[tuple[object, ...]]) -> None:
        self.inserts.append((query, list(rows)))


class FakeClickhouseResource:
    def __init__(
        self, read_client: FakeReadClient, write_client: FakeWriteClient
    ) -> None:
        self.clients = iter((read_client, write_client))

    @contextmanager
    def get_connection(self):
        yield next(self.clients)


class FakeMaxMindReader:
    def __init__(self, record: dict[str, object]) -> None:
        self.record = record
        self.lookups: list[str] = []

    def get_with_prefix_len(self, address):
        self.lookups.append(str(address))
        return self.record, 24


def test_geoip_asset_uses_stable_static_hash_buckets() -> None:
    partition_keys = COMMONCRAWL_IP_PARTITIONS.get_partition_keys()

    assert isinstance(COMMONCRAWL_IP_PARTITIONS, dg.StaticPartitionsDefinition)
    assert len(partition_keys) == COMMONCRAWL_IP_BUCKET_COUNT == 256
    assert partition_keys[0] == "bucket_000"
    assert partition_keys[-1] == "bucket_255"
    assert commoncrawl_ip_bucket_index("bucket_007") == 7
    assert commoncrawl_ip_geoip.partitions_def is COMMONCRAWL_IP_PARTITIONS


def test_geoip_bucket_key_validation_rejects_out_of_range_values() -> None:
    for partition_key in ("bucket_256", "bucket_-1", "07", "bucket_x"):
        with pytest.raises(ValueError):
            commoncrawl_ip_bucket_index(partition_key)


def test_ip_scope_classification_keeps_non_global_reasons() -> None:
    assert classify_ip_scope(ip_address("8.8.8.8")) == "global"
    assert classify_ip_scope(ip_address("10.0.0.1")) == "private"
    assert classify_ip_scope(ip_address("100.64.0.1")) == "cgnat"
    assert classify_ip_scope(ip_address("127.0.0.1")) == "loopback"
    assert classify_ip_scope(ip_address("169.254.1.1")) == "link_local"
    assert classify_ip_scope(ip_address("192.0.2.1")) == "documentation"
    assert classify_ip_scope(ip_address("224.0.0.1")) == "multicast"


def test_maxmind_records_map_to_clickhouse_enrichment_columns() -> None:
    city_build_epoch = datetime(2026, 7, 10, tzinfo=UTC)
    asn_build_epoch = datetime(2026, 7, 9, tzinfo=UTC)
    enriched_at = datetime(2026, 7, 10, 12, 30, tzinfo=UTC)
    enrichment = build_geoip_enrichment(
        bucket=7,
        address=ip_address("8.8.8.8"),
        city_lookup=MaxMindLookup(
            record={
                "continent": {"code": "NA", "names": {"en": "North America"}},
                "country": {
                    "geoname_id": 6252001,
                    "iso_code": "US",
                    "names": {"en": "United States"},
                },
                "registered_country": {
                    "iso_code": "US",
                    "names": {"en": "United States"},
                },
                "subdivisions": [
                    {
                        "iso_code": "CA",
                        "names": {"en": "California"},
                    }
                ],
                "city": {
                    "geoname_id": 5375480,
                    "names": {"en": "Mountain View"},
                },
                "location": {
                    "latitude": 37.4056,
                    "longitude": -122.0775,
                    "accuracy_radius": 1000,
                    "time_zone": "America/Los_Angeles",
                },
            },
            prefix_length=24,
        ),
        asn_lookup=MaxMindLookup(
            record={
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "Google LLC",
            },
            prefix_length=24,
        ),
        city_build_epoch=city_build_epoch,
        asn_build_epoch=asn_build_epoch,
        enriched_at=enriched_at,
    )

    assert enrichment.bucket == 7
    assert enrichment.ip == "8.8.8.8"
    assert enrichment.ip_version == 4
    assert enrichment.ip_scope == "global"
    assert enrichment.city_lookup_status == "found"
    assert enrichment.asn_lookup_status == "found"
    assert enrichment.continent_code == "NA"
    assert enrichment.country_iso_code == "US"
    assert enrichment.country_geoname_id == 6252001
    assert enrichment.subdivision_iso_codes == ("CA",)
    assert enrichment.subdivision_names == ("California",)
    assert enrichment.city_geoname_id == 5375480
    assert enrichment.city_name == "Mountain View"
    assert enrichment.latitude == 37.4056
    assert enrichment.longitude == -122.0775
    assert enrichment.accuracy_radius_km == 1000
    assert enrichment.timezone == "America/Los_Angeles"
    assert enrichment.asn == 15169
    assert enrichment.asn_organization == "Google LLC"
    assert enrichment.city_network == "8.8.8.0/24"
    assert enrichment.asn_network == "8.8.8.0/24"
    assert enrichment.city_db_build_epoch == city_build_epoch
    assert enrichment.asn_db_build_epoch == asn_build_epoch
    assert enrichment.enriched_at == enriched_at


def test_non_global_ip_does_not_claim_maxmind_matches() -> None:
    build_epoch = datetime(2026, 7, 10, tzinfo=UTC)
    enrichment = build_geoip_enrichment(
        bucket=3,
        address=ip_address("10.0.0.1"),
        city_lookup=None,
        asn_lookup=None,
        city_build_epoch=build_epoch,
        asn_build_epoch=build_epoch,
        enriched_at=build_epoch,
    )

    assert enrichment.ip_scope == "private"
    assert enrichment.city_lookup_status == "not_global"
    assert enrichment.asn_lookup_status == "not_global"
    assert enrichment.latitude is None
    assert enrichment.longitude is None
    assert enrichment.asn is None


def test_geoip_bucket_streams_candidates_and_inserts_resumable_batches() -> None:
    build_epoch = datetime(2026, 7, 10, tzinfo=UTC)
    read_client = FakeReadClient([("8.8.8.8", 4), ("10.0.0.1", 4)])
    write_client = FakeWriteClient()
    clickhouse = FakeClickhouseResource(read_client, write_client)
    city_reader = FakeMaxMindReader(
        {"country": {"iso_code": "US", "names": {"en": "United States"}}}
    )
    asn_reader = FakeMaxMindReader(
        {
            "autonomous_system_number": 15169,
            "autonomous_system_organization": "Google LLC",
        }
    )

    result = _enrich_geoip_bucket(
        context=dg.build_asset_context(partition_key="bucket_007"),
        clickhouse=clickhouse,
        bucket_index=7,
        city_reader=city_reader,
        asn_reader=asn_reader,
        city_build_epoch=build_epoch,
        asn_build_epoch=build_epoch,
        enriched_at=build_epoch,
        insert_batch_size=1,
    )

    assert read_client.query == GEOIP_CANDIDATES_SQL
    assert read_client.params == {
        "bucket_index": 7,
        "city_db_build_epoch": build_epoch,
        "asn_db_build_epoch": build_epoch,
    }
    assert read_client.settings == {"max_block_size": 1}
    assert result == {
        "candidate_count": 2,
        "inserted_count": 2,
        "city_found_count": 1,
        "city_not_found_count": 0,
        "city_not_global_count": 1,
        "asn_found_count": 1,
        "asn_not_found_count": 0,
        "asn_not_global_count": 1,
        "scope_counts": {"global": 1, "private": 1},
    }
    assert city_reader.lookups == ["8.8.8.8"]
    assert asn_reader.lookups == ["8.8.8.8"]
    assert len(write_client.inserts) == 2
    assert all(query == GEOIP_INSERT_SQL for query, _rows in write_client.inserts)
    inserted_rows = [rows[0] for _query, rows in write_client.inserts]
    assert inserted_rows[0][GEOIP_COLUMNS.index("city_lookup_status")] == "found"
    assert inserted_rows[1][GEOIP_COLUMNS.index("city_lookup_status")] == "not_global"


def test_maxmind_resource_supports_databases_directly_in_directory(
    tmp_path: Path,
) -> None:
    city = tmp_path / "GeoLite2-City.mmdb"
    asn = tmp_path / "GeoLite2-ASN.mmdb"
    city.touch()
    asn.touch()

    resource = MaxMindDatabaseResource(database_directory=str(tmp_path))

    assert resource.database_paths() == (city, asn)


def test_geoip_migration_owns_registry_and_enrichment_tables() -> None:
    migration = (
        MIGRATIONS_DIR / "000115_corpscout_commoncrawl_ip_geoip.up.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_addresses" in migration
    assert "AggregatingMergeTree()" in migration
    assert "SimpleAggregateFunction(min" in migration
    assert "SimpleAggregateFunction(max" in migration
    assert "REFRESH EVERY" not in migration
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in migration
    assert "cityHash64(ip) % 256" in migration
    assert "ORDER BY (bucket, ip_version, ip)" in migration
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_geoip" in migration
    assert "ReplacingMergeTree(enriched_at)" in migration


def test_geoip_candidates_finalize_incremental_ip_registry() -> None:
    assert "FROM corpscout.commoncrawl_ip_addresses FINAL" in GEOIP_CANDIDATES_SQL
    assert "WHERE bucket = %(bucket_index)s" in GEOIP_CANDIDATES_SQL


def test_geoip_insert_column_order_matches_migration() -> None:
    migration = (
        MIGRATIONS_DIR / "000115_corpscout_commoncrawl_ip_geoip.up.sql"
    ).read_text(encoding="utf-8")

    assert _insert_columns(GEOIP_INSERT_SQL) == GEOIP_COLUMNS
    assert _create_table_columns(migration, "corpscout.commoncrawl_ip_geoip") == (
        GEOIP_COLUMNS
    )


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
