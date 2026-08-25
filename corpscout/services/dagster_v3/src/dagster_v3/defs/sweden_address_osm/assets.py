import tempfile
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_address_osm import clickhouse as osm_clickhouse
from dagster_v3.defs.sweden_address_osm import tables
from dagster_v3.defs.sweden_address_osm.normalize import replace_osm_address_points
from dagster_v3.defs.sweden_address_osm.resources import (
    latest_snapshot_manifest,
    sync_osm_snapshot,
)


@dg.asset(
    name="sweden_osm_pbf_s3",
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "openstreetmap", "pbf", "geofabrik"},
    description=(
        "Downloads the current Geofabrik Sweden OpenStreetMap PBF, verifies "
        "its published MD5 checksum, and stores an immutable content-addressed "
        "snapshot plus an auditable run manifest in RustFS/S3."
    ),
)
def sweden_osm_pbf_s3(
    context: dg.AssetExecutionContext,
    sweden_address_osm_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_osm_snapshot(
        object_store=sweden_address_osm_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "source_url": tables.SOURCE_URL,
            "resolved_url": snapshot.resolved_url,
            "object_key": snapshot.object_key,
            "manifest_key": snapshot.manifest_key,
            "source_md5": snapshot.source_md5,
            "sha256": snapshot.sha256,
            "size_bytes": snapshot.size_bytes,
            "downloaded": snapshot.downloaded,
            "last_modified": snapshot.last_modified,
            "license": "ODbL 1.0",
        }
    )


@dg.asset(
    name="sweden_osm_addresses_duckdb",
    deps=[dg.AssetKey("sweden_osm_pbf_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "openstreetmap", "pbf", "duckdb", "spatial"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Builds Sweden-only DuckDB indexes for OSM address points and named "
        "road segments. Coordinates are WGS84 longitude and latitude; every "
        "row retains its OSM identifier and snapshot provenance."
    ),
)
def sweden_osm_addresses_duckdb(
    sweden_address_osm_duckdb: DuckDBResource,
    sweden_address_osm_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_snapshot_manifest(sweden_address_osm_object_store)
    tables.DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sweden_address_osm_parse_") as temp_dir:
        pbf_path = Path(temp_dir) / "sweden-latest.osm.pbf"
        sweden_address_osm_object_store.download_file(
            str(manifest["object_key"]),
            pbf_path,
            bucket=tables.S3_BUCKET,
        )
        with sweden_address_osm_duckdb.get_connection() as connection:
            counts = replace_osm_address_points(
                connection=connection,
                pbf_path=pbf_path,
                source_url=str(manifest["resolved_url"]),
                source_object_key=str(manifest["object_key"]),
                source_md5=str(manifest["source_md5"]),
                source_snapshot_at=_source_snapshot_at(manifest),
                source_retrieved_at=datetime.fromisoformat(
                    str(manifest["retrieved_at"])
                ),
            )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "source_object_key": str(manifest["object_key"]),
            "source_md5": str(manifest["source_md5"]),
            "source_snapshot_at": _source_snapshot_at(manifest).isoformat(),
            "duckdb_table": tables.QUALIFIED_ADDRESS_TABLE,
            "street_segment_table": tables.QUALIFIED_STREET_SEGMENT_TABLE,
            "duckdb_path": str(tables.DUCKDB_PATH),
        }
    )


def _source_snapshot_at(manifest: dict[str, object]) -> datetime:
    last_modified = str(manifest.get("last_modified", "")).strip()
    if last_modified:
        parsed = parsedate_to_datetime(last_modified)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return datetime.fromisoformat(str(manifest["retrieved_at"])).astimezone(UTC)


@dg.asset(
    name="sweden_osm_addresses_clickhouse",
    deps=[dg.AssetKey("sweden_osm_addresses_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Publishes the Sweden OSM address-point and named-road gazetteer from the "
        "host-local build DuckDB into ClickHouse (corpscout.se_osm_address_points, "
        "corpscout.se_osm_street_segments) via a staged atomic EXCHANGE, adding a "
        "resolver-normalized normalized_match_key that lines up with "
        "se_address_geocodes so rewrite yield can be measured by SQL join."
    ),
)
def sweden_osm_addresses_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with sweden_address_osm_duckdb.get_connection() as connection:
        result = osm_clickhouse.publish_sweden_osm_gazetteer(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            published_at=datetime.now(UTC),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "address_points": result.address_points,
            "street_segments": result.street_segments,
            "address_points_table": (
                f"{osm_clickhouse.CLICKHOUSE_DATABASE}."
                f"{osm_clickhouse.ADDRESS_POINTS_TABLE_CH}"
            ),
            "street_segments_table": (
                f"{osm_clickhouse.CLICKHOUSE_DATABASE}."
                f"{osm_clickhouse.STREET_SEGMENTS_TABLE_CH}"
            ),
        }
    )


@dg.asset_check(
    asset=sweden_osm_addresses_clickhouse,
    name="gazetteer_tables_are_non_empty_and_mirror_duckdb",
    description=(
        "Fails when either published ClickHouse gazetteer table is empty or its row "
        "count does not mirror the DuckDB build table it was published from."
    ),
)
def sweden_osm_gazetteer_row_count_check(
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with sweden_address_osm_duckdb.get_connection() as connection:
        [(duckdb_address_points,)] = connection.execute(
            f"select count(*) from {tables.QUALIFIED_ADDRESS_TABLE}"
        ).fetchall()
        [(duckdb_street_segments,)] = connection.execute(
            f"select count(*) from {tables.QUALIFIED_STREET_SEGMENT_TABLE}"
        ).fetchall()
    with clickhouse.get_connection() as client:
        [(clickhouse_address_points,)] = client.execute(
            f"SELECT count() FROM {osm_clickhouse.CLICKHOUSE_DATABASE}."
            f"{osm_clickhouse.ADDRESS_POINTS_TABLE_CH}"
        )
        [(clickhouse_street_segments,)] = client.execute(
            f"SELECT count() FROM {osm_clickhouse.CLICKHOUSE_DATABASE}."
            f"{osm_clickhouse.STREET_SEGMENTS_TABLE_CH}"
        )
    address_points_ok = osm_clickhouse.row_count_is_within_band(
        clickhouse_count=int(clickhouse_address_points),
        duckdb_count=int(duckdb_address_points),
    )
    street_segments_ok = osm_clickhouse.row_count_is_within_band(
        clickhouse_count=int(clickhouse_street_segments),
        duckdb_count=int(duckdb_street_segments),
    )
    return dg.AssetCheckResult(
        passed=address_points_ok and street_segments_ok,
        metadata={
            "clickhouse_address_points": int(clickhouse_address_points),
            "duckdb_address_points": int(duckdb_address_points),
            "clickhouse_street_segments": int(clickhouse_street_segments),
            "duckdb_street_segments": int(duckdb_street_segments),
        },
    )


@dg.asset_check(
    asset=sweden_osm_addresses_clickhouse,
    name="matched_geocodes_find_their_osm_point_by_match_key",
    description=(
        "Samples matched_exact geocode outcomes computed against this gazetteer's OSM "
        "snapshot and confirms they find their OSM address point again -- by OSM id, and "
        "by normalized_match_key wherever the OSM point carries a postcode. A "
        "normalization regression collapses the postcode-bearing key agreement."
    ),
)
def sweden_osm_gazetteer_match_key_join_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    sample_limit = 50_000
    with clickhouse.get_connection() as client:
        [
            (
                sample_size,
                osm_id_present,
                key_matches,
                postcode_bearing,
                key_matches_postcode_bearing,
            )
        ] = client.execute(
            osm_clickhouse.GAZETTEER_MATCH_JOIN_SQL,
            {"sample_limit": sample_limit},
        )
    sample_size = int(sample_size)
    osm_id_present = int(osm_id_present)
    key_matches = int(key_matches)
    postcode_bearing = int(postcode_bearing)
    key_matches_postcode_bearing = int(key_matches_postcode_bearing)
    passed = osm_clickhouse.gazetteer_match_join_is_healthy(
        sample_size=sample_size,
        osm_id_present=osm_id_present,
        postcode_bearing=postcode_bearing,
        key_matches_postcode_bearing=key_matches_postcode_bearing,
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "sample_size": sample_size,
            "osm_id_present": osm_id_present,
            "osm_id_present_rate": (
                osm_id_present / sample_size if sample_size else 0.0
            ),
            "key_matches": key_matches,
            "key_match_rate": key_matches / sample_size if sample_size else 0.0,
            "postcode_bearing": postcode_bearing,
            "key_match_rate_postcode_bearing": (
                key_matches_postcode_bearing / postcode_bearing
                if postcode_bearing
                else 0.0
            ),
        },
    )


sweden_address_osm_job = dg.define_asset_job(
    name="sweden_address_osm_job",
    selection=dg.AssetSelection.assets("sweden_osm_addresses_clickhouse").upstream(),
)

defs = dg.Definitions(
    assets=[
        sweden_osm_pbf_s3,
        sweden_osm_addresses_duckdb,
        sweden_osm_addresses_clickhouse,
    ],
    asset_checks=[
        sweden_osm_gazetteer_row_count_check,
        sweden_osm_gazetteer_match_key_join_check,
    ],
    jobs=[sweden_address_osm_job],
    resources={
        "sweden_address_osm_duckdb": duckdb_resource(tables.DUCKDB_PATH),
        "sweden_address_osm_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
