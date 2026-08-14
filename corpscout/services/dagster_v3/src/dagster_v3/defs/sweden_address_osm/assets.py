import tempfile
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
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
        "Builds a Sweden-only DuckDB address-point index from OSM address "
        "nodes and address-tagged ways. Coordinates are WGS84 longitude and "
        "latitude; every row retains its OSM identifier and snapshot provenance."
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


sweden_address_osm_job = dg.define_asset_job(
    name="sweden_address_osm_job",
    selection=dg.AssetSelection.assets("sweden_osm_addresses_duckdb").upstream(),
)

defs = dg.Definitions(
    assets=[sweden_osm_pbf_s3, sweden_osm_addresses_duckdb],
    jobs=[sweden_address_osm_job],
    resources={
        "sweden_address_osm_duckdb": duckdb_resource(tables.DUCKDB_PATH),
        "sweden_address_osm_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
