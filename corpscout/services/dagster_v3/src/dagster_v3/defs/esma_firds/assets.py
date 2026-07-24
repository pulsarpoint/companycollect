import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
    safe_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esma_firds import parser, source, state, tables

FIRDS_DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
FIRDS_TIMEZONE = "Europe/Paris"

FULL_RAW_ASSET = "esma_firds_full_raw_files_s3"
DELTA_RAW_ASSET = "esma_firds_delta_raw_files_s3"
CANCELLATIONS_RAW_ASSET = "esma_firds_cancellations_raw_files_s3"
DUCKDB_ASSET = "esma_firds_instrument_events_duckdb"
CURRENT_DUCKDB_ASSET = "esma_firds_instruments_current_duckdb"
CLICKHOUSE_ASSET = "esma_firds_clickhouse"

MINIMUM_CURRENT_ROWS = 1_000_000
MINIMUM_COUNTRY_COUNT = 25
MINIMUM_MIC_COUNT = 100


class FirdsDiscoveryConfig(dg.Config):
    lookback_days: int = 21
    publication_to: str = ""


@dataclass(frozen=True)
class StoredArchive:
    source_file: source.FirdsSourceFile
    archive_key: str
    retrieved_at: str
    archive_sha256: str
    archive_size_bytes: int


@dg.asset(
    name=FULL_RAW_ASSET,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "xml", "esma"},
    description=(
        "Discovers the newest complete weekly ESMA FIRDS FULINS file set and "
        "stores its immutable ZIP archives and provenance metadata in S3."
    ),
)
def esma_firds_full_raw_files_s3(
    context: dg.AssetExecutionContext,
    config: FirdsDiscoveryConfig,
    esma_firds_resource: source.FirdsResource,
    esma_firds_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    plan = _discover_download_plan(config, esma_firds_resource)
    return _sync_raw_files(
        context=context,
        source_files=plan.full.files,
        object_store=esma_firds_object_store,
        resource=esma_firds_resource,
        publication_date=plan.full.publication_date,
        file_type="FULINS",
    )


@dg.asset(
    name=DELTA_RAW_ASSET,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "xml", "esma"},
    description=(
        "Discovers complete daily ESMA FIRDS DLTINS sets since the newest "
        "weekly baseline and stores immutable ZIP archives and provenance metadata."
    ),
)
def esma_firds_delta_raw_files_s3(
    context: dg.AssetExecutionContext,
    config: FirdsDiscoveryConfig,
    esma_firds_resource: source.FirdsResource,
    esma_firds_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    plan = _discover_download_plan(config, esma_firds_resource)
    source_files = tuple(
        source_file
        for file_set in plan.deltas
        for source_file in file_set.files
    )
    return _sync_raw_files(
        context=context,
        source_files=source_files,
        object_store=esma_firds_object_store,
        resource=esma_firds_resource,
        publication_date=(
            plan.deltas[-1].publication_date
            if plan.deltas
            else plan.full.publication_date
        ),
        file_type="DLTINS",
    )


@dg.asset(
    name=CANCELLATIONS_RAW_ASSET,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "xml", "esma"},
    description=(
        "Stores the newest complete ESMA FIRDS FULCAN consolidated cancellation "
        "set as immutable reconciliation evidence."
    ),
)
def esma_firds_cancellations_raw_files_s3(
    context: dg.AssetExecutionContext,
    config: FirdsDiscoveryConfig,
    esma_firds_resource: source.FirdsResource,
    esma_firds_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    plan = _discover_download_plan(config, esma_firds_resource)
    if plan.cancellations is None:
        return dg.MaterializeResult(
            metadata={
                "file_type": "FULCAN",
                "file_count": 0,
                "downloaded_count": 0,
                "reused_count": 0,
                "status": "no_complete_set_in_discovery_window",
            }
        )
    return _sync_raw_files(
        context=context,
        source_files=plan.cancellations.files,
        object_store=esma_firds_object_store,
        resource=esma_firds_resource,
        publication_date=plan.cancellations.publication_date,
        file_type="FULCAN",
    )


@dg.asset(
    name=DUCKDB_ASSET,
    deps=[
        dg.AssetKey(FULL_RAW_ASSET),
        dg.AssetKey(DELTA_RAW_ASSET),
        dg.AssetKey(CANCELLATIONS_RAW_ASSET),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "xml", "duckdb", "esma"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Streams complete FIRDS XML file sets into auditable DuckDB staging, "
        "then builds immutable instrument event history."
    ),
)
def esma_firds_instrument_events_duckdb(
    context: dg.AssetExecutionContext,
    esma_firds_object_store: ObjectStoreResource,
    esma_firds_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    stored_archives = _read_stored_archives(esma_firds_object_store)
    selected_archives, selected_sets = _select_archives_for_ingestion(stored_archives)

    parsed_files = 0
    parsed_rows = 0
    FIRDS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with safe_duckdb_connection(esma_firds_duckdb) as connection:
        state.ensure_duckdb_tables(connection)
        pending_archives = tuple(
            archive
            for archive in selected_archives
            if not state.raw_file_is_ingested(
                connection,
                source_file_id=archive.source_file.source_file_id,
                source_file_checksum=archive.source_file.checksum,
            )
        )
        pending_file_identities = {
            (
                archive.source_file.source_file_id,
                archive.source_file.checksum,
            )
            for archive in pending_archives
        }
        for file_set in selected_sets:
            if any(
                (
                    source_file.source_file_id,
                    source_file.checksum,
                )
                in pending_file_identities
                for source_file in file_set.files
            ):
                state.invalidate_snapshot_set(
                    connection,
                    file_type=file_set.file_type,
                    publication_date=file_set.publication_date.isoformat(),
                )
    skipped_files = len(selected_archives) - len(pending_archives)

    with tempfile.TemporaryDirectory(prefix="esma_firds_parse_") as temp_dir:
        temp_path = Path(temp_dir)
        for index, stored_archive in enumerate(pending_archives):
            local_path = _download_archive(
                object_store=esma_firds_object_store,
                archive=stored_archive,
                target_path=(
                    temp_path
                    / f"{index:05d}_{stored_archive.source_file.file_name}"
                ),
            )
            with safe_duckdb_connection(esma_firds_duckdb) as connection:
                source_file = stored_archive.source_file
                if state.raw_file_is_ingested(
                    connection,
                    source_file_id=source_file.source_file_id,
                    source_file_checksum=source_file.checksum,
                ):
                    skipped_files += 1
                    continue
                record_count = _replace_archive_records(
                    connection=connection,
                    stored_archive=stored_archive,
                    local_path=local_path,
                    source_run_id=context.run_id,
                    log_info=context.log.info,
                )
                state.record_raw_file_ingestion(
                    connection,
                    source_file_id=source_file.source_file_id,
                    source_file_name=source_file.file_name,
                    source_file_type=source_file.file_type,
                    source_publication_date=source_file.publication_date.isoformat(),
                    source_file_checksum=source_file.checksum,
                    source_object_key=stored_archive.archive_key,
                    record_count=record_count,
                    source_run_id=context.run_id,
                )
                parsed_files += 1
                parsed_rows += record_count
            local_path.unlink()

        with safe_duckdb_connection(esma_firds_duckdb) as connection:
            state.ensure_duckdb_tables(connection)
            for file_set in selected_sets:
                _assert_file_set_ingested(connection, file_set)
                state.mark_snapshot_set_complete(
                    connection,
                    file_type=file_set.file_type,
                    publication_date=file_set.publication_date.isoformat(),
                    file_count=len(file_set.files),
                    source_run_id=context.run_id,
                )

            counts = state.rebuild_event_history(connection)
            coverage = state.event_coverage_metadata(connection)

    metadata = {
        **counts,
        **coverage,
        "selected_file_count": len(selected_archives),
        "parsed_file_count": parsed_files,
        "skipped_file_count": skipped_files,
        "parsed_record_count": parsed_rows,
        "complete_set_count": len(selected_sets),
    }
    context.log.info("Rebuilt FIRDS DuckDB state", extra=metadata)
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name=CURRENT_DUCKDB_ASSET,
    deps=[dg.AssetKey(DUCKDB_ASSET)],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql", "esma"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Reconstructs current EU-wide FIRDS instrument state from the newest "
        "complete full baseline and all complete later delta publications."
    ),
)
def esma_firds_instruments_current_duckdb(
    context: dg.AssetExecutionContext,
    esma_firds_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with safe_duckdb_connection(esma_firds_duckdb) as connection:
        counts = state.rebuild_current_state(
            connection,
            as_of_date=datetime.now(UTC).date(),
            minimum_current_rows=MINIMUM_CURRENT_ROWS,
            minimum_country_count=MINIMUM_COUNTRY_COUNT,
            minimum_mic_count=MINIMUM_MIC_COUNT,
        )
        country_rows = state.current_rows_by_country(connection)
        mic_rows = state.current_rows_by_mic(connection)
    metadata = {
        **counts,
        "rows_by_competent_authority_country": country_rows,
        "top_250_mics_by_rows": mic_rows,
    }
    context.log.info("Rebuilt FIRDS current instrument state", extra=counts)
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name=CLICKHOUSE_ASSET,
    deps=[dg.AssetKey(CURRENT_DUCKDB_ASSET)],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "esma"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Atomically publishes FIRDS event history and current instrument state "
        "from DuckDB into migration-owned ClickHouse tables."
    ),
)
def esma_firds_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    esma_firds_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(esma_firds_duckdb) as connection:
        counts = export_esma_firds_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )
    context.log.info("Published FIRDS tables to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_esma_firds_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
) -> dict[str, int]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=tables.CLICKHOUSE_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            tables=(
                (tables.EVENTS_TABLE, tables.EVENTS_EXPORT_COLUMNS),
                (tables.CURRENT_TABLE, tables.CURRENT_EXPORT_COLUMNS),
            ),
        )
    return {
        "event_rows": row_counts[tables.EVENTS_TABLE],
        "current_rows": row_counts[tables.CURRENT_TABLE],
    }


def _discover_download_plan(
    config: FirdsDiscoveryConfig,
    resource: source.FirdsResource,
) -> source.FirdsDownloadPlan:
    if config.lookback_days <= 0:
        raise ValueError("FIRDS lookback_days must be positive")
    publication_to = (
        date.fromisoformat(config.publication_to)
        if config.publication_to.strip()
        else datetime.now(UTC).date()
    )
    publication_from = publication_to - timedelta(days=config.lookback_days)
    files = resource.discover_files(
        publication_from=publication_from,
        publication_to=publication_to,
    )
    return source.build_download_plan(files)


def _sync_raw_files(
    *,
    context: dg.AssetExecutionContext,
    source_files: tuple[source.FirdsSourceFile, ...],
    object_store: ObjectStoreResource,
    resource: source.FirdsResource,
    publication_date: date,
    file_type: str,
) -> dg.MaterializeResult:
    results = resource.sync_files(
        files=source_files,
        object_store=object_store,
        log_info=context.log.info,
    )
    downloaded = sum(result.downloaded for result in results)
    archive_bytes = sum(result.archive_size_bytes for result in results)
    return dg.MaterializeResult(
        metadata={
            "file_type": file_type,
            "publication_date": publication_date.isoformat(),
            "file_count": len(results),
            "downloaded_count": downloaded,
            "reused_count": len(results) - downloaded,
            "archive_size_bytes": archive_bytes,
        }
    )


def _read_stored_archives(
    object_store: ObjectStoreResource,
) -> tuple[StoredArchive, ...]:
    metadata_keys = sorted(
        key
        for key in object_store.list_keys(
            tables.S3_RAW_PREFIX,
            bucket=tables.S3_BUCKET,
        )
        if key.endswith("/metadata.json")
    )
    if not metadata_keys:
        raise ValueError("No FIRDS archive metadata found in object storage")
    archives: list[StoredArchive] = []
    for metadata_key in metadata_keys:
        metadata = json.loads(
            object_store.read_bytes(
                metadata_key,
                bucket=tables.S3_BUCKET,
            ).decode("utf-8")
        )
        source_file = source.source_file_from_archive_metadata(metadata)
        archive_key = str(metadata.get("archive_key", "")).strip()
        retrieved_at = str(metadata.get("retrieved_at", "")).strip()
        archive_sha256 = str(metadata.get("archive_sha256", "")).strip().lower()
        archive_size_bytes = int(metadata.get("archive_size_bytes", 0))
        if archive_key != source.archive_object_key(source_file):
            raise ValueError(
                f"Unexpected FIRDS archive key in {metadata_key}: {archive_key}"
            )
        if retrieved_at == "":
            raise ValueError(f"Missing FIRDS retrieved_at in {metadata_key}")
        retrieved_timestamp = datetime.fromisoformat(
            retrieved_at.replace("Z", "+00:00")
        )
        if retrieved_timestamp.tzinfo is None:
            raise ValueError(
                f"FIRDS retrieved_at must include a timezone in {metadata_key}"
            )
        if len(archive_sha256) != 64:
            raise ValueError(f"Invalid FIRDS archive SHA-256 in {metadata_key}")
        if archive_size_bytes <= 0:
            raise ValueError(f"Invalid FIRDS archive size in {metadata_key}")
        archives.append(
            StoredArchive(
                source_file=source_file,
                archive_key=archive_key,
                retrieved_at=retrieved_timestamp.astimezone(UTC).isoformat(),
                archive_sha256=archive_sha256,
                archive_size_bytes=archive_size_bytes,
            )
        )
    return tuple(archives)


def _select_archives_for_ingestion(
    archives: tuple[StoredArchive, ...],
) -> tuple[tuple[StoredArchive, ...], tuple[source.FirdsFileSet, ...]]:
    latest_archive_by_slot: dict[
        tuple[str, date, str, int],
        StoredArchive,
    ] = {}
    for archive in archives:
        source_file = archive.source_file
        slot = (
            source_file.file_type,
            source_file.publication_date,
            source_file.cfi_category,
            source_file.part_number,
        )
        current = latest_archive_by_slot.get(slot)
        if current is None or (
            archive.retrieved_at,
            source_file.source_file_id,
            source_file.checksum,
        ) > (
            current.retrieved_at,
            current.source_file.source_file_id,
            current.source_file.checksum,
        ):
            latest_archive_by_slot[slot] = archive
    effective_archives = tuple(latest_archive_by_slot.values())
    archive_by_identity = {
        (
            archive.source_file.source_file_id,
            archive.source_file.checksum,
        ): archive
        for archive in effective_archives
    }
    if len(archive_by_identity) != len(effective_archives):
        raise ValueError("Duplicate FIRDS archive metadata identity in object storage")
    source_files = tuple(
        archive.source_file for archive in effective_archives
    )
    full_sets = source.complete_file_sets(source_files, file_type="FULINS")
    complete_full = [file_set for file_set in full_sets if file_set.is_complete]
    if not complete_full:
        raise ValueError("No complete stored FIRDS FULINS baseline")
    if full_sets[-1] != complete_full[-1]:
        raise ValueError(
            "Newest stored FIRDS FULINS publication is incomplete; refusing rebuild"
        )
    earliest_full = complete_full[0]
    latest_full = complete_full[-1]

    delta_sets = source.complete_file_sets(source_files, file_type="DLTINS")
    relevant_delta_sets = tuple(
        file_set
        for file_set in delta_sets
        if file_set.publication_date >= earliest_full.publication_date
    )
    incomplete_deltas = [
        file_set.publication_date.isoformat()
        for file_set in relevant_delta_sets
        if not file_set.is_complete
    ]
    if incomplete_deltas:
        raise ValueError(
            "Stored FIRDS DLTINS publications are incomplete: "
            + ", ".join(incomplete_deltas)
        )

    cancellation_sets = source.complete_file_sets(
        source_files,
        file_type="FULCAN",
    )
    complete_cancellations = [
        file_set for file_set in cancellation_sets if file_set.is_complete
    ]
    selected_sets: list[source.FirdsFileSet] = [earliest_full]
    if latest_full.publication_date != earliest_full.publication_date:
        selected_sets.append(latest_full)
    selected_sets.extend(relevant_delta_sets)
    if complete_cancellations:
        if cancellation_sets[-1] != complete_cancellations[-1]:
            raise ValueError(
                "Newest stored FIRDS FULCAN publication is incomplete; refusing rebuild"
            )
        selected_sets.append(complete_cancellations[-1])

    selected_archives = tuple(
        archive_by_identity[(source_file.source_file_id, source_file.checksum)]
        for file_set in selected_sets
        for source_file in file_set.files
    )
    return selected_archives, tuple(selected_sets)


def _download_archive(
    *,
    object_store: ObjectStoreResource,
    archive: StoredArchive,
    target_path: Path,
) -> Path:
    object_store.download_file(
        archive.archive_key,
        target_path,
        bucket=tables.S3_BUCKET,
    )
    actual_size = target_path.stat().st_size
    if actual_size != archive.archive_size_bytes:
        raise ValueError(
            f"Stored FIRDS archive size mismatch for "
            f"{archive.source_file.file_name}: "
            f"expected {archive.archive_size_bytes}, got {actual_size}"
        )
    digest = sha256()
    with target_path.open("rb") as archive_stream:
        while chunk := archive_stream.read(source.DOWNLOAD_CHUNK_BYTES):
            digest.update(chunk)
    if digest.hexdigest() != archive.archive_sha256:
        raise ValueError(
            f"Stored FIRDS archive SHA-256 mismatch for "
            f"{archive.source_file.file_name}"
        )
    return target_path


def _replace_archive_records(
    *,
    connection: Any,
    stored_archive: StoredArchive,
    local_path: Path,
    source_run_id: str,
    log_info: Any,
) -> int:
    source_file = stored_archive.source_file
    raw_table = {
        "FULINS": tables.FULL_RECORDS_RAW_TABLE,
        "DLTINS": tables.DELTA_EVENTS_RAW_TABLE,
        "FULCAN": tables.CANCELLATIONS_RAW_TABLE,
    }[source_file.file_type]
    try:
        with ZipFile(local_path) as archive:
            xml_members = [
                member
                for member in archive.infolist()
                if not member.is_dir() and member.filename.lower().endswith(".xml")
            ]
            if len(xml_members) != 1:
                raise ValueError(
                    f"FIRDS archive {source_file.file_name} must contain exactly "
                    f"one XML member, found {len(xml_members)}"
                )
            with archive.open(xml_members[0]) as xml_stream:
                records = parser.iter_firds_records(
                    xml_stream,
                    context=parser.FirdsFileContext(
                        source_file_id=source_file.source_file_id,
                        source_file_name=source_file.file_name,
                        source_file_type=source_file.file_type,
                        source_file_checksum=source_file.checksum,
                        source_publication_date=(
                            source_file.publication_date.isoformat()
                        ),
                        source_download_url=source_file.download_url,
                        source_object_key=stored_archive.archive_key,
                        source_run_id=source_run_id,
                        source_retrieved_at=stored_archive.retrieved_at,
                    ),
                )
                return state.replace_source_file_records(
                    connection,
                    table=raw_table,
                    source_file_id=source_file.source_file_id,
                    records=records,
                    log_info=log_info,
                )
    except BadZipFile as exc:
        raise ValueError(
            f"Invalid FIRDS ZIP archive: {source_file.file_name}"
        ) from exc


def _assert_file_set_ingested(
    connection: Any,
    file_set: source.FirdsFileSet,
) -> None:
    missing = [
        source_file.file_name
        for source_file in file_set.files
        if not state.raw_file_is_ingested(
            connection,
            source_file_id=source_file.source_file_id,
            source_file_checksum=source_file.checksum,
        )
    ]
    if missing:
        raise ValueError(
            "FIRDS file set is not fully ingested: " + ", ".join(missing)
        )


esma_firds_delta_refresh_job = dg.define_asset_job(
    "esma_firds_delta_refresh_job",
    selection=dg.AssetSelection.assets(
        DELTA_RAW_ASSET,
        DUCKDB_ASSET,
        CURRENT_DUCKDB_ASSET,
        CLICKHOUSE_ASSET,
    ),
)

esma_firds_weekly_refresh_job = dg.define_asset_job(
    "esma_firds_weekly_refresh_job",
    selection=dg.AssetSelection.assets(
        FULL_RAW_ASSET,
        DELTA_RAW_ASSET,
        CANCELLATIONS_RAW_ASSET,
        DUCKDB_ASSET,
        CURRENT_DUCKDB_ASSET,
        CLICKHOUSE_ASSET,
    ),
)

esma_firds_delta_daily = dg.ScheduleDefinition(
    name="esma_firds_delta_daily",
    job=esma_firds_delta_refresh_job,
    cron_schedule="40 10 * * *",
    execution_timezone=FIRDS_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

esma_firds_full_weekly = dg.ScheduleDefinition(
    name="esma_firds_full_weekly",
    job=esma_firds_weekly_refresh_job,
    cron_schedule="50 12 * * 0",
    execution_timezone=FIRDS_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            esma_firds_full_raw_files_s3,
            esma_firds_delta_raw_files_s3,
            esma_firds_cancellations_raw_files_s3,
            esma_firds_instrument_events_duckdb,
            esma_firds_instruments_current_duckdb,
            esma_firds_clickhouse,
        ],
        jobs=[
            esma_firds_delta_refresh_job,
            esma_firds_weekly_refresh_job,
        ],
        schedules=[
            esma_firds_delta_daily,
            esma_firds_full_weekly,
        ],
        resources={
            "esma_firds_resource": source.FirdsResource(),
            "esma_firds_object_store": ObjectStoreResource(
                bucket=tables.S3_BUCKET
            ),
            "esma_firds_duckdb": duckdb_resource(FIRDS_DUCKDB_PATH),
        },
    )
