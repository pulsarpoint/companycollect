import calendar
import json
import time
from datetime import date
from pathlib import Path

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.ted_procurement import tables
from dagster_v3.defs.ted_procurement.client import (
    XML_THROTTLE_SECONDS,
    build_monthly_query,
    fetch_notice_xml,
    iter_search_notices,
)
from dagster_v3.defs.ted_procurement.parser import parse_award_notice_xml
from dagster_v3.defs.ted_procurement.publish import (
    apply_ted_usd_conversion,
    build_publish_tables,
    export_ted_clickhouse,
    list_parsed_partitions,
    normalize_national_id,
    partition_duckdb_path,
)

GROUP_NAME = "ted_procurement"
TED_DUCKDB_POOL = "ted_procurement_duckdb"
TED_DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME

# eForms became mandatory on TED late 2023; earlier notices use the legacy
# schema this parser does not support (design doc).
MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2024-01-01", end_offset=1
)
BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


def _month_bounds(partition_key: str) -> tuple[str, str]:
    start = date.fromisoformat(partition_key)
    last_day = calendar.monthrange(start.year, start.month)[1]
    end = start.replace(day=last_day)
    next_day = date.fromordinal(end.toordinal() + 1)
    return start.strftime("%Y%m%d"), next_day.strftime("%Y%m%d")


def _first_text(value: object) -> str:
    """Collapse the API's multilingual dict / list value shapes to one string."""
    if value is None:
        return ""
    if isinstance(value, dict):
        for preferred in ("eng", "fin", "swe"):
            if value.get(preferred):
                return _first_text(value[preferred])
        for inner in value.values():
            return _first_text(inner)
        return ""
    if isinstance(value, list):
        return _first_text(value[0]) if value else ""
    return str(value)


@dg.asset(
    name="ted_monthly_snapshot",
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=GROUP_NAME,
    kinds={"python", "s3"},
    pool=TED_DUCKDB_POOL,
    description=(
        "One publication month of TED award notices for the configured "
        "countries: search-API listing + per-notice eForms XML mirrored to S3 "
        "with a manifest and _SUCCESS marker. Existing XML objects are reused."
    ),
)
def ted_monthly_snapshot(
    context: dg.AssetExecutionContext,
    ted_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    date_start, date_end = _month_bounds(partition_key)
    prefix = tables.s3_partition_prefix(partition_key)
    ted_object_store.ensure_bucket()

    listing_by_scope: dict[tuple[str, str], dict[str, str]] = {}
    manifest_by_publication: dict[str, dict[str, str]] = {}
    downloaded = reused = 0
    for country in tables.COUNTRIES:
        query = build_monthly_query(
            place_codes=(country.place_code,),
            date_start=date_start,
            date_end=date_end,
        )
        context.log.info("TED search: %s", query)
        for notice in iter_search_notices(query=query):
            publication_number = str(notice.get("publication-number", ""))
            if publication_number == "":
                continue
            listing_by_scope[(country.country_iso2, publication_number)] = {
                "publication_number": publication_number,
                "publication_date": str(notice.get("publication-date", "")),
                "notice_type": str(notice.get("notice-type", "")),
                "buyer_name": _first_text(notice.get("buyer-name")),
                "notice_title": _first_text(notice.get("notice-title")),
                "total_value": _first_text(notice.get("total-value")),
                "total_value_currency": _first_text(notice.get("total-value-cur")),
                "country_iso2": country.country_iso2,
                "place_country": country.place_code,
            }
            if publication_number in manifest_by_publication:
                continue
            xml_key = f"{prefix}xml/{publication_number}.xml"
            if ted_object_store.exists(xml_key):
                reused += 1
            else:
                ted_object_store.write_bytes(
                    xml_key, fetch_notice_xml(publication_number=publication_number)
                )
                downloaded += 1
                time.sleep(XML_THROTTLE_SECONDS)
            manifest_by_publication[publication_number] = {
                "publication_number": publication_number,
                "xml_key": xml_key,
            }

    listing_rows = list(listing_by_scope.values())
    manifest = list(manifest_by_publication.values())
    ted_object_store.write_bytes(
        f"{prefix}listing.jsonl",
        "\n".join(json.dumps(row, ensure_ascii=False) for row in listing_rows).encode(),
    )
    ted_object_store.write_bytes(
        f"{prefix}manifest.jsonl",
        "\n".join(json.dumps(row) for row in manifest).encode(),
    )
    ted_object_store.write_json(
        f"{prefix}_SUCCESS.json",
        json.dumps({"notices": len(listing_rows), "partition": partition_key}),
    )
    return dg.MaterializeResult(
        metadata={
            "notices": len(listing_rows),
            "xml_downloaded": downloaded,
            "xml_reused": reused,
            "s3_prefix": f"s3://{tables.S3_BUCKET}/{prefix}",
        }
    )


@dg.asset(
    name="ted_monthly_duckdb",
    deps=[dg.AssetKey("ted_monthly_snapshot")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=TED_DUCKDB_POOL,
    description=(
        "Parses one month of eForms XML from S3 into a partition DuckDB: "
        "listing, notice_docs (buyer refs), organizations (with normalized "
        "national ids), winner_links."
    ),
)
def ted_monthly_duckdb(
    context: dg.AssetExecutionContext,
    ted_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    prefix = tables.s3_partition_prefix(partition_key)
    if not ted_object_store.exists(f"{prefix}_SUCCESS.json"):
        raise ValueError(
            f"TED snapshot partition {partition_key} has no _SUCCESS marker — "
            "materialize ted_monthly_snapshot first"
        )
    listing = [
        json.loads(line)
        for line in ted_object_store.read_bytes(f"{prefix}listing.jsonl")
        .decode()
        .splitlines()
        if line.strip()
    ]
    manifest = [
        json.loads(line)
        for line in ted_object_store.read_bytes(f"{prefix}manifest.jsonl")
        .decode()
        .splitlines()
        if line.strip()
    ]

    docs: list[tuple[str, str]] = []
    orgs: list[tuple[str, str, str, str, str, str]] = []
    winner_links: list[tuple[str, str, str, int, str, str, str]] = []
    for entry in manifest:
        publication_number = entry["publication_number"]
        parsed = parse_award_notice_xml(ted_object_store.read_bytes(entry["xml_key"]))
        docs.append((publication_number, parsed.buyer_org_ref))
        for org in parsed.organizations:
            orgs.append(
                (
                    publication_number,
                    org.org_ref,
                    org.name,
                    org.national_id_raw,
                    normalize_national_id(org.country, org.national_id_raw),
                    org.country,
                )
            )
        for winner in parsed.winners:
            winner_links.append(
                (
                    publication_number,
                    winner.lot_id,
                    winner.tender_id,
                    winner.winner_ordinal,
                    winner.org_ref,
                    winner.awarded_amount,
                    winner.awarded_currency,
                )
            )

    target = partition_duckdb_path(partition_key)
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(target))
    try:
        connection.execute(
            "create or replace table listing (publication_number varchar, "
            "publication_date varchar, notice_type varchar, buyer_name varchar, "
            "notice_title varchar, total_value varchar, total_value_currency varchar, "
            "country_iso2 varchar, place_country varchar)"
        )
        if listing:
            connection.executemany(
                "insert into listing values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        row["publication_number"],
                        row["publication_date"],
                        row["notice_type"],
                        row["buyer_name"],
                        row["notice_title"],
                        row["total_value"],
                        row["total_value_currency"],
                        row["country_iso2"],
                        row["place_country"],
                    )
                    for row in listing
                ],
            )
        connection.execute(
            "create or replace table notice_docs (publication_number varchar, "
            "buyer_org_ref varchar)"
        )
        if docs:
            connection.executemany("insert into notice_docs values (?, ?)", docs)
        connection.execute(
            "create or replace table organizations (publication_number varchar, "
            "org_ref varchar, name varchar, national_id_raw varchar, "
            "national_id varchar, country varchar)"
        )
        if orgs:
            connection.executemany(
                "insert into organizations values (?, ?, ?, ?, ?, ?)", orgs
            )
        connection.execute(
            "create or replace table winner_links (publication_number varchar, "
            "lot_id varchar, tender_id varchar, winner_ordinal integer, "
            "org_ref varchar, awarded_amount varchar, awarded_currency varchar)"
        )
        if winner_links:
            connection.executemany(
                "insert into winner_links values (?, ?, ?, ?, ?, ?, ?)", winner_links
            )
    finally:
        connection.close()
    return dg.MaterializeResult(
        metadata={
            "notices": len(listing),
            "organizations": len(orgs),
            "winner_links": len(winner_links),
            "duckdb_path": str(target),
        }
    )


@dg.asset(
    name="ted_publish_clickhouse",
    deps=[dg.AssetKey("ted_monthly_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=TED_DUCKDB_POOL,
    metadata={
        "tables": [
            tables.QUALIFIED_TED_NOTICES_TABLE,
            tables.QUALIFIED_TED_NOTICE_WINNERS_TABLE,
        ]
    },
    description=(
        "Unions every parsed monthly partition, resolves buyer/winner "
        "organizations, converts to USD and replaces corpscout.ted_notices + "
        "corpscout.ted_notice_winners."
    ),
)
def ted_publish_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    ted_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    TED_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ted_procurement_duckdb.get_connection() as connection:
        counts = build_publish_tables(
            duckdb_connection=connection,
            partitions=list_parsed_partitions(),
            source_run_id=context.run_id,
            log=context.log.info,
        )
        fx_counts = apply_ted_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    with read_only_duckdb_connection(ted_procurement_duckdb) as connection:
        export_counts = export_ted_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata={**counts, **fx_counts, **export_counts})


ted_procurement_job = dg.define_asset_job(
    "ted_procurement_job",
    selection=dg.AssetSelection.assets("ted_monthly_snapshot", "ted_monthly_duckdb"),
    partitions_def=MONTHLY_PARTITIONS,
)
ted_publish_job = dg.define_asset_job(
    "ted_publish_job",
    selection=dg.AssetSelection.assets("ted_publish_clickhouse"),
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[ted_procurement_job],
    request_job=ted_publish_job,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def ted_publish_after_monthly_parse(
    context: dg.RunStatusSensorContext,
) -> dg.RunRequest:
    return dg.RunRequest(run_key=context.dagster_run.run_id)


# Monthly: refresh the just-closed month then publish. Backfills run from the UI.
ted_procurement_schedule = dg.ScheduleDefinition(
    name="ted_procurement_schedule",
    job=ted_procurement_job,
    cron_schedule="35 5 3 * *",
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[ted_monthly_snapshot, ted_monthly_duckdb, ted_publish_clickhouse],
    jobs=[ted_procurement_job, ted_publish_job],
    schedules=[ted_procurement_schedule],
    sensors=[ted_publish_after_monthly_parse],
    resources={
        "ted_procurement_duckdb": duckdb_resource(TED_DUCKDB_PATH),
        "ted_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
