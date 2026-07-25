import uuid
from datetime import UTC, date, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.instrument_venues import tables
from dagster_v3.defs.instrument_venues.eodhd import (
    build_eodhd_instrument_venues_sql,
)
from dagster_v3.defs.instrument_venues.firds import (
    build_firds_instrument_venues_sql,
)

INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS = (
    "esma_firds_clickhouse",
    "eodhd_reference_complete",
)

_REQUIRED_CLICKHOUSE_TABLES = (
    tables.INSTRUMENT_VENUES_TABLE,
    "firds_instruments_current",
    "eodhd_symbols",
    "eodhd_symbol_mics",
    "eodhd_exchanges",
)

_QUALITY_COLUMNS = (
    "row_count",
    "isin_count",
    "mic_count",
    "venue_key_count",
    "firds_rows",
    "eodhd_rows",
    "invalid_rows",
    "latest_source_publication_date",
)


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def _quality_sql(stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(isin) AS isin_count,
    uniqExact(mic) AS mic_count,
    uniqExact((isin, mic, venue_source)) AS venue_key_count,
    countIf(venue_source = '{tables.FIRDS_VENUE_SOURCE}') AS firds_rows,
    countIf(venue_source = '{tables.EODHD_VENUE_SOURCE}') AS eodhd_rows,
    countIf(isin = '' OR mic = '' OR venue_source = '' OR evidence_tier = '')
        AS invalid_rows,
    max(source_publication_date) AS latest_source_publication_date
FROM {stage_table}"""


def _validate_quality(quality: dict[str, object]) -> None:
    row_count = int(quality["row_count"])
    venue_key_count = int(quality["venue_key_count"])
    invalid_rows = int(quality["invalid_rows"])

    if row_count == 0:
        raise ValueError(
            "Instrument venue projection produced no instrument venue rows"
        )
    if venue_key_count != row_count:
        raise ValueError(
            "Instrument venue grain mismatch: "
            f"rows={row_count} unique_keys={venue_key_count}"
        )
    if invalid_rows != 0:
        raise ValueError(f"Instrument venues contain invalid rows: {invalid_rows}")
    for source_column in ("firds_rows", "eodhd_rows"):
        if int(quality[source_column]) == 0:
            raise ValueError(
                f"Instrument venue source contributed no rows: {source_column}"
            )


def replace_instrument_venues_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild the cross-source instrument/venue table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=_REQUIRED_CLICKHOUSE_TABLES,
    )
    stage_name = f"_tmp_{tables.INSTRUMENT_VENUES_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.INSTRUMENT_VENUES_TABLE)
    parameters = {"source_run_id": source_run_id, "resolved_at": resolved_at}

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_firds_instrument_venues_sql(qualified_stage), parameters
            )
            client.execute(
                build_eodhd_instrument_venues_sql(qualified_stage), parameters
            )
            row = client.execute(_quality_sql(qualified_stage))[0]
            quality = {
                column: value
                for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
            }
            _validate_quality(quality)
            client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise

    latest = quality["latest_source_publication_date"]
    return {
        **quality,
        "latest_source_publication_date": (
            latest.isoformat()
            if isinstance(latest, (date, datetime))
            else str(latest or "")
        ),
        "table": tables.QUALIFIED_INSTRUMENT_VENUES_TABLE,
        "source_run_id": source_run_id,
    }


@dg.asset(
    name="instrument_venues_clickhouse",
    deps=[dg.AssetKey(key) for key in INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="instrument_venues_clickhouse",
    metadata={"table": tables.QUALIFIED_INSTRUMENT_VENUES_TABLE},
    description=(
        "Rebuilds the cross-source instrument and venue table from FIRDS "
        "current admissions and EODHD symbol/MIC pairs. Says what trades "
        "where, not who owns it."
    ),
)
def instrument_venues_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_instrument_venues_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Rebuilt instrument venues: rows=%s firds=%s eodhd=%s isins=%s",
        metadata["row_count"],
        metadata["firds_rows"],
        metadata["eodhd_rows"],
        metadata["isin_count"],
    )
    return dg.MaterializeResult(metadata=metadata)


instrument_venues_job = dg.define_asset_job(
    "instrument_venues_job",
    selection=dg.AssetSelection.assets("instrument_venues_clickhouse"),
)

defs = dg.Definitions(
    assets=[instrument_venues_clickhouse],
    jobs=[instrument_venues_job],
)
