import uuid
from datetime import UTC, date, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.instrument_issuer import tables

INSTRUMENT_ISSUER_UPSTREAM_ASSET_KEYS = ("esma_firds_clickhouse",)

_REQUIRED_CLICKHOUSE_TABLES = (
    tables.INSTRUMENT_ISSUER_TABLE,
    "firds_instrument_events",
)

_QUALITY_COLUMNS = (
    "row_count",
    "isin_count",
    "issuer_count",
    "mapping_key_count",
    "ambiguous_isin_count",
    "invalid_identity_rows",
    "malformed_isin_rows",
    "malformed_issuer_rows",
    "earliest_first_seen_date",
    "latest_last_seen_date",
)

_ISIN_LENGTH = 12
_LEI_LENGTH = 20


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def build_firds_instrument_issuer_sql(stage_table: str) -> str:
    """Project durable ISIN to issuer identity out of FIRDS event history.

    Reads firds_instrument_events rather than firds_instruments_current on
    purpose. Who issued an ISIN does not change when the instrument stops
    trading, so sourcing identity from current state would erase the mapping on
    every delisting. Terminated and cancelled events are retained.

    Neither country nor CFI category is filtered. FIRDS is instrument-scoped, so
    EU-admitted instruments of non-EU issuers resolve here as well.

    Venue and instrument-classification facts are deliberately absent: those
    belong to corpscout.instrument_venues.
    """
    columns = ", ".join(tables.INSTRUMENT_ISSUER_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
WITH firds_identity AS
(
    SELECT
        upperUTF8(trimBoth(e.isin)) AS isin,
        upperUTF8(trimBoth(e.issuer_lei)) AS issuer_id,
        e.source_publication_date AS source_publication_date
    FROM corpscout.firds_instrument_events AS e
    WHERE trimBoth(e.isin) != ''
      AND trimBoth(e.issuer_lei) != ''
)
SELECT
    isin,
    '{tables.LEI_ISSUER_SCHEME}' AS issuer_scheme,
    issuer_id,
    '{tables.FIRDS_MAPPING_SOURCE}' AS mapping_source,
    min(source_publication_date) AS first_seen_date,
    max(source_publication_date) AS last_seen_date,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM firds_identity
GROUP BY
    isin,
    issuer_id"""


def _quality_sql(stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(isin) AS isin_count,
    uniqExact(issuer_id) AS issuer_count,
    uniqExact((isin, issuer_scheme, issuer_id, mapping_source))
        AS mapping_key_count,
    (
        SELECT count()
        FROM
        (
            SELECT isin
            FROM {stage_table}
            GROUP BY isin
            HAVING uniqExact((issuer_scheme, issuer_id)) > 1
        )
    ) AS ambiguous_isin_count,
    countIf(
        isin = '' OR issuer_scheme = '' OR issuer_id = '' OR mapping_source = ''
    ) AS invalid_identity_rows,
    countIf(length(isin) != {_ISIN_LENGTH}) AS malformed_isin_rows,
    countIf(
        issuer_scheme = '{tables.LEI_ISSUER_SCHEME}'
        AND length(issuer_id) != {_LEI_LENGTH}
    ) AS malformed_issuer_rows,
    min(first_seen_date) AS earliest_first_seen_date,
    max(last_seen_date) AS latest_last_seen_date
FROM {stage_table}"""


def _quality_metadata(row: tuple[object, ...]) -> dict[str, object]:
    return {column: value for column, value in zip(_QUALITY_COLUMNS, row, strict=True)}


def _validate_quality(quality: dict[str, object]) -> None:
    """Gate on structural faults only.

    Malformed ISIN/LEI syntax is reported but never fatal: it is upstream data
    noise, and refusing to publish because of it would blank a populated table
    over rows that consumers can filter themselves.
    """
    row_count = int(quality["row_count"])
    mapping_key_count = int(quality["mapping_key_count"])
    invalid_identity_rows = int(quality["invalid_identity_rows"])

    if row_count == 0:
        raise ValueError("FIRDS projection produced no instrument issuer mappings")
    if mapping_key_count != row_count:
        raise ValueError(
            "Instrument issuer grain mismatch: "
            f"rows={row_count} unique_keys={mapping_key_count}"
        )
    if invalid_identity_rows != 0:
        raise ValueError(
            "Instrument issuer mappings contain invalid identity rows: "
            f"{invalid_identity_rows}"
        )


def _as_iso_date(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value or "")


def replace_instrument_issuer_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild the cross-source ISIN to issuer identity mapping."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=_REQUIRED_CLICKHOUSE_TABLES,
    )
    stage_name = f"_tmp_{tables.INSTRUMENT_ISSUER_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.INSTRUMENT_ISSUER_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_firds_instrument_issuer_sql(qualified_stage),
                {
                    "source_run_id": source_run_id,
                    "resolved_at": resolved_at,
                },
            )
            quality = _quality_metadata(client.execute(_quality_sql(qualified_stage))[0])
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

    return {
        **quality,
        "earliest_first_seen_date": _as_iso_date(quality["earliest_first_seen_date"]),
        "latest_last_seen_date": _as_iso_date(quality["latest_last_seen_date"]),
        "table": tables.QUALIFIED_INSTRUMENT_ISSUER_TABLE,
        "source_run_id": source_run_id,
    }


@dg.asset(
    name="instrument_issuer_clickhouse",
    deps=[dg.AssetKey(key) for key in INSTRUMENT_ISSUER_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="instrument_issuer_clickhouse",
    metadata={"table": tables.QUALIFIED_INSTRUMENT_ISSUER_TABLE},
    description=(
        "Rebuilds the cross-source ISIN to issuer identity mapping from FIRDS "
        "event history. Issuer identity only: a row is not evidence that the "
        "instrument is currently traded."
    ),
)
def instrument_issuer_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_instrument_issuer_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Rebuilt instrument issuer mappings: rows=%s isins=%s ambiguous_isins=%s",
        metadata["row_count"],
        metadata["isin_count"],
        metadata["ambiguous_isin_count"],
    )
    return dg.MaterializeResult(metadata=metadata)


instrument_issuer_job = dg.define_asset_job(
    "instrument_issuer_job",
    selection=dg.AssetSelection.assets("instrument_issuer_clickhouse"),
)

defs = dg.Definitions(
    assets=[instrument_issuer_clickhouse],
    jobs=[instrument_issuer_job],
)
