from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import (
    address_canonicalization,
    address_geocoding,
    geocode_store,
    shared_address_geocoding,
    shared_addresses,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)

GROUP_NAME = "sweden_company"
GEOCODE_ASSET_KEY = "sweden_company_address_geocodes_clickhouse"
GEOCODE_RESULT_ASSET_KEY = "sweden_company_address_geocode_results_clickhouse"
CANONICAL_DUCKDB_ASSET_KEY = "sweden_company_canonical_addresses_duckdb"
CANONICAL_CLICKHOUSE_ASSET_KEY = "sweden_company_canonical_addresses_clickhouse"
SHARED_ADDRESSES_DUCKDB_ASSET_KEY = "sweden_shared_addresses_duckdb"
SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY = "sweden_shared_addresses_clickhouse"
SHARED_GEOCODE_DUCKDB_ASSET_KEY = "sweden_shared_address_osm_matches_duckdb"
SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY = "sweden_address_geocodes_clickhouse"
ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY = "sweden_address_resolution_golden_evaluation"
ADDRESS_RESOLUTION_SHADOW_ASSET_KEY = "sweden_address_resolution_shadow_duckdb"
ADDRESS_RESOLUTION_CURRENT_ASSET_KEY = "sweden_address_resolution_current_duckdb"
GEOCODE_STORE_ASSET_KEY = "sweden_address_geocode_store_clickhouse"
GEOCODE_STORE_BACKFILL_ASSET_KEY = "sweden_address_geocode_store_backfill_clickhouse"
WEEKLY_CRON_SCHEDULE = "5 4 * * 2"
WEEKLY_EXECUTION_TIMEZONE = "Europe/Stockholm"
MAX_OSM_SNAPSHOT_AGE = timedelta(days=9)
MIN_EXACT_MATCH_RATE_PERCENT = 5.0
MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS = 2.0


@dataclass(frozen=True)
class SwedenAddressGeocodeStats:
    company_addresses: int
    matched_exact: int
    unique_geocodes: int
    source_run_count: int
    latest_osm_snapshot_at: datetime | None

    @property
    def exact_match_rate_percent(self) -> float:
        if self.company_addresses == 0:
            return 0.0
        return 100.0 * self.matched_exact / self.company_addresses


def fetch_sweden_address_geocode_stats(client: Any) -> SwedenAddressGeocodeStats:
    [
        (
            matched_exact,
            unique_geocodes,
            source_run_count,
            latest_osm_snapshot_at,
        )
    ] = client.execute(
        f"""
        SELECT
            count(),
            uniqExact(tuple(company_id, address_key)),
            uniqExact(source_run_id),
            max(source_snapshot_at)
        FROM {address_geocoding.QUALIFIED_CLICKHOUSE_TABLE}
        """
    )
    [(company_addresses,)] = client.execute(
        f"""
        SELECT count()
        FROM {address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE}
        """
    )
    return SwedenAddressGeocodeStats(
        company_addresses=int(company_addresses),
        matched_exact=int(matched_exact),
        unique_geocodes=int(unique_geocodes),
        source_run_count=int(source_run_count),
        latest_osm_snapshot_at=latest_osm_snapshot_at,
    )


def fetch_sweden_address_geocode_result_counts(client: Any) -> dict[str, int]:
    rows = client.execute(
        f"""
        SELECT match_status, count()
        FROM {address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE}
        GROUP BY match_status
        ORDER BY match_status
        """
    )
    return {str(status): int(count) for status, count in rows}


def exact_match_rate_is_stable(
    *,
    current_percent: float,
    previous_percent: float | None,
) -> bool:
    if current_percent < MIN_EXACT_MATCH_RATE_PERCENT:
        return False
    if previous_percent is None:
        return True
    return (
        abs(current_percent - previous_percent)
        <= MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS
    )


def osm_snapshot_is_fresh(*, snapshot_at: datetime | None, now: datetime) -> bool:
    if snapshot_at is None:
        return False
    normalized_snapshot_at = (
        snapshot_at.replace(tzinfo=UTC)
        if snapshot_at.tzinfo is None
        else snapshot_at.astimezone(UTC)
    )
    return now.astimezone(UTC) - normalized_snapshot_at <= MAX_OSM_SNAPSHOT_AGE


def previous_exact_match_rate_percent(
    instance: dg.DagsterInstance,
    *,
    current_run_id: str,
) -> float | None:
    records = instance.fetch_materializations(
        dg.AssetKey(GEOCODE_ASSET_KEY),
        limit=10,
    ).records
    for record in records:
        if record.event_log_entry.run_id == current_run_id:
            continue
        materialization = record.asset_materialization
        if materialization is None:
            continue
        value = materialization.metadata.get("exact_match_rate_percent")
        numeric_value = getattr(value, "value", None)
        if isinstance(numeric_value, int | float):
            return float(numeric_value)
    return None


@dg.asset(
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=osm_tables.DUCKDB_POOL,
    description=(
        "Normalizes current Swedish source-address observations into one "
        "canonical address per company and retains a member row for every "
        "Bolagsverket and SCB record."
    ),
)
def sweden_company_canonical_addresses_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    normalized_at = datetime.now(UTC)
    with clickhouse.get_connection() as clickhouse_client:
        with sweden_address_osm_duckdb.get_connection() as connection:
            counts = (
                address_canonicalization.replace_sweden_company_canonical_addresses(
                    connection=connection,
                    clickhouse_client=clickhouse_client,
                    normalization_run_id=context.run_id,
                    normalized_at=normalized_at,
                    log=context.log.info,
                )
            )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "canonical_table": (
                address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE
            ),
            "member_table": address_canonicalization.QUALIFIED_ADDRESS_MEMBERS_TABLE,
            "normalized_at": normalized_at.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(CANONICAL_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "canonical_table": (
            address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE
        ),
        "member_table": (
            address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE
        ),
    },
    description=(
        "Atomically publishes canonical Sweden company addresses and their "
        "complete source-observation membership to ClickHouse."
    ),
)
def sweden_company_canonical_addresses_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_canonicalization.CLICKHOUSE_DATABASE,
        tables=(
            address_canonicalization.CANONICAL_ADDRESSES_TABLE,
            address_canonicalization.ADDRESS_MEMBERS_TABLE,
        ),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_canonicalization.ENRICHMENT_SCHEMA,
                clickhouse_database=address_canonicalization.CLICKHOUSE_DATABASE,
                tables=(
                    (
                        address_canonicalization.CANONICAL_ADDRESSES_TABLE,
                        address_canonicalization.CANONICAL_ADDRESS_COLUMNS,
                    ),
                    (
                        address_canonicalization.ADDRESS_MEMBERS_TABLE,
                        address_canonicalization.ADDRESS_MEMBER_COLUMNS,
                    ),
                ),
                log=context.log.info,
            )
    return dg.MaterializeResult(
        metadata={
            "canonical_addresses": rows[
                address_canonicalization.CANONICAL_ADDRESSES_TABLE
            ],
            "source_members": rows[address_canonicalization.ADDRESS_MEMBERS_TABLE],
            "canonical_table": (
                address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE
            ),
            "member_table": (
                address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE
            ),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(CANONICAL_CLICKHOUSE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=osm_tables.DUCKDB_POOL,
    description=(
        "Builds one country-level identity per normalized address and one "
        "reviewable link for every company using that address."
    ),
)
def sweden_shared_addresses_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_canonicalization.CLICKHOUSE_DATABASE,
        tables=(
            shared_addresses.SHARED_ADDRESSES_TABLE,
            shared_addresses.COMPANY_ADDRESS_LINKS_TABLE,
        ),
    )
    built_at = datetime.now(UTC)
    with clickhouse.get_connection() as clickhouse_client:
        reviews = clickhouse_client.execute(
            f"""
            SELECT
                company_id,
                toString(address_id),
                review_status,
                reviewed_at,
                reviewed_by,
                review_note
            FROM {shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE}
            WHERE review_status != 'unreviewed'
            """
        )
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = shared_addresses.replace_sweden_shared_addresses(
            connection=connection,
            company_address_link_reviews=reviews,
            address_identity_run_id=context.run_id,
            address_identity_built_at=built_at,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "shared_address_table": (shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE),
            "company_address_link_table": (
                shared_addresses.QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE
            ),
            "carried_forward_reviews": len(reviews),
            "address_identity_built_at": built_at.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(SHARED_ADDRESSES_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "shared_address_table": (
            shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE
        ),
        "company_address_link_table": (
            shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
        ),
    },
    description=(
        "Atomically publishes shared Sweden addresses and reviewable "
        "company-address associations to ClickHouse."
    ),
)
def sweden_shared_addresses_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_canonicalization.CLICKHOUSE_DATABASE,
        tables=(
            shared_addresses.SHARED_ADDRESSES_TABLE,
            shared_addresses.COMPANY_ADDRESS_LINKS_TABLE,
        ),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_canonicalization.ENRICHMENT_SCHEMA,
                clickhouse_database=address_canonicalization.CLICKHOUSE_DATABASE,
                tables=(
                    (
                        shared_addresses.SHARED_ADDRESSES_TABLE,
                        shared_addresses.SHARED_ADDRESS_COLUMNS,
                    ),
                    (
                        shared_addresses.COMPANY_ADDRESS_LINKS_TABLE,
                        shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS,
                    ),
                ),
                log=context.log.info,
            )
    return dg.MaterializeResult(
        metadata={
            "shared_addresses": rows[shared_addresses.SHARED_ADDRESSES_TABLE],
            "company_address_links": rows[shared_addresses.COMPANY_ADDRESS_LINKS_TABLE],
            "shared_address_table": (
                shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE
            ),
            "company_address_link_table": (
                shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
            ),
        }
    )


@dg.asset(
    deps=[
        dg.AssetKey(SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    description=(
        "Matches every distinct Sweden address identity to OSM once, retaining "
        "exact building, approximate site, area, or street, ambiguous, unmatched, "
        "invalid, foreign, and postal-box outcomes."
    ),
)
def sweden_shared_address_osm_matches_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    matched_at = datetime.now(UTC)
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = shared_address_geocoding.replace_sweden_shared_address_osm_matches(
            connection=connection,
            geocode_run_id=context.run_id,
            matched_at=matched_at,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_table": (
                shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
            ),
            "matched_at": matched_at.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(ADDRESS_RESOLUTION_CURRENT_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "table": (shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE)
    },
    description=(
        "Atomically publishes one complete OSM geocoding outcome per shared "
        "Sweden address identity."
    ),
)
def sweden_address_geocodes_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_canonicalization.CLICKHOUSE_DATABASE,
        tables=(shared_address_geocoding.ADDRESS_GEOCODES_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_canonicalization.ENRICHMENT_SCHEMA,
                duckdb_table=shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
                clickhouse_database=address_canonicalization.CLICKHOUSE_DATABASE,
                clickhouse_table=shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
                columns=shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS,
                truncate=True,
                log=context.log.info,
            )
            status_counts = {
                str(status): int(count)
                for status, count in clickhouse_client.execute(
                    f"""
                    SELECT match_status, count()
                    FROM {
                        shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
                    }
                    GROUP BY match_status
                    ORDER BY match_status
                    """
                )
            }
            [(geolocated,)] = clickhouse_client.execute(
                f"""
                SELECT count()
                FROM {
                    shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
                }
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """
            )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "geolocated": int(geolocated),
            "exact_match_rate_percent": (
                100.0 * status_counts.get("matched_exact", 0) / rows
                if rows > 0
                else 0.0
            ),
            **status_counts,
            "table": (
                shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
            ),
        }
    )


def build_store_append_regression_sql() -> str:
    """Rows that would swallow the outcomes this run just appended.

    ReplacingMergeTree(matched_at) keeps the LARGEST matched_at per key. A pre-existing row
    for one of this run's key triples carrying a newer instant makes this run's outcome
    invisible the moment it lands -- a silent no-op no row count would show. Equal instants
    are this run's own rows and are content-identical by the versioning contract, so the
    comparison is strictly greater-than.
    """
    return f"""SELECT count()
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
WHERE (address_id, policy_version, reference_md5) IN (
        SELECT address_id, policy_version, reference_md5
        FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
        WHERE geocode_run_id = %(geocode_run_id)s
      )
  AND matched_at > %(matched_at)s"""


def build_geocode_store_backfill_sql() -> str:
    """The one-time append of today's serving rows as policy-v5 outcomes.

    matched_at is COPIED from the serving row, never restamped. That instant is what the
    outcome actually claims, and copying it is what makes a second backfill run replace each
    row with byte-identical content instead of bumping 2.09M versions. source_md5 is
    promoted to the reference_md5 key role -- the pre-flight below refuses to run if any
    serving row is missing it, because an empty key column is not attributable.
    """
    carried = ",\n    ".join(
        column for column in geocode_store.SERVING_COLUMNS if column != "address_id"
    )
    columns = ", ".join(geocode_store.STORE_COLUMNS)
    return f"""INSERT INTO {
        geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
    } ({columns})
SELECT
    address_id,
    %(policy_version)s AS policy_version,
    ifNull(source_md5, '') AS reference_md5,
    {carried}
FROM {shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE}"""


GEOCODE_STORE_BACKFILL_SQL = build_geocode_store_backfill_sql()

BACKFILL_PREFLIGHT_SQL = f"""SELECT
    count(),
    countIf(isNull(source_md5) OR source_md5 = '')
FROM {shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE}"""


@dg.asset(
    name=GEOCODE_STORE_ASSET_KEY,
    deps=[dg.AssetKey(ADDRESS_RESOLUTION_CURRENT_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "Appends this run's resolved Sweden address outcomes to the permanent "
        "versioned geocode store, stamped with the resolver policy and the OSM "
        "reference snapshot they were computed against."
    ),
)
def sweden_address_geocode_store_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(geocode_store.GEOCODE_STORE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        [(appended, matched_at, policy_versions, references)] = connection.execute(
            f"""
            select
                count(*),
                max(matched_at),
                count(distinct policy_version),
                count(distinct reference_md5)
            from {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
            """
        ).fetchall()
        if int(policy_versions) > 1 or int(references) > 1:
            raise ValueError(
                "One promotion appends outcomes for one policy and one reference"
            )
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=geocode_store.ENRICHMENT_SCHEMA,
                duckdb_table=geocode_store.GEOCODE_APPEND_TABLE,
                clickhouse_database=geocode_store.CLICKHOUSE_DATABASE,
                clickhouse_table=geocode_store.GEOCODE_STORE_TABLE,
                columns=geocode_store.STORE_COLUMNS,
                truncate=False,
                log=context.log.info,
            )
            [(regressions,)] = clickhouse_client.execute(
                build_store_append_regression_sql(),
                {"geocode_run_id": context.run_id, "matched_at": matched_at},
            )
            [(store_rows, store_identities)] = clickhouse_client.execute(
                f"""
                SELECT count(), uniqExact(address_id)
                FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
                """
            )
    if int(regressions) != 0:
        raise ValueError(
            f"{regressions} stored outcomes are newer than this run's appended rows "
            "for the same identity, policy and reference"
        )
    return dg.MaterializeResult(
        metadata={
            "appended_rows": rows,
            "expected_appended_rows": int(appended),
            "store_rows": int(store_rows),
            "store_identities": int(store_identities),
            "table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
        }
    )


class SwedenGeocodeStoreBackfillConfig(dg.Config):
    """A bare Materialize click is a preview -- it reports what a real run would append."""

    execute: bool = False


@dg.asset(
    name=GEOCODE_STORE_BACKFILL_ASSET_KEY,
    deps=[dg.AssetKey(SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "One-time append of the whole current Sweden serving geocode table into the "
        "versioned store, stamped with the resolver policy that produced it and each "
        "row's own OSM snapshot MD5. Idempotent: a second run replaces each row with "
        "identical content."
    ),
)
def sweden_address_geocode_store_backfill_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeStoreBackfillConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(
            geocode_store.GEOCODE_STORE_TABLE,
            shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
        ),
    )
    policy_version = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
    with clickhouse.get_connection() as client:
        [(serving_rows, missing_reference)] = client.execute(BACKFILL_PREFLIGHT_SQL)
        if int(serving_rows) == 0:
            raise ValueError("The Sweden serving geocode table is empty")
        if int(missing_reference) != 0:
            raise ValueError(
                f"{missing_reference} serving rows carry no OSM snapshot MD5 and "
                "cannot be given a reference identity"
            )
        if not config.execute:
            return dg.MaterializeResult(
                metadata={
                    "preview": True,
                    "serving_rows": int(serving_rows),
                    "policy_version": policy_version,
                }
            )
        client.execute(GEOCODE_STORE_BACKFILL_SQL, {"policy_version": policy_version})
        [(store_rows, store_identities, policies)] = client.execute(
            f"""
            SELECT count(), uniqExact(address_id), uniqExact(policy_version)
            FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
            """
        )
    context.log.info(
        "Backfilled %s serving rows into the geocode store as %s",
        serving_rows,
        policy_version,
    )
    return dg.MaterializeResult(
        metadata={
            "preview": False,
            "serving_rows": int(serving_rows),
            "store_rows": int(store_rows),
            "store_identities": int(store_identities),
            "store_policy_versions": int(policies),
            "policy_version": policy_version,
        }
    )


@dg.asset(
    deps=[
        dg.AssetKey(CANONICAL_CLICKHOUSE_ASSET_KEY),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    description=(
        "Matches canonical Swedish company addresses to the national OSM address "
        "index by postal code, street, and house number, with city and Sweden-wide "
        "fallbacks for incomplete OSM records. Spatially compact candidates and "
        "same-postcode streets produce flagged approximate markers; separated "
        "candidates remain ambiguous."
    ),
)
def sweden_company_address_osm_matches_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    matched_at = datetime.now(UTC)
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = address_geocoding.replace_sweden_company_address_osm_matches(
            connection=connection,
            source_run_id=context.run_id,
            matched_at=matched_at,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_table": address_geocoding.QUALIFIED_MATCH_RESULTS_TABLE,
            "matched_geocodes_table": address_geocoding.QUALIFIED_GEOCODES_TABLE,
            "matched_at": matched_at.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_company_address_osm_matches_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": address_geocoding.QUALIFIED_CLICKHOUSE_TABLE},
    description=(
        "Atomically replaces the Sweden company address geocode table "
        "with only unique exact OSM matches and their complete source provenance."
    ),
)
def sweden_company_address_geocodes_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_geocoding.CLICKHOUSE_DATABASE,
        tables=(address_geocoding.CLICKHOUSE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_geocoding.ENRICHMENT_SCHEMA,
                duckdb_table=address_geocoding.GEOCODES_TABLE,
                clickhouse_database=address_geocoding.CLICKHOUSE_DATABASE,
                clickhouse_table=address_geocoding.CLICKHOUSE_TABLE,
                columns=address_geocoding.CLICKHOUSE_EXPORT_COLUMNS,
                truncate=True,
                log=context.log.info,
            )
            stats = fetch_sweden_address_geocode_stats(clickhouse_client)
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": address_geocoding.QUALIFIED_CLICKHOUSE_TABLE,
            "company_addresses": stats.company_addresses,
            "unique_geocodes": stats.unique_geocodes,
            "exact_match_rate_percent": stats.exact_match_rate_percent,
            "source_run_count": stats.source_run_count,
            "latest_osm_snapshot_at": (
                stats.latest_osm_snapshot_at.isoformat()
                if stats.latest_osm_snapshot_at is not None
                else None
            ),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(GEOCODE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE},
    description=(
        "Atomically replaces the Sweden company address geocoding outcome table "
        "with one classified result per current address, including exact, "
        "approximate-site, approximate-area, approximate-street, ambiguous, "
        "unmatched, invalid, foreign, and postal-box outcomes."
    ),
)
def sweden_company_address_geocode_results_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_geocoding.CLICKHOUSE_DATABASE,
        tables=(address_geocoding.CLICKHOUSE_RESULTS_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_geocoding.ENRICHMENT_SCHEMA,
                duckdb_table=address_geocoding.MATCH_RESULTS_TABLE,
                clickhouse_database=address_geocoding.CLICKHOUSE_DATABASE,
                clickhouse_table=address_geocoding.CLICKHOUSE_RESULTS_TABLE,
                columns=address_geocoding.CLICKHOUSE_RESULTS_EXPORT_COLUMNS,
                truncate=True,
                log=context.log.info,
            )
            status_counts = fetch_sweden_address_geocode_result_counts(
                clickhouse_client
            )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE,
            **status_counts,
        }
    )


@dg.asset_check(
    asset=sweden_company_address_geocode_results_clickhouse,
    name="all_current_addresses_classified",
    description=(
        "Fails when the outcome table is not a one-to-one classification of "
        "the current Sweden company-address set."
    ),
)
def sweden_company_address_geocode_results_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(result_rows, unique_results, source_run_count)] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(tuple(company_id, address_key)),
                uniqExact(source_run_id)
            FROM {address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE}
            """
        )
        [(company_addresses,)] = client.execute(
            f"""
            SELECT count()
            FROM {address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE}
            """
        )
    return dg.AssetCheckResult(
        passed=(
            int(result_rows) == int(company_addresses) == int(unique_results)
            and int(source_run_count) == 1
        ),
        metadata={
            "company_addresses": int(company_addresses),
            "result_rows": int(result_rows),
            "unique_results": int(unique_results),
            "source_run_count": int(source_run_count),
        },
    )


@dg.asset_check(
    asset=sweden_company_canonical_addresses_clickhouse,
    name="all_source_observations_have_one_canonical_address",
    description=(
        "Fails when a source address in the normalization snapshot is missing or "
        "duplicated in the bridge, or a canonical company-address key is duplicated."
    ),
)
def sweden_company_canonical_addresses_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(source_rows,)] = client.execute(
            """
            SELECT count()
            FROM corpscout.se_company_addresses_current
            WHERE has_address = 1 AND has_observation = 1
            """
        )
        [
            (
                member_rows,
                unique_member_rows,
                member_normalization_runs,
                member_normalization_run_id,
            )
        ] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(tuple(
                    company_id,
                    address_source,
                    address_type,
                    address_key
                )),
                uniqExact(normalization_run_id),
                any(normalization_run_id)
            FROM {address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE}
            """
        )
        [
            (
                canonical_rows,
                unique_canonical_rows,
                canonical_member_rows,
                canonical_normalization_runs,
                canonical_normalization_run_id,
            )
        ] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(tuple(company_id, canonical_address_key)),
                sum(member_count),
                uniqExact(normalization_run_id),
                any(normalization_run_id)
            FROM {address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE}
            """
        )
    passed = (
        int(member_rows) == int(unique_member_rows) == int(canonical_member_rows)
        and int(canonical_rows) == int(unique_canonical_rows)
        and int(canonical_rows) <= int(member_rows)
        and int(member_normalization_runs) == 1
        and int(canonical_normalization_runs) == 1
        and member_normalization_run_id == canonical_normalization_run_id
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "current_source_observations": int(source_rows),
            "snapshot_source_observations": int(member_rows),
            "unique_source_members": int(unique_member_rows),
            "canonical_addresses": int(canonical_rows),
            "unique_canonical_addresses": int(unique_canonical_rows),
            "canonical_member_total": int(canonical_member_rows),
            "deduplicated_observations": int(member_rows) - int(canonical_rows),
            "upstream_observations_since_snapshot": max(
                int(source_rows) - int(member_rows),
                0,
            ),
            "normalization_runs": int(canonical_normalization_runs),
        },
    )


@dg.asset_check(
    asset=sweden_shared_addresses_clickhouse,
    name="all_company_addresses_link_to_one_shared_address",
    description=(
        "Fails when canonical company addresses are missing or duplicated in the "
        "shared-address graph, or aggregate evidence totals disagree."
    ),
)
def sweden_shared_addresses_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(canonical_rows, expected_link_rows, canonical_evidence)] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(tuple(
                    company_id,
                    country_code,
                    normalized_street,
                    normalized_postal_code,
                    normalized_post_town
                )),
                sum(member_count)
            FROM {address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE}
            """
        )
        [
            (
                link_rows,
                unique_link_rows,
                link_evidence,
                link_runs,
                invalid_review_statuses,
            )
        ] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(tuple(company_id, address_id)),
                sum(evidence_count),
                uniqExact(address_identity_run_id),
                countIf(review_status NOT IN ('unreviewed', 'confirmed', 'rejected'))
            FROM {shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE}
            """
        )
        [
            (
                address_rows,
                unique_address_rows,
                address_evidence,
                address_company_total,
                address_runs,
            )
        ] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(address_id),
                sum(evidence_count),
                sum(company_count),
                uniqExact(address_identity_run_id)
            FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE}
            """
        )
    passed = (
        int(expected_link_rows) == int(link_rows) == int(unique_link_rows)
        and int(canonical_evidence) == int(link_evidence)
        and int(address_rows) == int(unique_address_rows)
        and int(link_evidence) == int(address_evidence)
        and int(link_rows) == int(address_company_total)
        and int(link_runs) == int(address_runs) == 1
        and int(invalid_review_statuses) == 0
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "canonical_company_addresses": int(canonical_rows),
            "expected_company_address_links": int(expected_link_rows),
            "canonical_source_evidence": int(canonical_evidence),
            "company_address_links": int(link_rows),
            "unique_company_address_links": int(unique_link_rows),
            "shared_addresses": int(address_rows),
            "unique_shared_addresses": int(unique_address_rows),
            "link_evidence": int(link_evidence),
            "shared_address_evidence": int(address_evidence),
            "shared_address_company_total": int(address_company_total),
            "invalid_review_statuses": int(invalid_review_statuses),
            "address_identity_runs": int(address_runs),
        },
    )


@dg.asset_check(
    asset=sweden_address_geocodes_clickhouse,
    name="all_shared_addresses_have_one_geocoding_outcome",
    description=(
        "Fails when the shared-address geocode table is not a complete, unique, "
        "single-run classification of the current address identities."
    ),
)
def sweden_shared_address_geocodes_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(address_rows, unique_addresses, address_identity_runs)] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(address_id),
                uniqExact(address_identity_run_id)
            FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE}
            """
        )
        [
            (
                result_rows,
                unique_results,
                result_identity_runs,
                geocode_runs,
                invalid_statuses,
                invalid_coordinates,
                missing_geocoded_coordinates,
                unexpected_coordinates,
                invalid_precision,
                missing_snapshot_provenance,
            )
        ] = client.execute(
            f"""
            SELECT
                count(),
                uniqExact(address_id),
                uniqExact(address_identity_run_id),
                uniqExact(geocode_run_id),
                countIf(match_status NOT IN (
                    'matched_exact',
                    'matched_corrected',
                    'matched_site',
                    'matched_area',
                    'matched_street',
                    'ambiguous',
                    'unmatched',
                    'invalid_address',
                    'foreign_address',
                    'postal_box',
                    'property_identifier'
                )),
                countIf(
                    isNull(latitude) != isNull(longitude)
                    OR (isNotNull(latitude) AND (latitude < -90 OR latitude > 90))
                    OR (
                        isNotNull(longitude)
                        AND (longitude < -180 OR longitude > 180)
                    )
                ),
                countIf(
                    match_status IN (
                        'matched_exact',
                        'matched_corrected',
                        'matched_site',
                        'matched_area',
                        'matched_street'
                    )
                    AND (isNull(latitude) OR isNull(longitude))
                ),
                countIf(
                    match_status NOT IN (
                        'matched_exact',
                        'matched_corrected',
                        'matched_site',
                        'matched_area',
                        'matched_street'
                    )
                    AND (isNotNull(latitude) OR isNotNull(longitude))
                ),
                countIf(
                    match_status IN ('matched_exact', 'matched_corrected')
                        AND geocode_precision != 'building'
                    OR match_status = 'matched_site'
                        AND geocode_precision != 'site'
                    OR match_status = 'matched_area'
                        AND geocode_precision != 'area'
                    OR match_status = 'matched_street'
                        AND geocode_precision != 'street'
                    OR match_status NOT IN (
                        'matched_exact',
                        'matched_corrected',
                        'matched_site',
                        'matched_area',
                        'matched_street'
                    )
                        AND geocode_precision != ''
                ),
                countIf(
                    isNull(source_url)
                    OR isNull(source_object_key)
                    OR isNull(source_md5)
                    OR isNull(source_snapshot_at)
                    OR isNull(source_retrieved_at)
                )
            FROM {shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE}
            """
        )
        [(identity_mismatches,)] = client.execute(
            f"""
            SELECT count()
            FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE} address
            LEFT JOIN {
                shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
            } geocode USING (address_id)
            WHERE geocode.geocode_run_id = ''
               OR geocode.address_identity_run_id != address.address_identity_run_id
            """
        )
    passed = (
        int(address_rows) == int(unique_addresses)
        and int(address_rows) == int(result_rows) == int(unique_results)
        and int(address_identity_runs) == int(result_identity_runs) == 1
        and int(geocode_runs) == 1
        and int(identity_mismatches) == 0
        and int(invalid_statuses) == 0
        and int(invalid_coordinates) == 0
        and int(missing_geocoded_coordinates) == 0
        and int(unexpected_coordinates) == 0
        and int(invalid_precision) == 0
        and int(missing_snapshot_provenance) == 0
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "shared_addresses": int(address_rows),
            "unique_shared_addresses": int(unique_addresses),
            "geocode_results": int(result_rows),
            "unique_geocode_results": int(unique_results),
            "address_identity_runs": int(address_identity_runs),
            "geocode_runs": int(geocode_runs),
            "identity_mismatches": int(identity_mismatches),
            "invalid_statuses": int(invalid_statuses),
            "invalid_coordinates": int(invalid_coordinates),
            "missing_geocoded_coordinates": int(missing_geocoded_coordinates),
            "unexpected_coordinates": int(unexpected_coordinates),
            "invalid_precision": int(invalid_precision),
            "missing_snapshot_provenance": int(missing_snapshot_provenance),
        },
    )


@dg.asset_check(
    asset=sweden_address_geocodes_clickhouse,
    name="shared_geocoding_matches_company_baseline",
    description=(
        "Compares company-link coverage from shared geocodes with the existing "
        "company-address outcome table."
    ),
)
def sweden_shared_address_geocodes_baseline_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(baseline_exact, baseline_geolocated)] = client.execute(
            f"""
            SELECT
                uniqExactIf(
                    tuple(
                        canonical.company_id,
                        canonical.country_code,
                        canonical.normalized_street,
                        canonical.normalized_postal_code,
                        canonical.normalized_post_town
                    ),
                    result.match_status = 'matched_exact'
                ),
                uniqExactIf(
                    tuple(
                        canonical.company_id,
                        canonical.country_code,
                        canonical.normalized_street,
                        canonical.normalized_postal_code,
                        canonical.normalized_post_town
                    ),
                    isNotNull(result.latitude) AND isNotNull(result.longitude)
                )
            FROM {
                address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE
            } canonical
            INNER JOIN {address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE} result
                ON result.company_id = canonical.company_id
               AND result.address_key = canonical.canonical_address_key
            """
        )
        [(shared_exact, shared_geolocated)] = client.execute(
            f"""
            SELECT
                countIf(geocode.match_status = 'matched_exact'),
                countIf(
                    isNotNull(geocode.latitude) AND isNotNull(geocode.longitude)
                )
            FROM {
                shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
            } link
            INNER JOIN {
                shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
            } geocode USING (address_id)
            """
        )
    return dg.AssetCheckResult(
        passed=(
            int(shared_exact) == int(baseline_exact)
            and int(shared_geolocated) == int(baseline_geolocated)
        ),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "baseline_exact_company_addresses": int(baseline_exact),
            "shared_exact_company_links": int(shared_exact),
            "baseline_geolocated_company_addresses": int(baseline_geolocated),
            "shared_geolocated_company_links": int(shared_geolocated),
            "exact_difference": int(shared_exact) - int(baseline_exact),
            "geolocated_difference": int(shared_geolocated) - int(baseline_geolocated),
        },
    )


@dg.asset_check(
    asset=sweden_company_address_geocodes_clickhouse,
    name="exact_match_rate_stable",
    description=(
        "Warns when exact OSM coverage drops below the operational floor or moves "
        "by more than two percentage points from the previous materialization."
    ),
)
def sweden_company_address_exact_match_rate_check(
    context: dg.AssetCheckExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        stats = fetch_sweden_address_geocode_stats(client)
    previous_percent = previous_exact_match_rate_percent(
        context.instance,
        current_run_id=context.run.run_id,
    )
    current_percent = stats.exact_match_rate_percent
    change = (
        current_percent - previous_percent if previous_percent is not None else None
    )
    return dg.AssetCheckResult(
        passed=(
            stats.matched_exact == stats.unique_geocodes
            and stats.source_run_count == 1
            and exact_match_rate_is_stable(
                current_percent=current_percent,
                previous_percent=previous_percent,
            )
        ),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "company_addresses": stats.company_addresses,
            "matched_exact": stats.matched_exact,
            "unique_geocodes": stats.unique_geocodes,
            "source_run_count": stats.source_run_count,
            "exact_match_rate_percent": current_percent,
            "previous_exact_match_rate_percent": previous_percent,
            "change_percentage_points": change,
            "minimum_match_rate_percent": MIN_EXACT_MATCH_RATE_PERCENT,
            "maximum_change_percentage_points": (
                MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS
            ),
        },
    )


@dg.asset_check(
    asset=sweden_company_address_geocodes_clickhouse,
    name="osm_snapshot_fresh",
    description="Warns when published coordinates come from an OSM snapshot over nine days old.",
)
def sweden_company_address_osm_snapshot_freshness_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        stats = fetch_sweden_address_geocode_stats(client)
    snapshot_at = stats.latest_osm_snapshot_at
    snapshot_age_hours = (
        (checked_at - snapshot_at.astimezone(UTC)).total_seconds() / 3600
        if snapshot_at is not None and snapshot_at.tzinfo is not None
        else None
    )
    return dg.AssetCheckResult(
        passed=osm_snapshot_is_fresh(snapshot_at=snapshot_at, now=checked_at),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "latest_osm_snapshot_at": (
                snapshot_at.isoformat() if snapshot_at is not None else None
            ),
            "snapshot_age_hours": snapshot_age_hours,
            "maximum_snapshot_age_hours": MAX_OSM_SNAPSHOT_AGE.total_seconds() / 3600,
        },
    )


sweden_company_address_geocoding_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_job",
    selection=dg.AssetSelection.assets(
        CANONICAL_DUCKDB_ASSET_KEY,
        CANONICAL_CLICKHOUSE_ASSET_KEY,
        SHARED_ADDRESSES_DUCKDB_ASSET_KEY,
        SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY,
        SHARED_GEOCODE_DUCKDB_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
        "sweden_company_address_osm_matches_duckdb",
        GEOCODE_ASSET_KEY,
        GEOCODE_RESULT_ASSET_KEY,
    ),
)

sweden_shared_address_identity_job = dg.define_asset_job(
    name="sweden_shared_address_identity_job",
    selection=dg.AssetSelection.assets(
        SHARED_ADDRESSES_DUCKDB_ASSET_KEY,
        SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "shared_address_identity"},
    description=(
        "Builds and publishes country-level Sweden address identities and "
        "company-address links without changing geocoding consumers."
    ),
)

sweden_shared_address_geocoding_job = dg.define_asset_job(
    name="sweden_shared_address_geocoding_job",
    selection=dg.AssetSelection.assets(
        SHARED_GEOCODE_DUCKDB_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "shared_address_geocoding"},
    description=(
        "Matches current shared Sweden addresses to the existing OSM index and "
        "publishes one classified geocoding outcome per address identity."
    ),
)

sweden_company_address_geocoding_weekly_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_weekly_job",
    selection=dg.AssetSelection.assets(
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        CANONICAL_DUCKDB_ASSET_KEY,
        CANONICAL_CLICKHOUSE_ASSET_KEY,
        SHARED_ADDRESSES_DUCKDB_ASSET_KEY,
        SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY,
        SHARED_GEOCODE_DUCKDB_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
        "sweden_company_address_osm_matches_duckdb",
        GEOCODE_ASSET_KEY,
        GEOCODE_RESULT_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Refreshes the Sweden Geofabrik snapshot and OSM address index, then "
        "rematches current company addresses and publishes exact geocodes."
    ),
)

sweden_address_geocode_store_backfill_job = dg.define_asset_job(
    name="sweden_address_geocode_store_backfill_job",
    selection=dg.AssetSelection.assets(GEOCODE_STORE_BACKFILL_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_geocode_store_backfill"},
    description=(
        "One-time append of the current Sweden serving geocode table into the "
        "versioned store. Requires execute: true -- a bare materialization previews."
    ),
)

sweden_company_address_geocoding_weekly = dg.ScheduleDefinition(
    name="sweden_company_address_geocoding_weekly",
    job=sweden_company_address_geocoding_weekly_job,
    cron_schedule=WEEKLY_CRON_SCHEDULE,
    execution_timezone=WEEKLY_EXECUTION_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Weekly Sweden OSM snapshot, address-index, exact company-address match, "
        "and ClickHouse publication."
    ),
)


defs = dg.Definitions(
    assets=[
        sweden_company_canonical_addresses_duckdb,
        sweden_company_canonical_addresses_clickhouse,
        sweden_shared_addresses_duckdb,
        sweden_shared_addresses_clickhouse,
        sweden_shared_address_osm_matches_duckdb,
        sweden_address_geocodes_clickhouse,
        sweden_address_geocode_store_clickhouse,
        sweden_address_geocode_store_backfill_clickhouse,
        sweden_company_address_osm_matches_duckdb,
        sweden_company_address_geocodes_clickhouse,
        sweden_company_address_geocode_results_clickhouse,
    ],
    asset_checks=[
        sweden_company_canonical_addresses_complete_check,
        sweden_shared_addresses_complete_check,
        sweden_shared_address_geocodes_complete_check,
        sweden_shared_address_geocodes_baseline_check,
        sweden_company_address_exact_match_rate_check,
        sweden_company_address_osm_snapshot_freshness_check,
        sweden_company_address_geocode_results_complete_check,
    ],
    jobs=[
        sweden_company_address_geocoding_job,
        sweden_shared_address_identity_job,
        sweden_shared_address_geocoding_job,
        sweden_company_address_geocoding_weekly_job,
        sweden_address_geocode_store_backfill_job,
    ],
    schedules=[sweden_company_address_geocoding_weekly],
)
