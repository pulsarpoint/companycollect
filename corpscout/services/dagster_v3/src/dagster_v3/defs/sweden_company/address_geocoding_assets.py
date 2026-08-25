import uuid
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
    geocode_demand,
    geocode_legacy_adoption,
    geocode_store,
    shared_address_geocoding,
    shared_addresses,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

GROUP_NAME = "sweden_company"
CANONICAL_DUCKDB_ASSET_KEY = "sweden_company_canonical_addresses_duckdb"
CANONICAL_CLICKHOUSE_ASSET_KEY = "sweden_company_canonical_addresses_clickhouse"
SHARED_ADDRESSES_DUCKDB_ASSET_KEY = "sweden_shared_addresses_duckdb"
SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY = "sweden_shared_addresses_clickhouse"
GEOCODE_DEMAND_ASSET_KEY = "sweden_address_geocode_demand_duckdb"
SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY = "sweden_address_geocodes_clickhouse"
ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY = "sweden_address_resolution_golden_evaluation"
ADDRESS_RESOLUTION_SHADOW_ASSET_KEY = "sweden_address_resolution_shadow_duckdb"
ADDRESS_RESOLUTION_CURRENT_ASSET_KEY = "sweden_address_resolution_current_duckdb"
GEOCODE_STORE_ASSET_KEY = "sweden_address_geocode_store_clickhouse"
GEOCODE_STORE_BACKFILL_ASSET_KEY = "sweden_address_geocode_store_backfill_clickhouse"
LEGACY_ADOPTION_ASSET_KEY = "sweden_address_geocode_legacy_adoption_clickhouse"
WEEKLY_CRON_SCHEDULE = "5 4 * * 2"
WEEKLY_EXECUTION_TIMEZONE = "Europe/Stockholm"
MAX_OSM_SNAPSHOT_AGE = timedelta(days=9)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MIN_EXACT_MATCH_RATE_PERCENT = 5.0
MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS = 2.0


_GEOCODED_STATUS_LIST = ", ".join(
    f"'{status}'" for status in geocode_store.GEOCODED_STATUSES
)
_VALID_STATUS_LIST = ", ".join(f"'{status}'" for status in geocode_store.VALID_STATUSES)


@dataclass(frozen=True)
class SwedenGeocodeExactMatchStats:
    company_address_links: int
    matched_exact_links: int
    geocoded_links: int

    @property
    def exact_match_rate_percent(self) -> float:
        if self.company_address_links == 0:
            return 0.0
        return 100.0 * self.matched_exact_links / self.company_address_links


EXACT_MATCH_RATE_SQL = f"""SELECT
    count(),
    countIf(ifNull(geocode.match_status, '') = 'matched_exact'),
    countIf(ifNull(geocode.match_status, '') IN ({_GEOCODED_STATUS_LIST}))
FROM {shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE} AS link
LEFT JOIN (
{geocode_store.build_current_geocodes_sql(columns=("address_id", "match_status"))}
) AS geocode ON geocode.address_id = link.address_id"""

SNAPSHOT_FRESHNESS_SQL = f"""SELECT max(source_snapshot_at)
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}"""


def fetch_sweden_geocode_exact_match_stats(client: Any) -> SwedenGeocodeExactMatchStats:
    """Company-address links, and how many of them the store currently geocodes.

    The denominator is LINKS, not canonical addresses: the canonical ClickHouse table is
    retiring, and a link is the thing a rate over company addresses is actually about. The
    numerator joins the versioned read, so the two share a grain -- counting identities
    against a link denominator would produce a number that means nothing.
    """
    [(links, matched_exact, geocoded)] = client.execute(EXACT_MATCH_RATE_SQL)
    return SwedenGeocodeExactMatchStats(
        company_address_links=int(links),
        matched_exact_links=int(matched_exact),
        geocoded_links=int(geocoded),
    )


def fetch_sweden_geocode_snapshot_freshness(client: Any) -> datetime | None:
    """The newest OSM snapshot any stored outcome was computed against."""
    [(snapshot_at,)] = client.execute(SNAPSHOT_FRESHNESS_SQL)
    return snapshot_at


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


EXACT_MATCH_RATE_CHECK_NAME = "exact_match_rate_stable"
EXACT_MATCH_RATE_CHECK_KEY = dg.AssetCheckKey(
    dg.AssetKey(GEOCODE_STORE_ASSET_KEY),
    EXACT_MATCH_RATE_CHECK_NAME,
)


def previous_exact_match_rate_percent(
    instance: dg.DagsterInstance,
    *,
    current_run_id: str,
) -> float | None:
    """The rate this check last reported, read out of this check's OWN history.

    IT USED TO READ MATERIALIZATIONS, AND IT CANNOT ANY MORE. The series was the legacy
    publish asset's materialization metadata, which carried the rate as a side effect of
    the asset computing it. That asset retires with the legacy pair, and the store asset
    this check now hangs off publishes no rate at all -- it appends the outcomes a
    promotion produced and reports what it appended. Following the check to its new host
    while still reading materializations would answer None on every run forever: the
    +/-2pp term would be permanently inert and nothing would say so.

    A PLANNED RECORD IS NOT AN EVALUATION, AND IT IS NOT `None` EITHER.
    `AssetCheckExecutionRecord.evaluation` is an unchecked cast over the event's
    `event_specific_data`, so a PLANNED row hands back an `AssetCheckEvaluationPlanned`,
    which has no `.metadata`. PLANNED rows are written at RUN CREATION for every planned
    check, so any week that fails before this check evaluates -- the store asset alone has
    five raise sites, and an operator can terminate a run -- leaves one sitting on top of
    the history. Reading it raises AttributeError, this run is then stored PLANNED too, and
    every following week raises on ITS predecessor: one bad week would kill the series
    permanently. The isinstance guard steps over it to the usable evaluation underneath.

    `dg.AssetCheckEvaluation` is public, and the guard is a positive statement about what
    this needs rather than an enumeration of what it does not. Filtering the query by
    status instead would need a private import, and invites a narrower filter than
    intended: the natural constant (COMPLETED_...STATUSES = {FAILED, SUCCEEDED}) keeps
    FAILED evaluations, but a hand-written {SUCCEEDED} filter would drop them -- and a
    FAILED evaluation carries a perfectly good rate. Keeping it is deliberate: a drift
    alarm's baseline must be the last MEASURED value, not the last approved one, or a
    genuine step makes the check fire forever against a frozen baseline. This check is a
    WARN, so a week it failed is exactly the week worth comparing the next one against.

    An evaluation is written after the check body returns, so the current run's own record
    is normally absent; `run_id` is still compared, because a check retried inside one run
    must not be compared against itself.
    """
    records = instance.event_log_storage.get_asset_check_execution_history(
        EXACT_MATCH_RATE_CHECK_KEY,
        limit=10,
    )
    for record in records:
        if record.run_id == current_run_id:
            continue
        evaluation = record.evaluation
        if not isinstance(evaluation, dg.AssetCheckEvaluation):
            continue
        value = evaluation.metadata.get("exact_match_rate_percent")
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


class SwedenGeocodeDemandConfig(dg.Config):
    """`rematch_all` is the explicit operator action of spec 4.2 item 3 -- the same
    spelled-out-in-config pattern the se_company finals use for `resolve_all`."""

    rematch_all: bool = False


@dg.asset(
    name=GEOCODE_DEMAND_ASSET_KEY,
    deps=[
        dg.AssetKey(SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE},
    description=(
        "Decides which Sweden address identities this run must match: those with no "
        "resolver outcome, those whose outcome came from a different policy version, "
        "and non-geocoded outcomes whose OSM reference snapshot has moved."
    ),
)
def sweden_address_geocode_demand_duckdb(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeDemandConfig,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(geocode_store.GEOCODE_STORE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        reference_md5 = geocode_demand.fresh_reference_md5(connection)
        with clickhouse.get_connection() as clickhouse_client:
            loaded = geocode_demand.load_current_resolver_outcomes(
                connection=connection,
                clickhouse_client=clickhouse_client,
                log=context.log.info,
            )
        counts = geocode_demand.replace_pending_address_identities(
            connection=connection,
            policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            reference_md5=reference_md5,
            rematch_all=config.rematch_all,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **{
                key: value
                for key, value in counts.items()
                if not isinstance(value, dict | list)
            },
            "loaded_previous_outcomes": loaded,
            "reason_counts": dg.MetadataValue.json(counts["reason_counts"]),
            "table": geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE,
        }
    )


def build_derived_current_geocodes_sql() -> str:
    """The serving table's contents: the store's versioned read, projected to 26 columns.

    Deliberately not a second expression of the rule -- it IS geocode_store's fragment,
    asked for the serving column list. A separate ranking here would let the serving table
    and the se_company_address final disagree about which outcome is current while both
    looked internally consistent.
    """
    return geocode_store.build_current_geocodes_sql(
        columns=geocode_store.SERVING_COLUMNS
    )


def _parity_side_sql(label: str, table: str) -> str:
    return f"""SELECT
    '{label}' AS side,
    count() AS rows,
    uniqExact(address_id) AS identities,
    sum(cityHash64(
        toString(address_id),
        toString(match_status),
        ifNull(toString(latitude), ''),
        ifNull(toString(longitude), ''),
        toString(matched_at)
    )) AS content_hash
FROM {table}"""


DERIVED_PARITY_SQL = (
    _parity_side_sql(
        "derived", shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
    )
    + "\nUNION ALL\n"
    # The subquery is ALIASED. An unaliased `FROM ( ... LIMIT 1 BY ... )` is the one shape
    # in this plan ClickHouse has no other reason to parse, and
    # tests/test_sweden_geocode_derivation.py executes this constant precisely so the
    # question is settled by the engine rather than by confidence.
    + _parity_side_sql(
        "store", f"(\n{build_derived_current_geocodes_sql()}\n) AS store_read"
    )
)


def _derived_parity_result(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """One two-sided read, shared by the two checks that host it.

    WHAT THE DIGEST SEES, AND WHAT IT DOES NOT. `cityHash64` covers five of the serving
    table's 26 columns -- address_id, match_status, latitude, longitude, matched_at --
    which are the columns that decide WHICH outcome is current and where it puts a
    company on a map. A table wrong only in the descriptive remainder
    (geocode_precision, match_confidence, candidate_count, the source_* provenance)
    hashes identically and passes here. That is deliberate: the digest is a swap
    tripwire, not a column-by-column diff, and widening it would trade a cheap weekly
    read for one that reserializes 2.09M rows of provenance. The invariants over those
    columns are check 3's job, not this one's.
    """
    with clickhouse.get_connection() as client:
        sides = {
            str(side): (int(rows), int(identities), int(content_hash))
            for side, rows, identities, content_hash in client.execute(
                DERIVED_PARITY_SQL
            )
        }
    derived, store = sides["derived"], sides["store"]
    return dg.AssetCheckResult(
        passed=derived == store,
        metadata={
            "derived_rows": derived[0],
            "store_rows": store[0],
            "derived_identities": derived[1],
            "store_identities": store[1],
            "content_hashes_agree": derived[2] == store[2],
        },
    )


class SwedenDerivedGeocodesConfig(dg.Config):
    # Shrink-guard override (see sweden_financial/clickhouse.py's
    # guard_against_clickhouse_table_shrink) -- MUST stay False by default. The store is
    # append-only, so the derived identity set can only grow; a smaller one means the
    # store this ran against is not the whole store. Only set True via explicit run
    # config, for a shrink an operator has confirmed is real.
    allow_shrink: bool = False


@dg.asset(
    deps=[dg.AssetKey(GEOCODE_STORE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "openstreetmap"},
    metadata={
        "table": (shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE)
    },
    description=(
        "Derives the Sweden shared-address serving table from the versioned geocode "
        "store: the current outcome per identity, staged and swapped atomically. "
        "Transitional -- consumers migrate to the store's versioned read on their own "
        "schedule and this table retires when the last one has."
    ),
)
def sweden_address_geocodes_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenDerivedGeocodesConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Project the store onto the serving table, in one atomic swap.

    NO DUCKDB, DELIBERATELY. Until this task the serving table was an export of the
    promotion's own output, which under demand-driven matching holds only the identities
    that were due -- so an ordinary week would have published three rows over 2.09M. The
    store answers for every identity, including the ones nobody re-decided, and their
    `matched_at` comes back exactly as it was stored: no weekly restamp, which is what
    keeps the se_company_address change scan proportional to what actually changed.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(
            geocode_store.GEOCODE_STORE_TABLE,
            shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
        ),
    )
    target = shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
    stage = (
        f"{geocode_store.CLICKHOUSE_DATABASE}."
        f"_tmp_{shared_address_geocoding.ADDRESS_GEOCODES_TABLE}_{uuid.uuid4().hex}"
    )
    columns = ", ".join(geocode_store.SERVING_COLUMNS)
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} ({columns})\n"
                f"{build_derived_current_geocodes_sql()}"
            )
            [(rows, identities)] = client.execute(
                f"SELECT count(), uniqExact(address_id) FROM {stage}"
            )
            if int(rows) == 0:
                raise ValueError(
                    f"{stage} has 0 rows -- refusing to replace {target}. The geocode "
                    "store is empty or the versioned read returned nothing."
                )
            if int(rows) != int(identities):
                raise ValueError(
                    f"{stage} holds {rows} rows for {identities} identities -- the "
                    "versioned read must yield exactly one outcome per identity"
                )
            # THE STORE ONLY GROWS, SO THE DERIVED SET ONLY GROWS. An append-only store
            # cannot lose an identity, so a stage smaller than the table it is about to
            # replace does not mean fewer addresses -- it means this ran against a store
            # that is not the whole store (backfill not run, wrong cluster, rows pruned).
            # Neither refusal above sees that: a store holding one demand week stages a
            # perfectly well-formed handful of rows, one per identity, and would replace
            # 2.09M served identities with them. The parity check cannot see it either --
            # both of its sides read that same store and would agree.
            guard_against_clickhouse_table_shrink(
                qualified_table=target,
                existing_row_count=clickhouse_table_row_count(client, target),
                staged_row_count=int(rows),
                allow_shrink=config.allow_shrink,
            )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
        status_counts = {
            str(status): int(count)
            for status, count in client.execute(
                f"""
                SELECT match_status, count()
                FROM {target}
                GROUP BY match_status
                ORDER BY match_status
                """
            )
        }
        [(geolocated,)] = client.execute(
            f"""
            SELECT count()
            FROM {target}
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            """
        )
    context.log.info("Derived %s from the geocode store: %s rows", target, rows)
    return dg.MaterializeResult(
        metadata={
            "rows": int(rows),
            "geolocated": int(geolocated),
            "exact_match_rate_percent": (
                100.0 * status_counts.get("matched_exact", 0) / int(rows)
                if int(rows) > 0
                else 0.0
            ),
            **status_counts,
            "table": target,
        }
    )


@dg.asset_check(
    asset=sweden_address_geocodes_clickhouse,
    name="derived_current_matches_the_store",
    description=(
        "Fails when the derived Sweden serving geocode table disagrees with the "
        "versioned store read it is supposed to be a projection of."
    ),
)
def sweden_address_geocodes_derived_parity_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """The transition's one safety net.

    The derivation IS this read, so the interesting failure is not an arithmetic mistake
    -- it is a stage that never swapped, leaving last week's table in place. That table
    has a plausible row count and the wrong rows, so the content hash is what makes this
    check able to fail at all. It retires with the serving table.
    """
    return _derived_parity_result(clickhouse)


def build_store_append_regression_sql() -> str:
    """Rows that would swallow the outcomes this run just appended.

    ReplacingMergeTree(matched_at) keeps the LARGEST matched_at per key. A pre-existing row
    for one of this run's key triples carrying a newer instant makes this run's outcome
    invisible the moment it lands -- a silent no-op no row count would show. Equal instants
    are this run's own rows and are content-identical by the versioning contract, so the
    comparison is strictly greater-than.

    THE COMPARISON IS IN INTEGER MILLISECONDS, NOT DATETIMES, AND THAT IS NOT A STYLE
    CHOICE. `clickhouse_driver` binds a datetime parameter through `escape_datetime`, which
    is `strftime('%Y-%m-%d %H:%M:%S')` after `astimezone(server_tz)`: the sub-second part is
    DROPPED and the rendered literal is in the server's timezone, not the column's. Against
    a millisecond-stamped `DateTime64(3, 'UTC')` column that truncated literal is strictly
    smaller than this run's own rows, so every one of them counts as a regression and the
    asset raises on essentially every run. `toUnixTimestamp64Milli` is the column's own tick
    count, and an int parameter is rendered verbatim -- exact on both sides, timezone-free
    by construction. `epoch_milliseconds` below is the other half of the pair.
    """
    return f"""SELECT count()
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
WHERE (address_id, policy_version, reference_md5) IN (
        SELECT address_id, policy_version, reference_md5
        FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
        WHERE geocode_run_id = %(geocode_run_id)s
      )
  AND toUnixTimestamp64Milli(matched_at) > %(matched_at_ms)s"""


def epoch_milliseconds(value: datetime) -> int:
    """A DuckDB instant as the exact tick a `DateTime64(3, 'UTC')` column stores for it.

    `clickhouse_driver` writes a datetime as `int(timestamp()) * 1000 + microsecond // 1000`
    -- floor, not round -- so flooring the same instant here reproduces the stored tick
    bit-for-bit and this run's own rows compare EQUAL, never greater.

    DuckDB hands back TIMESTAMPTZ values in the SESSION's timezone, not UTC (a machine set
    to Europe/Belgrade returns 14:00+02:00 for a 12:00Z instant). Subtracting an aware
    epoch is offset-proof, so the tick is the same however the value arrives; the naive
    branch is defensive only.
    """
    stamped = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return (stamped - _EPOCH) // timedelta(milliseconds=1)


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
        "Appends the promoted Sweden address outcomes to the permanent versioned "
        "geocode store, stamped with the resolver policy and the OSM reference "
        "snapshot they were computed against. One promotion appends only the "
        "identities the demand scan selected, so an unchanged week appends nothing."
    ),
)
def sweden_address_geocode_store_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Append one promotion's outcomes to the versioned store.

    WHAT THIS APPENDS IS THE OUTCOMES THIS RUN ACTUALLY COMPUTED. The demand scan upstream
    selects the identities that are due and the promotion evaluates only those, so spec
    section 5's per-outcome `matched_at` -- an instant that means "this outcome was really
    decided then" -- now holds: an identity nobody re-decided keeps the instant its stored
    outcome already carries, because no newer row for it is written at all.

    THE PENDING SET, NOT THE HAND-OFF TABLE, DECIDES WHETHER THERE IS WORK. With nothing
    pending this asset appends nothing and returns -- the expected end of a week in which
    no address changed, no identity appeared and the OSM snapshot held still. It does not
    even look at the hand-off table then, because the promotion leaves that table as the
    last PROMOTING run left it, and re-appending those rows under this run's name would
    restamp outcomes nobody re-decided. With identities pending, an empty hand-off table
    is instead a silent no-op -- promotion was asked for outcomes and produced none -- and
    it raises.

    PRECONDITION, shared with the shadow and the promotion: se_address_pending_identities
    must exist in this DuckDB, which means sweden_address_geocode_demand_duckdb must have
    run against it at least once. Where it has not, the pending read above fails with a
    DuckDB CatalogException naming that table; materialize the demand asset.

    The DuckDB hand-off table is a persistent copy of the promotion and is never cleaned up
    here -- deliberately. The ClickHouse asset opens its own connection, so a temporary
    table would not survive to be read, and leaving it in place is what lets a failed
    ClickHouse leg be retried without re-running the promotion.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(geocode_store.GEOCODE_STORE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        # The pending set is read BEFORE the hand-off table, not after: on an unchanged
        # week the promotion writes no hand-off table at all, so there may be nothing
        # there to count -- or, worse, last week's rows, which are already in the store
        # and must not be appended a second time under this run's name.
        if geocode_demand.pending_identity_count(connection) == 0:
            context.log.info(
                "No Sweden address identities were pending; appending nothing to the "
                "geocode store"
            )
            return dg.MaterializeResult(
                metadata={
                    "appended_rows": 0,
                    "expected_appended_rows": 0,
                    "geocode_run_id": "",
                    "short_circuit": True,
                    "table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
                }
            )
        [
            (appended, matched_at, policy_versions, references, runs, append_run_id)
        ] = connection.execute(
            f"""
            select
                count(*),
                max(matched_at),
                count(distinct policy_version),
                count(distinct reference_md5),
                count(distinct geocode_run_id),
                first(geocode_run_id)
            from {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
            """
        ).fetchall()
        # Identities WERE pending, so an empty hand-off table here is a silent no-op the
        # guard below cannot catch: max(matched_at) would be None, the driver would render
        # it NULL, and `> NULL` answers NULL for every row.
        if int(appended) == 0:
            raise ValueError(
                "The Sweden geocode append table is empty; promotion produced no "
                "outcomes to append"
            )
        if int(policy_versions) > 1 or int(references) > 1 or int(runs) > 1:
            raise ValueError(
                "One promotion appends outcomes for one policy, one reference and "
                "one geocode run"
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
            # The run id comes from the rows themselves, never from context.run_id:
            # this asset can materialize on its own over a hand-off table an earlier run
            # wrote, and keying the guard off THIS run's id would then match no rows and
            # pass vacuously. The invariants above guarantee the column is single-valued.
            [(regressions,)] = clickhouse_client.execute(
                build_store_append_regression_sql(),
                {
                    "geocode_run_id": str(append_run_id),
                    "matched_at_ms": epoch_milliseconds(matched_at),
                },
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
            "geocode_run_id": str(append_run_id),
            "short_circuit": False,
            "store_rows": int(store_rows),
            "store_identities": int(store_identities),
            "table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
        }
    )


@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    # ORDERING, NOT COVERAGE. Without this the twin runs as soon as its own asset is
    # done, so a week that appends to the store evaluates STORE -> TWIN -> DERIVATION and
    # compares a store that has already grown against a serving table that has not been
    # rebuilt yet -- red every single promoting week, on the one safety net that is
    # supposed to mean something when it goes red. Naming the derivation makes the order
    # STORE -> DERIVATION -> TWIN when both are selected, and when only the store is
    # selected the twin still runs, which is the coverage it exists for.
    additional_deps=[dg.AssetKey(SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY)],
    name="derived_current_matches_the_store",
    description=(
        "The same transition tripwire, evaluated from the store's side: fails when "
        "the derived Sweden serving geocode table disagrees with the versioned store "
        "read it is supposed to be a projection of."
    ),
)
def sweden_address_geocode_store_derived_parity_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """The twin, and the reason there are two.

    A check runs with the asset it hangs off. During the transition these two assets
    move independently -- the store is appended to by every promoting job, and a
    retried or manually materialized store leg does not rebuild the serving table. In
    those runs the tripwire on the serving asset does not fire at all, which is
    precisely when the two tables are most likely to have drifted apart. Same query,
    same severity, no second expression of anything: both call one function. Both retire
    with the serving table.
    """
    return _derived_parity_result(clickhouse)


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
    """Import today's serving table into the store once, as policy-v5 outcomes.

    The default run WRITES NOTHING: it reports what a real run would append and returns.
    Only `execute: true` inserts, which is what keeps a stray Materialize click on a
    2.09M-row table harmless. Re-running with the gate on is safe -- every row keeps the
    serving row's own `matched_at`, so a second import replaces each row with identical
    content instead of bumping 2.09M versions.
    """
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


class SwedenGeocodeLegacyAdoptionConfig(dg.Config):
    """One shot, owner-gated. A bare Materialize click measures and writes nothing."""

    execute: bool = False
    sample_size: int = 20


@dg.asset(
    name=LEGACY_ADOPTION_ASSET_KEY,
    deps=[dg.AssetKey(GEOCODE_STORE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "One-time import of the retired per-company matcher's exact decisions for "
        "Sweden address identities the resolver refuses, as versioned "
        "legacy_adopted_v1 outcomes. Requires execute: true. It has run, and its "
        "source table was dropped afterwards by migration 000319 -- this asset is "
        "kept as the record of the import, and can no longer execute."
    ),
)
def sweden_address_geocode_legacy_adoption_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeLegacyAdoptionConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Import the legacy matcher's trapped exact decisions once, as adopted outcomes.

    The default run WRITES NOTHING: it reports the identities a real run would adopt and
    the identities the rule refuses, then returns. Only `execute: true` inserts.

    IT HAS RUN, AND IT STAYS AS THE RECORD OF THAT RUN. Its source table retires with the
    legacy matcher (migration 000319), which the import had to precede -- so after the drop
    this asset can no longer do anything, by construction. It is kept because it is where a
    reader looks for "where did the store's legacy_adopted_v1 rows come from", and because
    the assert below names se_company_address_geocode_results: a Materialize click after
    the drop fails on that name rather than somewhere inside a three-way join.

    `matched_at` is ONE instant for the whole import, bound as a parameter. That is the
    single place in this plan where a run-wide stamp is correct: every adopted row
    genuinely was decided at that instant, by this import, and the source rows carry no
    instant of their own worth promoting. It is bound in integer milliseconds rather than
    as a datetime -- `clickhouse_driver` renders a bound datetime in the SERVER's
    timezone with the sub-second part dropped, which a `DateTime64(3, 'UTC')` column then
    reads back as UTC. See `build_store_append_regression_sql` for the same pairing.

    RE-RUNNING IS SAFE AND IS NOT A NO-OP TO BE AFRAID OF. The store's key is
    (address_id, policy_version, reference_md5), so a second import replaces each row
    rather than appending beside it; only `matched_at` and `geocode_run_id` move. And an
    identity the resolver has answered in the meantime is no longer selected at all,
    because the selection reads the resolver family -- so a re-run cannot re-promote an
    adopted row over a resolver outcome that has already superseded it.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(
            geocode_store.GEOCODE_STORE_TABLE,
            geocode_legacy_adoption.CLICKHOUSE_RESULTS_TABLE,
            shared_addresses.COMPANY_ADDRESS_LINKS_TABLE,
        ),
    )
    # Truncated to the store's own resolution before anything reads it, per
    # geocode_store.StoredOutcome: matched_at is DateTime64(3, 'UTC'), so an instant
    # carrying microseconds would be REPORTED here and STORED one floor-division away,
    # and the metadata would name an instant the store does not hold.
    now = datetime.now(UTC)
    imported_at = now.replace(microsecond=1000 * (now.microsecond // 1000))
    with clickhouse.get_connection() as client:
        [(adoptable, legacy_rows, companies)] = client.execute(
            geocode_legacy_adoption.ADOPTION_MEASUREMENT_SQL
        )
        [(disagreeing,)] = client.execute(
            geocode_legacy_adoption.ADOPTION_DISAGREEMENT_SQL
        )
        [(distinct_companies,)] = client.execute(
            geocode_legacy_adoption.ADOPTION_COMPANY_COUNT_SQL
        )
        # Which non-geocoded resolver statuses the import is adopting through. Spec 4.4
        # talks about `ambiguous`; the rule admits every non-geocoded status, and the
        # owner sees the split here before authorising a permanent write.
        by_resolver_status = {
            str(status): int(count)
            for status, count in client.execute(
                geocode_legacy_adoption.ADOPTION_STATUS_BREAKDOWN_SQL
            )
        }
        # The breakdown is a wrapper around the same selection the adoption count reads,
        # and 12e reads the two together as one population seen two ways. A narrowing
        # added to the wrapper alone would leave both queries answering about different
        # sets, and no assertion over either number on its own can see that.
        if sum(by_resolver_status.values()) != int(adoptable):
            raise ValueError(
                "The Sweden adoption status breakdown accounts for "
                f"{sum(by_resolver_status.values())} of {int(adoptable)} adoptable "
                "identities -- the split and the adoption count no longer describe one "
                "population"
            )
        measurements = {
            "adoptable_identities": int(adoptable),
            "adoptable_by_resolver_status": dg.MetadataValue.json(by_resolver_status),
            "contributing_legacy_rows": int(legacy_rows or 0),
            # DISTINCT companies. `contributing_company_address_pairs` is the sum of the
            # per-identity counts, which counts a company once per adopted address it
            # sits at -- the plan's headline is a company count, so both are reported
            # and neither can be mistaken for the other.
            "contributing_companies": int(distinct_companies or 0),
            "contributing_company_address_pairs": int(companies or 0),
            "refused_disagreeing_identities": int(disagreeing),
        }
        if not config.execute:
            return dg.MaterializeResult(metadata={"preview": True, **measurements})
        if int(adoptable) == 0:
            raise ValueError(
                "No Sweden legacy exact decisions are adoptable -- refusing to run an "
                "import that would write nothing"
            )
        client.execute(
            geocode_legacy_adoption.ADOPTION_INSERT_SQL,
            {
                "geocode_run_id": context.run_id,
                "imported_at": epoch_milliseconds(imported_at),
            },
        )
        # This run's own rows only. NOT a test of the GROUP BY -- `optimize_on_insert`
        # collapses same-key rows inside one INSERT block, so a broken grain would dedup
        # itself away silently. See ADOPTED_GRAIN_SQL for what it does catch.
        [(adopted_rows, adopted_identities)] = client.execute(
            geocode_legacy_adoption.ADOPTED_GRAIN_SQL,
            {"geocode_run_id": context.run_id},
        )
        [(adopted_total,)] = client.execute(geocode_legacy_adoption.ADOPTED_TOTAL_SQL)
        sample = client.execute(
            geocode_legacy_adoption.ADOPTION_SAMPLE_SQL,
            {"sample_size": config.sample_size},
        )
    if int(adopted_rows) != int(adopted_identities):
        raise ValueError(
            f"The adoption import wrote {adopted_rows} rows for "
            f"{adopted_identities} identities in one run -- an identity landed under "
            "two OSM reference versions"
        )
    context.log.info(
        "Adopted %s Sweden legacy exact decisions into the geocode store",
        adopted_identities,
    )
    return dg.MaterializeResult(
        metadata={
            "preview": False,
            **measurements,
            "adopted_identities": int(adopted_identities),
            "adopted_identities_total": int(adopted_total),
            "imported_at": imported_at.isoformat(),
            "sample": dg.MetadataValue.json([list(map(str, row)) for row in sample]),
        }
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
        "Fails when the published Sweden shared-address graph disagrees with its own "
        "company-address links."
    ),
)
def sweden_shared_addresses_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Fails when the published shared-address graph disagrees with its own links.

    The canonical denominator this check used to carry is asserted by
    shared_addresses._assert_shared_address_invariants inside the DuckDB build, which aborts
    before publishing rather than reporting afterwards -- and the canonical ClickHouse table
    it read is retiring (spec section 4.5).
    """
    with clickhouse.get_connection() as client:
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
        int(link_rows) == int(unique_link_rows)
        and int(address_rows) == int(unique_address_rows)
        and int(link_evidence) == int(address_evidence)
        and int(link_rows) == int(address_company_total)
        and int(link_runs) == int(address_runs) == 1
        and int(invalid_review_statuses) == 0
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
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


STORE_INVARIANTS_SQL = f"""SELECT
    count(),
    uniqExact(tuple(address_id, policy_version, reference_md5)),
    uniqExact(address_id),
    countIf(match_status NOT IN ({_VALID_STATUS_LIST})),
    countIf(reference_md5 = '' OR policy_version = ''),
    countIf(
        isNull(latitude) != isNull(longitude)
        OR (isNotNull(latitude) AND (latitude < -90 OR latitude > 90))
        OR (isNotNull(longitude) AND (longitude < -180 OR longitude > 180))
    ),
    countIf(
        match_status IN ({_GEOCODED_STATUS_LIST})
        AND (isNull(latitude) OR isNull(longitude))
    ),
    countIf(
        match_status NOT IN ({_GEOCODED_STATUS_LIST})
        AND (isNotNull(latitude) OR isNotNull(longitude))
    ),
    countIf(
        match_status IN ('matched_exact', 'matched_corrected')
            AND geocode_precision != 'building'
        OR match_status = 'matched_site' AND geocode_precision != 'site'
        OR match_status = 'matched_area' AND geocode_precision != 'area'
        OR match_status = 'matched_street' AND geocode_precision != 'street'
        OR match_status NOT IN ({_GEOCODED_STATUS_LIST})
            AND geocode_precision != ''
    ),
    countIf(
        isNull(source_url)
        OR isNull(source_object_key)
        OR isNull(source_md5)
        OR isNull(source_snapshot_at)
        OR isNull(source_retrieved_at)
    )
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}"""

# The anti-join runs FROM THE IDENTITY SIDE, and only that direction. A permanent versioned
# store legitimately keeps outcomes for identities the register has since dropped, so
# "every stored outcome has a live identity" is false by design and would report a growing
# number nobody can act on. "Every live identity has an outcome" is the direction that can
# catch a demand scan which skipped somebody, which is the failure worth catching.
STORE_COVERAGE_SQL = f"""SELECT count()
FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE} AS address
LEFT ANTI JOIN {
    geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
} AS store ON store.address_id = address.address_id"""

# Only the adopted identities are ranked -- the filter goes on the INNER read, where the
# store's sorting key leads with address_id, so this reads the ~19k imported identities
# rather than all 2.09M.
_ADOPTED_EXACT_FILTER_SQL = (
    "address_id IN (\n"
    "        SELECT address_id\n"
    f"        FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}\n"
    f"        WHERE policy_version = '{geocode_store.LEGACY_ADOPTED_POLICY_VERSION}'\n"
    "          AND match_status = 'matched_exact'\n"
    "    )"
)

ADOPTION_DEMOTION_SQL = f"""SELECT count()
FROM (
{
    geocode_store.build_current_geocodes_sql(
        columns=("address_id", "policy_version", "match_status"),
        address_filter_sql=_ADOPTED_EXACT_FILTER_SQL,
    )
}
) AS served
WHERE served.policy_version != '{geocode_store.LEGACY_ADOPTED_POLICY_VERSION}'
  AND served.match_status NOT IN ('matched_exact', 'matched_corrected')"""


@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    name="every_stored_outcome_is_attributable_and_consistent",
    description=(
        "Fails when a current Sweden address identity has no stored outcome, or the "
        "versioned geocode store holds a duplicate (identity, policy, reference) row, "
        "an unknown status, an outcome with no version identity, or a coordinate that "
        "disagrees with its status."
    ),
)
def sweden_address_geocode_store_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Check 3, at the store's grain.

    The old check demanded ONE row per identity and ONE geocode run across the whole table,
    and joined the outcome table to the identity table on their shared address_identity_run
    id. All three are now wrong by design: several attributable outcomes per identity is
    the store working, a demand-driven run appends only what it matched so a table spanning
    many runs is the normal state, and an outcome carries the identity run it was decided
    under rather than the current one. Uniqueness moves to the key TRIPLE, which is the
    grain that replaced them and the only version of that term which can still fail. What
    survives unchanged is everything about a single row -- the status allowlist, the
    coordinate/precision agreement, the provenance -- plus the two new key columns, which
    must never be empty because an outcome that cannot be attributed is exactly what this
    design set out to eliminate.

    WHAT `rows == unique_keys` CAN AND CANNOT SEE. It is the ReplacingMergeTree contract
    stated as an assertion, and it discriminates: a duplicate key triple means un-merged
    parts the versioned read has to rank around, which is safe but worth knowing about, and
    a persistent inequality means the append path is writing the same key twice across
    runs. It cannot see a key written twice inside ONE insert block: the default
    `optimize_on_insert = 1` collapses those before they land (verified both ways on 26.5),
    so this term's silence is not evidence that the append never produced one.
    """
    with clickhouse.get_connection() as client:
        [
            (
                rows,
                unique_keys,
                identities,
                invalid_statuses,
                missing_versions,
                invalid_coordinates,
                missing_geocoded_coordinates,
                unexpected_coordinates,
                invalid_precision,
                missing_provenance,
            )
        ] = client.execute(STORE_INVARIANTS_SQL)
        [(identities_without_outcome,)] = client.execute(STORE_COVERAGE_SQL)
        [(demoted_adoptions,)] = client.execute(ADOPTION_DEMOTION_SQL)
    passed = (
        # The one term that carries the store's grain: one row per (identity, matcher,
        # reference). `count() >= uniqExact(address_id)` over the same table was the obvious
        # thing to write here and is a tautology -- it cannot fail, whatever the store holds.
        int(rows) == int(unique_keys)
        and int(identities_without_outcome) == 0
        and int(invalid_statuses) == 0
        and int(missing_versions) == 0
        and int(invalid_coordinates) == 0
        and int(missing_geocoded_coordinates) == 0
        and int(unexpected_coordinates) == 0
        and int(invalid_precision) == 0
        and int(missing_provenance) == 0
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "store_rows": int(rows),
            "unique_version_keys": int(unique_keys),
            "identities": int(identities),
            "identities_without_outcome": int(identities_without_outcome),
            "invalid_statuses": int(invalid_statuses),
            "outcomes_missing_a_version_identity": int(missing_versions),
            "invalid_coordinates": int(invalid_coordinates),
            "missing_geocoded_coordinates": int(missing_geocoded_coordinates),
            "unexpected_coordinates": int(unexpected_coordinates),
            "invalid_precision": int(invalid_precision),
            "missing_snapshot_provenance": int(missing_provenance),
            # REPORTED, NEVER GATED. Identities whose SERVED outcome has moved off an
            # imported `matched_exact` onto a lower-precision resolver outcome. That trade
            # is what the read rule is for -- a resolver answer that geocodes takes over --
            # but under the demand rule (spec section 4.2) a geocoded outcome is never
            # re-selected, so the demotion is terminal in practice even though both rows
            # stay in the store. Nothing here is broken, so nothing here may go red; this
            # counter exists so somebody can watch the number rather than discover it.
            # It counts over the whole store, so retired identities are included: their
            # rows remain, their served answer can still be demoted, and excluding them
            # would mean an identity join this query deliberately does not carry.
            "adopted_exact_identities_demoted": int(demoted_adoptions),
        },
    )


@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    name=EXACT_MATCH_RATE_CHECK_NAME,
    description=(
        "Warns when exact OSM coverage of Sweden company-address links drops below "
        "the operational floor or moves by more than two percentage points from the "
        "previous materialization."
    ),
)
def sweden_company_address_exact_match_rate_check(
    context: dg.AssetCheckExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Check 5, over links and the versioned read.

    The two terms that retire with the legacy pair -- `matched_exact == unique_geocodes`
    and `source_run_count == 1` -- described a table that held exact matches only and was
    rebuilt whole every week. Neither is true of an append-only store, and neither ever
    said anything about coverage. What is left is the rate, which is what this check was
    for.

    THE SERIES DOES NOT CARRY OVER. `previous_exact_match_rate_percent` reads this check's
    own evaluations, and this check has none under its new key, so the first run after the
    deploy sees None and the +/-2pp term is inert for one week. The absolute number steps
    as well: the denominator moves from canonical company addresses to company-address
    links, so week one is not comparable with the old series either.
    """
    with clickhouse.get_connection() as client:
        stats = fetch_sweden_geocode_exact_match_stats(client)
    previous_percent = previous_exact_match_rate_percent(
        context.instance,
        current_run_id=context.run.run_id,
    )
    current_percent = stats.exact_match_rate_percent
    change = (
        current_percent - previous_percent if previous_percent is not None else None
    )
    return dg.AssetCheckResult(
        passed=exact_match_rate_is_stable(
            current_percent=current_percent,
            previous_percent=previous_percent,
        ),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "company_address_links": stats.company_address_links,
            "matched_exact_links": stats.matched_exact_links,
            "geocoded_links": stats.geocoded_links,
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
    asset=sweden_address_geocode_store_clickhouse,
    name="osm_snapshot_fresh",
    description=(
        "Warns when stored Sweden coordinates come from an OSM snapshot over nine "
        "days old."
    ),
)
def sweden_company_address_osm_snapshot_freshness_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Check 6, over the store alone.

    `max(source_snapshot_at)` is taken over the WHOLE store rather than the versioned read:
    the store is append-only, so the newest snapshot any outcome was computed against is
    the newest snapshot the resolver has seen, and ranking 2.09M identities to learn it
    would cost a great deal to answer the same question.
    """
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        snapshot_at = fetch_sweden_geocode_snapshot_freshness(client)
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
        GEOCODE_DEMAND_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
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
        GEOCODE_DEMAND_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "shared_address_geocoding"},
    description=(
        "Selects the Sweden address identities that are due, matches those against "
        "the existing OSM index, and publishes one classified geocoding outcome per "
        "identity it decided."
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
        GEOCODE_DEMAND_ASSET_KEY,
        ADDRESS_RESOLUTION_GOLDEN_ASSET_KEY,
        ADDRESS_RESOLUTION_SHADOW_ASSET_KEY,
        ADDRESS_RESOLUTION_CURRENT_ASSET_KEY,
        SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY,
        GEOCODE_STORE_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Refreshes the Sweden Geofabrik snapshot and OSM address index, then "
        "resolves the Sweden address identities that are due and appends their "
        "outcomes to the versioned geocode store."
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

sweden_address_geocode_legacy_adoption_job = dg.define_asset_job(
    name="sweden_address_geocode_legacy_adoption_job",
    selection=dg.AssetSelection.assets(LEGACY_ADOPTION_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_geocode_legacy_adoption"},
    description=(
        "One-time, owner-gated import of the legacy per-company matcher's exact "
        "Sweden decisions into the versioned geocode store. Requires execute: true "
        "-- a bare materialization previews."
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
        sweden_address_geocode_demand_duckdb,
        sweden_address_geocodes_clickhouse,
        sweden_address_geocode_store_clickhouse,
        sweden_address_geocode_store_backfill_clickhouse,
        sweden_address_geocode_legacy_adoption_clickhouse,
    ],
    asset_checks=[
        sweden_company_canonical_addresses_complete_check,
        sweden_shared_addresses_complete_check,
        sweden_address_geocodes_derived_parity_check,
        sweden_address_geocode_store_derived_parity_check,
        sweden_address_geocode_store_complete_check,
        sweden_company_address_exact_match_rate_check,
        sweden_company_address_osm_snapshot_freshness_check,
    ],
    jobs=[
        sweden_company_address_geocoding_job,
        sweden_shared_address_identity_job,
        sweden_shared_address_geocoding_job,
        sweden_company_address_geocoding_weekly_job,
        sweden_address_geocode_store_backfill_job,
        sweden_address_geocode_legacy_adoption_job,
    ],
    schedules=[sweden_company_address_geocoding_weekly],
)
