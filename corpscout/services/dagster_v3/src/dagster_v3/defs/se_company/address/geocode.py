"""The address entity's geocode step (spec 2026-09-06 sections 3.7 and 6, slice 2a).

Normalized addresses in, one served outcome per LOCATION KEY out. Pure orchestration: no
Dagster, no resources -- the ClickHouse client and the DuckDB workbench connection are
parameters, so the fold, a backfill script and the tests all drive the same code.

THE THREE THINGS THIS MODULE IS.

1. A CACHE READ over `corpscout.se_address_geocodes`. The store is keyed by `address_id`,
   which for rows this entity writes holds the LOCATION key (the seven components without
   care-of -- one physical address is matched once whoever receives mail there). The current
   outcome per key comes from `geocode_store.build_current_geocodes_sql`, the one read rule;
   a row is a HIT when its `(policy_version, reference_md5)` is the pair this run computes
   with, or when it is an ADOPTED row (the imported family, which is on no resolver version
   at all and must not be re-matched merely for that -- mirrors `is_adopted` in the store's
   stage-2 rank). Everything else is a miss.

2. An ENGINE CALL for the misses. The same resolver the Sweden shadow evaluation runs
   (`address_resolution_shadow.replace_sweden_address_resolution_shadow`), on the same
   once-per-extract reference documents, over per-run tables named with the run id and
   dropped in a `finally`. Nothing in the engine, the policy or the OSM assets changes here.

3. A FALLBACK OVERLAY on the way out, for `unmatched`/`ambiguous`/`postal_box`. The store
   keeps the matcher's RAW outcome; the coarse postcode-or-city centroid is applied on READ
   and never written, exactly as `geocode_serving_overlay` does it for the shared-address
   serving read (whose constants this module reuses rather than re-spelling). So an identity
   the matcher could not place is served a coarse coordinate today and takes a precise one
   the moment a later extract matches it -- with nothing to un-write.

WHY THE QUERY SCOPE IS THE REFERENCE SCOPE. Every retrieval strategy in
`address_resolution/resolution.py` joins `query.index_scope = reference.index_scope`. The
reference documents are stamped with `address_resolution_shadow.INDEX_SCOPE`, so a scope of
this module's own invention would make every candidate join miss and report every address
`unmatched` -- silently, since `unmatched` is a legitimate outcome. `INDEX_SCOPE` is
therefore reused from the shadow, not re-spelled.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace as dataclass_replace
from datetime import datetime
from typing import Any

from dagster_v3.defs.address_resolution.resolution import (
    replace_address_resolution_candidates,
    replace_address_resolution_results,
)
from dagster_v3.defs.address_resolution.search_documents import (
    SEARCH_DOCUMENT_INPUT_COLUMNS,
    replace_address_search_document_input_table,
    replace_address_search_documents,
    replace_address_street_variants,
)
from dagster_v3.defs.se_company.address.normalize_se import NormalizedAddress
from dagster_v3.defs.sweden_company import geocode_serving_overlay, geocode_store
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
    SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
    SWEDEN_STREET_VARIANT_LANGUAGES,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    INDEX_SCOPE,
    QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
    ensure_reference_documents,
)
from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

ENRICHMENT_SCHEMA = geocode_store.ENRICHMENT_SCHEMA
QUALIFIED_STORE_TABLE = geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE

# `address_identity_run_id` for a row this entity matches. The adoption step (Task 4) writes
# `adopted:<old address_id>` instead, which is how an adopted row is recognized on read
# alongside its `legacy_adopted_v1` policy version.
ADDRESS_ENTITY_RUN_ID = "address-entity"
ADOPTED_RUN_ID_PREFIX = "adopted:"

# Keys per cache-lookup statement. 5,000 * (64 hex + quotes + comma) renders ~350 KB, inside
# ClickHouse's 1 MiB default `max_query_size`, so no widened setting is needed. The fallback
# binds its (key, postcode, city) triples in the same chunks for the same reason.
CACHE_LOOKUP_CHUNK = 5_000

# What a served outcome needs off a cached row, plus the two version columns and the run id
# the hit/miss decision reads. A subset of geocode_store.STORE_COLUMNS.
CACHE_COLUMNS: tuple[str, ...] = (
    "address_id",
    "policy_version",
    "reference_md5",
    "address_identity_run_id",
    "match_status",
    "match_method",
    "match_confidence",
    "latitude",
    "longitude",
    "geocode_provider",
    "geocode_precision",
    "coordinate_method",
    "coordinate_locality",
    "coordinate_supporting_point_count",
    "coordinate_spread_meters",
)

# What this module reads back out of the engine's result table.
RESULT_COLUMNS: tuple[str, ...] = (
    "query_document_id",
    "resolution_status",
    "geocode_precision",
    "match_confidence",
    "match_strategy",
    "latitude",
    "longitude",
    "coordinate_spread_meters",
    "supporting_record_count",
    "matched_locality",
    "candidate_record_ids",
    "candidate_record_urls",
    "candidate_record_count",
)

# The per-run tables in the workbench, in build order. Dropped in a `finally`.
RUN_TABLE_STEPS: tuple[str, ...] = (
    "input",
    "query",
    "variants",
    "candidates",
    "results",
)

GEOCODE_PROVIDER = "osm"
COORDINATE_METHOD = "resolver"


@dataclass(frozen=True, slots=True)
class GeocodeOutcome:
    """One SERVED outcome for a location key: the matcher's row, or a centroid over it."""

    location_key: str
    match_status: str
    match_method: str
    match_confidence: float | None
    latitude: float | None
    longitude: float | None
    geocode_provider: str
    geocode_precision: str
    coordinate_method: str
    coordinate_locality: str
    coordinate_supporting_point_count: int
    coordinate_spread_meters: float | None
    policy_version: str
    reference_md5: str
    from_cache: bool


def cache_lookup_sql() -> str:
    """The current store outcome for the bound keys, through the store's own read rule."""
    return geocode_store.build_current_geocodes_sql(
        columns=CACHE_COLUMNS, address_filter_sql="address_id IN %(keys)s"
    )


def cache_insert_sql() -> str:
    return (
        f"INSERT INTO {QUALIFIED_STORE_TABLE}"
        f" ({', '.join(geocode_store.STORE_COLUMNS)}) VALUES"
    )


def fallback_sql() -> str:
    """The finest centroid for each bound `(key, postal_code, city)` triple.

    Returns `key, tier, latitude, longitude, locality, point_count, spread_meters`, one row
    per key that lands on a tier; a key with no acceptable centroid is simply absent, so the
    caller leaves its raw outcome alone. The ladder, the 3,000 m postcode cap and the join
    keys are `geocode_serving_overlay`'s, reused rather than restated: this is the same
    overlay, computed for keys held in memory instead of for a table of address ids.

    Nested rather than alias-reuse for the same reason the overlay nests -- `_tier` is read
    several times by the outer projection -- and every LEFT JOIN column that gates the tier
    is read through `ifNull`, so the answer does not depend on `join_use_nulls`.
    """
    postcode = f"_tier = '{geocode_serving_overlay.POSTCODE_PRECISION}'"
    city = f"_tier = '{geocode_serving_overlay.CITY_PRECISION}'"
    keyed = (
        "SELECT\n"
        "        tupleElement(requested.row, 1) AS key,\n"
        f"        {postcode_key_sql('tupleElement(requested.row, 2)')} AS _pc_key,\n"
        f"        {city_key_sql('tupleElement(requested.row, 3)')} AS _city_key\n"
        "    FROM (SELECT arrayJoin(%(rows)s) AS row) AS requested"
    )
    tiered = (
        "SELECT\n"
        "        keyed.key AS key,\n"
        "        keyed._pc_key AS _pc_key,\n"
        "        keyed._city_key AS _city_key,\n"
        "        pc.latitude AS _pc_lat,\n"
        "        pc.longitude AS _pc_lon,\n"
        "        pc.point_count AS _pc_n,\n"
        "        pc.spread_meters AS _pc_spread,\n"
        "        cc.latitude AS _cc_lat,\n"
        "        cc.longitude AS _cc_lon,\n"
        "        cc.point_count AS _cc_n,\n"
        "        cc.spread_meters AS _cc_spread,\n"
        "        multiIf(\n"
        "            ifNull(pc.point_count, 0) > 0 AND ifNull(pc.spread_meters, 1e18) <= "
        f"{geocode_serving_overlay.POSTCODE_SPREAD_MAX_METERS},"
        f" '{geocode_serving_overlay.POSTCODE_PRECISION}',\n"
        "            ifNull(cc.point_count, 0) > 0,"
        f" '{geocode_serving_overlay.CITY_PRECISION}',\n"
        "            ''\n"
        "        ) AS _tier\n"
        f"    FROM (\n    {keyed}\n    ) AS keyed\n"
        f"    LEFT JOIN {geocode_serving_overlay.POSTCODE_CENTROIDS_TABLE} AS pc"
        " ON pc.key = keyed._pc_key\n"
        f"    LEFT JOIN {geocode_serving_overlay.CITY_CENTROIDS_TABLE} AS cc"
        " ON cc.key = keyed._city_key"
    )
    return (
        "SELECT\n"
        "    key,\n"
        "    _tier AS tier,\n"
        f"    multiIf({postcode}, _pc_lat, {city}, _cc_lat, NULL) AS latitude,\n"
        f"    multiIf({postcode}, _pc_lon, {city}, _cc_lon, NULL) AS longitude,\n"
        f"    multiIf({postcode}, _pc_key, {city}, _city_key, '') AS locality,\n"
        f"    multiIf({postcode}, _pc_n, {city}, _cc_n, 0) AS point_count,\n"
        f"    multiIf({postcode}, _pc_spread, {city}, _cc_spread, NULL) AS spread_meters\n"
        f"FROM (\n    {tiered}\n) AS tiered\n"
        "WHERE _tier != ''"
    )


def query_documents_sql(table: str) -> str:
    """The per-run input table projected into the engine's input columns, in order."""
    projection = ",\n    ".join(SEARCH_DOCUMENT_INPUT_COLUMNS)
    return f"select\n    {projection}\nfrom {table}"


def search_text(address: NormalizedAddress) -> str:
    """The display line without its care-of part -- the text the matcher scores.

    The location key ignores care-of, so the matched text must too: `c/o Anna Svensson,
    Storgatan 5, 111 22 Stockholm` and `Storgatan 5, 111 22 Stockholm` are the same place
    and must produce the same candidates. The care-of is always the FIRST comma-separated
    part of the display line when there is one (normalize_se builds the line that way), so
    dropping it is a prefix removal, not a re-render -- the delivered casing of the rest is
    preserved exactly.
    """
    line = address.normalized_address
    if not address.care_of:
        return line
    head, separator, rest = line.partition(", ")
    if separator and head.casefold().startswith("c/o "):
        return rest
    return line


def store_row(
    address: NormalizedAddress,
    result: Mapping[str, Any],
    *,
    address_id: str,
    policy_version: str,
    reference_md5: str,
    run_id: str,
    matched_at: datetime,
) -> tuple[Any, ...]:
    """One geocode_store.STORE_COLUMNS-ordered insert tuple from one engine result row.

    The matcher's RAW outcome: no centroid, no relabelling. Non-nullable String columns get
    `''` and never `None` (migration 000317); `coordinate_method` is Nullable and carries
    `NULL` when there is no coordinate to have a method for. The seven `source_*` columns
    describe an IMPORTED row's provenance and stay NULL for a row this resolver computed.
    """
    has_coordinate = result["latitude"] is not None and result["longitude"] is not None
    values: dict[str, Any] = {
        "address_id": address_id,
        "policy_version": policy_version,
        "reference_md5": reference_md5,
        "address_identity_run_id": ADDRESS_ENTITY_RUN_ID,
        "normalized_match_key": address.normalized_address,
        "match_status": result["resolution_status"],
        "candidate_count": int(result["candidate_record_count"]),
        "candidate_record_ids": list(result["candidate_record_ids"]),
        "candidate_record_urls": list(result["candidate_record_urls"]),
        "match_method": result["match_strategy"],
        "match_confidence": float(result["match_confidence"]),
        "latitude": result["latitude"],
        "longitude": result["longitude"],
        "geocode_provider": GEOCODE_PROVIDER if has_coordinate else "",
        "geocode_precision": result["geocode_precision"],
        "coordinate_method": COORDINATE_METHOD if has_coordinate else None,
        "coordinate_locality": result["matched_locality"],
        "coordinate_supporting_point_count": int(result["supporting_record_count"]),
        "coordinate_spread_meters": result["coordinate_spread_meters"],
        "source_record_id": None,
        "source_record_url": None,
        "source_url": None,
        "source_object_key": None,
        "source_md5": None,
        "source_snapshot_at": None,
        "source_retrieved_at": None,
        "geocode_run_id": run_id,
        "matched_at": matched_at,
    }
    return tuple(values[column] for column in geocode_store.STORE_COLUMNS)


def geocode_addresses(
    addresses: Mapping[str, NormalizedAddress],
    *,
    clickhouse: Any,
    duckdb: Any,
    run_id: str,
    matched_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, GeocodeOutcome]:
    """One served outcome per location key: cache, then matcher, then centroid overlay."""
    if not addresses:
        _log(log, "geocode: no addresses")
        return {}
    reference_md5 = ensure_reference_documents(duckdb, log=log)
    policy_version = SWEDEN_ADDRESS_RESOLUTION_POLICY.version

    outcomes = _read_cache(
        clickhouse,
        sorted(addresses),
        policy_version=policy_version,
        reference_md5=reference_md5,
    )
    misses = {key: address for key, address in addresses.items() if key not in outcomes}
    _log(
        log,
        "geocode: %d cache hits, %d misses (policy %s, reference %s)",
        len(outcomes),
        len(misses),
        policy_version,
        reference_md5,
    )

    if misses:
        results = _match(duckdb, misses, run_id=run_id, log=log)
        rows = [
            store_row(
                misses[key],
                result,
                address_id=key,
                policy_version=policy_version,
                reference_md5=reference_md5,
                run_id=run_id,
                matched_at=matched_at,
            )
            for key, result in results.items()
        ]
        clickhouse.execute(cache_insert_sql(), rows)
        for key, result in results.items():
            outcomes[key] = _outcome_from_result(
                key,
                result,
                policy_version=policy_version,
                reference_md5=reference_md5,
            )
        geocoded = sum(
            1
            for result in results.values()
            if result["resolution_status"] in geocode_store.GEOCODED_STATUSES
        )
        _log(
            log,
            "geocode: matched %d, %d geocoded, %d rows cached",
            len(results),
            geocoded,
            len(rows),
        )

    filled = _apply_fallback(clickhouse, outcomes, addresses)
    _log(
        log,
        "geocode: %d outcomes served, %d filled by a centroid",
        len(outcomes),
        filled,
    )
    return outcomes


def _read_cache(
    clickhouse: Any,
    keys: Sequence[str],
    *,
    policy_version: str,
    reference_md5: str,
) -> dict[str, GeocodeOutcome]:
    sql = cache_lookup_sql()
    hits: dict[str, GeocodeOutcome] = {}
    for chunk in _chunks(keys, CACHE_LOOKUP_CHUNK):
        for raw in clickhouse.execute(sql, {"keys": list(chunk)}):
            row = dict(zip(CACHE_COLUMNS, raw, strict=True))
            if not _is_hit(
                row, policy_version=policy_version, reference_md5=reference_md5
            ):
                continue
            hits[row["address_id"]] = GeocodeOutcome(
                location_key=row["address_id"],
                match_status=row["match_status"],
                match_method=row["match_method"],
                match_confidence=row["match_confidence"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                geocode_provider=row["geocode_provider"],
                geocode_precision=row["geocode_precision"],
                coordinate_method=row["coordinate_method"] or "",
                coordinate_locality=row["coordinate_locality"] or "",
                coordinate_supporting_point_count=row[
                    "coordinate_supporting_point_count"
                ],
                coordinate_spread_meters=row["coordinate_spread_meters"],
                policy_version=row["policy_version"],
                reference_md5=row["reference_md5"],
                from_cache=True,
            )
    return hits


def _is_hit(
    row: Mapping[str, Any], *, policy_version: str, reference_md5: str
) -> bool:
    """An adopted row, or a resolver row computed with exactly this run's two versions.

    The adopted family is the one-time import of the retired per-company matcher's
    decisions: it is on no resolver policy and no OSM extract of its own, so re-matching it
    for that reason alone would throw the import away on the first run. Recognized the way
    the store's rank does (`policy_version = 'legacy_adopted_v1'`), plus the run-id prefix
    the adoption step stamps, so either half alone still identifies the row.
    """
    if row["policy_version"] == geocode_store.LEGACY_ADOPTED_POLICY_VERSION:
        return True
    if str(row["address_identity_run_id"]).startswith(ADOPTED_RUN_ID_PREFIX):
        return True
    return (
        row["policy_version"] == policy_version
        and row["reference_md5"] == reference_md5
    )


def _match(
    duckdb: Any,
    addresses: Mapping[str, NormalizedAddress],
    *,
    run_id: str,
    log: Callable[..., object] | None,
) -> dict[str, Mapping[str, Any]]:
    """The resolver over the misses, in per-run tables that never outlive the call."""
    duckdb.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    tables = run_tables(run_id)
    try:
        replace_address_search_document_input_table(
            duckdb, table_name=tables["input"]
        )
        placeholders = ", ".join("?" for _ in SEARCH_DOCUMENT_INPUT_COLUMNS)
        duckdb.executemany(
            f"insert into {tables['input']} values ({placeholders})",
            [_input_row(key, address) for key, address in addresses.items()],
        )
        replace_address_search_documents(
            duckdb,
            source_sql=query_documents_sql(tables["input"]),
            table_name=tables["query"],
        )
        replace_address_street_variants(
            duckdb,
            document_table=tables["query"],
            variant_table=tables["variants"],
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
            exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
            separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
        )
        replace_address_resolution_candidates(
            duckdb,
            query_table=tables["query"],
            query_street_variant_table=tables["variants"],
            reference_table=QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
            candidate_table=tables["candidates"],
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        )
        replace_address_resolution_results(
            duckdb,
            query_table=tables["query"],
            candidate_table=tables["candidates"],
            result_table=tables["results"],
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        )
        rows = duckdb.execute(
            f"select {', '.join(RESULT_COLUMNS)} from {tables['results']}"
        ).fetchall()
    finally:
        for table in tables.values():
            duckdb.execute(f"drop table if exists {table}")
    results = {
        row[0]: dict(zip(RESULT_COLUMNS, row, strict=True)) for row in rows
    }
    missing = set(addresses) - set(results)
    if missing:
        raise ValueError(
            f"the resolver returned no row for {len(missing)} of {len(addresses)} "
            "query documents"
        )
    _log(log, "geocode: resolver returned %d results", len(results))
    return results


def run_tables(run_id: str) -> dict[str, str]:
    """The per-run table names, one per build step, keyed by step.

    The run id is reduced to its hex characters: a Dagster run id is a dashed UUID, and the
    dashes would need quoting in a DuckDB identifier. Two concurrent runs therefore never
    share a table, and a crashed run leaves at most one set behind under its own name.
    """
    suffix = "".join(
        character for character in run_id.lower() if character in "0123456789abcdef"
    )
    return {
        step: f"{ENRICHMENT_SCHEMA}._address_fold_{step}_{suffix}"
        for step in RUN_TABLE_STEPS
    }


def _input_row(key: str, address: NormalizedAddress) -> tuple[Any, ...]:
    """One engine input row. A box carries `address_kind = 'postal_box'`, which is what
    makes the resolver report `postal_box` for it without inventing a pin for a mail drop."""
    return (
        INDEX_SCOPE,
        key,
        "SE",
        address.normalized_address,
        search_text(address),
        address.street_name or "",
        address.house_number or "",
        address.unit or "",
        address.postal_code or "",
        address.city or "",
        "postal_box" if address.box else "physical",
        "",
        None,
        None,
        None,
        0,
        key,
        "",
    )


def _outcome_from_result(
    key: str,
    result: Mapping[str, Any],
    *,
    policy_version: str,
    reference_md5: str,
) -> GeocodeOutcome:
    has_coordinate = result["latitude"] is not None and result["longitude"] is not None
    return GeocodeOutcome(
        location_key=key,
        match_status=result["resolution_status"],
        match_method=result["match_strategy"],
        match_confidence=result["match_confidence"],
        latitude=result["latitude"],
        longitude=result["longitude"],
        geocode_provider=GEOCODE_PROVIDER if has_coordinate else "",
        geocode_precision=result["geocode_precision"],
        coordinate_method=COORDINATE_METHOD if has_coordinate else "",
        coordinate_locality=result["matched_locality"],
        coordinate_supporting_point_count=int(result["supporting_record_count"]),
        coordinate_spread_meters=result["coordinate_spread_meters"],
        policy_version=policy_version,
        reference_md5=reference_md5,
        from_cache=False,
    )


def _apply_fallback(
    clickhouse: Any,
    outcomes: dict[str, GeocodeOutcome],
    addresses: Mapping[str, NormalizedAddress],
) -> int:
    """Overlay the coarse centroid on every eligible outcome, cache hits included.

    Cache hits are covered because the overlay is never stored: an identity cached as
    `unmatched` must be filled on EVERY read, not only on the run that matched it.
    """
    eligible = sorted(
        key
        for key, outcome in outcomes.items()
        if outcome.match_status in geocode_serving_overlay.FALLBACK_ELIGIBLE_STATUSES
    )
    if not eligible:
        return 0
    sql = fallback_sql()
    filled = 0
    for chunk in _chunks(eligible, CACHE_LOOKUP_CHUNK):
        rows = [
            (
                key,
                addresses[key].postal_code or "",
                addresses[key].city or "",
            )
            for key in chunk
        ]
        for key, tier, latitude, longitude, locality, point_count, spread in (
            clickhouse.execute(sql, {"rows": rows})
        ):
            if tier not in (
                geocode_serving_overlay.POSTCODE_PRECISION,
                geocode_serving_overlay.CITY_PRECISION,
            ):
                continue
            outcomes[key] = dataclass_replace(
                outcomes[key],
                match_status=geocode_serving_overlay.GEOCODE_FALLBACK_STATUS,
                latitude=latitude,
                longitude=longitude,
                geocode_provider=geocode_serving_overlay.GEOCODE_FALLBACK_PROVIDER,
                geocode_precision=tier,
                coordinate_method=(
                    geocode_serving_overlay.GEOCODE_FALLBACK_COORDINATE_METHOD
                ),
                coordinate_locality=locality,
                coordinate_supporting_point_count=int(point_count),
                coordinate_spread_meters=spread,
            )
            filled += 1
    return filled


def _chunks(values: Sequence[str], size: int) -> list[Sequence[str]]:
    return [values[start : start + size] for start in range(0, len(values), size)]


def _log(log: Callable[..., object] | None, message: str, *arguments: object) -> None:
    if log is not None:
        log(message, *arguments)
