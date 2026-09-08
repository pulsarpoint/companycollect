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
   with, or when it is a `legacy_adopted_v1` row (the imported family, which is on no
   resolver version at all and must not be re-matched merely for that -- mirrors
   `is_adopted` in the store's stage-2 rank). Everything else is a miss, an `adopted:` row
   from the adoption step included: it carries the ORIGINAL outcome's versions, so it is
   exactly as stale as what it copied and is re-matched after the next extract like any
   other row.

2. An ENGINE CALL for the misses. The same resolver the Sweden shadow evaluation runs
   (`address_resolution_shadow.replace_sweden_address_resolution_shadow`), on the same
   once-per-extract reference documents AND their fuzzy street postings, over per-run tables
   named with the run id and dropped in a `finally`. Nothing in the engine, the policy or the
   OSM assets changes here.

   BOTH shared inputs are per-EXTRACT caches in the enrichment schema, not per-call work:
   `ensure_reference_postings` builds the documents (keyed on the extract md5) and the
   postings (keyed on the md5, the policy version and the documents' own build stamp) only
   when one of them moved, and the candidate step is handed the postings by name. That matters because this function is
   called once per fold PAGE: rebuilding the postings -- an unnest of every reference
   street's deletion signatures plus a DISTINCT over millions of rows -- inside every page
   is what the one-shot rematch could afford and a paging caller cannot. The five per-run
   tables (input, query, variants, candidates, results) stay per call; so do the QUERY-side
   postings the engine builds from them.

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

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace as dataclass_replace
from datetime import datetime
from typing import Any

import pyarrow as pa

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
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
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
    QUALIFIED_REFERENCE_POSTINGS_TABLE,
    QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
    ensure_reference_postings,
)
from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

ENRICHMENT_SCHEMA = geocode_store.ENRICHMENT_SCHEMA
QUALIFIED_STORE_TABLE = geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE

# `address_identity_run_id` for a row this entity matches. The adoption step (Task 4) writes
# `adopted:<old address_id>` instead, which says WHERE a row came from -- provenance for a
# human reading the store. It is deliberately NOT part of the hit/miss decision (`_is_hit`):
# an adopted row keeps the versions of the outcome it copied and ages out with them.
ADDRESS_ENTITY_RUN_ID = "address-entity"
ADOPTED_RUN_ID_PREFIX = "adopted:"

# `parse_status` values the fold filters out before this function is called (spec section 6
# step 3): they have no location to match and no coordinate to serve.
UNMATCHABLE_PARSE_STATUSES = ("foreign", "no_address")

# Keys per id-bound statement, for both the cache lookup and the fallback's (key, postcode,
# city) triples. clickhouse-driver substitutes them CLIENT-side, so they land in the
# statement TEXT: 5,000 64-hex keys render the lookup to 341,333 bytes and the fallback to
# 461,740 bytes -- both past ClickHouse's 262,144-byte DEFAULT `max_query_size` (Code: 62,
# "Max query size exceeded"), which is why every id-bound read below passes
# GEOCODE_QUERY_SETTINGS. See tests/test_se_company_address_geocode.py for the measurement.
CACHE_LOOKUP_CHUNK = 5_000

# The raised setting, on the same 1 MiB precedent as basic_info/batch.py's
# ID_BOUND_QUERY_SETTINGS: >2x the measured worst case above. max_execution_time bounds the
# other failure mode -- a pathological read must fail visibly rather than hold the fold's
# pool slot forever.
GEOCODE_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

# The extract's provenance, read exactly as address_resolution_promotion.py reads it
# (`_replace_promotion_stage`'s `_sweden_address_resolution_osm_provenance`): one row,
# `first(... order by source_record_id)` per column, off the same workbench table
# `geocode_demand.fresh_reference_md5` takes the reference md5 from.
EXTRACT_PROVENANCE_SQL = f"""select
    first(source_url order by source_record_id),
    first(source_object_key order by source_record_id),
    first(source_md5 order by source_record_id),
    first(source_snapshot_at order by source_record_id),
    first(source_retrieved_at order by source_record_id)
from {osm_tables.QUALIFIED_ADDRESS_TABLE}"""

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
    "matched_at",
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

# A Swedish property designation (`Bergshamra 2:14`): a cadastral unit, not a street
# address. The Python twin of the shadow query projection's `regexp_matches(street_address,
# '(?i)(^|[[:space:]])[0-9]+:[0-9]+($|[[:space:],])')` -- same pattern, POSIX classes spelled
# as their `re` equivalents, matched against the same street text the shadow matches.
_PROPERTY_DESIGNATION = re.compile(r"(^|\s)[0-9]+:[0-9]+($|[\s,])")


@dataclass(frozen=True, slots=True)
class GeocodeOutcome:
    """One SERVED outcome for a location key: the matcher's row, or a centroid over it.

    `matched_at`: when the outcome was computed -- the cached row's stamp for a hit, this
    run's for a miss; the fold publishes it as `geocoded_at`.
    """

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
    matched_at: datetime


@dataclass(frozen=True, slots=True)
class ExtractProvenance:
    """The OSM extract every row this run writes was matched against.

    No stored outcome may carry a NULL in one of these five -- the contract the retired
    store-completeness check used to assert (`missing_provenance`, deleted with the demand
    chain in slice 4b) -- so the entity's rows carry them exactly as the promotion's
    imported rows did. The two per-RECORD columns (`source_record_id`,
    `source_record_url`) are a different thing and stay NULL: they name one imported source
    record, which a resolver answer over several candidates does not have, and the check
    does not count them.
    """

    source_url: str
    source_object_key: str
    source_md5: str
    source_snapshot_at: datetime
    source_retrieved_at: datetime


def extract_provenance(duckdb: Any) -> ExtractProvenance:
    """The one provenance row of the workbench's current extract.

    `first(...)` over an empty table returns a row of NULLs rather than no row, so an empty
    workbench is caught here instead of writing five NULLs into the store and failing the
    check on the next run.
    """
    [row] = duckdb.execute(EXTRACT_PROVENANCE_SQL).fetchall()
    if any(value is None for value in row):
        raise ValueError(
            f"{osm_tables.QUALIFIED_ADDRESS_TABLE} carries no extract provenance"
            " -- refusing to write geocode rows the store check would reject"
        )
    return ExtractProvenance(*row)


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


def street_line(address: NormalizedAddress) -> str:
    """The location half of the search text: the box, or street/number/unit."""
    if address.box:
        return f"Box {address.box}"
    return " ".join(
        part
        for part in (address.street_name, address.house_number, address.unit)
        if part
    )


def search_text(address: NormalizedAddress) -> str:
    """The text the matcher scores, COMPOSED from the components -- never carved out of the
    display line.

    The location key ignores care-of, so the matched text must too: `c/o Anna Svensson,
    Storgatan 5, 111 22 Stockholm` and `Storgatan 5, 111 22 Stockholm` are the same place
    and must produce the same candidates. Composing from the seven location components is
    the only way to guarantee that, because a care-of may itself contain a comma
    (`c/o Firm AB, Dept 4`) -- dropping the display line's first comma-separated part would
    then leave `Dept 4` in the matched text and score two identical locations differently.
    The components are exactly what `location_key` hashes, so equal keys now imply equal
    search text by construction rather than by the shape of the rendered line.

    The postcode is emitted UNSPACED (`11122`, not `111 22`): the shadow evaluation's query
    documents carry the register's own unspaced `postal_code`, and the engine's
    `raw_full_exact` strategy compares normalized FULL text, so a spaced twin here would
    score the same address differently in the entity than in the shadow.
    """
    postal = " ".join(
        part
        for part in (address.postal_code or "", address.city or "")
        if part
    )
    return ", ".join(part for part in (street_line(address), postal) if part)


def store_row(
    address: NormalizedAddress,
    result: Mapping[str, Any],
    *,
    address_id: str,
    policy_version: str,
    reference_md5: str,
    run_id: str,
    matched_at: datetime,
    provenance: ExtractProvenance,
) -> tuple[Any, ...]:
    """One geocode_store.STORE_COLUMNS-ordered insert tuple from one engine result row.

    The matcher's RAW outcome: no centroid, no relabelling. Non-nullable String columns get
    `''` and never `None` (migration 000317); `coordinate_method` is Nullable and carries
    `NULL` when there is no coordinate to have a method for. The two per-RECORD `source_*`
    columns describe an IMPORTED row's one source record and stay NULL here; the five
    per-EXTRACT ones carry `provenance`, because the live store check `missing_provenance`
    gates on them (see ExtractProvenance).
    """
    if provenance.source_md5 != reference_md5:
        raise ValueError(
            f"the extract provenance names snapshot {provenance.source_md5!r} but the row"
            f" would be keyed on reference_md5 {reference_md5!r} -- both are"
            " `first(source_md5 order by source_record_id)` off the same table and must"
            " agree"
        )
    has_coordinate = result["latitude"] is not None and result["longitude"] is not None
    values: dict[str, Any] = {
        "address_id": address_id,
        "policy_version": policy_version,
        "reference_md5": reference_md5,
        "address_identity_run_id": ADDRESS_ENTITY_RUN_ID,
        "normalized_match_key": address.normalized_address,
        "match_status": result["resolution_status"],
        # UInt16 (migration 000317): a common street in a big city can return more than
        # 65,535 candidates, and clickhouse-driver would reject the whole block. Clamped
        # exactly as address_resolution_promotion.py's `least(65535, ...)` clamps it.
        "candidate_count": min(65535, int(result["candidate_record_count"])),
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
        "source_url": provenance.source_url,
        "source_object_key": provenance.source_object_key,
        "source_md5": provenance.source_md5,
        "source_snapshot_at": provenance.source_snapshot_at,
        "source_retrieved_at": provenance.source_retrieved_at,
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
    _reject_unmatchable(addresses)
    reference_md5 = ensure_reference_postings(duckdb, log=log)
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
        provenance = extract_provenance(duckdb)
        rows = [
            store_row(
                misses[key],
                result,
                address_id=key,
                policy_version=policy_version,
                reference_md5=reference_md5,
                run_id=run_id,
                matched_at=matched_at,
                provenance=provenance,
            )
            for key, result in results.items()
        ]
        clickhouse.execute(cache_insert_sql(), rows, settings=GEOCODE_QUERY_SETTINGS)
        for key, result in results.items():
            outcomes[key] = _outcome_from_result(
                key,
                result,
                policy_version=policy_version,
                reference_md5=reference_md5,
                matched_at=matched_at,
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


def _reject_unmatchable(addresses: Mapping[str, NormalizedAddress]) -> None:
    """`foreign` and `no_address` rows never reach this function (spec section 6 step 3).

    The fold filters them out (slice 2b): they carry their status and no coordinates, and
    there is nothing for the matcher to look for. Refused loudly rather than matched,
    because the resolver would happily write an `unmatched` row into the cache for one and
    the mistake would then look like an ordinary miss forever.
    """
    for key in sorted(addresses):
        status = addresses[key].parse_status
        if status in UNMATCHABLE_PARSE_STATUSES:
            raise ValueError(
                f"geocode_addresses was handed a {status!r} address (location key {key})"
                " -- the fold filters these out before the geocode step"
            )


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
        for raw in clickhouse.execute(
            sql, {"keys": list(chunk)}, settings=GEOCODE_QUERY_SETTINGS
        ):
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
                matched_at=row["matched_at"],
            )
    return hits


def _is_hit(
    row: Mapping[str, Any], *, policy_version: str, reference_md5: str
) -> bool:
    """An imported row, or a row computed with exactly this run's two versions.

    `legacy_adopted_v1` is the one-time import of the retired per-company matcher's
    decisions: it is on no resolver policy and no OSM extract of its own, so re-matching it
    for that reason alone would throw the import away on the first run. Recognized the way
    the store's rank does (`policy_version = 'legacy_adopted_v1'`) and nothing else.

    The `adopted:` run-id prefix the adoption step stamps is NOT a second hit condition
    (2026-09-06 review). Adoption copies an old identity's outcome with its ORIGINAL
    `policy_version` and `reference_md5`, so an adopted row is exactly as stale as what it
    copied; treating the prefix as a hit would pin every adopted key to its imported answer
    forever, through every future policy bump and every future OSM extract.
    """
    if row["policy_version"] == geocode_store.LEGACY_ADOPTED_POLICY_VERSION:
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
    """The resolver over the misses, in per-run tables that never outlive the call.

    The reference documents and their fuzzy postings are NOT per-run: both are built once per
    OSM extract by `ensure_reference_postings` (the caller) and passed in by name.
    """
    duckdb.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    tables = run_tables(run_id)
    try:
        replace_address_search_document_input_table(
            duckdb, table_name=tables["input"]
        )
        insert_input_rows(
            duckdb,
            tables["input"],
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
            reference_postings_table=QUALIFIED_REFERENCE_POSTINGS_TABLE,
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


_INPUT_SCHEMA = pa.schema(
    [
        ("index_scope", pa.string()),
        ("document_id", pa.string()),
        ("country_code", pa.string()),
        ("raw_address", pa.string()),
        ("search_text", pa.string()),
        ("street_name", pa.string()),
        ("house_number", pa.string()),
        ("unit", pa.string()),
        ("postal_code", pa.string()),
        ("locality", pa.string()),
        ("address_kind", pa.string()),
        ("reference_precision", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("coordinate_spread_meters", pa.float64()),
        ("supporting_record_count", pa.uint32()),
        ("source_record_id", pa.string()),
        ("source_record_url", pa.string()),
    ]
)
# Not an assert: this runs at import time under load_from_defs_folder, and `python -O`
# would strip it.
if tuple(_INPUT_SCHEMA.names) != tuple(SEARCH_DOCUMENT_INPUT_COLUMNS):
    raise ValueError("_INPUT_SCHEMA must list SEARCH_DOCUMENT_INPUT_COLUMNS in order")


def insert_input_rows(duckdb: Any, table: str, rows: Sequence[tuple[Any, ...]]) -> None:
    """Insert the query rows as ONE vectorised statement.

    The rows become an Arrow table registered as a view and copied with a single
    `INSERT ... SELECT`. Neither of the row-by-row alternatives survives production: a bare
    `executemany` autocommits and syncs the write-ahead log per row (0.28 s each, measured
    2026-09-07), and `executemany` inside one transaction keeps every row's append state in
    DuckDB's buffer pool, which exhausted the 97 GiB limit at 462,683 rows the same day.
    """
    if not rows:
        return
    columns = list(zip(*rows, strict=True))
    arrow = pa.table(
        {
            field.name: pa.array(values, type=field.type)
            for field, values in zip(_INPUT_SCHEMA, columns, strict=True)
        }
    )
    view = f"{table.rsplit('.', 1)[-1]}_arrow"
    duckdb.register(view, arrow)
    try:
        duckdb.execute(f"insert into {table} select * from {view}")
    finally:
        duckdb.unregister(view)


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


def address_kind(address: NormalizedAddress) -> str:
    """What the engine keys its non-matching statuses off.

    A box is a mail drop: `postal_box` is what stops the resolver inventing a pin for one.
    A PROPERTY DESIGNATION (`Bergshamra 2:14` -- a cadastral unit, not a street address) is
    `property_identifier`, mirroring the refinement `address_resolution_shadow`'s query
    projection applies, so the address entity and the shadow evaluation classify the same
    text the same way. It matters twice: the engine reports the status instead of hunting
    for a street that does not exist, and `property_identifier` is deliberately NOT
    fallback-eligible, so a designation is never dressed up with a centroid.
    """
    if address.box:
        return "postal_box"
    if _PROPERTY_DESIGNATION.search(street_line(address)):
        return "property_identifier"
    return "physical"


def _input_row(key: str, address: NormalizedAddress) -> tuple[Any, ...]:
    """One engine input row, in SEARCH_DOCUMENT_INPUT_COLUMNS order."""
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
        address_kind(address),
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
    matched_at: datetime,
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
        matched_at=matched_at,
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
            clickhouse.execute(sql, {"rows": rows}, settings=GEOCODE_QUERY_SETTINGS)
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
