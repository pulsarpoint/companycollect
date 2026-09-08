"""Warm the geocode cache in bulk (spec section 6, amended 2026-09-07).

The matcher engine scans the whole OSM reference several times per call, so its cost is
mostly per call, not per query: the v7 rematch did 2.09M queries in one call in 3.2 h, while
a fold page of 20,000 companies (about 22,000 keys) took 66 to 112 minutes. This asset hands
every current location key to `geocode_addresses` in chunks of 150,000, so the population is
matched in bulk once per OSM extract and the fold pages hit the cache.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.geocode import GEOCODE_QUERY_SETTINGS, geocode_addresses
from dagster_v3.defs.se_company.address.normalize_se import LOCATION_FIELDS, NormalizedAddress, location_key
from dagster_v3.defs.sweden_company.geocode_serving_overlay import GEOCODE_FALLBACK_PROVIDER
from dagster_v3.defs.sweden_company.geocode_store import GEOCODED_STATUSES

# A 500,000-key chunk exhausted DuckDB's 97 GiB buffer pool on prod 2026-09-07; 150,000 ran
# fourteen chunks at about 24 minutes each.
CHUNK_SIZE = 150_000
WARM_STATUSES: tuple[str, ...] = ("ok", "partial")
# The seven location fields in the normalizer's order, plus one display line per group.
KEY_COLUMNS: tuple[str, ...] = LOCATION_FIELDS


@dataclass(frozen=True, slots=True)
class WarmCounts:
    keys: int
    chunks: int
    cache_hits: int
    matched: int      # keys the matcher resolved this run (misses)
    geocoded: int     # outcomes with a matcher coordinate (GEOCODED_STATUSES)
    fallback: int     # outcomes served by the centroid overlay

    def as_metadata(self) -> dict[str, int]:
        return {
            "keys": self.keys, "chunks": self.chunks, "cache_hits": self.cache_hits,
            "matched": self.matched, "geocoded": self.geocoded, "fallback": self.fallback,
        }


def keys_sql(*, limit: int = 0) -> str:
    """Every distinct location (the seven fields, care-of excluded) of the current
    non-draft ok/partial normalized rows, with one display line each. FINAL so a key whose
    current version is no_address does not warm through an older version."""
    columns = ", ".join(KEY_COLUMNS)
    statuses = ", ".join(f"'{status}'" for status in WARM_STATUSES)
    sql = (
        f"SELECT {columns}, any(normalized_address) AS normalized_address\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE source != 'reviewer_draft' AND parse_status IN ({statuses})\n"
        f"GROUP BY {columns}\n"
        f"ORDER BY {columns}"
    )
    if limit > 0:
        sql += "\nLIMIT %(limit)s"
    return sql


def address_from_row(row: Mapping[str, Any]) -> NormalizedAddress:
    """The normalizer's view of one key: no care-of (the location key has none), `ok` when
    both postcode and city are present, else `partial`."""
    status = "ok" if row["postal_code"] and row["city"] else "partial"
    return NormalizedAddress(
        None, row["box"], row["street_name"], row["house_number"], row["unit"], row["postal_code"], row["city"],
        row["country_code"], row["normalized_address"], status, "",
    )


def addresses_from_rows(rows: list[tuple[Any, ...]]) -> dict[str, NormalizedAddress]:
    out: dict[str, NormalizedAddress] = {}
    for values in rows:
        row = dict(zip((*KEY_COLUMNS, "normalized_address"), values, strict=True))
        address = address_from_row(row)
        out[location_key(address)] = address
    return out


def _chunks(keys: list[str], size: int) -> list[list[str]]:
    return [keys[i : i + size] for i in range(0, len(keys), size)]


def warm_geocodes(
    client: Any,
    duckdb: Any,
    *,
    run_id: str,
    matched_at: datetime,
    chunk_size: int = CHUNK_SIZE,
    limit: int = 0,
    log: Callable[..., object] | None = None,
) -> WarmCounts:
    """Read every current location key and geocode it through the cache-then-matcher
    function in bulk chunks. `geocode_addresses` inserts the fresh outcomes into the cache
    itself, so a crash between chunks costs only the chunk in flight."""
    params = {"limit": limit} if limit > 0 else {}
    rows = client.execute(keys_sql(limit=limit), params, settings=GEOCODE_QUERY_SETTINGS)
    addresses = addresses_from_rows(rows)
    keys = sorted(addresses)
    if log is not None:
        log("warm: %d rows, %d distinct location keys", len(rows), len(keys))
    cache_hits = matched = geocoded = fallback = 0
    chunks = _chunks(keys, chunk_size)
    for index, chunk in enumerate(chunks, start=1):
        outcomes = geocode_addresses(
            {key: addresses[key] for key in chunk}, clickhouse=client, duckdb=duckdb,
            run_id=run_id, matched_at=matched_at, log=log,
        )
        chunk_hits = sum(1 for o in outcomes.values() if o.from_cache)
        cache_hits += chunk_hits
        matched += len(outcomes) - chunk_hits
        geocoded += sum(1 for o in outcomes.values() if o.match_status in GEOCODED_STATUSES)
        fallback += sum(1 for o in outcomes.values() if o.geocode_provider == GEOCODE_FALLBACK_PROVIDER)
        if log is not None:
            log("warm: chunk %d/%d keys=%d hits=%d matched=%d", index, len(chunks), len(chunk), chunk_hits, len(chunk) - chunk_hits)
    return WarmCounts(keys=len(keys), chunks=len(chunks), cache_hits=cache_hits, matched=matched, geocoded=geocoded, fallback=fallback)
