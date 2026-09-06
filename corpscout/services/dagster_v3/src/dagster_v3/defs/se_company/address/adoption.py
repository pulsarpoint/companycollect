"""The one-time adoption of current geocode outcomes onto slice-2a location keys (spec
2026-09-06 section 6, Task 4 of the 2a plan).

Before this asset runs, `corpscout.se_address_geocodes` holds one row per OLD address
identity from `se_addresses_current` -- the pre-location-key grain. The address entity's
own geocode step (geocode.py, Task 3) reads and writes that same store keyed by LOCATION
key instead: seven components with care-of dropped, so several old identities that differ
only in who receives mail there now collapse onto one key. Without this asset the first
fold would treat every location key as a fresh miss and send the whole population through
the resolver, for identities the store had ALREADY decided.

THE COPY, NOT A RECOMPUTATION. For each old identity this asset normalizes it with the SAME
normalizer the address entity uses (`normalize_se_address`), computes its location key,
and -- when the identity's CURRENT outcome is worth keeping -- copies that outcome into a
NEW row under the location key, stamping `address_identity_run_id =
"adopted:<old address_id>"` (recognized by `geocode._is_hit` alongside
`LEGACY_ADOPTED_POLICY_VERSION`, so the address entity never re-matches it) and
`geocode_run_id` = this run's id. Every other column -- `policy_version`, `reference_md5`
and `matched_at` included -- is copied UNCHANGED: this asset makes no matching decision of
its own, it only relabels an existing one.

WHICH OUTCOMES ARE WORTH KEEPING (the 2026-09-06 sizing ruling). Of 2,090,981 old
identities only 1,138,307 carry a GEOCODED current outcome -- adopting only those would
still send the other ~950k straight into the resolver on the first fold, most of them for
no reason: they already carry a valid `unmatched`/`ambiguous`/`postal_box` decision FOR THE
CURRENT matcher and OSM extract, and re-matching them would only reproduce that same
answer. So an outcome is adopted when EITHER it is GEOCODED (any policy/reference -- a good
fix stays good regardless of which extract found it), OR its `(policy_version,
reference_md5)` is this run's CURRENT pair (then any status, geocoded or not, is a valid
cache entry for a matcher that has already looked and decided). Everything else -- a
non-geocoded outcome on a stale policy or a stale extract -- is `skipped_stale`: exactly
the retry population `geocode_demand` already exists to route back through the resolver.

SEVERAL OLD IDENTITIES, ONE KEY. Dropping care-of (and, here, folding case/whitespace
differences the old identity grain preserved) collapses distinct old identities onto one
location key. The first identity in `address_id` order is kept as the key's
representative -- its current outcome is the one copied -- and the rest are counted
`collapsed`, never looked up: with roughly half the population non-geocoded, hunting
through every collapsed identity's own outcome for a possibly-better answer is a second
policy decision this one-off import does not need to make.

IDEMPOTENT BY KEY. `existing_keys_sql()` skips any location key the store already holds a
row for, so a second run of this asset over an unchanged `se_addresses_current` adopts
nothing a first run already committed.
"""

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.assets import GROUP_NAME
from dagster_v3.defs.se_company.address.geocode import (
    ADOPTED_RUN_ID_PREFIX,
    CACHE_LOOKUP_CHUNK,
    QUALIFIED_STORE_TABLE,
    cache_insert_sql,
)
from dagster_v3.defs.se_company.address.normalize_se import (
    RawAddress,
    location_key,
    normalize_se_address,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.geocode_demand import QUERY_BATCH_SIZE
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODE_STORE_TABLE,
    GEOCODED_STATUSES,
    STORE_COLUMNS,
    build_current_geocodes_sql,
)

# `se_addresses_current`'s own table name (spelled here, not imported from
# shared_addresses.py, which pulls in address_canonicalization's pyarrow/libpostal
# dependency chain for a single string -- geocode_store.py makes the identical choice for
# CLICKHOUSE_DATABASE and says why).
IDENTITIES_TABLE = "se_addresses_current"
QUALIFIED_IDENTITIES_TABLE = f"{tables.DATABASE}.{IDENTITIES_TABLE}"

IDENTITY_COLUMNS: tuple[str, ...] = (
    "address_id",
    "street_address",
    "postal_code",
    "post_town",
    "country_code",
)

# The normalizer statuses that carry a usable location. `no_address`/`foreign` identities
# have no key to adopt onto (normalize_se.py).
NORMALIZED_KEY_STATUSES = ("ok", "partial")


def identities_page_sql() -> str:
    """One keyset page of old address identities, address_id ascending."""
    return (
        f"SELECT {', '.join(IDENTITY_COLUMNS)}\n"
        f"FROM {QUALIFIED_IDENTITIES_TABLE}\n"
        "WHERE address_id > %(after)s\n"
        "ORDER BY address_id\n"
        "LIMIT %(page_size)s"
    )


def current_outcomes_sql() -> str:
    """The store's current-outcome read rule (both stages of the rank, adopted rows
    included), bound to a page's representative OLD address ids."""
    return build_current_geocodes_sql(
        columns=STORE_COLUMNS, address_filter_sql="address_id IN %(ids)s"
    )


def existing_keys_sql() -> str:
    """Which of a batch of NEW location keys already hold a store row -- skip those."""
    return (
        f"SELECT DISTINCT address_id FROM {QUALIFIED_STORE_TABLE}"
        " WHERE address_id IN %(keys)s"
    )


def current_reference_md5_sql() -> str:
    """The newest resolver row's reference_md5 for the current policy -- the run's default
    'current pair' when the operator does not pin one in config."""
    return (
        f"SELECT reference_md5 FROM {QUALIFIED_STORE_TABLE}\n"
        "WHERE policy_version = %(policy_version)s\n"
        "ORDER BY matched_at DESC\n"
        "LIMIT 1"
    )


def adopted_row(
    outcome_row: Mapping[str, Any], *, new_key: str, old_id: str, run_id: str
) -> tuple[Any, ...]:
    """One STORE_COLUMNS-ordered insert tuple: `outcome_row` copied column for column,
    except `address_id` (the NEW location key), `address_identity_run_id`
    (`adopted:<old_id>`) and `geocode_run_id` (this run). `matched_at`, `policy_version` and
    `reference_md5` stay the ORIGINAL outcome's -- this asset makes no matching decision of
    its own, it only relabels an existing one."""
    values: dict[str, Any] = dict(outcome_row)
    values["address_id"] = new_key
    values["address_identity_run_id"] = f"{ADOPTED_RUN_ID_PREFIX}{old_id}"
    values["geocode_run_id"] = run_id
    return tuple(values[column] for column in STORE_COLUMNS)


def is_adoptable(
    outcome: Mapping[str, Any], *, policy_version: str, reference_md5: str
) -> bool:
    """The 2026-09-06 sizing ruling: geocoded on any version, or any status on THIS run's
    (policy_version, reference_md5) pair."""
    if outcome["match_status"] in GEOCODED_STATUSES:
        return True
    return (
        outcome["policy_version"] == policy_version
        and outcome["reference_md5"] == reference_md5
    )


@dataclass(frozen=True, slots=True)
class AdoptCounts:
    identities: int
    normalized: int
    collapsed: int
    geocoded: int
    skipped_stale: int
    existing: int
    adopted: int
    execute: bool

    def as_metadata(self) -> dict[str, Any]:
        return {
            "identities": self.identities,
            "normalized": self.normalized,
            "collapsed": self.collapsed,
            "geocoded": self.geocoded,
            "skipped_stale": self.skipped_stale,
            "existing": self.existing,
            "adopted": self.adopted,
            "execute": self.execute,
        }


def resolve_current_reference_md5(client: Any, *, policy_version: str) -> str:
    """`current_reference_md5_sql()`'s newest row for `policy_version`, or raise -- an
    empty answer means there is no resolver history at all to adopt onto, which the
    default (unconfigured) `reference_md5` relies on existing."""
    rows = list(
        client.execute(current_reference_md5_sql(), {"policy_version": policy_version})
    )
    if not rows:
        raise ValueError(
            f"no {QUALIFIED_STORE_TABLE} row exists for policy_version={policy_version!r}"
            " -- cannot derive the current reference_md5 (pass config.reference_md5"
            " explicitly)"
        )
    return str(rows[0][0])


def _chunks(values: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _identity_pages(
    client: Any, *, page_size: int
) -> Iterator[list[Sequence[Any]]]:
    """Keyset pages of `identities_page_sql()`, above an ever-increasing `address_id`
    cursor. Mirrors `geocode_demand.load_current_resolver_outcomes`'s own page loop."""
    after = ""
    while True:
        page = list(
            client.execute(
                identities_page_sql(), {"after": after, "page_size": page_size}
            )
        )
        if not page:
            return
        yield page
        after = str(page[-1][0])
        if len(page) < page_size:
            return


def _current_outcomes(client: Any, old_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    outcomes: dict[str, dict[str, Any]] = {}
    for chunk in _chunks(old_ids, CACHE_LOOKUP_CHUNK):
        for raw in client.execute(current_outcomes_sql(), {"ids": list(chunk)}):
            row = dict(zip(STORE_COLUMNS, raw, strict=True))
            outcomes[str(row["address_id"])] = row
    return outcomes


def _existing_keys(client: Any, keys: Sequence[str]) -> set[str]:
    existing: set[str] = set()
    for chunk in _chunks(keys, CACHE_LOOKUP_CHUNK):
        for (key,) in client.execute(existing_keys_sql(), {"keys": list(chunk)}):
            existing.add(str(key))
    return existing


def adopt_geocode_keys(
    client: Any,
    *,
    policy_version: str,
    reference_md5: str,
    run_id: str,
    page_size: int = QUERY_BATCH_SIZE,
    execute: bool = False,
    log: Callable[..., object] | None = None,
) -> AdoptCounts:
    """Walk every old address identity once, adopt its current outcome onto its location
    key. `execute=False` (the default) counts what a real run would do and writes nothing."""
    identities = normalized = collapsed = geocoded = 0
    skipped_stale = existing = adopted = 0
    seen_keys: dict[str, str] = {}  # location_key -> the representative's old address_id

    for page in _identity_pages(client, page_size=page_size):
        identities += len(page)
        page_representatives: list[str] = []
        new_key_by_old_id: dict[str, str] = {}
        for old_id, street_address, postal_code, post_town, country_code in page:
            old_id = str(old_id)
            normalized_address = normalize_se_address(
                RawAddress(
                    street_address=street_address,
                    postal_code=postal_code,
                    post_town=post_town,
                    country_code=country_code,
                )
            )
            if normalized_address.parse_status not in NORMALIZED_KEY_STATUSES:
                continue
            normalized += 1
            key = location_key(normalized_address)
            if key in seen_keys:
                collapsed += 1
                continue
            seen_keys[key] = old_id
            new_key_by_old_id[old_id] = key
            page_representatives.append(old_id)

        if page_representatives:
            outcomes_by_old_id = _current_outcomes(client, page_representatives)
            adoptable_keys: list[str] = []
            rows_by_key: dict[str, dict[str, Any]] = {}
            for old_id in page_representatives:
                outcome = outcomes_by_old_id.get(old_id)
                if outcome is None:
                    continue
                if is_adoptable(
                    outcome, policy_version=policy_version, reference_md5=reference_md5
                ):
                    geocoded += 1
                    new_key = new_key_by_old_id[old_id]
                    adoptable_keys.append(new_key)
                    rows_by_key[new_key] = outcome
                else:
                    skipped_stale += 1

            if adoptable_keys:
                already_stored = _existing_keys(client, adoptable_keys)
                existing += len(already_stored)
                rows_to_insert = [
                    adopted_row(
                        rows_by_key[key],
                        new_key=key,
                        old_id=seen_keys[key],
                        run_id=run_id,
                    )
                    for key in adoptable_keys
                    if key not in already_stored
                ]
                adopted += len(rows_to_insert)
                if execute and rows_to_insert:
                    client.execute(cache_insert_sql(), rows_to_insert)

        if log is not None:
            log(
                "adopt: page rows=%d totals identities=%d normalized=%d collapsed=%d "
                "geocoded=%d skipped_stale=%d existing=%d adopted=%d",
                len(page),
                identities,
                normalized,
                collapsed,
                geocoded,
                skipped_stale,
                existing,
                adopted,
            )

    return AdoptCounts(
        identities=identities,
        normalized=normalized,
        collapsed=collapsed,
        geocoded=geocoded,
        skipped_stale=skipped_stale,
        existing=existing,
        adopted=adopted,
        execute=execute,
    )


class AdoptKeysConfig(dg.Config):
    """A bare Materialize click WRITES NOTHING: `execute` defaults false and the run only
    reports what it would adopt. `reference_md5` pins the run's 'current pair' (with
    `policy_version`) for the amendment's second adopt condition; left blank, it is derived
    from the store's own newest row for the current policy (`resolve_current_reference_md5`)."""

    execute: bool = False
    page_size: int = Field(default=QUERY_BATCH_SIZE, ge=1, le=200_000)
    reference_md5: str = ""


@dg.asset(
    name="se_address_geocodes_adopt_keys",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": QUALIFIED_STORE_TABLE, "reads": QUALIFIED_IDENTITIES_TABLE},
    description=(
        "One-off: recomputes the slice-2a location key for every old address identity in "
        "se_addresses_current and copies that identity's current geocode outcome into "
        "se_address_geocodes under the new key, so the address entity's first fold "
        "rematches almost nothing. execute=false (the default) only counts."
    ),
)
def se_address_geocodes_adopt_keys(
    context: dg.AssetExecutionContext,
    config: AdoptKeysConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(GEOCODE_STORE_TABLE, IDENTITIES_TABLE)
    )
    policy_version = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
    with clickhouse.get_connection() as client:
        reference_md5 = config.reference_md5 or resolve_current_reference_md5(
            client, policy_version=policy_version
        )
        counts = adopt_geocode_keys(
            client,
            policy_version=policy_version,
            reference_md5=reference_md5,
            run_id=context.run_id,
            page_size=config.page_size,
            execute=config.execute,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts.as_metadata(),
            "policy_version": policy_version,
            "reference_md5": reference_md5,
            "table": QUALIFIED_STORE_TABLE,
        }
    )
