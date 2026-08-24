"""The Sweden address geocode store: its ClickHouse contract and its ONE read rule.

The store (`corpscout.se_address_geocodes`, migration 000317) holds one row per
(address identity, matcher, reference snapshot). "The current outcome for an identity" is
therefore a READ RULE over several rows, not a table -- and that rule lives here exactly
once, as SQL for the ClickHouse consumers and as a pure function for the demand scan.
Nothing else may re-express it: a consumer that inlined its own ranking would serve a
different coordinate from the one the demand scan believes is stored, and neither side
would raise.

THE RULE, IN TWO STAGES.

Stage 1 -- per matcher family, newest wins. Rank by (matched_at, reference_md5,
policy_version) descending and keep the first row per (address_id, is_adopted). At most two
rows per identity survive: its best resolver outcome and its best imported outcome.

Stage 2 -- choose between the survivors. Rank by (servable, matched_at, is_resolver,
reference_md5, policy_version) descending and keep the first row per address_id, where
`servable` is 1 for an adopted row or a GEOCODED resolver row and 0 for a resolver row that
did not geocode.

Read out, stage 2 says: a resolver `ambiguous` never takes a coordinate away from an
imported `legacy_adopted_v1` exact, however recent it is (that is what the import in spec
section 4.4 is for); a resolver outcome that DOES geocode takes over as soon as it is as new
as the adopted row, and does not take over if it is older.

WHY TWO STAGES AND NOT ONE RANK. The spec's two sentences -- "newest matched_at per
address_id across versions" and "legacy_adopted_v1 outranked by any same-or-newer resolver
outcome that is GEOCODED" -- are cyclic over three rows. Adopted exact at T1, resolver
matched_exact at T2, resolver ambiguous at T3: the resolver exact beats the adopted row, the
adopted row beats the resolver ambiguous, and the ambiguous (newest) beats the resolver
exact. Any flat rank breaks that cycle by silently dropping one of the sentences. Splitting
the demotion out to a comparison BETWEEN families, after each family has already been
reduced to its newest row, makes both stages plain total orders and keeps all three
sentences.

WHY NO `FINAL`. ReplacingMergeTree(matched_at) collapses rows sharing the full key triple,
but before a merge `FINAL` picks among equal-version rows in part order, which is not
deterministic. Ranking explicitly makes two reads of an unchanged store answer identically,
which the transition parity check and the harness both depend on. Rows sharing the whole key
AND matched_at are content-identical by the versioning contract, so the trailing rank
components never choose between differing content -- they only make the choice stable.
"""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

# Mirrors address_canonicalization.CLICKHOUSE_DATABASE / ENRICHMENT_SCHEMA. Spelled here so
# this module stays import-light: defs/se_company/address.py imports it, and
# address_canonicalization pulls in pyarrow and libpostal. tests/test_sweden_geocode_store.py
# asserts the two spellings agree.
CLICKHOUSE_DATABASE = "corpscout"
ENRICHMENT_SCHEMA = "sweden_company_enrichment"

GEOCODE_STORE_TABLE = "se_address_geocodes"
QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{GEOCODE_STORE_TABLE}"
)
# What promotion hands the ClickHouse append asset ...
GEOCODE_APPEND_TABLE = "se_address_geocodes_append"
QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE = f"{ENRICHMENT_SCHEMA}.{GEOCODE_APPEND_TABLE}"
# ... and what the demand asset loads back out of ClickHouse for the run to reason about.
PREVIOUS_OUTCOMES_TABLE = "se_address_geocodes_previous"
QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE = (
    f"{ENRICHMENT_SCHEMA}.{PREVIOUS_OUTCOMES_TABLE}"
)

LEGACY_ADOPTED_POLICY_VERSION = "legacy_adopted_v1"
LEGACY_ADOPTED_MATCH_METHOD = "legacy_adopted"
RESOLVER_ONLY_FILTER_SQL = f"policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'"
IS_ADOPTED_SQL = f"toUInt8(policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}')"

GEOCODED_STATUSES = (
    "matched_exact",
    "matched_corrected",
    "matched_site",
    "matched_area",
    "matched_street",
)
VALID_STATUSES = (
    *GEOCODED_STATUSES,
    "ambiguous",
    "unmatched",
    "invalid_address",
    "foreign_address",
    "postal_box",
    "property_identifier",
)

STORE_KEY_COLUMNS = ("address_id", "policy_version", "reference_md5")
# Migration 000317's declaration order. The append binds these positionally.
STORE_COLUMNS = (
    *STORE_KEY_COLUMNS,
    "address_identity_run_id",
    "normalized_match_key",
    "match_status",
    "candidate_count",
    "candidate_record_ids",
    "candidate_record_urls",
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
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "geocode_run_id",
    "matched_at",
)
# What se_address_geocodes_current holds: the store minus the two version columns. Equal to
# shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS, asserted rather than imported so this
# module keeps no dependency on the matcher-era module.
SERVING_COLUMNS = tuple(
    column
    for column in STORE_COLUMNS
    if column not in ("policy_version", "reference_md5")
)
# Columns both ranks read. The inner SELECT projects these whatever the caller asked for --
# SERVING_COLUMNS omits both version columns, and the choice rank needs them.
RANK_INPUT_COLUMNS = (
    "address_id",
    "policy_version",
    "reference_md5",
    "match_status",
    "matched_at",
)


def _quoted(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


NEWEST_PER_FAMILY_RANK_SQL = "tuple(matched_at, reference_md5, policy_version)"

CURRENT_OUTCOME_CHOICE_RANK_SQL = (
    "tuple(\n"
    f"        toUInt8(is_adopted = 1 OR match_status IN ({_quoted(GEOCODED_STATUSES)})),\n"
    "        matched_at,\n"
    "        1 - is_adopted,\n"
    "        reference_md5,\n"
    "        policy_version)"
)


def _inner_columns(columns: Sequence[str]) -> list[str]:
    inner = list(columns)
    for column in RANK_INPUT_COLUMNS:
        if column not in inner:
            inner.append(column)
    return inner


def build_current_geocodes_sql(
    *,
    columns: Sequence[str] = STORE_COLUMNS,
    address_filter_sql: str = "",
) -> str:
    """The current outcome per address identity, as a self-contained SELECT.

    ``address_filter_sql`` is inserted as the INNER query's WHERE and must constrain
    ``address_id`` -- the store's sorting key leads with it, so a page-sized read touches a
    few parts instead of ranking all 2.09M identities. It never goes on the outer query:
    that would be correct and would pay for the whole store on every page.

    The caller wraps the result in ``FROM ( ... ) AS <alias>``. There is no join here and no
    Nullable comparison, so the fragment answers identically under both join_use_nulls
    settings by construction.
    """
    outer_projection = ",\n    ".join(columns)
    inner_projection = ",\n        ".join(_inner_columns(columns))
    where = f"\n    WHERE {address_filter_sql}" if address_filter_sql else ""
    return (
        f"SELECT\n    {outer_projection}\n"
        "FROM (\n"
        f"    SELECT\n        {inner_projection},\n        {IS_ADOPTED_SQL} AS is_adopted\n"
        f"    FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}{where}\n"
        f"    ORDER BY address_id, is_adopted, {NEWEST_PER_FAMILY_RANK_SQL} DESC\n"
        "    LIMIT 1 BY address_id, is_adopted\n"
        ") AS candidates\n"
        f"ORDER BY address_id, {CURRENT_OUTCOME_CHOICE_RANK_SQL} DESC\n"
        "LIMIT 1 BY address_id"
    )


def build_current_resolver_geocodes_sql(
    *,
    columns: Sequence[str] = STORE_COLUMNS,
    address_filter_sql: str = "",
) -> str:
    """Stage 1 alone, over the resolver family: the newest resolver outcome per identity.

    This is what the demand scan reasons about, and it is deliberately NOT the served
    answer. An identity whose served answer is an imported adopted exact still has a
    resolver `ambiguous` behind it, and that ambiguous is what decides whether the identity
    belongs in the retry pool. Ranking the served answer here would make every adopted
    identity look permanently settled and the resolver would never try it again.

    ``address_filter_sql`` is parenthesized before it is ANDed with the adopted-exclusion.
    AND binds tighter than OR, so an unparenthesized ``a OR b`` caller filter would leave
    the exclusion attached to ``b`` alone and adopted rows would leak back into the resolver
    view through the first disjunct -- a wrong answer, not a syntax error.
    """
    projection = ",\n    ".join(columns)
    filters = [RESOLVER_ONLY_FILTER_SQL]
    if address_filter_sql:
        filters.insert(0, f"({address_filter_sql})")
    where = "\nWHERE " + "\n  AND ".join(filters)
    return (
        f"SELECT\n    {projection}\n"
        f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}{where}\n"
        f"ORDER BY address_id, {NEWEST_PER_FAMILY_RANK_SQL} DESC\n"
        "LIMIT 1 BY address_id"
    )


def is_geocoded(match_status: str) -> bool:
    return match_status in GEOCODED_STATUSES


@dataclass(frozen=True)
class StoredOutcome:
    """One stored row, reduced to what the ranks and the demand scan need.

    ``matched_at`` must carry the store's millisecond precision and no more: the SQL rank
    compares ``DateTime64(3, 'UTC')``, so a datetime holding sub-millisecond microseconds
    ranks differently here from the way the row it stands for ranks in ClickHouse. Outcomes
    read back out of the store get that for free; a caller stamping its own instant must
    truncate to milliseconds.
    """

    address_id: str
    policy_version: str
    reference_md5: str
    match_status: str
    matched_at: datetime


def is_adopted(outcome: StoredOutcome) -> bool:
    return outcome.policy_version == LEGACY_ADOPTED_POLICY_VERSION


def family_rank(outcome: StoredOutcome) -> tuple[datetime, str, str]:
    """Stage 1: the Python twin of NEWEST_PER_FAMILY_RANK_SQL."""
    return (outcome.matched_at, outcome.reference_md5, outcome.policy_version)


def choice_rank(outcome: StoredOutcome) -> tuple[int, datetime, int, str, str]:
    """Stage 2: the Python twin of CURRENT_OUTCOME_CHOICE_RANK_SQL."""
    servable = 1 if is_adopted(outcome) or is_geocoded(outcome.match_status) else 0
    return (
        servable,
        outcome.matched_at,
        0 if is_adopted(outcome) else 1,
        outcome.reference_md5,
        outcome.policy_version,
    )


def _newest(outcomes: Iterable[StoredOutcome]) -> StoredOutcome | None:
    best: StoredOutcome | None = None
    for outcome in outcomes:
        if best is None or family_rank(outcome) > family_rank(best):
            best = outcome
    return best


def current_resolver_outcome(
    outcomes: Iterable[StoredOutcome],
) -> StoredOutcome | None:
    """Stage 1 over the resolver family. This is what the demand scan reasons about."""
    return _newest(outcome for outcome in outcomes if not is_adopted(outcome))


def current_adopted_outcome(
    outcomes: Iterable[StoredOutcome],
) -> StoredOutcome | None:
    return _newest(outcome for outcome in outcomes if is_adopted(outcome))


def current_outcome(outcomes: Iterable[StoredOutcome]) -> StoredOutcome | None:
    rows = list(outcomes)
    candidates = [
        candidate
        for candidate in (
            current_resolver_outcome(rows),
            current_adopted_outcome(rows),
        )
        if candidate is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=choice_rank)


def current_outcomes_by_address(
    outcomes: Iterable[StoredOutcome],
) -> dict[str, StoredOutcome]:
    return _by_address(outcomes, current_outcome)


def current_resolver_outcomes_by_address(
    outcomes: Iterable[StoredOutcome],
) -> dict[str, StoredOutcome]:
    return _by_address(outcomes, current_resolver_outcome)


def _by_address(
    outcomes: Iterable[StoredOutcome],
    select: Callable[[list[StoredOutcome]], StoredOutcome | None],
) -> dict[str, StoredOutcome]:
    grouped: dict[str, list[StoredOutcome]] = {}
    for outcome in outcomes:
        grouped.setdefault(outcome.address_id, []).append(outcome)
    selected: dict[str, StoredOutcome] = {}
    for address_id, rows in grouped.items():
        chosen = select(rows)
        if chosen is not None:
            selected[address_id] = chosen
    return selected
