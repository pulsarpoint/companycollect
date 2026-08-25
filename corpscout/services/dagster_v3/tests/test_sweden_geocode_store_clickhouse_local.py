"""Executes the geocode store's read rule, its append semantics and its checks against the
migrations' own DDL in a disposable clickhouse-local. Proves the SQL runs on the deployed
ClickHouse version -- substring tests cannot, and the read rule is the one thing in this
design that no downstream failure would reveal if it were subtly wrong.

The fixture is ten identities, each a scenario the rule has to get right:

  SETTLED         one resolver matched_exact. The ordinary 2.09M case.
  RETRIED         resolver ambiguous, then matched_exact at a newer reference. The retry
                  pool doing its job -- the newer row wins because it is newer AND servable.
  REGRESSED       resolver matched_exact, then ambiguous at a newer reference. Newest still
                  wins: WITHIN the resolver family the rule is plain recency, and the
                  identity goes back into the retry pool. A flat rank gets this wrong.
  ADOPTED         a legacy_adopted_v1 exact, plus a STRICTLY NEWER resolver ambiguous. The
                  adopted row is still served, and the ambiguous is still what the demand
                  scan sees -- so the identity stays eligible for a rematch.
  RECLAIMED       the ADOPTED shape plus a later resolver matched_exact. The resolver takes
                  over and the adopted row is neither deleted nor merged, just outranked.
  TIED            an adopted exact and a resolver matched_exact carrying the SAME
                  matched_at, to the millisecond. The resolver row is served: stage 2 reads
                  "as new as it is" as enough to take over, and breaks the tie on
                  `1 - is_adopted`. RECLAIMED only shows that a STRICTLY newer resolver
                  exact wins; this is the boundary itself, which the pure twin has always
                  pinned and no engine ever executed until now.
  DEMOTED_STREET  an adopted exact that a later resolver `matched_street` DOES unseat.
  DEMOTED_AREA    the same with `matched_area`. These two are what ADOPTION_DEMOTION_SQL
                  is meant to count, and nothing else in this store is.
  SWALLOWED       one resolver matched_exact, and later a second append for the SAME key
                  triple stamped with an OLDER instant -- the silent no-op
                  build_store_append_regression_sql exists to see.
  RETIRED         a stored outcome for an identity se_addresses_current no longer carries.
                  A permanent store keeps those, and STORE_COVERAGE_SQL must stay silent
                  about them; reverse its anti-join and this row is what starts shouting.

Plus CHURNED, which has a company link and nothing else: no stored outcome and no identity
row. It is the LEFT JOIN miss in EXACT_MATCH_RATE_SQL, read through `ifNull` on both
settings, and it is deliberately kept out of se_addresses_current so that the coverage
anti-join keeps its own discrimination (0 in the shipped direction, 1 reversed).

The whole script runs twice, once with `join_use_nulls = 0` and once with 1. The read rule
has no joins, but the rate query and the coverage anti-join do, and both must answer
identically.

WHAT THIS FILE DELIBERATELY DOES NOT EXECUTE. DERIVED_PARITY_SQL, clean and drifted, is
executed in tests/test_sweden_geocode_derivation.py against the statements the derivation
asset itself issues -- which is where the aliased-subquery-aggregate shape belongs, since
that harness owns the serving table. Running it a third time here would replay two more
migrations for a query already settled by the engine twice.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    ADOPTION_DEMOTION_SQL,
    EXACT_MATCH_RATE_SQL,
    SNAPSHOT_FRESHNESS_SQL,
    STORE_COVERAGE_SQL,
    STORE_INVARIANTS_SQL,
    _ADOPTED_EXACT_FILTER_SQL,
    build_derived_current_geocodes_sql,
    build_store_append_regression_sql,
    epoch_milliseconds,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    SERVING_COLUMNS,
    STORE_COLUMNS,
    build_current_geocodes_sql,
    build_current_resolver_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
    _render,
)

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# 000274 creates both identity tables, 000278 adds the three address components
# SHARED_ADDRESS_COLUMNS carries, 000317 creates the store. The 000275 serving table and
# 000277's ALTER on it are absent on purpose: nothing under test here reads that table.
MIGRATIONS = (
    "000274_corpscout_se_shared_addresses.up.sql",
    "000278_corpscout_se_address_components.up.sql",
    "000317_corpscout_se_address_geocodes_store.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_addresses_current",
    "se_company_address_links_current",
    "se_address_geocodes",
})
_TABLE_RE = re.compile(
    r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE
)

STORE = QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
ADDRESSES = shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE
LINKS = shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
POLICY = "se-address-resolution-policy-v5"

# address_id is a FixedString(64) fingerprint of normalized address text.
(
    SETTLED,
    RETRIED,
    REGRESSED,
    ADOPTED,
    RECLAIMED,
    DEMOTED_STREET,
    DEMOTED_AREA,
    SWALLOWED,
    RETIRED,
) = (character * 64 for character in "123456789")
CHURNED = "a" * 64
# Outside the digit run because it arrived after the other nine were named; the shapes are
# named, not numbered, and renumbering them would churn every assertion below.
TIED = "b" * 64
STORED_IDENTITIES = (
    SETTLED,
    RETRIED,
    REGRESSED,
    ADOPTED,
    RECLAIMED,
    TIED,
    DEMOTED_STREET,
    DEMOTED_AREA,
    SWALLOWED,
    RETIRED,
)
# Every identity the register currently carries. RETIRED is gone from it (the store keeps
# its outcome anyway) and CHURNED never reached it.
CURRENT_IDENTITIES = tuple(
    identity for identity in STORED_IDENTITIES if identity != RETIRED
)

# Five weekly references and the one-off import, each with the OSM snapshot it was computed
# against. reference_md5 keys the store; source_snapshot_at is what the freshness check
# reads, and the two move together because a reference IS a snapshot.
MD5_1, MD5_2, MD5_3, MD5_4, MD5_5 = (f"md5-week-{week}" for week in range(1, 6))
MD5_LEGACY = "md5-legacy-import"
# The tie is ENGINEERED: no weekly pass lands on the import's own millisecond by itself.
# It gets its own reference rather than borrowing a week's, so that a reference still means
# exactly one OSM snapshot everywhere in this fixture.
MD5_TIE = "md5-tie"
SNAPSHOT_1 = datetime(2026, 7, 4, 1, tzinfo=UTC)
SNAPSHOT_2 = datetime(2026, 7, 11, 1, tzinfo=UTC)
SNAPSHOT_3 = datetime(2026, 7, 18, 1, tzinfo=UTC)
SNAPSHOT_4 = datetime(2026, 7, 25, 1, tzinfo=UTC)
SNAPSHOT_5 = datetime(2026, 8, 1, 1, tzinfo=UTC)
SNAPSHOT_LEGACY = datetime(2024, 1, 6, 1, tzinfo=UTC)
SNAPSHOT_TIE = datetime(2026, 7, 14, 1, tzinfo=UTC)
T_1 = datetime(2026, 7, 4, 3, tzinfo=UTC)
T_2 = datetime(2026, 7, 11, 3, tzinfo=UTC)
T_IMPORT = datetime(2026, 7, 15, 12, tzinfo=UTC)
# A non-zero millisecond part, and it is load-bearing: the regression guard's parameter is
# an integer tick count precisely because a `strftime('%Y-%m-%d %H:%M:%S')` literal would
# drop these 250ms, and `regression_truncated` below executes what that costs.
T_3 = datetime(2026, 7, 18, 3, 0, 0, 250_000, tzinfo=UTC)
T_4 = datetime(2026, 7, 25, 3, tzinfo=UTC)
T_5 = datetime(2026, 8, 1, 3, tzinfo=UTC)
RUN_1, RUN_2, RUN_3, RUN_4, RUN_5 = (f"run-week-{week}" for week in range(1, 6))
RUN_IMPORT = "run-adoption-import"
RUN_TIE = "run-tie"
RUN_LATE = "run-late-append"

COMPANIES = tuple(f"556000000{index}" for index in range(1, 6))
# (company, identity). CHURNED is linked and unstored; RETIRED is stored and unlinked.
MEMBERSHIPS = (
    (COMPANIES[0], SETTLED),
    (COMPANIES[0], REGRESSED),
    (COMPANIES[1], ADOPTED),
    (COMPANIES[1], TIED),
    (COMPANIES[2], RECLAIMED),
    (COMPANIES[2], DEMOTED_STREET),
    (COMPANIES[3], DEMOTED_AREA),
    (COMPANIES[3], RETRIED),
    (COMPANIES[4], SWALLOWED),
    (COMPANIES[4], CHURNED),
)

# STORE_INVARIANTS_SQL's own status/precision coupling, spelled out so the fixture cannot
# violate it by accident. Asserted exhaustive against GEOCODED_STATUSES below: a new
# geocoded status has to be given a precision here rather than defaulting to ''.
PRECISION_BY_STATUS = {
    "matched_exact": "building",
    "matched_corrected": "building",
    "matched_site": "site",
    "matched_area": "area",
    "matched_street": "street",
}
assert set(PRECISION_BY_STATUS) == set(GEOCODED_STATUSES)

_STORE_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "policy_version": f"'{POLICY}'",
    "reference_md5": f"'{MD5_1}'",
    "address_identity_run_id": "'identity-run-1'",
    "normalized_match_key": "'se|storgatan 1|11122|stockholm'",
    # The default row is a resolver `ambiguous`: no coordinate, no precision. Every
    # geocoded row says so explicitly through _geocoded().
    "match_status": "'ambiguous'",
    "candidate_count": "3",
    "candidate_record_ids": "[]",
    "candidate_record_urls": "[]",
    "match_method": "''",
    "match_confidence": "0.0",
    "latitude": "NULL",
    "longitude": "NULL",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "''",
    "coordinate_method": "NULL",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "0",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "NULL",
    "source_record_url": "NULL",
    # The five provenance columns STORE_INVARIANTS_SQL requires to be non-NULL.
    "source_url": "'https://download.geofabrik.de/sweden-latest.osm.pbf'",
    "source_object_key": "'osm/sweden-latest.osm.pbf'",
    "source_md5": f"'{MD5_1}'",
    "source_snapshot_at": _literal(SNAPSHOT_1),
    "source_retrieved_at": _literal(SNAPSHOT_1),
    "geocode_run_id": f"'{RUN_1}'",
    "matched_at": _literal(T_1),
}

_ADDRESS_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "canonical_display_address": "'Storgatan 1, 111 22 Stockholm'",
    "representative_address_source": "'bolagsverket'",
    "street_address": "'Storgatan 1'",
    "street_name": "'Storgatan'",
    "house_number": "'1'",
    "unit": "''",
    "postal_code": "'11122'",
    "post_town": "'Stockholm'",
    "country_code": "'SE'",
    "address_kind": "'street'",
    "normalized_street": "'storgatan 1'",
    "normalized_postal_code": "'11122'",
    "normalized_post_town": "'stockholm'",
    "address_types": "['postal']",
    "address_sources": "['bolagsverket']",
    "company_count": "1",
    "evidence_count": "1",
    "first_observed_at": _literal(T_1),
    "last_observed_at": _literal(T_1),
    "address_identity_run_id": "'identity-run-1'",
    "address_identity_built_at": _literal(T_1),
}

_LINK_DEFAULTS: dict[str, str] = {
    "company_id": "",
    "address_id": "",
    "canonical_address_key": f"'{'0' * 64}'",
    "address_types": "['postal']",
    "address_sources": "['bolagsverket']",
    "evidence_count": "1",
    "first_observed_at": _literal(T_1),
    "last_observed_at": _literal(T_1),
    "review_status": "'unreviewed'",
    "reviewed_at": "NULL",
    "reviewed_by": "''",
    "review_note": "''",
    "address_identity_run_id": "'identity-run-1'",
    "address_identity_built_at": _literal(T_1),
}


def _row(defaults: dict[str, str], columns: tuple[str, ...], **overrides: str) -> str:
    """One VALUES tuple, bound POSITIONALLY to the module's own column list.

    The assert is the point: a column added to the migration and forgotten here fails the
    script rather than silently shifting every value one place to the left.
    """
    row = {**defaults, **overrides}
    assert set(row) == set(columns), set(row) ^ set(columns)
    return "(" + ", ".join(row[column] for column in columns) + ")"


def _store_row(address_id: str, **overrides: str) -> str:
    return _row(
        _STORE_DEFAULTS, STORE_COLUMNS, address_id=f"'{address_id}'", **overrides
    )


def _reference(md5: str, snapshot: datetime, run: str, matched_at: datetime) -> dict[str, str]:
    """One weekly matcher pass: its OSM reference, its provenance and its append instant."""
    return {
        "reference_md5": f"'{md5}'",
        "source_md5": f"'{md5}'",
        "source_snapshot_at": _literal(snapshot),
        "source_retrieved_at": _literal(snapshot),
        "geocode_run_id": f"'{run}'",
        "matched_at": _literal(matched_at),
    }


def _geocoded(status: str, latitude: float, longitude: float) -> dict[str, str]:
    """A row that DID geocode: coordinates present and the precision its status implies."""
    return {
        "match_status": f"'{status}'",
        "candidate_count": "1",
        "candidate_record_ids": "['osm/way/1']",
        "candidate_record_urls": "['https://www.openstreetmap.org/way/1']",
        "match_method": "'country_street_house_exact_unique'",
        "match_confidence": "1.0",
        "latitude": str(latitude),
        "longitude": str(longitude),
        "geocode_precision": f"'{PRECISION_BY_STATUS[status]}'",
        "coordinate_method": "'osm_record'",
        "coordinate_supporting_point_count": "1",
        "source_record_id": "'osm/way/1'",
        "source_record_url": "'https://www.openstreetmap.org/way/1'",
    }


def _adopted(latitude: float, longitude: float) -> dict[str, str]:
    """The import's own shape: a legacy exact, attributed to the matcher that decided it.

    Written as one dict rather than two `**` expansions at the call site, because Python
    refuses duplicate keyword arguments and `match_method` is in both halves.
    """
    return _geocoded("matched_exact", latitude, longitude) | {
        "match_method": f"'{LEGACY_ADOPTED_MATCH_METHOD}'",
        "candidate_record_ids": "[]",
        "candidate_record_urls": "[]",
    }


WEEK_1 = _reference(MD5_1, SNAPSHOT_1, RUN_1, T_1)
WEEK_2 = _reference(MD5_2, SNAPSHOT_2, RUN_2, T_2)
WEEK_3 = _reference(MD5_3, SNAPSHOT_3, RUN_3, T_3)
WEEK_4 = _reference(MD5_4, SNAPSHOT_4, RUN_4, T_4)
WEEK_5 = _reference(MD5_5, SNAPSHOT_5, RUN_5, T_5)
IMPORT = _reference(MD5_LEGACY, SNAPSHOT_LEGACY, RUN_IMPORT, T_IMPORT) | {
    "policy_version": f"'{LEGACY_ADOPTED_POLICY_VERSION}'",
}
# A resolver pass whose append instant IS the import's, to the millisecond -- T_IMPORT, the
# same value IMPORT carries.
TIE = _reference(MD5_TIE, SNAPSHOT_TIE, RUN_TIE, T_IMPORT)

# The store as the fixture leaves it: eighteen rows over ten identities.
#
# Week 4 is the newest reference in the fixture and it touched exactly ONE identity --
# ADOPTED, which it answered `ambiguous`. That is demand-driven matching working, and it is
# also what makes SNAPSHOT_FRESHNESS_SQL discriminating here: the newest stored snapshot is
# carried ONLY by a row the versioned read discards.
FIXTURE_STORE_ROWS = (
    _store_row(SETTLED, **WEEK_1, **_geocoded("matched_exact", 59.30, 18.00)),
    _store_row(RETRIED, **WEEK_1),
    _store_row(RETRIED, **WEEK_2, **_geocoded("matched_exact", 59.31, 18.01)),
    _store_row(REGRESSED, **WEEK_1, **_geocoded("matched_exact", 59.32, 18.02)),
    _store_row(REGRESSED, **WEEK_2),
    _store_row(ADOPTED, **IMPORT, **_adopted(59.34, 18.04)),
    _store_row(ADOPTED, **WEEK_4),
    _store_row(RECLAIMED, **WEEK_1),
    _store_row(RECLAIMED, **IMPORT, **_adopted(59.35, 18.05)),
    _store_row(RECLAIMED, **WEEK_3, **_geocoded("matched_exact", 59.36, 18.06)),
    # Two rows, one instant: `IMPORT` and `TIE` both stamp T_IMPORT.
    _store_row(TIED, **IMPORT, **_adopted(59.43, 18.13)),
    _store_row(TIED, **TIE, **_geocoded("matched_exact", 59.44, 18.14)),
    _store_row(DEMOTED_STREET, **IMPORT, **_adopted(59.37, 18.07)),
    _store_row(DEMOTED_STREET, **WEEK_3, **_geocoded("matched_street", 59.38, 18.08)),
    _store_row(DEMOTED_AREA, **IMPORT, **_adopted(59.39, 18.09)),
    _store_row(DEMOTED_AREA, **WEEK_3, **_geocoded("matched_area", 59.40, 18.10)),
    _store_row(SWALLOWED, **WEEK_3, **_geocoded("matched_exact", 59.41, 18.11)),
    _store_row(RETIRED, **WEEK_1, **_geocoded("matched_exact", 59.42, 18.12)),
)
# Byte-identical to FIXTURE_STORE_ROWS[0]: re-matching an identity with the same matcher
# against the same OSM snapshot must reproduce the same answer, and re-appending it must be
# a no-op in content (spec section 5's idempotency).
REAPPENDED_SETTLED_ROW = FIXTURE_STORE_ROWS[0]
# The silent no-op: a late run answers for a key triple the store already holds a NEWER row
# for. ReplacingMergeTree(matched_at) keeps the newer one, so this append is invisible the
# moment it lands and no row count would show it.
LATE_SWALLOWED_ROW = _store_row(
    SWALLOWED,
    **(WEEK_3 | {"geocode_run_id": f"'{RUN_LATE}'", "matched_at": _literal(T_1)}),
    **_geocoded("matched_exact", 59.99, 18.99),
)
# The retry pool's payoff: REGRESSED, back in the pool because its newest row was
# `ambiguous`, is matched again at a new reference and starts serving a coordinate again.
REFERENCE_BUMP_ROW = _store_row(
    REGRESSED, **WEEK_5, **_geocoded("matched_exact", 59.50, 18.50)
)


def _insert(table: str, columns: tuple[str, ...], rows: tuple[str, ...]) -> str:
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n" + ",\n".join(rows) + ";"


def _fixture_statements() -> list[str]:
    return [
        _insert(
            ADDRESSES,
            shared_addresses.SHARED_ADDRESS_COLUMNS,
            tuple(
                _row(_ADDRESS_DEFAULTS, shared_addresses.SHARED_ADDRESS_COLUMNS,
                     address_id=f"'{identity}'")
                for identity in CURRENT_IDENTITIES
            ),
        ),
        _insert(
            LINKS,
            shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS,
            tuple(
                _row(_LINK_DEFAULTS, shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS,
                     company_id=f"'{company}'", address_id=f"'{identity}'")
                for company, identity in MEMBERSHIPS
            ),
        ),
        _insert(STORE, STORE_COLUMNS, FIXTURE_STORE_ROWS),
    ]


# --- the two natural wrong spellings of ADOPTION_DEMOTION_SQL -------------------------
#
# Both reuse the shipped adopted-exact population verbatim, so the ONLY thing that differs
# between the three counters is the rule. Neither is a strawman: each is what somebody
# writes who has read the check's name and not the read rule.

# (1) Read the RESOLVER view instead of the served answer. Plausible because the demand
# scan reads exactly that view -- but it counts an adopted identity whose newest resolver
# outcome is `ambiguous`, which is an identity STILL SERVING its adopted coordinate and
# still in the retry pool. Nothing has been demoted.
DEMOTION_VIA_RESOLVER_READ_SQL = f"""SELECT count()
FROM (
{
    build_current_resolver_geocodes_sql(
        columns=("address_id", "policy_version", "match_status"),
        address_filter_sql=_ADOPTED_EXACT_FILTER_SQL,
    )
}
) AS served
WHERE served.policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'
  AND served.match_status NOT IN ('matched_exact', 'matched_corrected')"""

# (2) Don't rank at all -- ask whether a non-exact resolver row EXISTS. Plausible because
# it is the cheapest query that mentions all the right words, and it additionally counts
# identities whose non-exact resolver row was itself superseded by a later exact one.
DEMOTION_VIA_EXISTENCE_SQL = f"""SELECT uniqExact(address_id)
FROM {STORE}
WHERE {_ADOPTED_EXACT_FILTER_SQL}
  AND policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'
  AND match_status NOT IN ('matched_exact', 'matched_corrected')"""

# The freshness query pointed at the versioned read instead of the whole store -- the
# mutation the docstring's "across every stored row" claim is about.
FRESHNESS_OVER_SERVED_SQL = f"""SELECT max(source_snapshot_at)
FROM (
{build_current_geocodes_sql()}
) AS served"""

# The coverage anti-join, reversed. A permanent store legitimately keeps outcomes for
# identities the register has dropped, so this direction reports a number nobody can act on.
COVERAGE_REVERSED_SQL = f"""SELECT count()
FROM {STORE} AS store
LEFT ANTI JOIN {ADDRESSES} AS address ON address.address_id = store.address_id"""

SERVED_COLUMNS = ("address_id", "policy_version", "reference_md5", "match_status")
RESOLVER_COLUMNS = ("address_id", "match_status", "reference_md5")
NULL_TSV = "\\N"  # how TabSeparated spells a NULL, on both join_use_nulls settings


def _schema_statements() -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order.

    000278 also alters two canonical/member tables this harness never reads, and whose DDL
    is not replayed here -- the per-statement filter is what keeps their ALTERs (which
    would fail against tables that were never created) out of the script.
    """
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


def _ordered(query: str, projection: str = "*") -> str:
    """A read wrapped so its row ORDER is the harness's, not the engine's.

    `LIMIT 1 BY` preserves the ORDER BY that feeds it, but nothing in the contract says a
    multi-threaded read has to hand the blocks back in that order, and a section compared
    row for row cannot afford to find out.
    """
    return f"SELECT {projection} FROM (\n{query}\n) ORDER BY address_id"


def _regression_probe(*, run_id: str, matched_at: datetime) -> str:
    return _render(
        build_store_append_regression_sql(),
        {"geocode_run_id": run_id, "matched_at_ms": epoch_milliseconds(matched_at)},
    )


def _script(*, join_use_nulls: int) -> str:
    parts = [f"SET join_use_nulls = {join_use_nulls};"]
    parts.extend(f"{statement};" for statement in _schema_statements())
    # The re-append and the late append both leave a second part holding a row the read
    # rule has to rank around. A background merge collapsing either one would turn the
    # assertions below into accidents, so merges are stopped for the whole script and
    # started only for the one OPTIMIZE that is itself under test.
    parts.append(f"SYSTEM STOP MERGES {STORE};")
    parts.extend(_fixture_statements())

    parts.append(_marked("served", _ordered(
        build_current_geocodes_sql(columns=SERVED_COLUMNS))))
    parts.append(_marked("resolver_view", _ordered(
        build_current_resolver_geocodes_sql(columns=RESOLVER_COLUMNS))))
    parts.append(_marked("derived", _ordered(build_derived_current_geocodes_sql())))

    parts.append(_marked("coverage", STORE_COVERAGE_SQL))
    parts.append(_marked("coverage_reversed", COVERAGE_REVERSED_SQL))
    parts.append(_marked("rate", EXACT_MATCH_RATE_SQL))
    parts.append(_marked("demotion", ADOPTION_DEMOTION_SQL))
    parts.append(_marked("demotion_via_resolver_read", DEMOTION_VIA_RESOLVER_READ_SQL))
    parts.append(_marked("demotion_via_existence", DEMOTION_VIA_EXISTENCE_SQL))
    parts.append(_marked("freshness", SNAPSHOT_FRESHNESS_SQL))
    parts.append(_marked("freshness_over_served", FRESHNESS_OVER_SERVED_SQL))

    parts.append(_marked("store_rows", f"SELECT count() FROM {STORE}"))
    parts.append(_insert(STORE, STORE_COLUMNS, (REAPPENDED_SETTLED_ROW,)))
    parts.append(_marked("store_rows_unmerged", f"SELECT count() FROM {STORE}"))
    parts.append(_marked("served_after_reappend", _ordered(
        build_current_geocodes_sql(columns=SERVED_COLUMNS))))
    # ... and now the ReplacingMergeTree contract, forced rather than waited for.
    # optimize_throw_if_noop makes a merge that did not happen an error instead of a
    # count that agrees for the wrong reason.
    parts.append(f"SYSTEM START MERGES {STORE};")
    parts.append(f"OPTIMIZE TABLE {STORE} FINAL SETTINGS optimize_throw_if_noop = 1;")
    parts.append(f"SYSTEM STOP MERGES {STORE};")
    parts.append(_marked("store_rows_after_optimize", f"SELECT count() FROM {STORE}"))
    parts.append(_marked("invariants", STORE_INVARIANTS_SQL))

    parts.append(_insert(STORE, STORE_COLUMNS, (LATE_SWALLOWED_ROW,)))
    parts.append(_marked("store_rows_after_swallowed_append",
                         f"SELECT count() FROM {STORE}"))
    parts.append(_marked("regression_swallowed",
                         _regression_probe(run_id=RUN_LATE, matched_at=T_1)))
    parts.append(_marked("regression_clean",
                         _regression_probe(run_id=RUN_3, matched_at=T_3)))
    # The same run, the same rows, the same store -- and the parameter a datetime binding
    # would have produced: T_3 with its 250ms dropped.
    parts.append(_marked("regression_truncated", _regression_probe(
        run_id=RUN_3, matched_at=T_3.replace(microsecond=0))))
    # All 26 columns again, not the four-column read: what the swallowed append changed
    # has to be nothing at all, coordinates included.
    parts.append(_marked("derived_after_swallowed_append",
                         _ordered(build_derived_current_geocodes_sql())))

    parts.append(_insert(STORE, STORE_COLUMNS, (REFERENCE_BUMP_ROW,)))
    parts.append(_marked("served_after_reference_bump", _ordered(
        build_current_geocodes_sql(columns=SERVED_COLUMNS))))
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(join_use_nulls=request.param),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def _served(rows: list[list[str]]) -> dict[str, tuple[str, str, str]]:
    """(policy_version, reference_md5, match_status) per identity."""
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


def _one(rows: list[list[str]]) -> int:
    [[value]] = rows
    return int(value)


def test_the_read_rule_answers_every_shape_the_store_can_hold(
    sections: dict[str, list[list[str]]],
) -> None:
    """The whole rule, executed. Each identity is a different way of getting it wrong."""
    served = _served(sections["served"])
    assert set(served) == set(STORED_IDENTITIES)
    assert served[SETTLED] == (POLICY, MD5_1, "matched_exact")
    # Newer AND servable: the retry pool's ordinary success.
    assert served[RETRIED] == (POLICY, MD5_2, "matched_exact")
    # Newest wins inside the resolver family even when the newest does not geocode.
    assert served[REGRESSED] == (POLICY, MD5_2, "ambiguous")
    assert served[ADOPTED] == (LEGACY_ADOPTED_POLICY_VERSION, MD5_LEGACY, "matched_exact")
    assert served[RECLAIMED] == (POLICY, MD5_3, "matched_exact")
    # Equal instants: the resolver takes over anyway.
    assert served[TIED] == (POLICY, MD5_TIE, "matched_exact")
    assert served[DEMOTED_STREET] == (POLICY, MD5_3, "matched_street")
    assert served[DEMOTED_AREA] == (POLICY, MD5_3, "matched_area")
    assert served[SWALLOWED] == (POLICY, MD5_3, "matched_exact")
    assert served[RETIRED] == (POLICY, MD5_1, "matched_exact")


def test_a_newer_resolver_ambiguous_does_not_unseat_an_adopted_exact(
    sections: dict[str, list[list[str]]],
) -> None:
    """Delete the `servable` component from the choice rank and this is what breaks: the
    import's recovered coordinates all revert to ambiguous at once, and every other section
    in this file still passes.

    ADOPTED's resolver `ambiguous` is STRICTLY NEWER than the adopted exact it must not
    displace, which is the only arrangement that can tell the rule apart from "newest
    matched_at wins".
    """
    policy, _reference_md5, status = _served(sections["served"])[ADOPTED]
    assert (policy, status) == (LEGACY_ADOPTED_POLICY_VERSION, "matched_exact")
    assert T_4 > T_IMPORT  # the ambiguous really is the newer row


def test_a_newer_resolver_ambiguous_DOES_unseat_an_older_resolver_exact(
    sections: dict[str, list[list[str]]],
) -> None:
    """The other half, and the reason the rule is two stages: promote `servable` above
    `matched_at` for the resolver family too and REGRESSED would keep serving a coordinate
    the current snapshot no longer supports, for ever, with nothing selecting it again."""
    assert _served(sections["served"])[REGRESSED] == (POLICY, MD5_2, "ambiguous")


def test_a_resolver_exact_as_new_as_the_adopted_row_takes_the_identity_over(
    sections: dict[str, list[list[str]]],
) -> None:
    """Stage 2's "as new as it is" boundary, executed. The pure twin has always pinned it;
    no engine had ever answered it until this identity.

    RECLAIMED's resolver exact is a WEEK newer than the adopted row it displaces, so it
    would win under a rule that demanded strictly newer as well. TIED's is not newer at
    all: both rows carry T_IMPORT to the millisecond, `servable` and `matched_at` are equal,
    and the choice falls through to `1 - is_adopted`, which prefers the resolver. Flip that
    component to `is_adopted`, or drop it, and this identity reverts to serving the import's
    coordinate under legacy_adopted_v1 -- while RECLAIMED, ADOPTED and every other shape
    here still answer correctly.

    It is also the row a REPEAT of the one-time store backfill would manufacture: that
    import copies each served row's own matched_at, so re-running it over a store that
    already holds adopted outcomes would land a policy-v5 exact on the adopted row's
    instant and take the coordinate over exactly like this. The backfill refuses a
    non-empty store for this reason.
    """
    served = _served(sections["served"])
    assert served[TIED] == (POLICY, MD5_TIE, "matched_exact")
    # The two rows really do carry one instant -- a tie, not a near miss.
    assert IMPORT["matched_at"] == TIE["matched_at"] == _literal(T_IMPORT)
    # And what gets published is the resolver's coordinate, not the import's.
    identity = SERVING_COLUMNS.index("address_id")
    latitude = SERVING_COLUMNS.index("latitude")
    coordinates = {row[identity]: row[latitude] for row in sections["derived"]}
    assert coordinates[TIED] == "59.44"


def test_the_demand_scan_sees_the_resolver_row_behind_an_adopted_answer(
    sections: dict[str, list[list[str]]],
) -> None:
    """Stage 1 alone, over the resolver family -- deliberately NOT the served answer.

    ADOPTED reads `ambiguous` here while serving an adopted exact, and that is what keeps
    the identity in the retry pool. Rank the SERVED answer here instead and every adopted
    identity looks permanently settled: 19,413 identities the resolver would never try
    again, with nothing anywhere raising.
    """
    resolver = {row[0]: (row[1], row[2]) for row in sections["resolver_view"]}
    assert set(resolver) == set(STORED_IDENTITIES)
    assert resolver[ADOPTED] == ("ambiguous", MD5_4)
    assert resolver[RECLAIMED] == ("matched_exact", MD5_3)
    assert resolver[REGRESSED] == ("ambiguous", MD5_2)
    # No legacy_adopted_v1 row survives the filter, whatever it was serving.
    assert MD5_LEGACY not in {reference for _status, reference in resolver.values()}


def test_the_serving_projection_is_the_read_rule_at_twenty_six_columns(
    sections: dict[str, list[list[str]]],
) -> None:
    """build_derived_current_geocodes_sql executed over the harder shapes: the derivation
    and the read are one expression, so they cannot disagree about which outcome is
    current."""
    rows = sections["derived"]
    assert {len(row) for row in rows} == {len(SERVING_COLUMNS)} == {26}
    identity = SERVING_COLUMNS.index("address_id")
    status = SERVING_COLUMNS.index("match_status")
    latitude = SERVING_COLUMNS.index("latitude")
    derived = {row[identity]: row[status] for row in rows}
    served = _served(sections["served"])
    assert derived == {address_id: value[2] for address_id, value in served.items()}
    # The coordinate the serving table would publish, which the four-column read cannot
    # show: the ADOPTED identity keeps the IMPORTED point, and the REGRESSED one publishes
    # no point at all rather than the one its superseded exact still holds.
    coordinates = {row[identity]: row[latitude] for row in rows}
    assert coordinates[ADOPTED] == "59.34"
    assert coordinates[REGRESSED] == NULL_TSV


def test_a_re_append_of_an_unchanged_outcome_changes_no_answer_and_no_row(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec section 5's idempotency, in both halves.

    In CONTENT the re-append is a no-op immediately: the read ranks around the extra part
    and answers identically, with merges stopped, so the answer never depended on a merge
    having run. In STORAGE it is a no-op once the ReplacingMergeTree collapses the key --
    forced here with OPTIMIZE FINAL rather than waited for, because a background merge is
    not something a test may hope for.
    """
    assert sections["served_after_reappend"] == sections["served"]
    assert _one(sections["store_rows"]) == len(FIXTURE_STORE_ROWS) == 18
    assert _one(sections["store_rows_unmerged"]) == 19
    assert _one(sections["store_rows_after_optimize"]) == 18


def test_the_store_invariants_hold_on_a_store_that_is_working(
    sections: dict[str, list[list[str]]],
) -> None:
    """STORE_INVARIANTS_SQL, executed after the OPTIMIZE. Eighteen rows, eighteen key
    triples, ten identities -- several outcomes per identity is the store working, and the
    grain that can still fail is the TRIPLE. Every violation counter reads zero, which is
    the fixture's own certificate: the statuses, the coordinate/precision agreements and
    the five provenance columns are all built to satisfy the shipped query, so a row this
    harness gets wrong shows up here rather than passing quietly.
    """
    [row] = sections["invariants"]
    rows, unique_keys, identities, *violations = (int(value) for value in row)
    assert (rows, unique_keys, identities) == (18, 18, len(STORED_IDENTITIES))
    assert violations == [0] * 7


def test_the_coverage_check_is_silent_about_a_retired_identity_and_only_that_way_round(
    sections: dict[str, list[list[str]]],
) -> None:
    """The anti-join's DIRECTION, executed. Every identity the register carries has an
    outcome, so the shipped query answers 0. RETIRED -- an outcome the store keeps for an
    identity se_addresses_current no longer has -- is what the reversed spelling reports,
    and it is a number nobody could act on."""
    assert _one(sections["coverage"]) == 0
    assert _one(sections["coverage_reversed"]) == 1


def test_the_exact_match_rate_counts_links_and_a_join_miss_as_no_geocode(
    sections: dict[str, list[list[str]]],
) -> None:
    """Ten links, six of them served a `matched_exact` and eight served any geocoded
    status. CHURNED is the LEFT JOIN miss: its company link exists and the store has never
    held a row for it, so `geocode.match_status` comes back '' under join_use_nulls = 0 and
    NULL under 1, and both settings answer 10/6/8.

    THE `ifNull` IS NOT WHAT MAKES THEM AGREE. `countIf` counts a row only when its
    predicate is TRUE, and a NULL predicate is not TRUE, so the miss is skipped under
    either setting with or without it. It is kept because it says out loud that a miss is
    "not geocoded" rather than unknown, and because the negated spellings of these
    predicates are NOT setting-proof: `ifNull(...) != 'matched_exact'` counts the miss,
    while a bare `!=` over the Nullable silently would not."""
    [row] = sections["rate"]
    assert [int(value) for value in row] == [len(MEMBERSHIPS), 6, 8]


def test_the_demotion_counter_counts_a_demotion_and_not_a_retry(
    sections: dict[str, list[list[str]]],
) -> None:
    """ADOPTION_DEMOTION_SQL executed, against the two wrong spellings of itself.

    Four identities hold an imported `matched_exact`. Only two of them -- DEMOTED_STREET
    and DEMOTED_AREA -- actually SERVE something coarser today, and 2 is what the shipped
    query answers.

    Reading the resolver view instead answers 3: it adds ADOPTED, whose newest resolver
    outcome is `ambiguous` but which is still serving its adopted coordinate and is still
    in the retry pool. Asking whether a non-exact resolver row merely EXISTS answers 4: it
    adds RECLAIMED as well, whose old `ambiguous` was superseded by a resolver exact a week
    later. Both wrong spellings would report a demotion that never happened -- a metric
    somebody would go looking for and not find.

    The three counters nest (shipped <= resolver-read <= existence), so the fixture has to
    separate all three at once; that is what ADOPTED and RECLAIMED are here for.
    """
    assert _one(sections["demotion"]) == 2
    assert _one(sections["demotion_via_resolver_read"]) == 3
    assert _one(sections["demotion_via_existence"]) == 4


def test_the_freshness_query_reads_every_stored_row_and_not_the_served_ones(
    sections: dict[str, list[list[str]]],
) -> None:
    """The newest OSM snapshot any stored outcome was computed against.

    Week 4 is the newest reference in this store and it touched exactly one identity,
    ADOPTED, which it answered `ambiguous` -- an outcome the read rule discards in favour
    of the adopted exact behind it. So the freshest snapshot is carried by a row nothing
    serves, and pointing the query at the versioned read instead reports week 3: a check
    meant to notice a stalled OSM download, quietly a week behind itself.
    """
    [[freshest]] = sections["freshness"]
    [[served]] = sections["freshness_over_served"]
    assert freshest.startswith(SNAPSHOT_4.strftime("%Y-%m-%d"))
    assert served.startswith(SNAPSHOT_3.strftime("%Y-%m-%d"))


def test_the_regression_guard_sees_an_append_that_was_swallowed(
    sections: dict[str, list[list[str]]],
) -> None:
    """build_store_append_regression_sql, executed -- the semantics, not the binding.

    RUN_LATE appends an outcome for a key triple the store already holds a NEWER row for,
    carrying a DIFFERENT coordinate. ReplacingMergeTree(matched_at) keeps the newer one, so
    the append is invisible the moment it lands: all 26 serving columns are unchanged for
    every identity, the row count grew, and nothing but this query can tell anyone. It
    counts 1.

    The same query over RUN_3 -- whose four rows ARE the newest for their triples, and one
    of whose triples the late append just landed in -- counts 0. That is what makes the
    first number a finding rather than a query that counts rows.
    """
    assert _one(sections["regression_swallowed"]) == 1
    assert _one(sections["regression_clean"]) == 0
    assert sections["derived_after_swallowed_append"] == sections["derived"]
    # The row really did land -- the append is a no-op in ANSWER, not in storage, and the
    # count is the half of that claim a reader would otherwise have to take on trust.
    stored_after_append = len(FIXTURE_STORE_ROWS) + 1
    assert _one(sections["store_rows_after_swallowed_append"]) == stored_after_append == 19


def test_a_second_truncated_parameter_would_report_every_row_as_a_regression(
    sections: dict[str, list[list[str]]],
) -> None:
    """Why the parameter is an integer tick count and not a datetime, executed.

    `clickhouse_driver` binds a datetime through `strftime('%Y-%m-%d %H:%M:%S')`, which
    drops the sub-second part. Against a millisecond-stamped column that literal is
    strictly smaller than the run's own rows, so every one of them counts as a regression
    -- all four of RUN_3's here, against the 0 the exact tick reports. The asset would
    raise on essentially every run, which is a pipeline that stops rather than a check that
    fires.
    """
    assert _one(sections["regression_truncated"]) == 4
    assert _one(sections["regression_clean"]) == 0


def test_a_new_reference_puts_a_regressed_identity_back_on_the_map(
    sections: dict[str, list[list[str]]],
) -> None:
    """The retry pool's end-to-end payoff, from the ClickHouse side.

    REGRESSED went back into the pool because its newest resolver outcome was `ambiguous`.
    A later week matches it at a new reference and the served answer flips to
    `matched_exact` again -- with the older ambiguous still stored beside it, attributable
    to the reference that produced it. The DuckDB half of this loop -- the demand rule
    selecting the identity in the first place -- is executed against a real in-memory
    DuckDB in tests/test_sweden_geocode_demand.py.
    """
    before = _served(sections["served"])[REGRESSED]
    after = _served(sections["served_after_reference_bump"])[REGRESSED]
    assert before == (POLICY, MD5_2, "ambiguous")
    assert after == (POLICY, MD5_5, "matched_exact")
    # Nothing else moved: a new reference appends beside, it does not overwrite.
    assert {
        identity: value
        for identity, value in _served(sections["served_after_reference_bump"]).items()
        if identity != REGRESSED
    } == {
        identity: value
        for identity, value in _served(sections["served"]).items()
        if identity != REGRESSED
    }
