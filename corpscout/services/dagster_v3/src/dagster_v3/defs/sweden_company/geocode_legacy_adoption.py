"""The one-time import of the retired per-company matcher's trapped exact decisions.

The resolver refuses ~19,413 companies' addresses as `ambiguous` that the legacy matcher
resolved `matched_exact` at confidence 1.0 on identical street text (measured on prod
2026-08-24). Those decisions live only in se_company_address_geocode_results, which retires
with the matcher. This module moves them into the versioned store as `legacy_adopted_v1`
outcomes so they stay attributable, rankable and reversible.

REVERSIBLE, NOT MERGED. An adopted row is distinguishable by its policy_version forever.
geocode_store's read rule serves it only while the identity's resolver outcome is
non-geocoded -- the moment a resolver run answers, the resolver's row outranks it and the
adopted row simply stops being served. Nothing is deleted and nothing is overwritten.

THE GRAIN CHANGE. The legacy table is keyed (company_id, canonical_address_key); the store
is keyed by address identity. Companies sharing an address produce several legacy rows for
one address_id, so the import groups by address_id and adopts only where every contributing
row agrees on the coordinate exactly. An identity whose legacy rows disagree is not adopted
-- "we had several different answers" is not a decision worth freezing into a store.

THE JOIN PATH is legacy -> se_company_address_links_current -> address_id. The design
document names se_company_address_members_current, which carries no address_id column at
all -- links is the only table holding that map.

THE SELECTION READS THE RESOLVER FAMILY, never the served answer. That is what makes the
import re-runnable: an identity the resolver has since geocoded drops out of the candidate
set on its own, so a second run cannot chase its own adopted rows, and an adopted row
already in the store never suppresses the identity's own re-selection while the resolver
still refuses it.

WHAT AN ADOPTED ROW DOES NOT DO -- THE POSTCODE-CONFLICT GATE (carried out of the Task 4
review). Once adopted rows exist, an identity SERVED by an adopted `matched_exact` still
presents its resolver `ambiguous`/`unmatched` to two readers that deliberately consult the
resolver family only: geocode_demand's pending scan, and the promotion's
postcode-conflict gate in address_resolution_promotion. For the demand scan that is the
point -- it is what keeps an adopted identity in the retry pool when the OSM snapshot
moves. For the gate it means the `in ('unmatched', 'ambiguous') -> false` arm admits a
street fallback for an identity whose served coordinate is an adopted building exact, and
the served precision drops from building to street the moment that fallback is promoted.

That is left exactly as it is, deliberately, for three reasons.

  1. It is the read rule's decision, already made. Spec 4.4 ranks ANY geocoded resolver
     outcome above an adopted row of equal or older vintage -- `matched_street` included.
     Re-litigating that precedence inside a promotion guard would put two different
     answers to one question in two places.
  2. The gate is a resolver-self-consistency guard, not a precedence rule. It exists so
     this policy's own street fallback cannot quietly undo a building match this policy
     previously made and the reference index still supports. An adopted row is by
     construction NOT this policy's decision; it is an imported signal the resolver
     refused to reproduce.
  3. Wiring adopted rows into it would be actively harmful. The gate does not filter rows
     -- it RAISES and aborts the whole promotion. Every adopted identity has a resolver
     `ambiguous`, which is what "several building candidates" looks like, so those
     identities would read as still-supported buildings and a single postcode-conflict
     fallback among ~19,413 of them would fail the weekly job outright.

Nothing is lost by leaving it: both rows stay in the store, the demotion is visible as a
version change rather than an overwrite, and the adopted coordinate is one
`policy_version` filter away. tests/test_sweden_geocode_legacy_adoption.py executes the
whole sequence -- adoption, the resolver view an adopted identity still presents, and the
street fallback taking over.
"""

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.address_geocoding import (
    QUALIFIED_CLICKHOUSE_RESULTS_TABLE,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    STORE_COLUMNS,
    build_current_resolver_geocodes_sql,
)

_GEOCODED = ", ".join(f"'{status}'" for status in GEOCODED_STATUSES)

# The identity's current RESOLVER answer -- adopted rows are irrelevant here, and an
# identity the resolver has already geocoded is never adopted.
_NON_GEOCODED_RESOLVER_SQL = build_current_resolver_geocodes_sql(
    columns=("address_id", "match_status")
)


def _selection_sql(agreement: str) -> str:
    """The join and the filters, with the coordinate-agreement test as the only variable.

    Spelled once and parameterised rather than string-replaced: the adopted count and the
    refused count have to describe the same population from opposite sides of one HAVING,
    and two separately maintained queries would eventually stop doing that.
    """
    return f"""FROM {QUALIFIED_CLICKHOUSE_RESULTS_TABLE} AS legacy
INNER JOIN {shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE} AS links
    ON links.company_id = legacy.company_id
   AND links.canonical_address_key = legacy.address_key
INNER JOIN (
{_NON_GEOCODED_RESOLVER_SQL}
) AS resolver ON resolver.address_id = links.address_id
WHERE legacy.match_status = 'matched_exact'
  AND legacy.match_confidence = 1.0
  AND isNotNull(legacy.latitude)
  AND isNotNull(legacy.longitude)
  AND resolver.match_status NOT IN ({_GEOCODED})
GROUP BY links.address_id
HAVING uniqExact(tuple(legacy.latitude, legacy.longitude)) {agreement}"""


_AGREED_SELECTION_SQL = _selection_sql("= 1")
_DISAGREED_SELECTION_SQL = _selection_sql("> 1")

ADOPTION_CANDIDATES_SQL = f"""SELECT
    links.address_id AS address_id,
    count() AS legacy_rows,
    uniqExact(legacy.company_id) AS companies
{_AGREED_SELECTION_SQL}"""

# Identities the rule REFUSES, reported beside the adoption count so the number is
# explainable rather than merely large.
ADOPTION_DISAGREEMENT_SQL = f"""SELECT
    count()
FROM (
    SELECT links.address_id
{_DISAGREED_SELECTION_SQL}
)"""

ADOPTION_INSERT_SQL = f"""INSERT INTO {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE} ({
    ", ".join(STORE_COLUMNS)
})
SELECT
    links.address_id AS address_id,
    '{LEGACY_ADOPTED_POLICY_VERSION}' AS policy_version,
    ifNull(any(legacy.source_md5), '') AS reference_md5,
    any(links.address_identity_run_id) AS address_identity_run_id,
    any(legacy.normalized_match_key) AS normalized_match_key,
    'matched_exact' AS match_status,
    toUInt16(any(legacy.candidate_count)) AS candidate_count,
    any(legacy.candidate_record_ids) AS candidate_record_ids,
    any(legacy.candidate_record_urls) AS candidate_record_urls,
    '{LEGACY_ADOPTED_MATCH_METHOD}' AS match_method,
    toFloat32(any(legacy.match_confidence)) AS match_confidence,
    any(legacy.latitude) AS latitude,
    any(legacy.longitude) AS longitude,
    any(legacy.geocode_provider) AS geocode_provider,
    any(legacy.geocode_precision) AS geocode_precision,
    any(legacy.coordinate_method) AS coordinate_method,
    any(legacy.coordinate_locality) AS coordinate_locality,
    toUInt32(any(legacy.coordinate_supporting_point_count))
        AS coordinate_supporting_point_count,
    any(legacy.coordinate_spread_meters) AS coordinate_spread_meters,
    any(legacy.source_record_id) AS source_record_id,
    any(legacy.source_record_url) AS source_record_url,
    any(legacy.source_url) AS source_url,
    any(legacy.source_object_key) AS source_object_key,
    any(legacy.source_md5) AS source_md5,
    any(legacy.source_snapshot_at) AS source_snapshot_at,
    any(legacy.source_retrieved_at) AS source_retrieved_at,
    %(geocode_run_id)s AS geocode_run_id,
    fromUnixTimestamp64Milli(toInt64(%(imported_at)s), 'UTC') AS matched_at
{_AGREED_SELECTION_SQL}"""

ADOPTION_SAMPLE_SQL = f"""SELECT
    toString(store.address_id),
    store.match_status,
    store.match_method,
    store.latitude,
    store.longitude,
    store.geocode_precision,
    toString(store.reference_md5)
FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE} AS store
WHERE store.policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'
ORDER BY store.address_id
LIMIT %(sample_size)s"""

# What the gated run measures after it writes: this run's own rows only. The store may
# already hold adopted rows from an earlier import, and counting those in would make a
# re-run's grain check answer for a population this run did not write.
ADOPTED_GRAIN_SQL = f"""SELECT count(), uniqExact(address_id)
FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
WHERE policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'
  AND geocode_run_id = %(geocode_run_id)s"""

ADOPTED_TOTAL_SQL = f"""SELECT uniqExact(address_id)
FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
WHERE policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'"""

ADOPTION_MEASUREMENT_SQL = f"""SELECT count(), sum(legacy_rows), sum(companies)
FROM (
{ADOPTION_CANDIDATES_SQL}
)"""
