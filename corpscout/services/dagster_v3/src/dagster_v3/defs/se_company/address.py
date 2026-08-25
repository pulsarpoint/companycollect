"""Final Swedish company addresses: several rows per company, merged from the per-source
artifacts and augmented with the geocode the shared-identity chain already computed.

Inputs: se_company_address_bolagsverket (the registered postal address -- authoritative
for the fields both sources describe), se_company_address_scb.
Rules: address_rules (pure). Every field is copied from its owning source; nothing here is
model-written, so there is no observation table, no model profile and no model columns.
The whole per-company resolution is address_rules.resolve_company_addresses -- merge,
geocode augmentation, ledger, set replacement, in the one order that is correct. This
module never calls the four steps itself.
Geocode: se_company_address_members_current -> se_company_address_links_current ->
corpscout.se_address_geocodes read through sweden_company/geocode_store's versioned read,
keyed by the source observation's address_fingerprint, resolved at resolve time and stored
on the row. An identity can hold several attributable outcomes -- one per (matcher, OSM
snapshot) -- and which one is current is that rule, not a row.
Set replacement: a resolution publishes the company's whole address set; a key it no
longer produces is republished is_current = false. Readers filter FINAL ... WHERE is_current.
Ledger: se_company_address_correction -- override_field / reject_address / undo; stale by
the named row's evidence_set_hash; corrections never abort a run.
Trigger: se_company_address_weekly after the artifacts; se_company_address_correction_sensor
(ledger rows -> scoped review job); manual runs scoped by company_ids.
Gate: the asset writes nothing unless the run config says execute: true -- a bare
"Materialize" click in the Dagster UI is a preview that runs the change scan and reports
what a real run would select.

KNOWN PROPERTY -- an address that VANISHES from a source does not re-trigger the scan.
The artifacts are append-only: a row is written when a source's evidence hash changes, and
a source that simply stops carrying a company's address writes nothing at all. No
observed_at moves, no ledger row appears, the geocode store is unrelated -- so the
change scan below has nothing to select the company on, and the published row stays
is_current = true even though the register no longer says it. The set replacement only
fires on a resolution that actually happens. The mitigation is the same machinery the info
pilot uses: a periodic or owner-triggered ``resolve_all`` pass (with an explicit
``resolve_all_before`` cutoff when it cannot fit in one run) re-resolves every in-scope
company and tombstones whatever left. Between passes a departed address stays published;
that is accepted, not overlooked.

Assets
  se_company_address_clickhouse -> corpscout.se_company_address
"""

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address_rules import (
    AddressOutcome,
    GeocodeFact,
    resolve_company_addresses,
)
from dagster_v3.defs.se_company.bolagsverket import SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
from dagster_v3.defs.se_company.bolagsverket import TABLE as BOLAGSVERKET_TABLE
from dagster_v3.defs.se_company.common import (
    LedgerRow,
    build_ledger_sql,
    ledger_row_from_row,
    ledger_sensor,
    normalized_se_company_ids,
    publish_with_stage,
)
from dagster_v3.defs.se_company.info_rules import ArtifactRow
from dagster_v3.defs.se_company.scb import ADDRESS_TABLE as SCB_TABLE
from dagster_v3.defs.se_company.scb import SE_COMPANY_ADDRESS_SCB_COLUMNS
from dagster_v3.defs.sweden_company import geocode_store

DATABASE = "corpscout"
GROUP_NAME = "se_company"
SE_COMPANY_ADDRESS = "se_company_address"
SE_COMPANY_ADDRESS_CORRECTION = "se_company_address_correction"
MEMBERS_TABLE = "se_company_address_members_current"
LINKS_TABLE = "se_company_address_links_current"
# A LEFT JOIN miss reads as this instant, not as a bare NULL comparison.
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"

# ClickHouse's default max_query_size (262,144 bytes) is how many bytes of a query STRING
# the server will parse before rejecting it (Code: 62, "Max query size exceeded"). The
# change scan below (build_changed_companies_sql) embeds %(company_ids)s four times, and
# clickhouse-driver substitutes every id into the query text client-side -- so an explicit
# scope grows the rendered statement by ~4 copies * (len(id) + 3) bytes per id. Measured at
# company_batch_size's cap of 5,000: 283,099 bytes for 10-digit organisationsnummer,
# 323,099 bytes for 12-digit sole-trader ids (normalized_se_company_ids admits both) --
# both already past the default. Shrinking company_batch_size instead would buy only ~1%
# headroom even at 4,000 (259,099 bytes for the 12-digit worst case), a margin that
# re-erodes on any future edit to build_changed_companies_sql. The scan therefore raises
# max_query_size per query -- see its use in materialize_se_company_address -- to a size
# comfortably clear of the measured worst case, rather than shrinking the batch to barely
# fit under the default.
SCAN_MAX_QUERY_SIZE = 1_048_576  # 1 MiB: >3x the measured 323,099-byte worst case at the cap

# The geocode store is named ONCE, in geocode_store, and this module does not re-declare it
# under any spelling: `geocode_store.GEOCODE_STORE_TABLE` is the bare name (what
# assert_clickhouse_tables_exist looks up in system.tables) and
# `geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE` is the qualified one (what SQL
# reads). A local alias for either would put one identifier with two values in one file,
# and a later edit dropping the `geocode_store.` prefix would silently swap them.
#
# The read rule is likewise imported rather than re-expressed: a second ranking here would
# let this final and the serving table disagree about which outcome is current while both
# looked internally consistent, which spec section 8 names as this design's sharpest risk.
#
# What the versioned read is allowed to look at for one page of companies. It constrains
# address_id, the store's sorting-key leading column, so a page prunes to a few parts
# instead of ranking all 2.09M identities.
GEOCODE_ADDRESS_FILTER_SQL = f"""address_id IN (
        SELECT address_id
        FROM {DATABASE}.{LINKS_TABLE}
        WHERE company_id IN %(company_ids)s
    )"""
# Only the six columns this module actually reads -- the store has 28.
GEOCODE_READ_COLUMNS = (
    "address_id",
    "match_status",
    "latitude",
    "longitude",
    "matched_at",
    "geocode_run_id",
)

# This module's READ contract: the artifact modules' own positional insert lists (each
# pinned to the migration by its own test) minus the envelope this module reads by name.
# A renamed or dropped artifact column therefore fails loudly here instead of silently
# shifting values, and no column list is ever hand-copied.
ARTIFACT_ENVELOPE = ("company_id", "source_record_uid", "observed_at", "source_run_id")
ARTIFACT_TABLES: dict[str, str] = {"bolagsverket": BOLAGSVERKET_TABLE, "scb": SCB_TABLE}
ARTIFACT_READS: dict[str, tuple[str, ...]] = {
    "bolagsverket": tuple(column for column in SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
                          if column not in ARTIFACT_ENVELOPE),
    "scb": tuple(column for column in SE_COMPANY_ADDRESS_SCB_COLUMNS
                 if column not in ARTIFACT_ENVELOPE),
}
# The artifact read contract: (column, expression) in projection order, the expressions
# templated on the source literal and that source's payload map. Same shape as the geocode
# and published contracts below, so all three SELECTs and all three row mappers are
# generated from one list each and none can be read at the wrong offset.
ARTIFACT_PROJECTION: tuple[tuple[str, str], ...] = (
    ("source", "'{source}'"),
    ("company_id", "company_id"),
    ("source_record_uid", "source_record_uid"),
    ("evidence_hash", "toString(evidence_hash)"),
    ("observed_at", "observed_at"),
    ("payload_json", "toJSONString(map({pairs}))"),
)
ARTIFACT_COLUMNS = tuple(column for column, _ in ARTIFACT_PROJECTION)

# Why the scan picked a company, projected beside its id and counted per page. The reasons
# OVERLAP by construction (a never-published company also has evidence newer than its epoch
# resolved_at), so they are counters, never a partition. Derived from ARTIFACT_TABLES so a
# third artifact adds its reason to the SQL and to the metadata at once.
SELECTION_REASONS = (
    "never_published", *(f"new_evidence_{source}" for source in ARTIFACT_TABLES),
    "new_geocode", "ledger_pending",
)

# Everything the metadata reports beside the selection reasons. Seeded to 0 like the
# reasons are, so an execute run that selects nothing still returns the SAME metadata
# SHAPE as one that resolved thousands -- a reader (the backoffice run panel, a later
# comparison between two runs) never has to tell "this run wrote nothing" apart from
# "this key did not exist yet".
METRIC_KEYS = (
    "selected_company_count", "address_count", "tombstone_count", "geocoded_count",
    "applied_correction_count", "stale_correction_count", "inserted_count", "total_count",
)

# This module's WRITE contract: se_company_address insert columns in DDL order (the
# MATERIALIZED evidence_set_hash is omitted) -- pinned against the migration by the test.
INSERT_COLUMNS = (
    "company_id", "address_key", "address_type", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
    "address_id", "latitude", "longitude", "geocode_status", "geocoded_at", "is_current",
    "sources", "source_record_uids", "evidence_hashes", "correction_ids",
    "source_run_id", "resolved_at",
)

# The geocode read contract: (column, expression) in projection order. The SELECT and the
# row mapper are both generated from this one list, so a column cannot be added on one
# side only or read at the wrong offset.
_GEOCODE_HIT = "ifNull(geocodes.geocode_run_id, '') != ''"
GEOCODE_PROJECTION: tuple[tuple[str, str], ...] = (
    ("company_id", "members.company_id"),
    ("address_fingerprint", "toString(members.address_key)"),
    ("address_id", "toString(links.address_id)"),
    ("has_geocode", f"toUInt8({_GEOCODE_HIT})"),
    ("latitude", "ifNull(toString(geocodes.latitude), '')"),
    ("longitude", "ifNull(toString(geocodes.longitude), '')"),
    ("geocode_status", f"if({_GEOCODE_HIT}, toString(geocodes.match_status), '')"),
    ("geocoded_at", f"if({_GEOCODE_HIT}, toString(geocodes.matched_at), '')"),
)
GEOCODE_COLUMNS = tuple(column for column, _ in GEOCODE_PROJECTION)

# The tombstone read contract, same shape. The geocode columns are deliberately absent:
# a tombstone republishes the address, not a coordinate this resolution did not verify.
PUBLISHED_PROJECTION: tuple[tuple[str, str], ...] = (
    ("company_id", "published.company_id"),
    ("address_key", "toString(published.address_key)"),
    ("address_type", "toString(published.address_type)"),
    ("care_of", "published.care_of"),
    ("street_address", "published.street_address"),
    ("normalized_address", "published.normalized_address"),
    ("postal_code", "published.postal_code"),
    ("city", "published.city"),
    ("country_code", "published.country_code"),
    ("is_current", "published.is_current"),
    ("sources", "published.sources"),
    ("source_record_uids", "published.source_record_uids"),
    ("evidence_hashes", "published.evidence_hashes"),
)
PUBLISHED_COLUMNS = tuple(column for column, _ in PUBLISHED_PROJECTION)


def _projection_sql(projection: Iterable[tuple[str, str]]) -> str:
    return ",\n    ".join(f"{expression} AS {column}" for column, expression in projection)


def build_changed_companies_sql() -> str:
    """Companies whose address set is missing, older than their evidence, older than their
    stored geocode outcomes, or touched by a correction.

    Reasons to resolve a company again: it has never been published; an artifact carries an
    observation newer than the published resolution; the geocode store gained a newer
    outcome for it; or the correction ledger gained a row after it. There is no model in
    this datatype, so there is no "still owed something" term -- a resolution is complete
    the moment it is written. What there is likewise no term for is an address that VANISHED
    from a source: nothing is appended when a source stops carrying one, so nothing here can
    see it. See the module docstring for why that is accepted and what clears it.

    ``artifacts`` is the DRIVING table -- every other CTE is LEFT JOINed onto it -- so a
    company with no artifact row in either source is never selected, whatever the ledger or
    the geocode store says about it. A correction filed against a company neither source
    carries an address for therefore sits unapplied, and the correction sensor's run would
    report it as selected-nothing rather than as an error. Inherited from info.py's shape
    and true of it too; a known property, not a defect of this scan.

    ``max(observed_at)`` per artifact and ``max(matched_at)`` per company need no FINAL --
    each IS its own table's ReplacingMergeTree version column, the artifacts' and the
    geocode store's alike, so an unmerged older duplicate can never be the maximum. That is
    a STRONGER property than it used to be: the geocodes were a snapshot rebuilt whole per
    run, where "no FINAL" rested on the rebuild leaving one row per identity; the store is
    ReplacingMergeTree(matched_at) itself, so this maximum is version-safe by the engine's
    own contract. ``max(resolved_at)`` over the final is version-safe for the same reason,
    which is why nothing in this query is FINAL: a full-table dedup pass over a 4.7M-row
    final would be paid on every page for a value that cannot change.

    Every LEFT JOIN miss is read explicitly through ``ifNull``. Bare comparisons work only
    while ``join_use_nulls = 0``; under ``join_use_nulls = 1`` a miss is NULL, the WHERE is
    NULL for every never-published company, and the scan returns zero rows -- the pipeline
    would silently stop resolving anything.

    THE GEOCODE TERM IS NOW AS NARROW AS THE STORE MAKES IT.
    It used to be the widest term here: se_address_geocodes_current was rebuilt whole every
    week with one wall-clock matched_at, so every geocoded company looked changed every
    Monday and the weekly config had to run uncapped. corpscout.se_address_geocodes appends
    an outcome only for an identity a run actually matched, and matched_at is that append's
    instant -- so this term now selects register churn plus real geocode changes, which is
    what it always meant to say.

    It reads the store RAW, not through the versioned read. max(matched_at) over every
    stored row for an identity is always greater than or equal to the current outcome's, so
    this can over-select -- an identity whose newest row is outranked (an adopted row a
    later resolver success beat, say) wakes its companies once more than strictly needed --
    but it can never MISS a company whose served coordinate moved. Over-selection costs one
    republished version with identical content; under-selection would leave a stale
    coordinate served forever.

    THE NO-MISS HALF RESTS ON ONE CONSTRAINT, and it is a constraint on WRITERS rather than
    a property of this query: an append is stamped with the instant it is made, so any
    append that changes the served row also raises the maximum this term reads. An importer
    that back-dated its rows -- stamping an outcome with the instant it was originally
    decided rather than the instant it was stored -- could change what the store serves
    without moving max(matched_at) at all, and nothing here would ever see it. Both writers
    that exist satisfy the constraint: the weekly promotion stamps the append's own instant,
    and the legacy_adopted_v1 import stamps its run's (geocode_legacy_adoption). The
    one-time store backfill does copy matched_at, but only from serving rows that were
    already the served ones, so it changes no served answer. A future importer of historical
    outcomes would have to add a term here, not just an INSERT.

    Ranking 2.09M identities on every scan page to avoid the over-selection would be the
    wrong trade, and it would put a second copy of the read rule in this module, which is
    the thing the design forbids.

    The weekly automated config still runs effectively UNCAPPED, and that is now pure
    defense rather than a requirement this term imposes. This scan has no memory: it is
    ordered by ``company_id`` and every run starts from the first id again, so a run stopped
    by ``max_companies`` resumes at the START of the scan, not where it stopped -- a capped
    run re-resolves the same leading slice forever and never reaches the tail. For a
    deliberately staged pass the mechanism that does carry memory is ``resolve_all`` with an
    explicit ``resolve_all_before`` cutoff reused across the runs of that pass. What the
    geocode term buys is unchanged: a re-geocode is evidence, and no company keeps a stale
    coordinate.

    ``resolve_all`` re-selects every in-scope company even though nothing moved -- for
    rules-only changes (new merge logic, a new artifact column) that no ``observed_at`` and
    no ledger row reflects. It carries a CUTOFF (``resolve_all_before``) because the scan
    has no memory of its own: it is ordered by ``company_id`` and every run starts from the
    first id again, so a pass capped below the table size would re-select the SAME slice
    forever (observed in production on the info final). A company whose published
    ``resolved_at`` is already at or after the cutoff has been rewritten by this pass and is
    skipped. The cutoff is ALWAYS bound -- ``parseDateTime64BestEffort`` is parsed whether
    or not the flag beside it is on, so an empty string would be an error, not a no-op.

    One page per call: the LIMIT is the page size and the caller resumes from
    ``after_company_id``. Each selected row also carries WHY it was selected -- the same
    expressions the WHERE is built from, spelled twice from one Python constant because a
    SELECT-list alias is not guaranteed visible to WHERE at the same level in ClickHouse.

    The geocode CTE joins ``se_address_geocodes AS points``, not ``AS geocodes``: the CTE is
    itself called ``geocodes`` and the outer query reads it by that name, so reusing the name
    inside its own body would be one identifier with two meanings and an analyzer-dependent
    query.
    """
    artifact_union = "\n        UNION ALL\n        ".join(
        f"SELECT '{source}' AS source, company_id, max(observed_at) AS source_observed_at"
        f" FROM {DATABASE}.{table} GROUP BY company_id"
        for source, table in ARTIFACT_TABLES.items())
    published_at = f"ifNull(published.resolved_at, {EPOCH_SQL})"
    # maxIf over no rows returns the type's default -- 1970-01-01 for DateTime64 -- which is
    # exactly the instant an unpublished company is compared against, so a source the
    # company has no row in never reads as new evidence.
    per_source = ",\n        ".join(
        f"maxIf(source_observed_at, source = '{source}') AS {source}_observed_at"
        for source in ARTIFACT_TABLES)
    geocoded_at = f"ifNull(geocodes.latest_geocoded_at, {EPOCH_SQL})"
    correction_at = f"ifNull(ledger.latest_correction_at, {EPOCH_SQL})"
    reasons = ",\n    ".join((
        "ifNull(published.company_id, '') = '' AS never_published",
        *(f"artifacts.{source}_observed_at > {published_at} AS new_evidence_{source}"
          for source in ARTIFACT_TABLES),
        f"{geocoded_at} > {published_at} AS new_geocode",
        f"{correction_at} > {published_at} AS ledger_pending",
    ))
    return f"""WITH artifacts AS (
    SELECT company_id, max(source_observed_at) AS latest_observed_at,
        {per_source}
    FROM (
        {artifact_union}
    )
    WHERE (%(all_companies)s = 1 OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
ledger AS (
    SELECT company_id, max(created_at) AS latest_correction_at
    FROM {DATABASE}.{SE_COMPANY_ADDRESS_CORRECTION}
    WHERE (%(all_companies)s = 1 OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
geocodes AS (
    SELECT links.company_id AS company_id, max(points.matched_at) AS latest_geocoded_at
    FROM {DATABASE}.{LINKS_TABLE} AS links
    INNER JOIN {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE} AS points
        ON points.address_id = links.address_id
    WHERE (%(all_companies)s = 1 OR links.company_id IN %(company_ids)s)
    GROUP BY links.company_id
),
published AS (
    SELECT final.company_id AS company_id, max(final.resolved_at) AS resolved_at
    FROM {DATABASE}.{SE_COMPANY_ADDRESS} AS final
    WHERE (%(all_companies)s = 1 OR final.company_id IN %(company_ids)s)
    GROUP BY final.company_id
)
SELECT artifacts.company_id AS company_id,
    {reasons}
FROM artifacts
LEFT JOIN published ON published.company_id = artifacts.company_id
LEFT JOIN ledger ON ledger.company_id = artifacts.company_id
LEFT JOIN geocodes ON geocodes.company_id = artifacts.company_id
WHERE (
        ifNull(published.company_id, '') = ''
     OR (%(resolve_all)s = 1 AND {published_at} < parseDateTime64BestEffort(%(resolve_all_before)s, 3, 'UTC'))
     OR artifacts.latest_observed_at > {published_at}
     OR {geocoded_at} > {published_at}
     OR {correction_at} > {published_at}
      )
  AND artifacts.company_id > %(after_company_id)s
ORDER BY artifacts.company_id
LIMIT %(page_size)s"""


def build_artifact_rows_sql() -> str:
    """One SELECT per artifact naming exactly the columns this module reads, as a JSON map.

    No ORDER BY: merge_company_addresses picks the newest row per source by explicit keys,
    so arrival order never changes the outcome (and a trailing ORDER BY after UNION ALL
    binds to the last SELECT in ClickHouse anyway).
    """
    selects = []
    for source, columns in ARTIFACT_READS.items():
        pairs = ", ".join(f"'{column}', ifNull(toString({column}), '')" for column in columns)
        projection = _projection_sql(
            (column, expression.format(source=source, pairs=pairs))
            for column, expression in ARTIFACT_PROJECTION)
        selects.append(f"""SELECT
    {projection}
FROM {DATABASE}.{ARTIFACT_TABLES[source]} FINAL
WHERE company_id IN %(company_ids)s""")
    return "\nUNION ALL\n".join(selects)


def build_geocodes_sql() -> str:
    """What the shared-identity chain knows about each of this page's source observations.

    members.address_key IS se_company_addresses_current.address_fingerprint (see
    sweden_company/address_canonicalization.py, which selects it as exactly that), which is
    why the artifacts carry the fingerprint: it is the only join key from a company's own
    observation into the cross-company address identity and its geocode.

    The members -> links join is INNER: an observation that never reached an address
    identity has nothing to say here and is simply absent, which augment_with_geocodes
    reads as "no geocode". The geocode join is LEFT, because an identity can exist before
    the geocoder has answered for it -- and that miss is GATED, not ifNull'd: ClickHouse
    fills a LEFT JOIN miss with each column's TYPE DEFAULT, so match_status would read ''
    and matched_at 1970-01-01 as if the geocoder had answered. The gate itself is
    ``ifNull(geocodes.geocode_run_id, '') != ''`` rather than a bare ``!= ''``, because
    under join_use_nulls = 1 the miss really is NULL and a bare comparison would be NULL
    too. ifNull stays only where the SOURCE column is genuinely Nullable (latitude,
    longitude), which is a different question -- and both are projected as text so every
    column of this query is a plain String on both settings.

    The geocode side is no longer a table but the geocode store's versioned read, imported
    whole from sweden_company/geocode_store.py. An identity can hold several attributable
    outcomes -- one per (matcher, OSM snapshot) -- and which one is CURRENT is a rule, not a
    row. That rule is not restated here: this query asks for it. The subquery carries its
    own address filter so the store is pruned on its sorting key before it is ranked; the
    %(company_ids)s parameter is therefore bound twice in one statement, once inside the
    subquery and once in the outer WHERE, which ClickHouse's named parameters handle by
    name rather than by position.
    """
    versioned_read = geocode_store.build_current_geocodes_sql(
        columns=GEOCODE_READ_COLUMNS,
        address_filter_sql=GEOCODE_ADDRESS_FILTER_SQL,
    )
    return f"""SELECT
    {_projection_sql(GEOCODE_PROJECTION)}
FROM {DATABASE}.{MEMBERS_TABLE} AS members
INNER JOIN {DATABASE}.{LINKS_TABLE} AS links
    ON links.company_id = members.company_id
   AND links.canonical_address_key = members.canonical_address_key
LEFT JOIN (
{versioned_read}
) AS geocodes ON geocodes.address_id = links.address_id
WHERE members.company_id IN %(company_ids)s"""


def build_published_rows_sql() -> str:
    """This page's already-published rows, as the tombstone decision needs them.

    FINAL is required here and only here: the final is keyed by (company_id, address_key)
    and appends a version per resolution, so without it a key's older version could be read
    as still current.
    """
    return f"""SELECT
    {_projection_sql(PUBLISHED_PROJECTION)}
FROM {DATABASE}.{SE_COMPANY_ADDRESS} AS published FINAL
WHERE published.company_id IN %(company_ids)s"""


def _artifact_row_from_row(row: Sequence[Any]) -> tuple[str, ArtifactRow]:
    """(company_id, row). payload_json is a name->string map, so typed NULLs arrive as ''
    and numbers as text; address_rules treats '' as missing.

    Read by NAME through ARTIFACT_COLUMNS, like the geocode and published mappers: the
    SELECT and this function are generated from one list, so neither can drift.
    """
    values = dict(zip(ARTIFACT_COLUMNS, row, strict=True))
    return (str(values["company_id"]), ArtifactRow(
        source=str(values["source"]), source_record_uid=str(values["source_record_uid"]),
        evidence_hash=str(values["evidence_hash"]), observed_at=values["observed_at"],
        values=json.loads(str(values["payload_json"]))))


def _float(value: object) -> float | None:
    text = str(value if value is not None else "").strip()
    return float(text) if text else None


def _geocode_fact_from_row(row: Sequence[Any]) -> tuple[str, str, GeocodeFact]:
    """(company_id, address_fingerprint, fact) -- every column arrives as text."""
    values = dict(zip(GEOCODE_COLUMNS, row, strict=True))
    stamp = str(values["geocoded_at"] or "").strip()
    return (str(values["company_id"]), str(values["address_fingerprint"]), GeocodeFact(
        address_id=str(values["address_id"]), latitude=_float(values["latitude"]),
        longitude=_float(values["longitude"]), geocode_status=str(values["geocode_status"]),
        has_geocode=bool(int(values["has_geocode"])),
        geocoded_at=datetime.fromisoformat(stamp).replace(tzinfo=UTC) if stamp else None))


def _geocode_rank(fact: GeocodeFact) -> int:
    """How much a fact says -- the same three tiers augment_with_geocodes prefers between."""
    if fact.has_geocode and fact.latitude is not None:
        return 2
    return 1 if fact.has_geocode else 0


def _published_outcome_from_row(row: Sequence[Any]) -> AddressOutcome:
    """A published row as an AddressOutcome, so with_set_replacement compares like with like.

    Read by NAME through PUBLISHED_COLUMNS rather than by offset: the projection and this
    mapper are generated from one list, so neither can drift from the other.
    """
    values = dict(zip(PUBLISHED_COLUMNS, row, strict=True))
    return AddressOutcome(
        company_id=str(values["company_id"]), address_key=str(values["address_key"]),
        address_type=str(values["address_type"]), care_of=values["care_of"],
        street_address=values["street_address"], normalized_address=values["normalized_address"],
        postal_code=values["postal_code"], city=values["city"],
        country_code=values["country_code"], is_current=bool(values["is_current"]),
        sources=tuple(str(value) for value in values["sources"]),
        source_record_uids=tuple(str(value) for value in values["source_record_uids"]),
        evidence_hashes=tuple(str(value) for value in values["evidence_hashes"]))


def _final_row(outcome: AddressOutcome, *, source_run_id: str, resolved_at: datetime) -> tuple[Any, ...]:
    """One insert tuple, projected through INSERT_COLUMNS.

    Built from a name -> value map rather than written positionally: AddressOutcome's
    dataclass field order is NOT the migration's (the geocode block sits after the
    provenance arrays on the dataclass and before is_current in the table), so astuple() or
    a hand-written tuple would silently transpose same-typed columns. address_fingerprints
    is deliberately not among them -- it is the merge's own scratch provenance, not a
    stored column.
    """
    values: dict[str, Any] = {
        "company_id": outcome.company_id,
        "address_key": outcome.address_key,
        "address_type": outcome.address_type,
        "care_of": outcome.care_of,
        "street_address": outcome.street_address,
        "normalized_address": outcome.normalized_address,
        "postal_code": outcome.postal_code,
        "city": outcome.city,
        "country_code": outcome.country_code,
        "address_id": outcome.address_id,
        "latitude": outcome.latitude,
        "longitude": outcome.longitude,
        "geocode_status": outcome.geocode_status,
        "geocoded_at": outcome.geocoded_at,
        "is_current": outcome.is_current,
        "sources": list(outcome.sources),
        "source_record_uids": list(outcome.source_record_uids),
        "evidence_hashes": list(outcome.evidence_hashes),
        "correction_ids": list(outcome.correction_ids),
        "source_run_id": source_run_id,
        "resolved_at": resolved_at,
    }
    return tuple(values[column] for column in INSERT_COLUMNS)


def _resolve_page(
    *, clickhouse: ClickhouseResource, companies: Sequence[str], metrics: dict[str, int],
    source_run_id: str, resolved_at: datetime, log: Callable[..., object] | None,
) -> None:
    """Read one page's evidence, resolve every company in it and publish the results."""
    params = {"company_ids": tuple(companies)}
    with clickhouse.get_connection() as client:
        rows_by_company: dict[str, list[ArtifactRow]] = defaultdict(list)
        for row in client.execute(build_artifact_rows_sql(), params):
            company_id, artifact_row = _artifact_row_from_row(row)
            rows_by_company[company_id].append(artifact_row)
        geocodes_by_company: dict[str, dict[str, GeocodeFact]] = defaultdict(dict)
        for row in client.execute(build_geocodes_sql(), params):
            company_id, fingerprint, fact = _geocode_fact_from_row(row)
            # One fingerprint can appear under several canonical addresses in the member
            # bridge (its ORDER BY carries address_source and address_type too), so this
            # page can hand the same observation two facts. The one that says most wins --
            # a coordinate over a bare answer, an answer over a mere identity -- so a
            # later ungeocoded duplicate never overwrites a coordinate. Ties keep the
            # first, which is what augment_with_geocodes does within one address too.
            existing = geocodes_by_company[company_id].get(fingerprint)
            if existing is None or _geocode_rank(fact) > _geocode_rank(existing):
                geocodes_by_company[company_id][fingerprint] = fact
        published_by_company: dict[str, list[AddressOutcome]] = defaultdict(list)
        for row in client.execute(build_published_rows_sql(), params):
            outcome = _published_outcome_from_row(row)
            published_by_company[outcome.company_id].append(outcome)
        ledger_by_company: dict[str, list[LedgerRow]] = defaultdict(list)
        for row in client.execute(build_ledger_sql(SE_COMPANY_ADDRESS_CORRECTION), params):
            item = ledger_row_from_row(row)
            ledger_by_company[item.company_id].append(item)

    # Page-local counters: what THIS page did. They are folded into the run's cumulative
    # metrics below, but the log line reports the page, because a line labelled "page:"
    # that prints running totals reads as a page that grew every time.
    page: dict[str, int] = defaultdict(int)
    final_rows: list[tuple[Any, ...]] = []
    for company_id in companies:
        # The COMPOSED entry point, and only it: resolve_company_addresses applies
        # merge -> geocode augmentation -> ledger -> set replacement in the one order that
        # is correct (the ledger has to decide rows this resolution produced, before the
        # replacement adds tombstones it must not stamp). Calling the four public steps
        # from here would put that order in this module, where it would drift.
        outcomes, stale = resolve_company_addresses(
            company_id,
            rows_by_company.get(company_id, []),
            geocodes=geocodes_by_company.get(company_id, {}),
            published=published_by_company.get(company_id, []),
            ledger=ledger_by_company.get(company_id, []),
        )
        page["address_count"] += sum(1 for outcome in outcomes if outcome.is_current)
        page["tombstone_count"] += sum(1 for outcome in outcomes if not outcome.is_current)
        page["geocoded_count"] += sum(1 for outcome in outcomes
                                      if outcome.is_current and outcome.latitude is not None)
        page["applied_correction_count"] += sum(len(outcome.correction_ids) for outcome in outcomes)
        page["stale_correction_count"] += len(stale)
        if stale and log is not None:
            log("Stale corrections skipped: company=%s ids=%s", company_id, [str(item) for item in stale])
        final_rows.extend(_final_row(outcome, source_run_id=source_run_id, resolved_at=resolved_at)
                          for outcome in outcomes)

    for key, value in page.items():
        metrics[key] += value

    if final_rows:
        # new_versions_only stays off: the final is keyed by (company_id, address_key) and a
        # new version per resolution is the point -- ReplacingMergeTree(resolved_at) keeps
        # the newest, tombstones included.
        counts = publish_with_stage(
            clickhouse=clickhouse, target=SE_COMPANY_ADDRESS, insert_columns=INSERT_COLUMNS,
            rows=final_rows,
            invalid_condition="trim(company_id) = '' OR empty(source_record_uids) OR trim(address_type) = ''",
            new_versions_only=False)
        metrics["inserted_count"] += counts.inserted
        metrics["total_count"] = counts.total
    if log is not None:
        log("se_company_address page: companies=%s rows=%s addresses=%s tombstones=%s "
            "geocoded=%s corrections=%s stale=%s",
            len(companies), len(final_rows), page["address_count"], page["tombstone_count"],
            page["geocoded_count"], page["applied_correction_count"], page["stale_correction_count"])


def materialize_se_company_address(
    *, clickhouse: ClickhouseResource, source_run_id: str, resolved_at: datetime,
    company_ids: Sequence[str], max_companies: int, company_batch_size: int, execute: bool,
    log: Callable[..., object] | None, resolve_all: bool = False, resolve_all_before: str = "",
) -> dict[str, object]:
    """Resolve the changed companies -- or, with ``execute`` false, only say which.

    A preview runs the change scan exactly as a real run does (every chunk, every page, the
    same flags) and reports what it selected. It reads nothing else: no artifact rows, no
    geocodes, no published rows, no ledger -- and it writes nothing. That is the whole point
    of the flag: a "Materialize" click in the Dagster UI, which carries no config at all,
    must be free and harmless.
    """
    # One cutoff for the whole run: every chunk and every page binds this same value, so a
    # row this run publishes can never be re-selected by a later page of the same run. Empty
    # config -> the run's own resolved_at. Always bound, resolve_all or not: the scan's
    # parseDateTime64BestEffort is parsed regardless of the flag beside it.
    resolve_all_cutoff = resolve_all_before.strip() or resolved_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    scope = normalized_se_company_ids(company_ids)
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(
        *ARTIFACT_TABLES.values(), SE_COMPANY_ADDRESS, SE_COMPANY_ADDRESS_CORRECTION,
        MEMBERS_TABLE, LINKS_TABLE, geocode_store.GEOCODE_STORE_TABLE))
    # An explicit scope is split into chunks of at most company_batch_size ids, each paged
    # on its own. company_batch_size's cap of 5,000 is not what keeps the rendered scan
    # query under ClickHouse's max_query_size -- see SCAN_MAX_QUERY_SIZE above, which the
    # scan's own client.execute call raises for exactly that.
    chunks = [tuple(scope[start : start + company_batch_size])
              for start in range(0, len(scope), company_batch_size)] or [()]
    metrics: dict[str, int] = defaultdict(int)
    # Seeded rather than left to the defaultdict: "nothing happened for this counter" must
    # read as 0 in the metadata, not as a missing key.
    for key in (*SELECTION_REASONS, *METRIC_KEYS):
        metrics[key] = 0
    stopped_at_cap = False

    for chunk in chunks:
        if stopped_at_cap:
            break
        base = {"all_companies": int(not chunk), "company_ids": chunk or ("",),
                "resolve_all": int(resolve_all), "resolve_all_before": resolve_all_cutoff}
        after_company_id = ""
        while True:
            remaining = max_companies - metrics["selected_company_count"]
            if remaining <= 0:
                # Reachable only after a FULL page (a short page breaks below), so the scan
                # may well have more to give: this flag means "the cap stopped us".
                stopped_at_cap = True
                break
            page_size = min(company_batch_size, remaining)
            with clickhouse.get_connection() as client:
                # settings=... raises max_query_size for this query only -- the render at
                # company_batch_size's cap measures past ClickHouse's 262,144-byte default
                # (SCAN_MAX_QUERY_SIZE's docstring above has the measured numbers).
                page = client.execute(build_changed_companies_sql(),
                                      {**base, "after_company_id": after_company_id, "page_size": page_size},
                                      settings={"max_query_size": SCAN_MAX_QUERY_SIZE})
            companies = [str(row[0]) for row in page]
            if not companies:
                break
            after_company_id = companies[-1]
            for row in page:
                for offset, reason in enumerate(SELECTION_REASONS, start=1):
                    if row[offset]:
                        metrics[reason] += 1
            metrics["selected_company_count"] += len(companies)
            if execute:
                _resolve_page(clickhouse=clickhouse, companies=companies, metrics=metrics,
                              source_run_id=source_run_id, resolved_at=resolved_at, log=log)
            if len(companies) < page_size:
                break  # a short page means the scan is exhausted
    if stopped_at_cap and log is not None:
        log("se_company_address stopped at the max_companies cap (%s): changed companies may "
            "remain, the next run resumes from the start of the scan", max_companies)
    if not execute:
        return {**metrics, "preview": True, "stopped_at_cap": stopped_at_cap,
                "source_run_id": source_run_id, "company_scope": list(scope)}
    return {**metrics, "stopped_at_cap": stopped_at_cap, "source_run_id": source_run_id,
            "company_scope": list(scope)}


class SECompanyAddressConfig(dg.Config):
    # False = preview: run the change scan, report what a real run would select, write
    # nothing. The default is False so that a "Materialize" click in the Dagster UI -- which
    # sends no config at all -- can never rewrite every company's address set. Every real
    # run says execute: true explicitly: the schedule, the correction sensor and any manual
    # launch all do.
    execute: bool = False
    company_ids: list[str] = Field(default_factory=list)
    # Bounded at 5,000,000, not the info final's 1,000,000 -- and the reason is the SCAN,
    # not the geocodes. The weekly whole-table geocode restamp that used to make
    # new_geocode select every geocoded company is gone: the store appends only what a run
    # actually matched, so an ordinary week now selects register churn plus real geocode
    # changes. That holds from the SECOND week after the repoint -- the one-time backfill
    # copied every serving row's matched_at as one instant that postdates every pre-deploy
    # publish, so the FIRST weekly re-selects the whole published population once, exactly
    # as an old week did. The bound stays wide anyway, because the scan has NO MEMORY. It is ordered
    # by company_id and every run starts from the first id again, so a cap below the number
    # of companies a run selects does not SAMPLE that population -- it rewrites the same
    # leading slice every week and never reaches the tail. Nothing tells the weekly config
    # that number in advance, so the only safe automated setting is effectively uncapped; a
    # deliberately staged pass uses resolve_all with an explicit resolve_all_before
    # instead, which is the only memory there is.
    max_companies: int = Field(default=1_000_000, ge=1, le=5_000_000)
    # Both the scan page size and the chunk size for an explicit company_ids scope. The
    # scan embeds the id list four times client-side, which at this cap renders up to
    # 323,099 bytes for 12-digit sole-trader ids -- past ClickHouse's 262,144-byte default
    # max_query_size -- so the scan's own client.execute call raises max_query_size
    # (SCAN_MAX_QUERY_SIZE) rather than this bound shrinking to fit under the default.
    company_batch_size: int = Field(default=5_000, ge=1, le=5_000)
    # True = re-resolve every in-scope company even though no evidence moved. For rules-only
    # changes (new merge logic, a new artifact column): nothing marks those companies as
    # changed, so an ordinary run would resolve none of them. It is also what clears an
    # address that vanished from a source -- see the module docstring.
    resolve_all: bool = False
    # The cutoff resolve_all resumes from: a company whose published resolved_at is already
    # at or after it has been rewritten by this pass and is skipped. ISO-8601 UTC, e.g.
    # "2026-08-24 18:30:00". Empty means "this pass is one run". A pass that CANNOT fit in
    # one run (max_companies below the company count) must give an EXPLICIT cutoff -- the
    # instant before the first run started -- and reuse it for every run of the pass.
    resolve_all_before: str = ""


@dg.asset(
    name="se_company_address_clickhouse",
    deps=[dg.AssetKey("se_company_address_bolagsverket_clickhouse"),
          dg.AssetKey("se_company_address_scb_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{SE_COMPANY_ADDRESS}"},
    description=("Every registered address of a Swedish company, merged across sources with "
                 "full provenance, augmented with the shared-identity geocode, tombstoned "
                 "when a source stops carrying it, and overridable from the backoffice. A UI "
                 "materialization without execute=true is a preview that writes nothing."),
)
def se_company_address_clickhouse(context: dg.AssetExecutionContext, config: SECompanyAddressConfig,
                                  clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    """changed companies -> artifact rows -> resolve_company_addresses -> publish."""
    metadata = materialize_se_company_address(
        clickhouse=clickhouse, source_run_id=context.run_id, resolved_at=datetime.now(UTC),
        company_ids=config.company_ids, max_companies=config.max_companies,
        company_batch_size=config.company_batch_size, execute=config.execute,
        log=context.log.info, resolve_all=config.resolve_all,
        resolve_all_before=config.resolve_all_before)
    return dg.MaterializeResult(metadata={**metadata, "table": f"{DATABASE}.{SE_COMPANY_ADDRESS}"})


se_company_address_job = dg.define_asset_job("se_company_address_job", selection=dg.AssetSelection.assets(
    "se_company_address_bolagsverket_clickhouse", "se_company_address_scb_clickhouse",
    "se_company_address_clickhouse"))
se_company_address_review_job = dg.define_asset_job(
    "se_company_address_review_job", selection=dg.AssetSelection.assets("se_company_address_clickhouse"))
# Both automated triggers must resolve for real, so both spell execute out: a sensor-launched
# or scheduled run carries only the config written here, and anything left out falls back to
# the asset's own defaults -- which for this asset means resolving nothing. The weekly
# schedule reuses this same dict, so a scheduled run would also fall back to max_companies'
# default of 1,000,000 -- a cap this scan cannot sample under, because it restarts from the
# first company_id every run and would rewrite the same leading slice forever instead of ever
# reaching the tail (Ruling A17, documented on SECompanyAddressConfig.max_companies above).
# max_companies is set here to run effectively uncapped. The correction sensor's runs are
# scoped by company_ids regardless of this cap, so sharing the same value there is harmless.
AUTOMATED_RUN_CONFIG: dict[str, Any] = {"execute": True, "max_companies": 5_000_000}
se_company_address_correction_sensor = ledger_sensor(
    name="se_company_address_correction_sensor", table=SE_COMPANY_ADDRESS_CORRECTION,
    job=se_company_address_review_job, asset_names=("se_company_address_clickhouse",),
    extra_config=AUTOMATED_RUN_CONFIG)
# 06:55 Monday, five minutes after se_company_info_weekly's 06:50: the (minute, hour) slot
# must be unique across every schedule in this deployment, and both datatypes read the same
# weekly register load.
se_company_address_weekly = dg.ScheduleDefinition(
    name="se_company_address_weekly", job=se_company_address_job, cron_schedule="55 6 * * 1",
    execution_timezone="UTC", default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"se_company_address_clickhouse": {"config": dict(AUTOMATED_RUN_CONFIG)}}})

defs = dg.Definitions(assets=[se_company_address_clickhouse],
                      jobs=[se_company_address_job, se_company_address_review_job],
                      sensors=[se_company_address_correction_sensor],
                      schedules=[se_company_address_weekly])
