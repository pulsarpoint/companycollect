"""SE people identity-rule evaluation (K1/K2/K3) -- the Phase C research asset.

Per `docs/superpowers/specs/2026-08-27-se-people-experiment-design.md` section 3.2: today's
identity key (K1, `first_token|last_token`) has 11,713 known collisions in the Swedish
person data. This module implements the three candidate rules as pure functions and a
one-off manual analysis asset (`se_company_person_identity_evaluation`) that reads the three
SE person source views (`company_people/source_views.py`, migration 000330), evaluates all
three rules over the full corpus, and writes the K1-vs-K3 disagreements ("K1 would have
merged these, K3 keeps them apart") to the scratch `se_company_person_collision_candidate`
table for backoffice review. The owner picks the production rule from these numbers --
nothing here is auto-merged or served.

THE THREE RULES.

- **K1** (baseline, today's): ``first_token|last_token``, casefolded. Defined LOCALLY below
  by copying `company_people/normalization.py`'s `_name_match_key` -- NOT imported/aliased,
  per controller ruling, so a future change to normalization's key (Task 3) cannot silently
  change this evaluation's frozen baseline. `test_k1_matches_normalizations_current_key`
  pins today's equality between the two independent implementations.
- **K2** (full-name): every token, casefolded, whitespace-normalized, diacritics PRESERVED
  (`str.casefold()` never strips a diacritic -- "e5" and "e4" are distinct Unicode code
  points from "e", so "Åsa Öberg" and "Asa Oberg" produce different K2 keys).
  Middle names split people K1 merges; a misspelling splits people K2 (and K1) both miss.
- **K3** (K2 + a deterministic reconciliation pass, scoped to one company): two K2 clusters
  merge iff (a) they share the same first+last tokens (i.e. the same K1 key) AND one
  cluster's token set is the *unique minimal* strict superset of the other's -- a lone
  middle-name variant folds into the one name it unambiguously extends -- or (b) two rows
  anywhere in the company share a non-empty `person_wikidata_id` (an authoritative link that
  crosses K1 buckets entirely, e.g. a name-variant pair the QID confirms is one person).
  "Unique minimal" is the ambiguity guard required by the plan's own test case: "Anna
  Svensson" is a token subset of BOTH "Anna B Svensson" and "Anna C Svensson", so it has two
  incomparable superset candidates and merges with NEITHER -- all three stay separate, and
  because they share one K1 key across three different final K3 groups, all three become one
  collision-candidate group. Contrast "Anna Maria Svensson" / "Anna Svensson": the smaller
  cluster has exactly one (unique) superset candidate, so K3 merges them and nothing is
  written to the candidate table for that pair.

QID-RULE STRUCTURAL CAVEAT. Rule (b) is implemented per spec 3.2(b) and is fully exercised by
unit tests, but under the CURRENT three source views it cannot fire on live data:
`person_wikidata_id` is populated only on wikidata-sourced rows, and `se_company_person_
wikidata` (migration 000330) is one-name-per-QID (one `wikidata_persons` row per QID), so a
company's wikidata rows never carry two *different* K2 spellings for the same QID today.
Consequently the evaluation's K3 numbers on the real corpus exercise rule (a) ONLY --
"QID-linking doesn't reduce the collision count" must NOT be read from those numbers; it is a
property of the current view shape, not evidence that the rule is unhelpful.

BLANK NAMES. The three views are raw pass-throughs of their upstream tables (source_views.py
module docstring) -- they do not filter empty/blank `full_name`. `k3_merge_groups` silently
drops any row whose `full_name` is empty or whitespace-only (defense in depth: a caller that
forgets to pre-filter can never let a blank name enter a cluster), and the evaluation/asset
layer independently counts every dropped row corpus-wide as `excluded_blank_name_count` --
required by the controller ruling so the metric is correct even if a future caller of
`k3_merge_groups` does its own filtering differently.

CANDIDATE WRITE STRATEGY: TRUNCATE-then-INSERT, not the EXCHANGE-TABLES stage pattern used
elsewhere in this codebase (e.g. `company_people/identity.py`,
`company_signals/register_assets.py`). The candidate table is explicitly experiment state,
never serving (migration 000330's comment; brief section 5's "report table, not serving"):
nothing reads it concurrently with a write, and a manual analysis run is not in any freshness
SLA a reader could observe mid-replacement. EXCHANGE's atomicity buys safety a scratch table
does not need at the cost of a second physical table plus rollback bookkeeping; TRUNCATE +
one batched INSERT is the simplest thing consistent with the table's own docstring. See
`se_company_person_identity_evaluation` below.
"""

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_people.source_views import (
    SE_COMPANY_ID_PATTERN,
    SE_COMPANY_PERSON_BOLAGSVERKET_VIEW,
    SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE,
    SE_COMPANY_PERSON_ESEF_VIEW,
    SE_COMPANY_PERSON_WIKIDATA_VIEW,
)

DATABASE = "corpscout"
GROUP_NAME = "se_company_person"

# The candidate table's own column order (migration 000330). `created_at` is included and
# always supplied explicitly (never left to the column's DEFAULT now()) -- matching this
# codebase's convention (draft.py's PERSON_DRAFT_COLUMNS, roles.py's ROLE_COLUMNS) of never
# depending on a server-side default for a value a caller can pass deterministically.
# Pinned against the migration's literal DDL column order by
# `test_candidate_columns_match_migration_column_order` in
# tests/test_se_company_person_identity_eval.py.
SE_COMPANY_PERSON_COLLISION_CANDIDATE_COLUMNS = (
    "company_id",
    "candidate_group_id",
    "person_key",
    "full_name",
    "source",
    "source_record_uid",
    "evidence_json",
    "created_at",
)

_WHITESPACE_PATTERN = re.compile(r"\s+")
_SE_COMPANY_ID_PATTERN = re.compile(SE_COMPANY_ID_PATTERN)


# ---------------------------------------------------------------------------
# K1 -- copied locally from normalization.py's `_name_match_key` (controller ruling: not
# imported/aliased). Keep this block textually in sync only by deliberate choice; a future
# Task 3 change to normalization's key must NOT change this frozen baseline silently.
# ---------------------------------------------------------------------------


def _normalized_name_k1(value: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", value.strip()).casefold()


def identity_key_k1(name: str) -> str:
    """K1 (baseline, today's): ``first_token|last_token``, casefolded.

    A blank/whitespace-only name yields ``""`` (never used directly -- callers exclude blank
    names before grouping; see the module docstring).
    """
    tokens = _normalized_name_k1(name).split()
    if len(tokens) < 2:
        return tokens[0] if tokens else ""
    return f"{tokens[0]}|{tokens[-1]}"


# ---------------------------------------------------------------------------
# K2 -- every token, diacritics preserved.
# ---------------------------------------------------------------------------


def identity_key_k2(name: str) -> str:
    """K2 (full-name): all tokens, casefolded, whitespace-normalized, diacritics preserved.

    ``str.casefold()`` does not strip diacritics -- "å"/"ä"/"ö" remain distinct
    code points from "a"/"o" -- so "Åsa Öberg" and "Asa Oberg" produce different keys,
    exactly as spec 3.2 requires. A blank/whitespace-only name yields ``""``.
    """
    tokens = _WHITESPACE_PATTERN.sub(" ", name.strip()).casefold().split()
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# Row / decision / result types.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PersonObservationRow:
    """One observation from any of the three SE person source views."""

    company_id: str
    source: str  # "bolagsverket" | "esef" | "wikidata"
    source_record_uid: str
    full_name: str
    person_wikidata_id: str = ""


@dataclass(frozen=True)
class MergeDecision:
    """One final K3-resolved person group within a single company.

    ``k1_keys`` is almost always a single-element set -- it holds more than one only when
    rule (b) (a shared Wikidata QID) links clusters that started in different K1 buckets.
    ``candidate_group_id`` is ``""`` unless ``is_collision_candidate`` is True.
    """

    company_id: str
    k3_person_key: str
    k1_keys: frozenset[str]
    rows: tuple[PersonObservationRow, ...]
    is_collision_candidate: bool
    candidate_group_id: str


@dataclass(frozen=True)
class CollisionCandidateMember:
    person_key: str
    row: PersonObservationRow


@dataclass(frozen=True)
class CollisionCandidateGroup:
    """All rows K1 would have merged into one person that K3 keeps apart."""

    company_id: str
    candidate_group_id: str
    members: tuple[CollisionCandidateMember, ...]


@dataclass(frozen=True)
class IdentityEvaluationResult:
    k1_person_count: int
    k2_person_count: int
    k3_person_count: int
    merge_count: int
    split_count: int
    collision_candidate_count: int
    excluded_blank_name_count: int
    candidate_groups: tuple[CollisionCandidateGroup, ...]


@dataclass(frozen=True)
class _Cluster:
    key: str
    rows: tuple[PersonObservationRow, ...]
    tokens: frozenset[str]
    k1_key: str


def _partition_blank_names(
    rows: Sequence[PersonObservationRow],
) -> tuple[list[PersonObservationRow], int]:
    non_blank = [row for row in rows if row.full_name.strip()]
    return non_blank, len(rows) - len(non_blank)


def _find(parent: dict[str, str], key: str) -> str:
    root = key
    while parent[root] != root:
        root = parent[root]
    while parent[key] != root:
        parent[key], key = root, parent[key]
    return root


def _union(parent: dict[str, str], left: str, right: str) -> None:
    left_root, right_root = _find(parent, left), _find(parent, right)
    if left_root != right_root:
        parent[left_root] = right_root


def _candidate_group_id(company_id: str, member_k2_keys: Sequence[str]) -> str:
    """Deterministic: SHA256 over the company plus the SORTED, de-duplicated member keys --
    never dependent on set/dict iteration order (which Python does not guarantee stable
    across runs for anything not explicitly sorted)."""
    payload = "se-company-person-collision-v1\n" + company_id + "\n" + "\n".join(
        sorted(set(member_k2_keys))
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def k3_merge_groups(company_rows: Sequence[PersonObservationRow]) -> list[MergeDecision]:
    """The K3 reconciliation pass for ONE company's observations (see module docstring for
    the two merge rules and the "unique minimal superset" ambiguity guard).

    Rows with a blank/whitespace-only ``full_name`` are silently dropped (defense in depth --
    see module docstring). Raises ``ValueError`` if the non-blank rows span more than one
    ``company_id``: K3 is explicitly company-scoped (spec 3.2), so a caller mixing companies
    into one call is a bug, not a case to silently merge across.
    """
    non_blank, _ = _partition_blank_names(company_rows)
    if not non_blank:
        return []

    company_ids = {row.company_id for row in non_blank}
    if len(company_ids) > 1:
        raise ValueError(
            "k3_merge_groups is company-scoped; got rows for multiple company_ids: "
            f"{sorted(company_ids)}"
        )
    company_id = next(iter(company_ids))

    grouped: dict[str, list[PersonObservationRow]] = defaultdict(list)
    for row in non_blank:
        k2_key = identity_key_k2(row.full_name)
        grouped[k2_key].append(row)

    cluster_list = [
        _Cluster(
            key=k2_key,
            rows=tuple(rows_for_key),
            tokens=frozenset(k2_key.split(" ")),
            k1_key=identity_key_k1(rows_for_key[0].full_name),
        )
        for k2_key, rows_for_key in grouped.items()
    ]

    parent = {cluster.key: cluster.key for cluster in cluster_list}

    # Rule (a): within each K1 bucket, a cluster merges into its UNIQUE minimal strict
    # superset (see module docstring's ambiguity guard).
    by_k1: dict[str, list[_Cluster]] = defaultdict(list)
    for cluster in cluster_list:
        by_k1[cluster.k1_key].append(cluster)

    for bucket in by_k1.values():
        for cluster in bucket:
            supersets = [
                other
                for other in bucket
                if other is not cluster and cluster.tokens < other.tokens
            ]
            minimal = [
                candidate
                for candidate in supersets
                if not any(
                    other.tokens < candidate.tokens
                    for other in supersets
                    if other is not candidate
                )
            ]
            if len(minimal) == 1:
                _union(parent, cluster.key, minimal[0].key)

    # Rule (b): a shared Wikidata QID links clusters company-wide, regardless of K1 bucket.
    qid_clusters: dict[str, list[_Cluster]] = defaultdict(list)
    for cluster in cluster_list:
        qids = {row.person_wikidata_id for row in cluster.rows if row.person_wikidata_id}
        for qid in qids:
            qid_clusters[qid].append(cluster)
    for clusters_for_qid in qid_clusters.values():
        anchor = clusters_for_qid[0]
        for other in clusters_for_qid[1:]:
            _union(parent, anchor.key, other.key)

    components: dict[str, list[_Cluster]] = defaultdict(list)
    for cluster in cluster_list:
        components[_find(parent, cluster.key)].append(cluster)

    # A K1 key "collides" iff its clusters ended up split across more than one final
    # component -- i.e. K1 would have merged them into one person and K3 declined to.
    k1_component_ids: dict[str, set[str]] = defaultdict(set)
    for cluster in cluster_list:
        k1_component_ids[cluster.k1_key].add(_find(parent, cluster.key))
    colliding_k1_keys = {
        k1_key for k1_key, roots in k1_component_ids.items() if len(roots) > 1
    }

    decisions: list[MergeDecision] = []
    for member_clusters in components.values():
        member_clusters_sorted = sorted(member_clusters, key=lambda cluster: cluster.key)
        person_key = "|".join(cluster.key for cluster in member_clusters_sorted)
        k1_keys = frozenset(cluster.k1_key for cluster in member_clusters)
        rows = tuple(
            sorted(
                (row for cluster in member_clusters_sorted for row in cluster.rows),
                key=lambda row: (row.source, row.source_record_uid),
            )
        )
        colliding_here = sorted(k1_keys & colliding_k1_keys)
        is_collision = bool(colliding_here)
        candidate_group_id = ""
        if is_collision:
            # Deterministic representative when a decision touches more than one colliding
            # K1 key (only reachable via a QID merge that crosses buckets -- see the module
            # docstring's QID structural caveat). Picking one representative here means the
            # OTHER (non-representative) colliding bucket's candidate group can end up
            # incomplete/one-sided: it is built from `by_k1[that bucket]`, but this
            # decision's rows -- which belong to that bucket too, via the cross-bucket
            # merge -- get filed only under the representative bucket's group, not the
            # other one. Unreachable from current live views (the QID rule is structurally
            # a no-op there, see module docstring) but must be revisited before K3 (or a
            # variant) is promoted to a served rule.
            representative_k1_key = colliding_here[0]
            candidate_group_id = _candidate_group_id(
                company_id,
                [cluster.key for cluster in by_k1[representative_k1_key]],
            )
        decisions.append(
            MergeDecision(
                company_id=company_id,
                k3_person_key=person_key,
                k1_keys=k1_keys,
                rows=rows,
                is_collision_candidate=is_collision,
                candidate_group_id=candidate_group_id,
            )
        )
    return sorted(decisions, key=lambda decision: decision.k3_person_key)


def _collision_candidate_groups(
    decisions: Sequence[MergeDecision],
) -> list[CollisionCandidateGroup]:
    members_by_group: dict[str, list[CollisionCandidateMember]] = defaultdict(list)
    company_by_group: dict[str, str] = {}
    for decision in decisions:
        if not decision.is_collision_candidate:
            continue
        company_by_group[decision.candidate_group_id] = decision.company_id
        for row in decision.rows:
            members_by_group[decision.candidate_group_id].append(
                CollisionCandidateMember(person_key=decision.k3_person_key, row=row)
            )
    groups = [
        CollisionCandidateGroup(
            company_id=company_by_group[group_id],
            candidate_group_id=group_id,
            members=tuple(
                sorted(
                    members,
                    key=lambda member: (
                        member.person_key,
                        member.row.source,
                        member.row.source_record_uid,
                    ),
                )
            ),
        )
        for group_id, members in members_by_group.items()
    ]
    return sorted(groups, key=lambda group: (group.company_id, group.candidate_group_id))


def evaluate_se_company_person_identity(
    rows: Sequence[PersonObservationRow],
) -> IdentityEvaluationResult:
    """The pure evaluation core: K1/K2/K3 person counts, merge/split vs K1, and the
    collision-candidate groups -- everything the asset needs, with no ClickHouse
    dependency. Exercised directly by unit tests and by the clickhouse-local executed test
    (which parses real view output into `PersonObservationRow` and calls this)."""
    non_blank, excluded_blank_name_count = _partition_blank_names(rows)

    by_company: dict[str, list[PersonObservationRow]] = defaultdict(list)
    for row in non_blank:
        by_company[row.company_id].append(row)

    k1_person_keys: set[tuple[str, str]] = set()
    k2_person_keys: set[tuple[str, str]] = set()
    all_decisions: list[MergeDecision] = []
    merge_count = 0
    split_count = 0

    for company_id, company_rows in by_company.items():
        k2_by_k1: dict[str, set[str]] = defaultdict(set)
        for row in company_rows:
            k1_key = identity_key_k1(row.full_name)
            k2_key = identity_key_k2(row.full_name)
            k1_person_keys.add((company_id, k1_key))
            k2_person_keys.add((company_id, k2_key))
            k2_by_k1[k1_key].add(k2_key)

        decisions = k3_merge_groups(company_rows)
        all_decisions.extend(decisions)

        # merge/split vs K1: classified only for K1 buckets with more than one distinct K2
        # spelling -- a bucket with exactly one spelling is unchanged by K1/K2/K3 alike and
        # is not a merge or a split of anything.
        final_components_per_k1: dict[str, set[str]] = defaultdict(set)
        for decision in decisions:
            for k1_key in decision.k1_keys:
                final_components_per_k1[k1_key].add(decision.k3_person_key)

        for k1_key, k2_keys in k2_by_k1.items():
            if len(k2_keys) <= 1:
                continue
            if len(final_components_per_k1.get(k1_key, set())) <= 1:
                merge_count += 1
            else:
                split_count += 1

    candidate_groups = tuple(_collision_candidate_groups(all_decisions))

    return IdentityEvaluationResult(
        k1_person_count=len(k1_person_keys),
        k2_person_count=len(k2_person_keys),
        k3_person_count=len(all_decisions),
        merge_count=merge_count,
        split_count=split_count,
        collision_candidate_count=len(candidate_groups),
        excluded_blank_name_count=excluded_blank_name_count,
        candidate_groups=candidate_groups,
    )


# ---------------------------------------------------------------------------
# ClickHouse read/write + the Dagster asset.
# ---------------------------------------------------------------------------


def _normalized_company_ids(company_ids: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({company_id.strip() for company_id in company_ids}))
    invalid = [
        company_id
        for company_id in normalized
        if not _SE_COMPANY_ID_PATTERN.fullmatch(company_id)
    ]
    if invalid:
        raise ValueError(f"Invalid SE company_ids: {invalid}")
    return normalized


def _company_filter_sql(company_ids: Sequence[str]) -> str:
    if not company_ids:
        return ""
    normalized = _normalized_company_ids(company_ids)
    values = ", ".join(f"'{company_id}'" for company_id in normalized)
    return f" WHERE company_id IN ({values})"


def _read_person_observation_rows(
    client: object, company_ids: Sequence[str]
) -> list[PersonObservationRow]:
    filter_sql = _company_filter_sql(company_ids)
    rows: list[PersonObservationRow] = []

    bolagsverket = client.execute(  # type: ignore[attr-defined]
        "SELECT company_id, full_name, source_record_uid "
        f"FROM {SE_COMPANY_PERSON_BOLAGSVERKET_VIEW}{filter_sql}"
    )
    rows.extend(
        PersonObservationRow(
            company_id=row[0],
            source="bolagsverket",
            source_record_uid=row[2],
            full_name=row[1],
        )
        for row in bolagsverket
    )

    esef = client.execute(  # type: ignore[attr-defined]
        "SELECT company_id, full_name, source_record_uid "
        f"FROM {SE_COMPANY_PERSON_ESEF_VIEW}{filter_sql}"
    )
    rows.extend(
        PersonObservationRow(
            company_id=row[0], source="esef", source_record_uid=row[2], full_name=row[1]
        )
        for row in esef
    )

    wikidata = client.execute(  # type: ignore[attr-defined]
        "SELECT company_id, full_name, source_record_uid, person_wikidata_id "
        f"FROM {SE_COMPANY_PERSON_WIKIDATA_VIEW}{filter_sql}"
    )
    rows.extend(
        PersonObservationRow(
            company_id=row[0],
            source="wikidata",
            source_record_uid=row[2],
            full_name=row[1],
            person_wikidata_id=row[3],
        )
        for row in wikidata
    )
    return rows


def _candidate_table_rows(
    candidate_groups: Sequence[CollisionCandidateGroup],
    *,
    created_at: datetime,
) -> list[tuple[str, str, str, str, str, str, str, datetime]]:
    rows: list[tuple[str, str, str, str, str, str, str, datetime]] = []
    for group in candidate_groups:
        for member in group.members:
            evidence_json = json.dumps(
                {
                    "k1_key": identity_key_k1(member.row.full_name),
                    "k2_key": identity_key_k2(member.row.full_name),
                    "source": member.row.source,
                    "source_record_uid": member.row.source_record_uid,
                },
                sort_keys=True,
            )
            rows.append(
                (
                    group.company_id,
                    group.candidate_group_id,
                    member.person_key,
                    member.row.full_name,
                    member.row.source,
                    member.row.source_record_uid,
                    evidence_json,
                    created_at,
                )
            )
    return rows


class SECompanyPersonIdentityEvaluationConfig(dg.Config):
    company_ids: list[str] = []
    write_candidates: bool = True


_SOURCE_ASSET_DEPS = (
    dg.AssetKey("se_financial_report_signatories_clickhouse"),
    dg.AssetKey("esef_document_people_clickhouse"),
    dg.AssetKey("company_identifier_clickhouse"),
    dg.AssetKey("wikidata_company_identifiers"),
    dg.AssetKey("wikidata_company_people"),
    dg.AssetKey("wikidata_persons"),
)


@dg.asset(
    name="se_company_person_identity_evaluation",
    deps=_SOURCE_ASSET_DEPS,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE},
    description=(
        "Manual one-off analysis (spec 3.2): evaluates the K1 (baseline)/K2 (full-name)/K3 "
        "(deterministic reconciliation) identity rules over the three SE person source "
        "views and writes K1-vs-K3 collision candidates for backoffice review. Never "
        "scheduled or eager -- launched from the UI with an optional company_ids scope. "
        "NOTE: the QID-link rule cannot fire on the current source views (one name per QID) "
        "-- these K3 numbers exercise the superset rule only; see module docstring."
    ),
)
def se_company_person_identity_evaluation(
    context: dg.AssetExecutionContext,
    config: SECompanyPersonIdentityEvaluationConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=(SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE.rsplit(".", 1)[-1],),
    )

    with clickhouse.get_connection() as client:
        rows = _read_person_observation_rows(client, config.company_ids)
        result = evaluate_se_company_person_identity(rows)

        if config.write_candidates:
            # Build the rows to write BEFORE the destructive TRUNCATE: a crash while
            # building `candidate_rows` (or between here and the INSERT) must never leave
            # the table emptied with no way to tell "zero collisions found" apart from
            # "crashed mid-write". This shrinks the failure window to the INSERT call
            # itself, which is the narrowest it can get without EXCHANGE-TABLES atomicity
            # (deliberately not used here -- see the module docstring's write-strategy
            # note).
            candidate_rows = _candidate_table_rows(
                result.candidate_groups, created_at=datetime.now(UTC)
            )
            client.execute(
                f"TRUNCATE TABLE IF EXISTS {SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE}"
            )
            if candidate_rows:
                columns = ", ".join(SE_COMPANY_PERSON_COLLISION_CANDIDATE_COLUMNS)
                client.execute(
                    f"INSERT INTO {SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE} "
                    f"({columns}) VALUES",
                    candidate_rows,
                )

    sampled_candidate_groups = [
        {
            "company_id": group.company_id,
            "candidate_group_id": group.candidate_group_id,
            "person_keys": sorted({member.person_key for member in group.members}),
            "member_count": len(group.members),
        }
        for group in result.candidate_groups[:20]
    ]

    context.log.info(
        "SE person identity evaluation: k1=%s k2=%s k3=%s merge_vs_k1=%s split_vs_k1=%s "
        "collision_candidates=%s excluded_blank_names=%s write_candidates=%s",
        result.k1_person_count,
        result.k2_person_count,
        result.k3_person_count,
        result.merge_count,
        result.split_count,
        result.collision_candidate_count,
        result.excluded_blank_name_count,
        config.write_candidates,
    )

    return dg.MaterializeResult(
        metadata={
            "k1_person_count": result.k1_person_count,
            "k2_person_count": result.k2_person_count,
            "k3_person_count": result.k3_person_count,
            "merge_count_vs_k1": result.merge_count,
            "split_count_vs_k1": result.split_count,
            "collision_candidate_count": result.collision_candidate_count,
            "excluded_blank_name_count": result.excluded_blank_name_count,
            "write_candidates": config.write_candidates,
            "sampled_candidate_groups": dg.MetadataValue.json(sampled_candidate_groups),
            "table": SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE,
        }
    )


se_company_person_identity_evaluation_job = dg.define_asset_job(
    "se_company_person_identity_evaluation_job",
    selection=dg.AssetSelection.assets("se_company_person_identity_evaluation"),
)


defs = dg.Definitions(
    assets=[se_company_person_identity_evaluation],
    jobs=[se_company_person_identity_evaluation_job],
)
