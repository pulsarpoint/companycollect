"""The per-company fold of normalized person rows into published persons (spec 2026-09-09
section 5).

Pure: no I/O, no clock (the batch passes `current_year`). This module decides which persons
exist, which observations each one merges, whose spelling is published, which roles and
which `data` the row carries, which rules moved them and which previously published keys are
withdrawn. `batch.py` reads, writes and counts.

WHY IDENTITY IS ONLY EVER WITHIN ONE COMPANY: Sweden publishes no person identifier, so
"same person" is a claim this fold can only make about observations of one company (spec
section 2). person_key therefore hashes the company id together with the canonical name.
"""

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import date, datetime
from typing import Any

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.precedence import precedence_for

FOLD_VERSION = "se-person-fold-v1"
# Only `ok` rows fold. `partial` (one name word, initials only) and `no_person` (a role word,
# a number, a company suffix) are stored with their notes and never become a person (4.4).
FOLDABLE_STATUS = "ok"
EXCLUDED_SOURCES: tuple[str, ...] = ("reviewer_draft",)
REVIEWER_SOURCE = "reviewer"
MERGE, SPLIT, HIDE = "merge", "split", "hide"
HIDDEN, WITHDRAWN = "hidden", "withdrawn"
CREATED, UPDATED, REACTIVATED = "created", "updated", "reactivated"
# Everything a fold compares to decide whether a person CHANGED. The three excluded columns
# differ on every run by construction, so comparing them would make every row "changed".
_COMPARED: tuple[str, ...] = tuple(
    column
    for column in tables.MAIN_COLUMNS
    if column not in ("folded_at", "fold_version", "source_run_id")
)
# Array columns: tuples in Python, lists on the way into clickhouse-driver.
_LIST_COLUMNS: tuple[str, ...] = (
    "sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS,
    "role_codes", "role_years", "current_roles",
)


@dataclass(frozen=True, slots=True)
class NormalizedRow:
    """One current normalized row (spec 3.2) as the batch read it."""

    company_id: str
    source: str
    slot: str
    normalized_id: str
    parse_status: str
    first_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    last_tokens: tuple[str, ...]
    display_first: str
    display_last: str
    display_name: str
    birth_year: int | None
    wikidata_id: str | None
    role_code: str | None
    role_year: int | None
    role_from: date | None
    role_to: date | None
    data: str

    def member_id(self) -> tuple[str, str]:
        """What a rule and a published row identify a member by."""
        return (self.source, self.slot)

    def tokens(self) -> tuple[str, ...]:
        return (*self.first_tokens, *self.middle_tokens, *self.last_tokens)


@dataclass(frozen=True, slots=True)
class PersonRule:
    """One active row of se_company_person_rule (spec 3.5)."""

    company_id: str
    rule_id: str
    kind: str
    person_keys: tuple[str, ...]
    slots: tuple[str, ...]


def _row_order(row: NormalizedRow) -> tuple[str, str]:
    return (row.source, row.slot)


def _name_key(row: NormalizedRow) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return (row.first_tokens, row.last_tokens)


def _years_conflict(a: NormalizedRow, b: NormalizedRow) -> bool:
    return a.birth_year is not None and b.birth_year is not None and a.birth_year != b.birth_year


def _unique_minimal_superset(
    subject: frozenset[str], candidates: Sequence[frozenset[str]]
) -> frozenset[str] | None:
    """The one smallest middle-token set that properly contains `subject`, or None when
    there is none or more than one. "Anna Svensson" folds into "Anna Maria Svensson" only
    while Maria is the ONLY minimal way to complete the name."""
    supersets = [candidate for candidate in candidates if subject < candidate]
    minimal = [
        candidate
        for candidate in supersets
        if not any(other < candidate for other in supersets)
    ]
    return minimal[0] if len(minimal) == 1 else None


def _middles_match(
    a: NormalizedRow,
    b: NormalizedRow,
    middles_by_name: Mapping[tuple[tuple[str, ...], tuple[str, ...]], tuple[frozenset[str], ...]],
) -> bool:
    left, right = frozenset(a.middle_tokens), frozenset(b.middle_tokens)
    if left == right:
        return True
    candidates = middles_by_name[_name_key(a)]
    if left < right:
        return _unique_minimal_superset(left, candidates) == right
    if right < left:
        return _unique_minimal_superset(right, candidates) == left
    return False


def _matches(a: NormalizedRow, b: NormalizedRow, middles_by_name) -> bool:
    """The guarded pairwise relation of spec 5.1."""
    if _years_conflict(a, b):
        return False
    if a.wikidata_id and b.wikidata_id and a.wikidata_id == b.wikidata_id:
        return True
    return _name_key(a) == _name_key(b) and _middles_match(a, b, middles_by_name)


def _middles_by_name(rows: Sequence[NormalizedRow]) -> dict:
    grouped: dict = defaultdict(set)
    for row in rows:
        grouped[_name_key(row)].add(frozenset(row.middle_tokens))
    return {name: tuple(middles) for name, middles in grouped.items()}


def _split_by_birth_year(
    members: Sequence[NormalizedRow], middles_by_name
) -> tuple[tuple[NormalizedRow, ...], ...]:
    """A closed set holding two birth years is split by year; year-less members attach,
    breadth first, to the sub-set holding a member they directly match. On a genuine tie
    (a year-less member matching both years) the smallest year wins, deterministically."""
    years = sorted({member.birth_year for member in members if member.birth_year is not None})
    if len(years) <= 1:
        return (tuple(members),)
    buckets: dict[int, list[NormalizedRow]] = {
        year: [member for member in members if member.birth_year == year] for year in years
    }
    pending = [member for member in members if member.birth_year is None]
    while pending:
        remaining: list[NormalizedRow] = []
        progressed = False
        for member in pending:
            hits = sorted(
                year
                for year, bucket in buckets.items()
                if any(_matches(member, other, middles_by_name) for other in bucket)
            )
            if hits:
                buckets[hits[0]].append(member)
                progressed = True
            else:
                remaining.append(member)
        if not progressed:
            # Unreachable for a connected set (every member is joined to some seed through
            # the closure), and here so no observation can ever be dropped silently.
            buckets[years[0]].extend(remaining)
            remaining = []
        pending = remaining
    return tuple(
        tuple(sorted(bucket, key=_row_order)) for _, bucket in sorted(buckets.items())
    )


def identity_sets_before_split(
    rows: Sequence[NormalizedRow],
) -> tuple[tuple[NormalizedRow, ...], ...]:
    """The transitive closure of the guarded relation, BEFORE the birth-year split (spec
    5.1). `fold_company_persons` counts the sets this returns that still hold two years, for
    the `sets_split_by_birth_year` metric.

    Every matching pair shares either the (first_tokens, last_tokens) pair or a QID, so the
    closure is computed inside those two groupings -- never over all pairs, which for a
    96-signatory company would be 4,560 comparisons and 5.5M rows of them on prod."""
    ordered = sorted(rows, key=_row_order)
    parent = list(range(len(ordered)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    middles_by_name = _middles_by_name(ordered)
    by_name: dict = defaultdict(list)
    by_qid: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(ordered):
        by_name[_name_key(row)].append(index)
        if row.wikidata_id:
            by_qid[row.wikidata_id].append(index)
    for indexes in (*by_name.values(), *by_qid.values()):
        for position, left in enumerate(indexes):
            for right in indexes[position + 1 :]:
                if _matches(ordered[left], ordered[right], middles_by_name):
                    union(left, right)

    grouped: dict[int, list[NormalizedRow]] = defaultdict(list)
    for index, row in enumerate(ordered):
        grouped[find(index)].append(row)
    return tuple(tuple(grouped[root]) for root in sorted(grouped))


def identity_sets(rows: Sequence[NormalizedRow]) -> tuple[tuple[NormalizedRow, ...], ...]:
    """The company's persons as sets of observations: the closure, then the birth-year
    split of any set that still holds two years (spec 5.1)."""
    middles_by_name = _middles_by_name(rows)
    sets: list[tuple[NormalizedRow, ...]] = []
    for members in identity_sets_before_split(rows):
        sets.extend(_split_by_birth_year(members, middles_by_name))
    return tuple(sets)


def canonical_tokens(members: Sequence[NormalizedRow]) -> tuple[str, ...]:
    """The folded tokens of the most complete member: most tokens, then the longest joined
    string, then alphabetically first (spec 5.1)."""

    def completeness(row: NormalizedRow) -> tuple[int, int, str]:
        tokens = row.tokens()
        joined = " ".join(tokens)
        return (-len(tokens), -len(joined), joined)

    return min(members, key=completeness).tokens()


def person_key(company_id: str, canonical: Sequence[str], discriminator: str = "") -> str:
    """sha256(company id, canonical name[, discriminator]), lower hex -- the same shape as
    `address/normalize_se.py::address_key`. The discriminator is only ever set when two sets
    of one company share a canonical name (`assign_keys`)."""
    parts = [company_id, " ".join(canonical)]
    if discriminator:
        parts.append(discriminator)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def assign_keys(
    company_id: str, sets: Sequence[Sequence[NormalizedRow]]
) -> list[tuple[tuple[NormalizedRow, ...], str]]:
    """(members, person_key) per set, with collisions broken.

    Two sets of one company can genuinely share a canonical name -- two Anna Svenssons with
    different birth years, or a split rule that separates two identical spellings -- and the
    main table is a ReplacingMergeTree ORDER BY (company_id, person_key), so a shared key
    would silently publish one person instead of two. Every colliding set (not only the
    later ones, which would move the plain key from one person to another when a new set
    appears) takes a discriminator: its birth year when it has one (a set never holds two,
    and a year does not move when slots come and go), else the smallest "source:slot" of
    its members (the split-rule case). A set alone under its name keeps the plain key,
    which is what makes keys stable across folds."""
    canonical = [tuple(canonical_tokens(members)) for members in sets]
    shared = {name for name in canonical if canonical.count(name) > 1}
    assigned: list[tuple[tuple[NormalizedRow, ...], str]] = []
    for members, name in zip(sets, canonical, strict=True):
        discriminator = ""
        if name in shared:
            years = {member.birth_year for member in members if member.birth_year is not None}
            discriminator = (
                str(min(years))
                if years
                else min(f"{member.source}:{member.slot}" for member in members)
            )
        assigned.append((tuple(members), person_key(company_id, name, discriminator)))
    return assigned


def _apply_merge(
    sets: list[list[NormalizedRow]], rule: PersonRule, previous_members: Mapping[str, frozenset]
) -> tuple[list[list[NormalizedRow]], bool]:
    """Join the sets holding the members of the rule's keys. A key that names no previously
    published row, or whose members are all gone, resolves to nothing: it is ignored and the
    rule reported stale (spec 5.2)."""
    targets: list[int] = []
    stale = False
    for key in rule.person_keys:
        members = previous_members.get(key, frozenset())
        hits = [
            index
            for index, group in enumerate(sets)
            if any(row.member_id() in members for row in group)
        ]
        if not hits:
            stale = True
            continue
        targets.extend(hits)
    ordered = sorted(set(targets))
    if len(ordered) < 2:
        return sets, stale
    joined = [row for index in ordered for row in sets[index]]
    rebuilt: list[list[NormalizedRow]] = []
    for index, group in enumerate(sets):
        if index == ordered[0]:
            rebuilt.append(sorted(joined, key=_row_order))
        elif index not in set(ordered):
            rebuilt.append(group)
    return rebuilt, stale


def _apply_split(
    sets: list[list[NormalizedRow]], rule: PersonRule
) -> tuple[list[list[NormalizedRow]], bool]:
    """The rule's slots leave their sets and form one set of their own (spec 5.2)."""
    wanted = set(rule.slots)
    moved = [row for group in sets for row in group if row.slot in wanted]
    if not moved:
        return sets, True
    kept = [[row for row in group if row.slot not in wanted] for group in sets]
    return [*[group for group in kept if group], sorted(moved, key=_row_order)], False


def apply_rules(
    sets: Sequence[Sequence[NormalizedRow]],
    rules: Sequence[PersonRule],
    previous_members: Mapping[str, frozenset],
) -> tuple[list[list[NormalizedRow]], int]:
    """Merge rules then split rules, each kind ordered by rule_id, and the count of rules
    with at least one key or slot that resolved to nothing. Hide rules are a flag on the
    finished row, not a regrouping, and are applied by `fold_company_persons`."""
    working = [list(members) for members in sets]
    stale = 0
    for kind in (MERGE, SPLIT):
        for rule in sorted((rule for rule in rules if rule.kind == kind), key=lambda r: r.rule_id):
            if kind == MERGE:
                working, rule_stale = _apply_merge(working, rule, previous_members)
            else:
                working, rule_stale = _apply_split(working, rule)
            stale += int(rule_stale)
    return working, stale


@dataclass(frozen=True, slots=True)
class RoleBlock:
    """The six role columns of spec 3.3, computed together because they share one union."""

    role_codes: tuple[str, ...]
    role_years: tuple[int, ...]
    role_sources: tuple[tuple[str, ...], ...]
    current_roles: tuple[str, ...]
    first_year: int | None
    last_year: int | None


@dataclass(frozen=True, slots=True)
class PublishedPerson:
    """One main-table row (spec 3.3): the 29 columns of tables.MAIN_COLUMNS in order.

    `folded_at` is the stamp the row carries in ClickHouse -- None for a row this fold just
    built, which the batch stamps at write time through `as_tuple`. `history_tuple` keeps the
    stored stamp instead, because a history row is the PREVIOUS image and its `folded_at` is
    when that image was published; together with `changed_at` it bounds the window the image
    was live.
    """

    company_id: str
    person_key: str
    display_name: str
    first_name: str
    last_name: str
    birth_year: int | None
    wikidata_id: str | None
    sources: tuple[str, ...]
    slots: tuple[str, ...]
    normalized_ids: tuple[str, ...]
    member_sources: tuple[str, ...]
    member_slots: tuple[str, ...]
    member_names: tuple[str, ...]
    member_birth_years: tuple[int | None, ...]
    member_wikidata_ids: tuple[str, ...]
    member_data: tuple[str, ...]
    role_codes: tuple[str, ...]
    role_years: tuple[int, ...]
    role_sources: tuple[tuple[str, ...], ...]
    current_roles: tuple[str, ...]
    first_year: int | None
    last_year: int | None
    text_source: str
    data: str
    active: int
    inactive_reason: str
    folded_at: datetime | None
    fold_version: str
    source_run_id: str

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        values = {field.name: getattr(self, field.name) for field in fields(self)}
        for name in _LIST_COLUMNS:
            values[name] = list(values[name])
        values["role_sources"] = [list(sources) for sources in self.role_sources]
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def history_tuple(
        self, *, changed_at: datetime, change_kind: str, fold_run_id: str
    ) -> tuple[Any, ...]:
        return (
            *self.as_tuple(self.folded_at or changed_at),
            changed_at,
            change_kind,
            fold_run_id,
        )

    def changed_against(self, other: "PublishedPerson | None") -> bool:
        if other is None:
            return True
        return any(getattr(self, name) != getattr(other, name) for name in _COMPARED)


@dataclass(frozen=True, slots=True)
class HistoryEntry:
    """One row of se_company_person_history: the PREVIOUS main row and what happened to it.
    A `created` entry carries the new row, the only image there is."""

    row: PublishedPerson
    change_kind: str


@dataclass(frozen=True, slots=True)
class FoldResult:
    rows: tuple[PublishedPerson, ...]
    history: tuple[HistoryEntry, ...]
    persons: int          # active rows in `rows`
    created: int
    updated: int
    hidden: int           # history rows of kind hidden, not the number of hidden rows
    withdrawn: int
    reactivated: int
    unchanged: int        # rows rewritten with no history row
    stale_rules: int
    sets_split_by_birth_year: int


def role_years_for(row: NormalizedRow, current_year: int) -> tuple[int, ...]:
    """The years one member's role was observed (spec 4.3).

    A fiscal year (Bolagsverket's report year, ESEF's document year) is the row's year and
    wins over any span the same row carries. Otherwise the span is expanded year by year,
    an open end meaning "still now" -- Wikidata's case, whose rows never carry a fiscal
    year. A row with a role and no year at all is taken as held now and makes the pair
    (code, current_year): on prod 290 of Wikidata's 466 roles carry no date, and dropping
    them would hide most Wikidata roles (controller ruling 2026-09-10). A row with only an
    end date makes (code, end year)."""
    if row.role_year is not None:
        return (int(row.role_year),)
    if row.role_from is not None:
        end = row.role_to.year if row.role_to is not None else current_year
        return tuple(range(row.role_from.year, max(end, row.role_from.year) + 1))
    if row.role_to is not None:
        return (row.role_to.year,)
    return (current_year,)


def role_block(members: Sequence[NormalizedRow], current_year: int) -> RoleBlock:
    """The union of (role code, year) pairs with the sources that saw each (spec 5.4).
    `members` is already in member order, which is the order the sources come out in."""
    seen: dict[tuple[str, int], list[str]] = {}
    for member in members:
        if not member.role_code:
            continue
        for year in role_years_for(member, current_year):
            sources = seen.setdefault((member.role_code, year), [])
            if member.source not in sources:
                sources.append(member.source)
    ordered = sorted(seen, key=lambda pair: (pair[1], pair[0]))
    years = tuple(year for _, year in ordered)
    last_year = max(years) if years else None
    return RoleBlock(
        role_codes=tuple(code for code, _ in ordered),
        role_years=years,
        role_sources=tuple(tuple(seen[pair]) for pair in ordered),
        current_roles=tuple(sorted({code for code, year in ordered if year == last_year})),
        first_year=min(years) if years else None,
        last_year=last_year,
    )


def _data_object(text: str) -> dict[str, Any]:
    """A member's `data` as a dict. The tables constrain it to a JSON object, so anything
    else is a hand-written row: it degrades to {} instead of failing a 20,000-company page."""
    try:
        parsed = json.loads(text or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _member_order(row: NormalizedRow, company_precedence) -> tuple[int, str, str]:
    return (-precedence_for(row.source, company_precedence), row.source, row.slot)


def merge_member_data(members: Sequence[NormalizedRow], company_precedence) -> str:
    """The members' `data` objects merged by `name` precedence (spec 5.5): the higher member
    wins a shared key, two objects merge one level down, arrays are replaced. A reviewer's
    value is taken whole -- "reviewer keys win outright" means their object is not merged
    into a machine source's. Keys are sorted so the row compares stably across folds."""
    ordered = sorted(members, key=lambda row: _member_order(row, company_precedence))
    merged: dict[str, Any] = {}
    for member in reversed(ordered):          # lowest precedence first, so higher overwrites
        reviewer = member.source == REVIEWER_SOURCE
        for key, value in _data_object(member.data).items():
            existing = merged.get(key)
            if not reviewer and isinstance(value, dict) and isinstance(existing, dict):
                merged[key] = {**existing, **value}
            else:
                merged[key] = value
    return json.dumps(merged, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _text_member(members: Sequence[NormalizedRow], company_precedence) -> NormalizedRow:
    """Whose spelling the person shows (spec 5.3): the highest `name` precedence, ties by
    the most complete spelling."""

    def order(row: NormalizedRow) -> tuple:
        return (
            -precedence_for(row.source, company_precedence),
            -len(row.tokens()),
            -len(row.display_name),
            row.display_name,
            row.source,
            row.slot,
        )

    return min(members, key=order)


def _published_from(
    company_id: str,
    members: Sequence[NormalizedRow],
    key: str,
    *,
    company_precedence,
    source_run_id: str,
    current_year: int,
    hidden: bool,
) -> PublishedPerson:
    ordered = sorted(members, key=lambda row: _member_order(row, company_precedence))
    text = _text_member(ordered, company_precedence)
    roles = role_block(ordered, current_year)
    sources: list[str] = []
    for member in ordered:
        if member.source not in sources:
            sources.append(member.source)
    return PublishedPerson(
        company_id=company_id,
        person_key=key,
        display_name=text.display_name,
        first_name=text.display_first,
        last_name=text.display_last,
        birth_year=next((m.birth_year for m in ordered if m.birth_year is not None), None),
        wikidata_id=next((m.wikidata_id for m in ordered if m.wikidata_id), None),
        sources=tuple(sources),
        slots=tuple(member.slot for member in ordered),
        normalized_ids=tuple(member.normalized_id for member in ordered),
        member_sources=tuple(member.source for member in ordered),
        member_slots=tuple(member.slot for member in ordered),
        member_names=tuple(member.display_name for member in ordered),
        member_birth_years=tuple(member.birth_year for member in ordered),
        member_wikidata_ids=tuple(member.wikidata_id or "" for member in ordered),
        member_data=tuple(member.data or "{}" for member in ordered),
        role_codes=roles.role_codes,
        role_years=roles.role_years,
        role_sources=roles.role_sources,
        current_roles=roles.current_roles,
        first_year=roles.first_year,
        last_year=roles.last_year,
        text_source=text.source,
        data=merge_member_data(ordered, company_precedence),
        active=0 if hidden else 1,
        inactive_reason=HIDDEN if hidden else "",
        folded_at=None,
        fold_version=FOLD_VERSION,
        source_run_id=source_run_id,
    )


def change_kind_for(previous: PublishedPerson, new: PublishedPerson) -> str:
    """What happened to a person whose columns changed (spec 3.4's five kinds). Reactivation
    covers both a withdrawn person coming back and a hide rule being reset."""
    if new.inactive_reason == WITHDRAWN and previous.inactive_reason != WITHDRAWN:
        return WITHDRAWN
    if new.inactive_reason == HIDDEN and previous.inactive_reason != HIDDEN:
        return HIDDEN
    if new.active == 1 and previous.active == 0:
        return REACTIVATED
    return UPDATED


def fold_company_persons(
    company_id: str,
    rows: Sequence[NormalizedRow],
    published: Sequence[PublishedPerson],
    rules: Sequence[PersonRule],
    company_precedence: Mapping[str, int] | None,
    *,
    source_run_id: str,
    current_year: int,
) -> FoldResult:
    """One company's whole published set, plus the history entries for what changed.

    `rows` are the company's current normalized rows with parse_status 'ok'; `published` its
    current main rows; `rules` its ACTIVE rules (the batch filters active = 0); and
    `company_precedence` its own precedence rows, if any. The result's `rows` are written to
    the main table as a set -- active, hidden and withdrawn alike -- so the company's fold
    watermark advances even when nothing about it changed."""
    for row in rows:
        if row.company_id != company_id:
            raise ValueError(f"row company_id {row.company_id!r} is not {company_id!r}")
        if row.parse_status != FOLDABLE_STATUS:
            raise ValueError(
                f"{row.source}/{row.slot}: parse_status {row.parse_status!r} never folds"
            )
        if row.source in EXCLUDED_SOURCES:
            raise ValueError(f"{row.source}/{row.slot}: source never folds")

    grouped = identity_sets(rows)
    sets_split = sum(
        1
        for members in identity_sets_before_split(rows)
        if len({member.birth_year for member in members if member.birth_year is not None}) > 1
    )
    previous_members = {
        person.person_key: frozenset(
            zip(person.member_sources, person.member_slots, strict=True)
        )
        for person in published
    }
    ruled, stale_rules = apply_rules(grouped, rules, previous_members)
    assigned = assign_keys(company_id, ruled)
    # A hide names a key as it was. It resolves by key equality first; when the key is
    # gone (a fuller spelling re-keyed the set), through the previous published members,
    # as a merge rule does -- otherwise a reviewer's Remove would silently reverse.
    live_keys = {key for _, key in assigned}
    member_pairs = {key: {(m.source, m.slot) for m in members} for members, key in assigned}
    hide_keys: set[str] = set()
    for rule in rules:
        if rule.kind != HIDE:
            continue
        matched = False
        for key in rule.person_keys:
            if key in live_keys:
                hide_keys.add(key)
                matched = True
                continue
            before = previous_members.get(key) or frozenset()
            for new_key, pairs in member_pairs.items():
                if before & pairs:
                    hide_keys.add(new_key)
                    matched = True
        if not matched:
            stale_rules += 1

    new_rows = [
        _published_from(
            company_id, members, key,
            company_precedence=company_precedence, source_run_id=source_run_id,
            current_year=current_year, hidden=key in hide_keys,
        )
        for members, key in assigned
    ]
    previous_by_key = {person.person_key: person for person in published}
    out: list[PublishedPerson] = []
    history: list[HistoryEntry] = []
    counts = {CREATED: 0, UPDATED: 0, HIDDEN: 0, WITHDRAWN: 0, REACTIVATED: 0}
    unchanged = 0
    for person in sorted(new_rows, key=lambda row: row.person_key):
        previous = previous_by_key.get(person.person_key)
        if previous is None:
            history.append(HistoryEntry(person, CREATED))
            counts[CREATED] += 1
        elif person.changed_against(previous):
            kind = change_kind_for(previous, person)
            history.append(HistoryEntry(previous, kind))
            counts[kind] += 1
        else:
            unchanged += 1
        out.append(person)
    for key in sorted(previous_by_key.keys() - {person.person_key for person in new_rows}):
        previous = previous_by_key[key]
        gone = replace(
            previous, active=0, inactive_reason=WITHDRAWN,
            fold_version=FOLD_VERSION, source_run_id=source_run_id,
        )
        if gone.changed_against(previous):
            history.append(HistoryEntry(previous, WITHDRAWN))
            counts[WITHDRAWN] += 1
        else:
            unchanged += 1
        out.append(gone)
    return FoldResult(
        rows=tuple(out),
        history=tuple(history),
        persons=sum(1 for person in out if person.active == 1),
        created=counts[CREATED],
        updated=counts[UPDATED],
        hidden=counts[HIDDEN],
        withdrawn=counts[WITHDRAWN],
        reactivated=counts[REACTIVATED],
        unchanged=unchanged,
        stale_rules=stale_rules,
        sets_split_by_birth_year=sets_split,
    )
