"""The per-company fold of normalized address rows into the published set (spec section 5,
amended 2026-09-06).

Pure: no I/O, no clock. The batch layer reads, geocodes and writes; this module decides
which addresses exist, which members each one merges, whose spelling is published, which
are hidden by a rule and which previously published keys are withdrawn. The geocode block
is attached afterwards by `PublishedAddress.with_geocode`, because a merged address's
location key is only known once the union of its members' components is.
"""

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import (
    NormalizedAddress,
    address_key,
    display_line,
    location_key,
)
from dagster_v3.defs.se_company.address.precedence import precedence_for
from dagster_v3.defs.sweden_company.geocode_serving_overlay import GEOCODE_FALLBACK_PROVIDER

FOLD_VERSION = "address-fold-v1"
PUBLISHABLE_STATUSES: tuple[str, ...] = ("ok", "partial", "foreign")
EXCLUDED_SOURCES: tuple[str, ...] = ("reviewer_draft",)
HIDDEN = "hidden"
WITHDRAWN = "withdrawn"
FOREIGN_GEOCODE_STATUS = "foreign"
# The one-sided components: equal, or missing on one side, for two rows to be compatible.
_ONE_SIDED: tuple[str, ...] = ("house_number", "unit", "care_of")
_COMPARED: tuple[str, ...] = (
    *tables.COMPONENT_COLUMNS, "country_code", "normalized_address", "kinds", "sources", "slots",
    "text_source", "active", "inactive_reason",
)


@dataclass(frozen=True, slots=True)
class NormalizedRow:
    """One current normalized row the batch read (spec 3.2), with the raw version's stamp."""

    company_id: str
    source: str
    slot: str
    normalized_id: str
    kind: str
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    address_key: str
    parse_status: str
    normalizer_version: str
    suggested_at: datetime

    def components(self) -> dict[str, str | None]:
        return {name: getattr(self, name) for name in tables.COMPONENT_COLUMNS}

    def completeness(self) -> int:
        return sum(1 for value in self.components().values() if value is not None)

    def as_normalized_address(self) -> NormalizedAddress:
        return NormalizedAddress(
            self.care_of, self.box, self.street_name, self.house_number, self.unit, self.postal_code, self.city,
            self.country_code, self.normalized_address, self.parse_status, "",
        )


@dataclass(frozen=True, slots=True)
class PublishedAddress:
    """One main-table row (spec 3.3) minus `folded_at`, which `as_tuple` takes."""

    company_id: str
    address_key: str
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    kinds: tuple[str, ...]
    sources: tuple[str, ...]
    slots: tuple[str, ...]
    normalized_ids: tuple[str, ...]
    text_source: str
    active: int
    inactive_reason: str
    latitude: float | None
    longitude: float | None
    geocode_status: str
    geocode_method: str
    geocode_confidence: float | None
    geocode_precision: str
    geocode_policy: str
    geocode_reference: str
    geocoded_at: datetime | None
    normalizer_version: str
    fold_version: str
    source_run_id: str

    def as_normalized_address(self) -> NormalizedAddress:
        status = FOREIGN_GEOCODE_STATUS if self.geocode_status == FOREIGN_GEOCODE_STATUS else _parse_status(
            postal_code=self.postal_code, city=self.city, street_name=self.street_name, box=self.box
        )
        return NormalizedAddress(
            self.care_of, self.box, self.street_name, self.house_number, self.unit, self.postal_code, self.city,
            self.country_code, self.normalized_address, status, "",
        )

    def location_key(self) -> str:
        return location_key(self.as_normalized_address())

    def needs_geocode(self) -> bool:
        """Candidates (active or hidden) whose geocode block is still empty. An empty
        `geocode_status` is the sentinel: a foreign candidate already carries
        `'foreign'` and a geocoded one the matcher's status, so neither needs a check of
        its own. Withdrawn rows keep the block they had."""
        return self.inactive_reason != WITHDRAWN and self.geocode_status == ""

    def with_geocode(self, outcome: GeocodeOutcome) -> "PublishedAddress":
        """Gates on `geocode_provider`, not `match_status`: the resolver itself can emit
        `matched_area` for a multi-candidate area match, and that outcome's `match_method`
        still names its own strategy. Only an outcome the serving overlay's centroid
        fallback produced (`geocode_provider == GEOCODE_FALLBACK_PROVIDER`) should publish
        its `coordinate_method` instead."""
        if outcome.location_key != self.location_key():
            raise ValueError(f"outcome for {outcome.location_key} handed to {self.location_key()}")
        method = outcome.coordinate_method if outcome.geocode_provider == GEOCODE_FALLBACK_PROVIDER else outcome.match_method
        return replace(
            self,
            latitude=outcome.latitude, longitude=outcome.longitude, geocode_status=outcome.match_status,
            geocode_method=method or "", geocode_confidence=outcome.match_confidence,
            geocode_precision=outcome.geocode_precision or "", geocode_policy=outcome.policy_version,
            geocode_reference=outcome.reference_md5, geocoded_at=outcome.matched_at,
        )

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        for name in ("kinds", "sources", "slots", "normalized_ids"):
            values[name] = list(values[name])
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def changed_against(self, other: "PublishedAddress | None") -> bool:
        if other is None:
            return True
        return any(getattr(self, name) != getattr(other, name) for name in _COMPARED)


@dataclass(frozen=True, slots=True)
class FoldResult:
    rows: tuple[PublishedAddress, ...]
    published: int
    hidden: int
    withdrawn: int


def _parse_status(*, postal_code: str | None, city: str | None, street_name: str | None, box: str | None) -> str:
    """The normalizer's own rule, recomputed over a published row's components (spec
    section 4): `ok` needs a location line AND both postal parts. The location clause is
    what normalizer v3 added -- a postcode-only address carries a postcode and a city and
    is still `partial`, because it has no street and no box."""
    return "ok" if postal_code and city and (street_name or box) else "partial"


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _sort_key(row: NormalizedRow, company_precedence: Mapping[str, int] | None) -> tuple:
    return (
        -row.completeness(),
        -precedence_for(row.source, company_precedence),
        -_as_utc(row.suggested_at).timestamp(),
        row.source,
        row.slot,
    )


def _location_equal(a: Mapping[str, str | None], b: Mapping[str, str | None], *, require_present: bool = False) -> bool:
    """The location line agrees: two equal boxes when either side has one, else two equal
    street names. `require_present` also demands the compared field is set on both sides,
    which is what a partial needs -- a row with no street line is nobody's neighbour."""
    field = "box" if (a["box"] is not None or b["box"] is not None) else "street_name"
    if require_present and (a[field] is None or b[field] is None):
        return False
    return a[field] == b[field]


def _one_sided_ok(a: Mapping[str, str | None], b: Mapping[str, str | None]) -> bool:
    return all(a[name] is None or b[name] is None or a[name] == b[name] for name in _ONE_SIDED)


class _Candidate:
    """A published address being assembled: its members in sort order and the union of
    their components."""

    def __init__(self, first: NormalizedRow) -> None:
        self.members: list[NormalizedRow] = [first]
        self.union: dict[str, str | None] = first.components()
        self.country_code = first.country_code

    def compatible(self, row: NormalizedRow) -> bool:
        c = row.components()
        return (
            row.country_code == self.country_code
            and c["postal_code"] == self.union["postal_code"]
            and c["city"] == self.union["city"]
            and _location_equal(c, self.union)
            and _one_sided_ok(c, self.union)
        )

    def partial_compatible(self, row: NormalizedRow) -> bool:
        """A `partial` row is missing its postcode OR its city (the normalizer marks both
        cases), so it can only be placed by the location line plus whichever of the two it
        does carry -- it must carry at least one, or it would match half the town."""
        c = row.components()
        return (
            row.country_code == self.country_code
            and _location_equal(c, self.union, require_present=True)
            and (c["postal_code"] is None or c["postal_code"] == self.union["postal_code"])
            and (c["city"] is None or c["city"] == self.union["city"])
            and (c["postal_code"] is not None or c["city"] is not None)
            and _one_sided_ok(c, self.union)
        )

    def add(self, row: NormalizedRow) -> None:
        self.members.append(row)
        for name, value in row.components().items():
            if self.union[name] is None:
                self.union[name] = value


def _published_from(candidate: _Candidate, company_id: str, hidden_keys: Set[str], source_run_id: str) -> PublishedAddress:
    first = candidate.members[0]
    union = candidate.union
    foreign = first.parse_status == "foreign"
    status = "foreign" if foreign else _parse_status(
        postal_code=union["postal_code"], city=union["city"],
        street_name=union["street_name"], box=union["box"],
    )
    identity = NormalizedAddress(
        union["care_of"], union["box"], union["street_name"], union["house_number"], union["unit"],
        union["postal_code"], union["city"], candidate.country_code, "", status, "",
    )
    text = first.normalized_address if union == first.components() else display_line(**union)
    key = address_key(identity)
    hidden = key in hidden_keys
    # `kinds` is the DISTINCT member kinds in member order (spec 3.3), so unlike `sources`,
    # `slots` and `normalized_ids` it is not index-parallel to the members: two members that
    # are both `postal` contribute one entry. Read it as a set, never zipped with the rest.
    kinds: list[str] = []
    for member in candidate.members:
        if member.kind not in kinds:
            kinds.append(member.kind)
    return PublishedAddress(
        company_id=company_id, address_key=key, **union, country_code=candidate.country_code,
        normalized_address=text, kinds=tuple(kinds),
        sources=tuple(m.source for m in candidate.members), slots=tuple(m.slot for m in candidate.members),
        normalized_ids=tuple(m.normalized_id for m in candidate.members), text_source=first.source,
        active=0 if hidden else 1, inactive_reason=HIDDEN if hidden else "",
        latitude=None, longitude=None, geocode_status=FOREIGN_GEOCODE_STATUS if foreign else "",
        geocode_method="", geocode_confidence=None, geocode_precision="", geocode_policy="", geocode_reference="",
        geocoded_at=None, normalizer_version=first.normalizer_version, fold_version=FOLD_VERSION,
        source_run_id=source_run_id,
    )


def fold_company_addresses(
    company_id: str,
    rows: Sequence[NormalizedRow],
    published: Sequence[PublishedAddress],
    hidden_keys: Set[str],
    company_precedence: Mapping[str, int] | None,
    *,
    source_run_id: str,
) -> FoldResult:
    """The company's new published set: every candidate (active, or hidden by a rule) and
    every previously published key without a candidate as withdrawn. Candidates carry no
    geocode block yet except the foreign ones (spec 5.2 to 5.3), and a published set never
    carries one key twice."""
    for row in rows:
        if row.company_id != company_id:
            raise ValueError(f"row company_id {row.company_id!r} is not {company_id!r}")
        if row.parse_status not in PUBLISHABLE_STATUSES:
            raise ValueError(f"{row.source}/{row.slot}: parse_status {row.parse_status!r} is not publishable")
        if row.source in EXCLUDED_SOURCES:
            raise ValueError(f"{row.source}/{row.slot}: source never folds")
    for previous in published:
        if previous.company_id != company_id:
            raise ValueError(f"published company_id {previous.company_id!r} is not {company_id!r}")
    ordered = sorted(rows, key=lambda r: _sort_key(r, company_precedence))
    candidates: list[_Candidate] = []
    for row in (r for r in ordered if r.parse_status != "partial"):
        target = next((c for c in candidates if c.compatible(row)), None)
        if target is None:
            candidates.append(_Candidate(row))
        else:
            target.add(row)
    for row in (r for r in ordered if r.parse_status == "partial"):
        matching = [c for c in candidates if c.partial_compatible(row)]
        if len(matching) == 1:
            matching[0].add(row)
            continue
        # No home, or too many to choose between: the row publishes on its own -- unless an
        # earlier row already started a candidate with exactly its components, which would
        # hash to the same address_key. The main table is
        # `ReplacingMergeTree(folded_at) ORDER BY (company_id, address_key)`, so two rows
        # sharing a key and a folded_at collapse to an arbitrary one of the pair; a
        # published set never carries one key twice.
        twin = next(
            (c for c in candidates if c.country_code == row.country_code and c.union == row.components()),
            None,
        )
        if twin is None:
            candidates.append(_Candidate(row))
        else:
            twin.add(row)

    out = [_published_from(c, company_id, hidden_keys, source_run_id) for c in candidates]
    live_keys = {row.address_key for row in out}
    withdrawn = [
        replace(previous, active=0, inactive_reason=WITHDRAWN, fold_version=FOLD_VERSION, source_run_id=source_run_id)
        for previous in published
        if previous.address_key not in live_keys
    ]
    hidden = sum(1 for row in out if row.inactive_reason == HIDDEN)
    return FoldResult(
        rows=tuple([*out, *withdrawn]),
        published=len(out) - hidden,
        hidden=hidden,
        withdrawn=len(withdrawn),
    )
