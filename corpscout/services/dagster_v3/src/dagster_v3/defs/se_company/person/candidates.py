"""Shared candidate construction for normalization snapshots and LLM matching."""

import hashlib
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dagster_v3.defs.se_company.person.fold import FOLDABLE_STATUS, NormalizedRow

# The sources a model may be asked about. `reviewer` and `reviewer_draft` are deliberately
# absent (spec 3.2): a human decision is not evidence to score.
MACHINE_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata", "ratsit")
# Spec section 8: a company with more candidates than this is skipped with an error rather
# than truncated silently. The prod maximum is 159.
MAX_CANDIDATES = 400
# Spec 3.2: at most this many distinct (role_code, role_year) pairs per candidate.
MAX_ROLES = 20


@dataclass(frozen=True, slots=True)
class Candidate:
    """One person as ONE source spells them (spec 3.2)."""

    id: str                                      # the group's smallest normalized_id
    source: str
    name: str                                    # the group's longest display_name
    given: str                                   # first_tokens + middle_tokens, joined
    surname: str                                 # last_tokens, joined
    birth_year: int | None
    age: int | None                              # data.age, Ratsit's only
    roles: tuple[tuple[str, int | None], ...]
    external: bool                               # data.external == 'true' (Ratsit's Extern)
    members: tuple[str, ...]                     # every normalized_id in the group, sorted


def _data_object(text: str) -> dict[str, Any]:
    """A row's `data` as a dict, degrading to {} for anything else -- the same contract
    `fold._data_object` keeps, repeated here so this module imports no fold private."""
    try:
        parsed = json.loads(text or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _age(rows: Sequence[NormalizedRow]) -> int | None:
    """The SMALLEST usable `data.age` any member carries, or None. Ratsit writes its `data`
    values as STRINGS (`toJSONString(mapFilter(...))` over `String`s), so "58" is the
    shape to expect and anything unparseable is no age at all.

    The smallest rather than the first one in input order: two slots of one candidate can
    carry ages stamped in different weeks (Ratsit re-stamps a person's age on their
    birthday), and the read's `ORDER BY company_id, source, slot` does not say which of them
    comes first in any way this function should depend on. `birth_year` already takes
    `years[0]` for the same reason, and a value that moves with row order would move the
    prompt and the input hash with it.
    """
    ages: list[int] = []
    for member in rows:
        value = _data_object(member.data).get("age")
        if value is None or isinstance(value, bool):
            continue
        try:
            ages.append(int(str(value).strip()))
        except (TypeError, ValueError):
            continue
    return min(ages) if ages else None


def _role_order(pair: tuple[str, int | None]) -> tuple[str, int]:
    """How roles are PRESENTED: role code, then year, a year-less pair first."""
    return (pair[0], -1 if pair[1] is None else pair[1])


def _capped_roles(
    pairs: Sequence[tuple[str, int | None]]
) -> tuple[tuple[str, int | None], ...]:
    """At most MAX_ROLES pairs, and when the cut bites it keeps the MOST RECENT ones.

    Cutting the presentation order instead would hand a forty-year candidate the 1990s and
    drop this decade: the years that decide whether two candidates are the same person are
    the recent ones, and a role's year is the only date the model gets. A year-less pair
    sorts last in the cut (a dated role is the stronger evidence) and what survives is
    presented in the ascending order the rest of the payload uses, so neither the prompt nor
    the input hash depends on how the cut was computed.
    """
    def recency(pair: tuple[str, int | None]) -> tuple[int, int, str]:
        year = pair[1]
        return (0 if year is not None else 1, -year if year is not None else 0, pair[0])

    return tuple(sorted(sorted(pairs, key=recency)[:MAX_ROLES], key=_role_order))


def _external(rows: Sequence[NormalizedRow]) -> bool:
    return any(
        str(_data_object(member.data).get("external", "")).strip().casefold() == "true"
        for member in rows
    )


def build_candidates(rows: Sequence[NormalizedRow]) -> list[Candidate]:
    """One company's candidates, ordered by (source, id).

    Grouped per source by `(first_tokens, middle_tokens, last_tokens)`. Only `ok` rows of
    the machine sources take part, so a reviewer row can never reach the model and a
    `partial`/`no_person` row can never become a candidate.
    """
    grouped: dict[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...]], list[NormalizedRow]]
    grouped = defaultdict(list)
    for row in rows:
        if row.parse_status != FOLDABLE_STATUS or row.source not in MACHINE_SOURCES:
            continue
        grouped[(row.source, row.first_tokens, row.middle_tokens, row.last_tokens)].append(row)
    candidates: list[Candidate] = []
    for (source, first, middle, last), members in grouped.items():
        normalized_ids = tuple(sorted({member.normalized_id for member in members}))
        years = sorted({member.birth_year for member in members if member.birth_year is not None})
        pairs = _capped_roles(
            sorted(
                {(member.role_code, member.role_year) for member in members if member.role_code},
                key=_role_order,
            )
        )
        candidates.append(
            Candidate(
                id=normalized_ids[0],
                source=source,
                # Longest spelling, ties broken alphabetically so the value never depends
                # on the order the rows came back in.
                name=min(
                    (member.display_name for member in members),
                    key=lambda name: (-len(name), name),
                ),
                given=" ".join((*first, *middle)),
                surname=" ".join(last),
                birth_year=years[0] if years else None,
                age=_age(members),
                roles=pairs,
                external=_external(members),
                members=normalized_ids,
            )
        )
    return sorted(candidates, key=lambda candidate: (candidate.source, candidate.id))


def in_scope(candidates: Sequence[Candidate]) -> bool:
    """Spec 3.2: a company is in scope when its candidates span at least two sources."""
    return len({candidate.source for candidate in candidates}) >= 2


def ordered_candidates(candidates: Sequence[Candidate]) -> list[Candidate]:
    """Stable model order independent of observation IDs; the parser uses this too."""
    def key(candidate: Candidate) -> tuple[str, str, str]:
        payload = _payload(candidate)
        del payload["id"]
        return candidate.source, json.dumps(payload, ensure_ascii=False, sort_keys=True), candidate.id

    return sorted(candidates, key=key)


def ordinal_id(index: int) -> str:
    """The id the MODEL sees for the candidate at `index` of the serialized order."""
    return f"c{index}"


def _payload(candidate: Candidate) -> dict[str, Any]:
    """One candidate as the model sees it. A value the register did not carry is left OUT
    rather than sent as null, so the prompt never asks the model to reason about absence."""
    payload: dict[str, Any] = {
        "id": candidate.id,
        "source": candidate.source,
        "name": candidate.name,
        "given": candidate.given,
        "surname": candidate.surname,
        "roles": [[code, year] for code, year in candidate.roles],
    }
    if candidate.birth_year is not None:
        payload["birth_year"] = candidate.birth_year
    if candidate.age is not None:
        payload["age"] = candidate.age
    if candidate.external:
        payload["external"] = True
    return payload


def serialize_candidates(candidates: Sequence[Candidate]) -> str:
    """The HASHED rendering: sorted by source then id, keys sorted, no spaces -- so the same
    candidate list always renders the same bytes. `input_hash` hashes this plus the members
    each candidate stands for, which the rendering itself does not carry.

    This is no longer what the model reads (`prompt_payload` is), and it keeps the full
    64-character normalized ids on purpose: a stored `input_hash` must not move because the
    prompt's shape changed, or the first run after such a change re-sends -- and re-pays for
    -- all 124,646 multi-source companies."""
    return json.dumps(
        [_payload(candidate) for candidate in sorted(candidates, key=lambda c: (c.source, c.id))],
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )


def prompt_payload(candidates: Sequence[Candidate]) -> str:
    """The user message in semantic order, under SHORT ordinal ids `c0`..`cN`.
    Keys are sorted with no spaces; observation IDs cannot move the model input.

    A normalized id is 64 hex characters -- about 16 tokens the model would have to read
    once and echo twice per pair, for an identifier it has no use for. The ordinal is one
    token, and `parse_match_response` maps it back to the candidate's real id, so nothing
    downstream ever sees `cN`. Members stay out of the prompt for the same reason they
    always have: the model is not asked about them."""
    return json.dumps(
        [
            {**_payload(candidate), "id": ordinal_id(index)}
            for index, candidate in enumerate(ordered_candidates(candidates))
        ],
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )


def input_hash(candidates: Sequence[Candidate]) -> str:
    """Historical cache key, retained only to recognize pre-fingerprint results.

    New attempts compare semantic data, bindings and configuration independently.
    Keep this encoding and its ID-based ordering stable during the migration.
    """
    ordered = sorted(candidates, key=lambda candidate: (candidate.source, candidate.id))
    payload = json.dumps(
        {
            "candidates": [_payload(candidate) for candidate in ordered],
            "members": [list(candidate.members) for candidate in ordered],
        },
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
