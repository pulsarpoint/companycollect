"""The LLM identity-matching phase (spec 2026-09-11 sections 3 and 4).

Between `normalize` and the fold: a company's normalized `ok` rows from the four machine
sources are grouped into candidates (one per source per exact name-token triple -- the same
within-source identity the fold already applies), the list is hashed, and a company whose
candidates span two or more sources is sent to the model once. The answer is a list of
unordered pairs with a confidence, stored in `se_company_person_match`; one state row per
company in `se_company_person_match_state` carries the hash, the usage, the raw text and any
error, and is what the next run's change scan compares against.

Nothing here decides which persons publish: the fold does, and it reads only the pairs at or
above `fold.MATCH_THRESHOLD`. Reviewer rows never reach the model -- a reviewer merges by
hand -- and the API key is read from the host environment at call time by
`se_company/info.py::build_llm_client`, never through run config.
"""

import hashlib
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dagster_v3.defs.se_company.person.fold import FOLDABLE_STATUS, NormalizedRow

PROMPT_VERSION = "se-person-match-v1"
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
    """`data.age` of the first member that carries a usable one. Ratsit writes its `data`
    values as STRINGS (`toJSONString(mapFilter(...))` over `String`s), so "58" is the
    shape to expect and anything unparseable is no age at all."""
    for member in rows:
        value = _data_object(member.data).get("age")
        if value is None or isinstance(value, bool):
            continue
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            continue
    return None


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
        pairs = sorted(
            {(member.role_code, member.role_year) for member in members if member.role_code},
            key=lambda pair: (pair[0], -1 if pair[1] is None else pair[1]),
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
                roles=tuple(pairs[:MAX_ROLES]),
                external=_external(members),
                members=normalized_ids,
            )
        )
    return sorted(candidates, key=lambda candidate: (candidate.source, candidate.id))


def in_scope(candidates: Sequence[Candidate]) -> bool:
    """Spec 3.2: a company is in scope when its candidates span at least two sources."""
    return len({candidate.source for candidate in candidates}) >= 2


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
    """The user message: sorted by source then id, keys sorted, no spaces -- so the same
    candidate list always renders the same bytes. `input_hash` hashes this plus the members
    each candidate stands for, which the message itself does not carry."""
    ordered = sorted(candidates, key=lambda candidate: (candidate.source, candidate.id))
    return json.dumps(
        [_payload(candidate) for candidate in ordered],
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )


def input_hash(candidates: Sequence[Candidate]) -> str:
    """sha256 of the candidate list, MEMBERS INCLUDED (spec 3.2). The change scan sends a
    company when this differs from its stored hash, or when it has no state row at all.

    The members are not in the prompt -- the model has no use for 64-character ids it is not
    asked about -- but they are in the hash. A new annual filing adds a slot whose name is
    identical to an existing candidate's: nothing the model would see changes, but the stored
    pair's `members_a`/`members_b` must name that slot too, or the fold unions a set the pair
    no longer fully describes. Hashing the members re-sends such a company, which is the only
    way the stored pair stays complete.
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
