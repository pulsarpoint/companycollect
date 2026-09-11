"""The LLM person-matching phase (spec 2026-09-11 sections 3 and 6).

Part 1 (Task 2): candidates, scope and the input hash.
Part 2 (Task 3): the prompt and the parser.
Part 3 (Task 4): the run loop against a fake ClickHouse client and a fake model.
"""

import json
from datetime import date

from dagster_v3.defs.se_company.person.fold import NormalizedRow
from dagster_v3.defs.se_company.person.match import (
    MACHINE_SOURCES,
    MAX_CANDIDATES,
    MAX_ROLES,
    build_candidates,
    in_scope,
    input_hash,
    serialize_candidates,
)

C = "5561552760"


def row(
    source: str = "bolagsverket",
    slot: str = "s1",
    *,
    first: str = "anna",
    middles: tuple[str, ...] = (),
    last: str = "svensson",
    display: str | None = None,
    birth_year: int | None = None,
    role_code: str | None = "board_member",
    role_year: int | None = 2024,
    role_from: date | None = None,
    role_to: date | None = None,
    data: str = "{}",
    parse_status: str = "ok",
    normalized_id: str | None = None,
    company_id: str = C,
) -> NormalizedRow:
    """One normalized row, the shape batch.normalized_row_from_row returns."""
    display_first = " ".join(part.title() for part in (first, *middles))
    display_last = last.title()
    return NormalizedRow(
        company_id=company_id, source=source, slot=slot,
        normalized_id=normalized_id or f"{source}-{slot}".ljust(64, "0"),
        parse_status=parse_status,
        first_tokens=(first,), middle_tokens=tuple(middles), last_tokens=(last,),
        display_first=display_first, display_last=display_last,
        display_name=display or f"{display_first} {display_last}",
        birth_year=birth_year, wikidata_id=None, role_code=role_code,
        role_year=role_year, role_from=role_from, role_to=role_to, data=data,
    )


def test_one_candidate_per_source_and_name_token_triple() -> None:
    """Spec 3.2: the exact-name identity the fold already applies WITHIN a source."""
    candidates = build_candidates([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2"),                       # same tokens, same source
        row("bolagsverket", "s3", middles=("maria",)),   # different middles, own candidate
        row("ratsit", "r1"),                             # same tokens, other source
    ])
    assert [(c.source, c.given, c.surname) for c in candidates] == [
        ("bolagsverket", "anna", "svensson"),
        ("bolagsverket", "anna maria", "svensson"),
        ("ratsit", "anna", "svensson"),
    ]
    first = candidates[0]
    assert first.members == ("bolagsverket-s1".ljust(64, "0"), "bolagsverket-s2".ljust(64, "0"))
    assert first.id == first.members[0]                  # the smallest normalized_id


def test_reviewer_rows_and_unparsed_rows_are_never_candidates() -> None:
    """A reviewer merges by hand (spec 3.2), and only `ok` rows fold."""
    candidates = build_candidates([
        row("bolagsverket", "s1"),
        row("reviewer", "v1"),
        row("reviewer_draft", "d1"),
        row("esef", "e1", parse_status="partial"),
    ])
    assert [c.source for c in candidates] == ["bolagsverket"]
    assert MACHINE_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")


def test_the_candidate_carries_the_longest_name_any_year_the_age_and_the_roles() -> None:
    candidates = build_candidates([
        row("ratsit", "r1", display="Erik Bo Bengtsson", first="erik",
            middles=("bo",), last="bengtsson", birth_year=1966,
            data='{"age":"60","external":"true"}', role_code="chief_executive_officer",
            role_year=2026),
        row("ratsit", "r2", display="E B Bengtsson", first="erik",
            middles=("bo",), last="bengtsson", role_code="board_member", role_year=2025),
    ])
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.name == "Erik Bo Bengtsson"          # the longest of the group
    assert candidate.given == "erik bo" and candidate.surname == "bengtsson"
    assert candidate.birth_year == 1966 and candidate.age == 60 and candidate.external is True
    assert candidate.roles == (("board_member", 2025), ("chief_executive_officer", 2026))


def test_roleless_rows_contribute_no_role_and_the_list_is_capped() -> None:
    rows = [row("bolagsverket", "s0", role_code=None, role_year=None)]
    rows += [row("bolagsverket", f"y{year}", role_year=year) for year in range(1990, 2030)]
    candidate = build_candidates(rows)[0]
    assert len(candidate.roles) == MAX_ROLES == 20
    assert candidate.roles[0] == ("board_member", 1990)   # sorted by code then year, capped
    assert build_candidates([row("bolagsverket", "s0", role_code=None, role_year=None)])[0].roles == ()


def test_a_malformed_data_object_degrades_instead_of_failing() -> None:
    """`data` is a String the tables constrain to a JSON object, but a hand-written row can
    still hold anything -- the same degradation the fold's _data_object applies."""
    candidate = build_candidates([row("ratsit", "r1", data="[1,2]")])[0]
    assert candidate.age is None and candidate.external is False
    other = build_candidates([row("ratsit", "r2", data='{"age":"not a number"}')])[0]
    assert other.age is None


def test_scope_needs_two_sources() -> None:
    one_source = build_candidates([row("bolagsverket", "s1"), row("bolagsverket", "s2", middles=("maria",))])
    assert in_scope(one_source) is False
    two_sources = build_candidates([row("bolagsverket", "s1"), row("esef", "e1")])
    assert in_scope(two_sources) is True
    assert in_scope([]) is False


def test_the_serialization_is_deterministic_and_sorted_by_source_then_id() -> None:
    rows = [row("ratsit", "r1", birth_year=1966), row("bolagsverket", "s1"), row("esef", "e1")]
    payload = json.loads(serialize_candidates(build_candidates(rows)))
    assert [entry["source"] for entry in payload] == ["bolagsverket", "esef", "ratsit"]
    assert payload[0] == {
        "id": "bolagsverket-s1".ljust(64, "0"), "source": "bolagsverket",
        "name": "Anna Svensson", "given": "anna", "surname": "svensson",
        "roles": [["board_member", 2024]],
    }
    assert payload[2]["birth_year"] == 1966
    # No key is emitted for a value the register did not carry.
    assert "birth_year" not in payload[0] and "age" not in payload[0] and "external" not in payload[0]
    assert serialize_candidates(build_candidates(rows)) == serialize_candidates(
        build_candidates(list(reversed(rows)))
    )


def test_the_input_hash_moves_only_when_the_candidate_list_moves() -> None:
    rows = [row("bolagsverket", "s1"), row("esef", "e1")]
    base = input_hash(build_candidates(rows))
    assert len(base) == 64 and base == input_hash(build_candidates(list(reversed(rows))))
    # A second slot with the SAME tokens joins an existing candidate's members. The PROMPT is
    # unchanged (the id, name and roles are the same), but the hash moves, because a stored
    # pair must name every member the fold will union.
    grown = build_candidates([*rows, row("bolagsverket", "s2")])
    assert serialize_candidates(grown) == serialize_candidates(build_candidates(rows))
    assert input_hash(grown) != base
    # A new spelling is a new candidate, so the hash moves.
    assert input_hash(build_candidates([*rows, row("wikidata", "q1", middles=("maria",))])) != base


def test_the_hard_candidate_cap_is_four_hundred() -> None:
    """Spec section 8: a company above the cap is skipped with an error, never truncated."""
    assert MAX_CANDIDATES == 400
