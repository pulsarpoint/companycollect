"""The LLM person-matching phase (spec 2026-09-11 sections 3 and 6).

Part 1 (Task 2): candidates, scope and the input hash.
Part 2 (Task 3): the prompt and the parser.
Part 3 (Task 4): the run loop against a fake ClickHouse client and a fake model.
"""

import json
from datetime import date

import pytest

from dagster_v3.defs.se_company.info import LlmProfileConfig
from dagster_v3.defs.se_company.person.fold import MATCH_THRESHOLD, NormalizedRow
from dagster_v3.defs.se_company.person.match import (
    MACHINE_SOURCES,
    MAX_CANDIDATES,
    MAX_ROLES,
    PROMPT_VERSION,
    REASON_LIMIT,
    SYSTEM_PROMPT,
    build_candidates,
    build_match_request,
    in_scope,
    input_hash,
    parse_match_response,
    serialize_candidates,
)

import httpx
from openai import OpenAIError, RateLimitError
from pydantic import ValidationError

from dagster_v3.defs.se_company.person import batch, tables
from dagster_v3.defs.se_company.person import match
from dagster_v3.defs.se_company.person.match import (
    MatchCounts,
    PersonMatchProfile,
    run_match,
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


def test_roleless_rows_contribute_no_role_and_the_cap_keeps_the_most_recent() -> None:
    """A forty-year candidate must reach the model with THIS decade's roles: the cut is by
    year descending, and what survives is still presented ascending."""
    rows = [row("bolagsverket", "s0", role_code=None, role_year=None)]
    rows += [row("bolagsverket", f"y{year}", role_year=year) for year in range(1990, 2030)]
    candidate = build_candidates(rows)[0]
    assert len(candidate.roles) == MAX_ROLES == 20
    assert candidate.roles[0] == ("board_member", 2010)   # 2010..2029, the newest twenty
    assert candidate.roles[-1] == ("board_member", 2029)
    assert list(candidate.roles) == sorted(candidate.roles)       # presented ascending
    assert build_candidates([row("bolagsverket", "s0", role_code=None, role_year=None)])[0].roles == ()


def test_a_year_less_role_is_the_first_to_fall_out_of_the_cap() -> None:
    """A dated role is the stronger evidence, so a pair with no year sorts last in the cut --
    and keeps its place at the front of the presented list when it survives."""
    rows = [row("bolagsverket", "n0", role_code="auditor", role_year=None)]
    rows += [row("bolagsverket", f"y{year}", role_year=year) for year in range(2010, 2030)]
    capped = build_candidates(rows)[0]
    assert ("auditor", None) not in capped.roles and len(capped.roles) == MAX_ROLES
    rows = [row("bolagsverket", "n0", role_code="auditor", role_year=None)]
    rows += [row("bolagsverket", f"y{year}", role_year=year) for year in range(2010, 2029)]
    fits = build_candidates(rows)[0]
    assert fits.roles[0] == ("auditor", None) and len(fits.roles) == MAX_ROLES


def test_a_malformed_data_object_degrades_instead_of_failing() -> None:
    """`data` is a String the tables constrain to a JSON object, but a hand-written row can
    still hold anything -- the same degradation the fold's _data_object applies."""
    candidate = build_candidates([row("ratsit", "r1", data="[1,2]")])[0]
    assert candidate.age is None and candidate.external is False
    other = build_candidates([row("ratsit", "r2", data='{"age":"not a number"}')])[0]
    assert other.age is None


def test_the_age_does_not_depend_on_the_order_the_members_came_back_in() -> None:
    """Two slots of one candidate can carry ages stamped in different weeks (Ratsit re-stamps
    a person's age on their birthday). The smallest wins whichever order they arrive in, the
    way birth_year already takes years[0] -- an age that followed row order would move the
    prompt and the input hash with it."""
    older = row("ratsit", "r1", data='{"age":"60"}')
    younger = row("ratsit", "r2", data='{"age":"59"}')
    assert build_candidates([older, younger])[0].age == 59
    assert build_candidates([younger, older])[0].age == 59
    assert input_hash(build_candidates([older, younger])) == input_hash(
        build_candidates([younger, older])
    )
    mixed = build_candidates([row("ratsit", "r1", data="{}"), row("ratsit", "r2", data='{"age":"58"}')])
    assert mixed[0].age == 58


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
    # PINNED. The hash is what decides whether a company is re-sent, so a change to it re-
    # sends -- and re-pays for -- every one of the 124,646 multi-source companies. Moving
    # the prompt to short ordinal ids deliberately did NOT move it: serialize_candidates
    # still hashes the full normalized ids and the members.
    assert base == "80d8193d045c30991e0fdb3622dffc997596ca1d93307d907563f2faba818b9a"
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


PROFILE = LlmProfileConfig(provider="deepseek", model="deepseek-v4-flash",
                           prompt_version=PROMPT_VERSION, max_tokens=4_000)

ERIK = row("ratsit", "r1", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
           last="bengtsson", birth_year=1966)
BO = row("esef", "e1", display="Bo Bengtsson", first="bo", last="bengtsson")
ANNA = row("bolagsverket", "s1")


def answer(*pairs) -> str:
    return json.dumps({"pairs": list(pairs)})


def test_the_system_prompt_states_the_swedish_naming_rules_and_the_short_ids() -> None:
    for phrase in (
        "call name", "tilltalsnamn", "Erik Bo Bengtsson", "Double surnames",
        "maiden or married", "Initials", "Bjorn", "birth year", "common surnames",
        "untrusted data", '"c0", "c1", "c2"', "Use only the short ids",
    ):
        assert phrase in SYSTEM_PROMPT, phrase
    assert '{"pairs": [{"a": "c0", "b": "c3", "confidence": 0.0-1.0' in SYSTEM_PROMPT
    assert PROMPT_VERSION == "se-person-match-v1"


def test_the_prompt_carries_ordinal_ids_and_no_normalized_id() -> None:
    """A normalized id is 64 hex characters -- about 16 tokens the model would read once and
    echo twice per pair for an identifier it has no use for. The model sees `c0`..`cN`; the
    HASHED rendering keeps the real ids, so no stored input_hash moves."""
    candidates = build_candidates([ERIK, BO, ANNA])
    payload = json.loads(match.prompt_payload(candidates))
    assert [entry["id"] for entry in payload] == ["c0", "c1", "c2"]
    # The serialized (hashed) order is source then id: bolagsverket, esef, ratsit.
    assert [entry["source"] for entry in payload] == ["bolagsverket", "esef", "ratsit"]
    for candidate in candidates:
        assert candidate.id not in match.prompt_payload(candidates)
    assert "members" not in match.prompt_payload(candidates)
    # Same keys as the hashed rendering, only the id differs.
    hashed = json.loads(serialize_candidates(candidates))
    assert [set(entry) for entry in payload] == [set(entry) for entry in hashed]
    assert [entry["name"] for entry in payload] == [entry["name"] for entry in hashed]
    assert match.ordinal_id(0) == "c0" and match.ordinal_id(17) == "c17"


def _wide(count: int):
    """`count` candidates, one source, one per name -- only their number matters here."""
    return build_candidates([
        row("bolagsverket", f"s{index}", first=f"first{index}", last=f"last{index}")
        for index in range(count)
    ])


def test_the_answer_budget_scales_with_the_candidate_list() -> None:
    """A fixed 4,000 truncates the answer for a long list (prod's widest company has 159
    candidates), and a truncation is a paid call whose pairs are lost. The profile's own
    max_tokens still wins when it is LARGER."""
    assert match.request_max_tokens(build_candidates([ERIK, BO]), PROFILE) == 4_000
    wide = _wide(100)
    assert len(wide) == 100
    assert match.request_max_tokens(wide, PROFILE) == 12_000 == 120 * 100
    generous = LlmProfileConfig(provider="deepseek", model="m",
                                prompt_version=PROMPT_VERSION, max_tokens=20_000)
    assert match.request_max_tokens(wide, generous) == 20_000
    assert build_match_request(wide, PROFILE)["max_tokens"] == 12_000


def test_the_answer_budget_never_leaves_the_profiles_own_range() -> None:
    """Both ends. 3 candidates ask for the 4,000 floor; the 400-candidate hard cap computes
    48,000 and is held at MAX_ANSWER_TOKENS, which is exactly the `le` run config itself may
    ask for -- a budget no caller could have set by hand is one the provider may refuse, and
    a refusal on every run is worse than a truncation the state row records."""
    assert match.request_max_tokens(_wide(3), PROFILE) == match.MIN_ANSWER_TOKENS == 4_000
    at_cap = _wide(MAX_CANDIDATES)
    assert len(at_cap) == MAX_CANDIDATES == 400
    assert 120 * MAX_CANDIDATES == 48_000                      # what the formula computes
    assert match.request_max_tokens(at_cap, PROFILE) == match.MAX_ANSWER_TOKENS == 32_000
    assert build_match_request(at_cap, PROFILE)["max_tokens"] == 32_000
    # The ceiling IS the profile field's own limit, in both the shared config and this
    # module's, so neither can drift away from it unnoticed.
    for model_class in (LlmProfileConfig, PersonMatchProfile):
        limits = [
            item.le
            for item in model_class.model_fields["max_tokens"].metadata
            if getattr(item, "le", None) is not None
        ]
        assert limits == [match.MAX_ANSWER_TOKENS], model_class.__name__


def test_the_request_is_the_prompt_the_ordinal_candidates_and_json_mode() -> None:
    candidates = build_candidates([ERIK, BO])
    request = build_match_request(candidates, PROFILE)
    assert request["model"] == "deepseek-v4-flash"
    assert request["temperature"] == 0 and request["max_tokens"] == 4_000
    assert request["response_format"] == {"type": "json_object"}
    assert request["messages"][0] == {"role": "system", "content": SYSTEM_PROMPT}
    assert request["messages"][1]["content"] == match.prompt_payload(candidates)
    assert request["messages"][1]["content"] != serialize_candidates(candidates)
    # deepseek-v4-flash is a reasoning model and its reasoning counts against max_tokens,
    # so the pass disables thinking exactly as the ESEF passes do.
    assert request["extra_body"] == {"thinking": {"type": "disabled"}}
    other = build_match_request(candidates, LlmProfileConfig(
        provider="openai", model="gpt-x", prompt_version=PROMPT_VERSION))
    assert "extra_body" not in other


def test_the_request_refuses_a_prompt_version_it_does_not_implement() -> None:
    with pytest.raises(ValueError, match="se-person-match-v1"):
        build_match_request(build_candidates([ERIK, BO]), LlmProfileConfig(
            provider="deepseek", model="deepseek-v4-flash", prompt_version="se-person-match-v0"))


def test_an_ordinal_answer_maps_back_to_the_normalized_ids() -> None:
    """The model answers in `c0`/`c1`; nothing downstream ever sees an ordinal, and the pair
    is stored with the two NORMALIZED ids ascending whichever order the model used."""
    candidates = build_candidates([ERIK, BO])
    low, high = sorted(candidate.id for candidate in candidates)
    for a, b in (("c0", "c1"), ("c1", "c0")):
        parsed = parse_match_response(
            answer({"a": a, "b": b, "confidence": 0.93, "reason": "call name"}), candidates)
        assert len(parsed.pairs) == 1
        pair = parsed.pairs[0]
        assert (pair.candidate_a, pair.candidate_b) == (low, high)
        assert pair.confidence == 0.93 and pair.reason == "call name"


def test_a_full_normalized_id_in_the_answer_counts_as_unknown() -> None:
    """The prompt never carries one, so an answer that does is the model inventing ids."""
    candidates = build_candidates([ERIK, BO])
    low, high = sorted(candidate.id for candidate in candidates)
    parsed = parse_match_response(
        answer({"a": low, "b": high, "confidence": 0.93, "reason": "call name"}), candidates)
    assert parsed.pairs == () and parsed.dropped_unknown == 1


def test_a_repeated_pair_keeps_the_higher_confidence() -> None:
    candidates = build_candidates([ERIK, BO])
    parsed = parse_match_response(
        answer(
            {"a": "c0", "b": "c1", "confidence": 0.4, "reason": "weak"},
            {"a": "c1", "b": "c0", "confidence": 0.9, "reason": "strong"},
        ),
        candidates,
    )
    assert len(parsed.pairs) == 1
    assert parsed.pairs[0].confidence == 0.9 and parsed.pairs[0].reason == "strong"


def test_a_same_source_pair_is_allowed() -> None:
    """Spec section 8: one source can spell the same person two ways across filings, so the
    parser has NO source filter -- the fold treats such a pair like any other."""
    candidates = build_candidates([
        row("esef", "e1", display="Bo Bengtsson", first="bo", last="bengtsson"),
        row("esef", "e2", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
            last="bengtsson"),
    ])
    assert [candidate.source for candidate in candidates] == ["esef", "esef"]
    parsed = parse_match_response(
        answer({"a": "c0", "b": "c1", "confidence": 0.9, "reason": "call name"}), candidates)
    assert len(parsed.pairs) == 1 and parsed.pairs[0].confidence == 0.9


def test_unknown_ids_self_pairs_and_out_of_range_confidences_are_dropped_and_counted() -> None:
    candidates = build_candidates([ERIK, BO, ANNA])
    assert len(candidates) == 3
    parsed = parse_match_response(
        answer(
            # `c9` is past the end of a three-candidate list; so is an id it invented.
            {"a": "c0", "b": "c9", "confidence": 0.9, "reason": "past the end"},
            {"a": "c1", "b": "z" * 64, "confidence": 0.9, "reason": "invented"},
            {"a": "c1", "b": "c1", "confidence": 0.9, "reason": "itself"},
            {"a": "c0", "b": "c1", "confidence": 1.4, "reason": "over"},
            {"a": "c0", "b": "c2", "confidence": -0.1, "reason": "under"},
            {"a": "c1", "b": "c2", "confidence": "high", "reason": "not a number"},
            "not an object",
        ),
        candidates,
    )
    assert parsed.pairs == ()
    assert parsed.dropped_unknown == 3          # past the end, invented, and the non-object
    assert parsed.dropped_self == 1
    assert parsed.dropped_confidence == 3


def test_the_birth_year_lock_stores_the_pair_at_zero_confidence() -> None:
    """Spec 3.3: the guard is ours, not the model's -- the pair is kept as evidence of what
    the model said, with confidence 0 so no fold can ever act on it."""
    other_year = row("bolagsverket", "s9", display="Erik Bo Bengtsson", first="erik",
                     middles=("bo",), last="bengtsson", birth_year=1971)
    candidates = build_candidates([ERIK, other_year])
    parsed = parse_match_response(
        answer({"a": "c0", "b": "c1", "confidence": 0.97, "reason": "identical name"}), candidates)
    assert len(parsed.pairs) == 1 and parsed.birth_year_locked == 1
    assert parsed.pairs[0].confidence == 0.0
    assert parsed.pairs[0].reason == "birth-year conflict"


def test_the_parser_accepts_prose_around_the_object_and_refuses_what_is_not_one() -> None:
    candidates = build_candidates([ERIK, BO])
    wrapped = f'Here you go: {answer({"a": "c0", "b": "c1", "confidence": 0.8, "reason": "ok"})} done'
    assert len(parse_match_response(wrapped, candidates).pairs) == 1
    assert parse_match_response('{"pairs": []}', candidates).pairs == ()
    for bad in (None, "", "no json here", "{not json}", '{"pairs": "none"}', '{"other": []}'):
        with pytest.raises(ValueError):
            parse_match_response(bad, candidates)


def test_a_reason_is_capped_so_one_answer_cannot_bloat_a_row() -> None:
    candidates = build_candidates([ERIK, BO])
    parsed = parse_match_response(
        answer({"a": "c0", "b": "c1", "confidence": 0.9, "reason": "x" * 5_000}), candidates)
    assert len(parsed.pairs[0].reason) == REASON_LIMIT == 500


def test_the_threshold_is_zero_point_eight_and_lives_in_the_fold() -> None:
    """Spec sections 4 and 9: the constant sits beside FOLD_VERSION, because the fold is
    what applies it -- a threshold change is a constant edit and a re-fold, not a re-match."""
    assert MATCH_THRESHOLD == 0.8


A, B, SOLO = "5560000001", "5560000002", "5560000003"


def normalized_tuple(row: NormalizedRow) -> tuple:
    """A NormalizedRow as current_candidates_sql returns it: NORMALIZED_SELECT_COLUMNS
    order, token tuples as the lists clickhouse-driver hands back."""
    values = {name: getattr(row, name) for name in batch.NORMALIZED_SELECT_COLUMNS}
    for name in ("first_tokens", "middle_tokens", "last_tokens"):
        values[name] = list(values[name])
    return tuple(values[name] for name in batch.NORMALIZED_SELECT_COLUMNS)


class FakeClient:
    """A clickhouse-driver-shaped client: the scan's scratch table, the two page reads, and
    every INSERT recorded in order. `state` rows are (company_id, input_hash, error), the
    three columns match_state_sql now returns."""

    def __init__(self, *, scope_pages, rows, state=()):
        self.scope_pages = [list(page) for page in scope_pages]
        self.rows = list(rows)
        self.state = list(state)
        self.statements: list[tuple[str, object, object]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE")):
            return []
        if sql.startswith(f"INSERT INTO {tables.SCRATCH_SCOPE_PREFIX}"):
            return []
        if sql.startswith("INSERT INTO"):
            self.inserts.append((sql, list(params)))
            return []
        if sql.startswith(f"SELECT company_id FROM {tables.SCRATCH_SCOPE_PREFIX}"):
            page = self.scope_pages.pop(0) if self.scope_pages else []
            return [(company_id,) for company_id in page]
        ids = set(params["company_ids"])
        if sql == match.current_candidates_sql():
            return [normalized_tuple(r) for r in self.rows if r.company_id in ids]
        if sql == match.match_state_sql():
            return [entry for entry in self.state if entry[0] in ids]
        raise AssertionError(sql)

    def rows_for(self, table: str) -> list[tuple]:
        prefix = f"INSERT INTO {table} ("
        return [row for sql, rows in self.inserts if sql.startswith(prefix) for row in rows]


class FakeModel:
    """Answers per company from a script, and raises for the companies named in `failures`."""

    def __init__(self, answers, failures=()):
        self.answers = dict(answers)
        self.failures = dict(failures)
        self.requests: list[tuple[str, dict]] = []

    def __call__(self, request, *, company_id):
        self.requests.append((company_id, request))
        if company_id in self.failures:
            raise self.failures[company_id]
        return match.CallResult(
            content=self.answers.get(company_id, '{"pairs": []}'),
            prompt_tokens=100, completion_tokens=20,
        )


def one_pair(a: str = "c0", b: str = "c1", *, confidence=0.93, reason="call name") -> str:
    """A model answer with exactly one scored pair, in the ordinal ids the prompt hands out
    (`answer` is Task 3's helper)."""
    return answer({"a": a, "b": b, "confidence": confidence, "reason": reason})


def rate_limited(message: str) -> RateLimitError:
    """openai's RateLimitError is an APIStatusError: it reads `response.status_code`, so it
    needs a real httpx response, not None."""
    request = httpx.Request("POST", "https://api.deepseek.com/v1/chat/completions")
    return RateLimitError(message, response=httpx.Response(429, request=request), body=None)


CONFIG = PersonMatchProfile(provider="deepseek", model="deepseek-v4-flash", page_size=10)


def multi(company_id: str) -> list[NormalizedRow]:
    """Two sources spelling one person: the call-name gap of spec section 1."""
    return [
        row("ratsit", f"{company_id}-r1", display="Erik Bo Bengtsson", first="erik",
            middles=("bo",), last="bengtsson", birth_year=1966, company_id=company_id,
            normalized_id=f"ratsit-{company_id}".ljust(64, "0")),
        row("bolagsverket", f"{company_id}-s1", display="Bo Bengtsson", first="bo",
            last="bengtsson", company_id=company_id,
            normalized_id=f"bolagsverket-{company_id}".ljust(64, "0")),
    ]


def test_the_scope_sql_gates_on_two_machine_sources() -> None:
    sql = match.match_scope_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in sql
    assert "parse_status = 'ok'" in sql
    assert "source IN ('bolagsverket', 'esef', 'wikidata', 'ratsit')" in sql
    assert "uniqExact(source) AS sources" in sql and "HAVING sources >= 2" in sql
    # No keyset tail: scope_pages runs this once into a scratch table and pages that.
    assert "%(after_company_id)s" not in sql and "LIMIT" not in sql
    # Reviewer rows never reach the model.
    assert "reviewer" not in sql


def test_the_page_reads_bind_ids_read_final_and_keep_error_state_rows() -> None:
    candidates_sql = match.current_candidates_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in candidates_sql
    assert candidates_sql.startswith(f"SELECT {', '.join(batch.NORMALIZED_SELECT_COLUMNS)}")
    assert "company_id IN %(company_ids)s" in candidates_sql
    assert "ORDER BY company_id, source, slot" in candidates_sql
    state_sql = match.match_state_sql()
    assert f"FROM {tables.QUALIFIED_MATCH_STATE_TABLE} FINAL" in state_sql
    assert "toString(input_hash) AS input_hash" in state_sql
    # EVERY stored company, errored rows included: which of them is worth paying for again
    # is is_transient_error's decision, not a WHERE clause's.
    assert state_sql.endswith("WHERE company_id IN %(company_ids)s")
    assert "error = ''" not in state_sql and state_sql.count("error") == 1
    assert match.match_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_MATCH_TABLE} "
        f"({', '.join(tables.MATCH_COLUMNS)}) VALUES"
    )
    assert match.match_state_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} "
        f"({', '.join(tables.MATCH_STATE_COLUMNS)}) VALUES"
    )


def test_the_run_calls_once_per_company_and_writes_pairs_then_state() -> None:
    rows = [*multi(A), row("bolagsverket", "x1", company_id=SOLO)]
    ids = sorted(candidate.id for candidate in build_candidates(multi(A)))
    model = FakeModel({A: one_pair()})
    client = FakeClient(scope_pages=[[A, SOLO]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert [company_id for company_id, _ in model.requests] == [A]
    assert (counts.companies, counts.pages, counts.called, counts.reused) == (2, 1, 1, 0)
    assert counts.skipped_single_source == 1 and counts.errors == 0
    assert (counts.pairs, counts.pairs_above_threshold) == (1, 1)
    assert (counts.prompt_tokens, counts.completion_tokens) == (100, 20)
    # The pairs are written BEFORE the state row that certifies them: a fold running between
    # the two statements must never see a state hash whose pairs are not there yet.
    assert [sql for sql, _ in client.inserts] == [
        match.match_insert_sql(), match.match_state_insert_sql()
    ]
    pair = dict(zip(tables.MATCH_COLUMNS, client.rows_for(tables.QUALIFIED_MATCH_TABLE)[0]))
    assert (pair["company_id"], pair["candidate_a"], pair["candidate_b"]) == (A, ids[0], ids[1])
    assert pair["confidence"] == 0.93 and pair["reason"] == "call name"
    assert pair["model"] == "deepseek-v4-flash" and pair["prompt_version"] == PROMPT_VERSION
    assert isinstance(pair["members_a"], list) and isinstance(pair["members_b"], list)
    state = dict(zip(tables.MATCH_STATE_COLUMNS,
                     client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)[0]))
    assert state["company_id"] == A and state["input_hash"] == pair["input_hash"]
    assert (state["candidates"], state["sources"], state["pairs"]) == (2, 2, 1)
    assert (state["prompt_tokens"], state["completion_tokens"]) == (100, 20)
    assert state["error"] == "" and state["source_run_id"] == "run-1"
    assert state["raw_response"] == model.answers[A]
    assert state["matched_at"] == pair["matched_at"]


def test_an_unchanged_input_hash_is_reused_and_never_called() -> None:
    rows = multi(A)
    stored = input_hash(build_candidates(rows))
    model = FakeModel({})
    client = FakeClient(scope_pages=[[A]], rows=rows, state=[(A, stored, "")])
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert model.requests == [] and client.inserts == []
    assert (counts.reused, counts.called, counts.pairs) == (1, 0, 0)
    assert counts.skipped_sticky == 0
    # changed_only=false re-sends the same company even with the hash stored.
    again = FakeClient(scope_pages=[[A]], rows=rows, state=[(A, stored, "")])
    counts = run_match(again, llm_client=None,
                       config=PersonMatchProfile(provider="deepseek", model="m",
                                                 page_size=10, changed_only=False),
                       source_run_id="run-2", call_model=FakeModel({}))
    assert counts.called == 1 and counts.reused == 0
    assert match.match_state_sql() not in [sql for sql, _, _ in again.statements]


def test_a_transient_error_is_re_sent_on_the_same_input() -> None:
    """A rate limit and an HTTP failure are the provider's weather: the same input is worth
    paying for again, which is why the state read no longer filters them out in SQL.

    `unexpected:` is transient too (controller ruling): its cause is usually a bug in
    match.py, and fixing a bug does not move a candidate hash -- treating it as sticky would
    strand every company it touched until its people changed, which can be a year."""
    rows = multi(A)
    stored = input_hash(build_candidates(rows))
    for error in ("rate_limited: slow down", "http_error: connection reset",
                  "unexpected: RuntimeError: boom"):
        model = FakeModel({A: one_pair()})
        client = FakeClient(scope_pages=[[A]], rows=rows, state=[(A, stored, error)])
        counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                           call_model=model)
        assert [company_id for company_id, _ in model.requests] == [A], error
        assert (counts.called, counts.reused, counts.skipped_sticky) == (1, 0, 0), error
        assert match.is_transient_error(error) is True


def test_a_sticky_error_is_skipped_and_writes_no_state_row() -> None:
    """An answer that did not parse and a list over the candidate cap are properties of the
    INPUT: re-sending it buys the same failure at the same price. Skipping it writes NO state
    row, so its matched_at stops moving and the fold stops re-selecting the company (F3)."""
    rows = multi(A)
    stored = input_hash(build_candidates(rows))
    for error in ("invalid_response: person match response carries no `pairs` list",
                  "invalid_response: truncated at 4000 completion tokens",
                  "invalid_response: empty",
                  "too many candidates"):
        model = FakeModel({A: one_pair()})
        client = FakeClient(scope_pages=[[A]], rows=rows, state=[(A, stored, error)])
        counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                           call_model=model)
        assert model.requests == [] and client.inserts == [], error
        assert (counts.skipped_sticky, counts.called, counts.reused) == (1, 0, 0), error
        assert match.is_transient_error(error) is False
    # A CHANGED input clears the stickiness: the company is sent again on its new hash.
    model = FakeModel({A: one_pair()})
    client = FakeClient(scope_pages=[[A]], rows=rows,
                        state=[(A, "0" * 64, "invalid_response: nonsense")])
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert counts.called == 1 and counts.skipped_sticky == 0


def test_a_truncated_or_empty_answer_keeps_its_usage_and_its_raw_text() -> None:
    """The call was paid for either way (F5): the state row must not claim 0 tokens and an
    empty raw_response for a company that cost thousands."""
    rows = [*multi(A), *multi(B)]

    class Unusable:
        def __call__(self, request, *, company_id):
            if company_id == A:
                return match.CallResult(content='{"pairs": [', prompt_tokens=1_200,
                                        completion_tokens=4_000, finish_reason="length")
            return match.CallResult(content="  ", prompt_tokens=900, completion_tokens=0,
                                    finish_reason="stop")

    client = FakeClient(scope_pages=[[A, B]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=Unusable())
    state = {
        entry[0]: dict(zip(tables.MATCH_STATE_COLUMNS, entry))
        for entry in client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)
    }
    assert state[A]["error"] == "invalid_response: truncated at 4000 completion tokens"
    assert (state[A]["prompt_tokens"], state[A]["completion_tokens"]) == (1_200, 4_000)
    assert state[A]["raw_response"] == '{"pairs": ['
    assert state[B]["error"] == "invalid_response: empty"
    assert (state[B]["prompt_tokens"], state[B]["raw_response"]) == (900, "  ")
    assert (counts.errors, counts.called) == (2, 0)
    assert (counts.prompt_tokens, counts.completion_tokens) == (2_100, 4_000)


def test_an_unexpected_exception_is_one_company_not_the_run() -> None:
    """P2: the three typed handlers do not cover a driver's own class or a bug in here."""
    rows = [*multi(A), *multi(B)]
    model = FakeModel({B: one_pair()}, failures={A: RuntimeError("boom")})
    client = FakeClient(scope_pages=[[A, B]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert (counts.called, counts.errors, counts.pairs) == (1, 1, 1)
    state = {
        entry[0]: dict(zip(tables.MATCH_STATE_COLUMNS, entry))
        for entry in client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)
    }
    assert state[A]["error"] == "unexpected: RuntimeError: boom"
    assert state[B]["error"] == ""
    # And it is retried: the cause is ours to fix, and a fix moves no candidate hash.
    assert match.is_transient_error(state[A]["error"]) is True
    assert match.TRANSIENT_ERROR_PREFIXES == ("rate_limited:", "http_error:", "unexpected:")


def test_a_page_of_provider_failures_stops_the_run_after_writing_it() -> None:
    """F2: a provider outage must not finish green. The first page's rows are written and
    stay; the page that trips the breaker is written too, and THEN the run raises, so the
    asset's RetryPolicy backs off instead of the next run re-paying for everything."""
    good = [f"55600001{index:02d}" for index in range(match.BREAKER_MIN_ATTEMPTS)]
    bad = [f"55600002{index:02d}" for index in range(match.BREAKER_MIN_ATTEMPTS)]
    rows = [entry for company_id in (*good, *bad) for entry in multi(company_id)]
    model = FakeModel({}, failures={company_id: rate_limited("slow down") for company_id in bad})
    client = FakeClient(scope_pages=[good, bad], rows=rows)
    config = PersonMatchProfile(provider="deepseek", model="m",
                                page_size=match.BREAKER_MIN_ATTEMPTS)
    with pytest.raises(RuntimeError, match="100%"):
        run_match(client, llm_client=None, config=config, source_run_id="run-1",
                  call_model=model)
    state = {
        entry[0]: dict(zip(tables.MATCH_STATE_COLUMNS, entry))
        for entry in client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)
    }
    assert set(state) == {*good, *bad}                  # both pages are on disk
    assert all(state[company_id]["error"] == "" for company_id in good)
    assert all(state[company_id]["error"].startswith("rate_limited: ") for company_id in bad)


def test_a_minority_of_failures_does_not_stop_the_run() -> None:
    """Half the page failing is not enough, and neither is a small page of failures: below
    BREAKER_MIN_ATTEMPTS the share is noise, not evidence about the provider."""
    ids = [f"55600003{index:02d}" for index in range(match.BREAKER_MIN_ATTEMPTS)]
    rows = [entry for company_id in ids for entry in multi(company_id)]
    half = {company_id: rate_limited("slow down") for company_id in ids[: len(ids) // 2]}
    client = FakeClient(scope_pages=[ids], rows=rows)
    counts = run_match(client, llm_client=None,
                       config=PersonMatchProfile(provider="deepseek", model="m",
                                                 page_size=match.BREAKER_MIN_ATTEMPTS),
                       source_run_id="run-1", call_model=FakeModel({}, failures=half))
    assert counts.errors == len(ids) // 2 == 10 and counts.called == 10
    small = ids[: match.BREAKER_MIN_ATTEMPTS - 1]
    tiny = FakeClient(scope_pages=[small], rows=rows)
    counts = run_match(tiny, llm_client=None,
                       config=PersonMatchProfile(provider="deepseek", model="m",
                                                 page_size=match.BREAKER_MIN_ATTEMPTS),
                       source_run_id="run-1",
                       call_model=FakeModel({}, failures={
                           company_id: rate_limited("slow down") for company_id in small}))
    assert counts.errors == len(small) == 19        # every call failed, below the minimum


def test_a_failing_company_is_recorded_and_the_run_continues() -> None:
    rows = [*multi(A), *multi(B)]
    model = FakeModel(
        {B: one_pair()},
        failures={A: OpenAIError("connection reset")},
    )
    client = FakeClient(scope_pages=[[A, B]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert (counts.called, counts.errors, counts.pairs) == (1, 1, 1)
    state = {
        entry[0]: dict(zip(tables.MATCH_STATE_COLUMNS, entry))
        for entry in client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)
    }
    assert state[A]["error"].startswith("http_error: ") and "connection reset" in state[A]["error"]
    assert state[A]["pairs"] == 0 and state[A]["raw_response"] == ""
    assert state[B]["error"] == ""


def test_a_rate_limit_and_a_malformed_answer_are_typed_separately() -> None:
    rows = [*multi(A), *multi(B)]
    model = FakeModel(
        {B: "the model forgot the JSON"},
        failures={A: rate_limited("slow down")},
    )
    client = FakeClient(scope_pages=[[A, B]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert counts.errors == 2 and counts.pairs == 0 and counts.called == 0
    state = {
        entry[0]: dict(zip(tables.MATCH_STATE_COLUMNS, entry))
        for entry in client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)
    }
    assert state[A]["error"].startswith("rate_limited: ")
    assert state[B]["error"].startswith("invalid_response: ")
    # The raw text of a malformed answer is kept, so the parse can be re-read against it.
    assert state[B]["raw_response"] == "the model forgot the JSON"
    # The usage of a call that answered but did not parse is still counted.
    assert (counts.prompt_tokens, counts.completion_tokens) == (100, 20)


def test_a_company_above_the_candidate_cap_is_skipped_with_an_error() -> None:
    """Spec section 8: never truncate the list silently."""
    rows = [
        row(source, f"s{index}", first=f"first{index}", last=f"last{index}", company_id=A,
            normalized_id=f"{source}-{index}".ljust(64, "0"))
        for source in ("bolagsverket", "ratsit")
        for index in range(match.MAX_CANDIDATES // 2 + 1)
    ]
    model = FakeModel({})
    client = FakeClient(scope_pages=[[A]], rows=rows)
    counts = run_match(client, llm_client=None, config=CONFIG, source_run_id="run-1",
                       call_model=model)
    assert model.requests == [] and counts.errors == 1 and counts.called == 0
    state = dict(zip(tables.MATCH_STATE_COLUMNS,
                     client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)[0]))
    assert state["error"] == "too many candidates" and state["candidates"] == len(rows)


def test_each_page_is_written_before_the_next_one_starts() -> None:
    """Spec 3.1: a killed run resumes by its own change scan, so a page's results must be on
    disk before the next page's calls begin."""
    rows = [*multi(A), *multi(B)]
    client = FakeClient(scope_pages=[[A], [B]], rows=rows)
    counts = run_match(client, llm_client=None,
                       config=PersonMatchProfile(provider="deepseek", model="m", page_size=1),
                       source_run_id="run-1", call_model=FakeModel({}))
    assert counts.pages == 2 and counts.called == 2
    order = [sql.split("(")[0].strip() for sql, _ in client.inserts]
    assert order == [
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE}",
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE}",
    ]           # no pairs from the empty answers, one state insert per page


def test_the_cap_stops_the_scan_without_paging_further() -> None:
    client = FakeClient(scope_pages=[[A], [B]], rows=[*multi(A), *multi(B)])
    counts = run_match(
        client, llm_client=None,
        config=PersonMatchProfile(provider="deepseek", model="m", page_size=1, max_companies=1),
        source_run_id="run-1", call_model=FakeModel({}),
    )
    assert counts.stopped_at_cap is True and counts.companies == 1 and counts.pages == 1
    # The scan's scratch table is created once and dropped even when the loop breaks early.
    starts = [sql.split()[0] for sql, _, _ in client.statements]
    assert starts.count("CREATE") == 1 and starts.count("DROP") == 1


def test_company_ids_page_in_memory_with_no_scan() -> None:
    client = FakeClient(scope_pages=[], rows=multi(A))
    counts = run_match(
        client, llm_client=None,
        config=PersonMatchProfile(provider="deepseek", model="m", company_ids=[A]),
        source_run_id="run-1", call_model=FakeModel({}),
    )
    assert counts.companies == 1 and counts.called == 1
    assert not [sql for sql, _, _ in client.statements if sql.startswith("CREATE TABLE")]


def test_the_profile_requires_provider_and_model_and_pins_the_prompt_version() -> None:
    with pytest.raises(ValidationError):
        PersonMatchProfile()
    with pytest.raises(ValidationError):
        PersonMatchProfile(provider="deepseek")
    with pytest.raises(ValidationError):
        PersonMatchProfile(provider="deepseek", model="m", prompt_version="se-person-match-v0")
    config = PersonMatchProfile(provider="deepseek", model="m")
    assert config.prompt_version == PROMPT_VERSION and config.base_url == "https://api.deepseek.com"
    assert config.temperature == 0 and config.max_tokens == 4_000
    assert config.changed_only is True and config.company_ids == []
    assert (config.page_size, config.concurrency, config.timeout_seconds) == (500, 8, 120)
    assert config.max_companies == 5_000_000
    assert match.PAGE_SIZE == 500
    # LlmProfileConfig caps concurrency at 8 -- these are paid calls on one vendor account.
    with pytest.raises(ValidationError):
        PersonMatchProfile(provider="deepseek", model="m", concurrency=9)
    assert PersonMatchProfile(provider="deepseek", model="m",
                              company_ids=["5560000002", "5560000001", "5560000001"]
                              ).company_ids == ["5560000001", "5560000002"]


def test_match_counts_as_metadata_names_every_counter() -> None:
    counts = MatchCounts(companies=1, pages=2, called=3, reused=4, skipped_sticky=11,
                         skipped_single_source=5, pairs=6, pairs_above_threshold=7, errors=8,
                         prompt_tokens=9, completion_tokens=10, stopped_at_cap=False)
    assert set(counts.as_metadata()) == {
        "companies", "pages", "called", "reused", "skipped_sticky",
        "skipped_single_source", "pairs", "pairs_above_threshold", "errors",
        "prompt_tokens", "completion_tokens", "stopped_at_cap", "prompt_version", "threshold",
    }
    assert counts.as_metadata()["skipped_sticky"] == 11
