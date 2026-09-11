# tests/test_se_company_person_fold.py
"""The pure person fold (spec 2026-09-09 section 5).

Part 1 (Task 2): identity, the canonical name and key, the rules.
Part 2 (Task 3): the person row, roles, `data`, lifecycle and history.
"""

import dataclasses
import json
from datetime import date

import pytest

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import (
    FOLD_VERSION,
    MATCH_THRESHOLD,
    HistoryEntry,
    MatchPair,
    NormalizedRow,
    PersonRule,
    PublishedPerson,  # noqa: F401  (the module's published surface, asserted through `fold`)
    canonical_tokens,
    fold_company_persons,
    identity_sets,
    pairs_within,
    person_key,
)

C = "5561552760"
OTHER = "5560125220"


def row(
    source: str = "bolagsverket",
    slot: str = "s1",
    *,
    first: str = "anna",
    middles: tuple[str, ...] = (),
    last: str = "svensson",
    display: str | None = None,
    birth_year: int | None = None,
    wikidata_id: str | None = None,
    role_code: str | None = None,
    role_year: int | None = None,
    role_from: date | None = None,
    role_to: date | None = None,
    data: str = "{}",
    company_id: str = C,
    parse_status: str = "ok",
) -> NormalizedRow:
    """One normalized `ok` row. The display spelling defaults to the title-cased tokens, so
    a test that cares about spelling passes `display` explicitly."""
    display_first = " ".join(part.title() for part in (first, *middles))
    display_last = last.title()
    return NormalizedRow(
        company_id=company_id, source=source, slot=slot,
        normalized_id=f"{source}-{slot}".ljust(64, "0"), parse_status=parse_status,
        first_tokens=(first,), middle_tokens=tuple(middles), last_tokens=(last,),
        display_first=display_first, display_last=display_last,
        display_name=display or f"{display_first} {display_last}",
        birth_year=birth_year, wikidata_id=wikidata_id, role_code=role_code,
        role_year=role_year, role_from=role_from, role_to=role_to, data=data,
    )


def fold(rows, published=(), rules=(), precedence=None, *, run="run-1", year=2026, matches=()):
    return fold_company_persons(
        C, rows, published, rules, precedence, source_run_id=run, current_year=year,
        matches=matches,
    )


def names(result) -> list[str]:
    return sorted(person.display_name for person in result.rows)


def test_two_sources_spelling_one_name_are_one_person() -> None:
    result = fold([row("bolagsverket", "s1"), row("esef", "e1")])
    assert len(result.rows) == 1
    assert result.rows[0].sources == ("bolagsverket", "esef")
    assert result.persons == 1
    assert result.sets_split_by_birth_year == 0


def test_anna_folds_with_anna_maria_when_she_is_the_only_more_complete_name() -> None:
    result = fold([row("bolagsverket", "s1"), row("esef", "e1", middles=("maria",))])
    assert len(result.rows) == 1
    # Spelling follows the name precedence (bolagsverket 900 beats esef 400); identity
    # follows the most complete member, so the key is Anna Maria's (spec 5.1 vs 5.3).
    assert result.rows[0].display_name == "Anna Svensson"
    assert result.rows[0].text_source == "bolagsverket"
    assert result.rows[0].person_key == person_key(C, ("anna", "maria", "svensson"))
    assert canonical_tokens([row(middles=("maria",))]) == ("anna", "maria", "svensson")


def test_anna_stays_alone_when_two_middle_names_compete() -> None:
    """The spec's own example: with both Anna Maria and Anna Karin present, `{}` has two
    minimal supersets, so the bare Anna joins neither and the two full names stay apart."""
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
        row("bolagsverket", "s3", middles=("karin",)),
    ])
    assert names(result) == ["Anna Karin Svensson", "Anna Maria Svensson", "Anna Svensson"]


def test_a_chain_of_middle_names_folds_through_its_unique_minimal_superset() -> None:
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
        row("bolagsverket", "s3", middles=("maria", "karin")),
    ])
    assert len(result.rows) == 1
    assert result.rows[0].display_name == "Anna Maria Karin Svensson"


def test_the_normalizers_tokens_already_fold_diacritics_and_hyphens() -> None:
    """Identity is over tokens, never over display text: the normalizer folded Hakan/Håkan
    and Sven-Erik/Sven Erik before the fold ever saw them."""
    from dagster_v3.defs.se_company.person.normalize_se import RawPerson, normalize_se_person

    a = normalize_se_person(RawPerson(source="esef", full_name="Håkan Öberg"))
    b = normalize_se_person(RawPerson(source="wikidata", full_name="Hakan Oberg"))
    assert (a.first_tokens, a.last_tokens) == (b.first_tokens, b.last_tokens) == (("hakan",), ("oberg",))
    hyphen = normalize_se_person(RawPerson(source="esef", full_name="Sven-Erik Nilsson"))
    spaced = normalize_se_person(RawPerson(source="wikidata", full_name="Sven Erik Nilsson"))
    assert (hyphen.first_tokens, hyphen.middle_tokens) == (spaced.first_tokens, spaced.middle_tokens)


def test_two_spellings_with_the_same_qid_are_one_person() -> None:
    result = fold([
        row("wikidata", "q1", first="karl", last="andersson", wikidata_id="Q42"),
        row("esef", "e1", first="carl", last="anderson", wikidata_id="Q42"),
    ])
    assert len(result.rows) == 1
    assert result.rows[0].wikidata_id == "Q42"
    # Member order is precedence descending: wikidata 600 before esef 400.
    assert result.rows[0].sources == ("wikidata", "esef")
    assert result.rows[0].display_name == "Karl Andersson"


def test_the_same_name_with_two_birth_years_is_two_persons_with_two_keys() -> None:
    result = fold([
        row("bolagsverket", "s1", birth_year=1970),
        row("bolagsverket", "s2", birth_year=1980),
    ])
    assert len(result.rows) == 2
    assert {person.birth_year for person in result.rows} == {1970, 1980}
    assert len({person.person_key for person in result.rows}) == 2


def test_a_year_less_row_between_two_birth_years_splits_by_year() -> None:
    """The year-less Anna Maria matches both dated rows through the middle-name rule, so the
    closed set holds two years and is split; she attaches to the sub-set she matched
    through, the smallest year on a genuine tie."""
    rows = [
        row("bolagsverket", "s1", middles=("maria",), birth_year=1970),
        row("bolagsverket", "s2", middles=("maria",), birth_year=1980),
        row("esef", "e1", middles=("maria",)),
    ]
    sets = identity_sets(rows)
    assert len(sets) == 2
    by_year = {members[0].birth_year: {member.slot for member in members} for members in sets}
    assert by_year == {1970: {"s1", "e1"}, 1980: {"s2"}}
    # The counter Task 6 reads as an acceptance readout must actually move: a bug pinning it
    # to 0 would otherwise pass every other assertion here.
    result = fold(rows)
    assert result.sets_split_by_birth_year == 1


def test_the_canonical_key_is_stable_across_two_folds() -> None:
    first = fold([row("bolagsverket", "s1")])
    again = fold(
        [row("bolagsverket", "s1"), row("esef", "e1")],
        published=[dataclasses.replace(first.rows[0], folded_at=None)],
    )
    assert again.rows[0].person_key == first.rows[0].person_key
    assert first.rows[0].person_key == person_key(C, ("anna", "svensson"))


def test_two_persons_with_the_same_canonical_name_never_share_a_key() -> None:
    """ReplacingMergeTree ORDER BY (company_id, person_key) would collapse them into one
    person, so both colliding sets take a discriminator."""
    result = fold([
        row("bolagsverket", "s1", birth_year=1970),
        row("bolagsverket", "s2", birth_year=1980),
    ])
    keys = {person.person_key for person in result.rows}
    assert len(keys) == 2
    assert person_key(C, ("anna", "svensson")) not in keys
    # the birth year is the discriminator: it does not move when a slot comes or goes
    assert person_key(C, ("anna", "svensson"), "1970") in keys
    assert person_key(C, ("anna", "svensson"), "1980") in keys


def test_two_sets_sharing_a_name_and_a_birth_year_still_get_two_keys() -> None:
    """A split rule over a set whose members all carry the SAME birth year leaves two sets
    sharing the canonical name AND the year, which the year alone cannot separate. The
    collision unit is therefore (canonical name, discriminator): a pair that is still shared
    falls back to "<year>:<smallest source:slot>", the sets being disjoint. Without it
    ReplacingMergeTree ORDER BY (company_id, person_key) collapses the two people the
    reviewer just separated back into one."""
    result = fold(
        [row("bolagsverket", "s1", birth_year=1970), row("esef", "e1", birth_year=1970)],
        rules=[PersonRule(C, "r" * 64, "split", (), ("e1",))],
    )
    assert len(result.rows) == 2
    keys = {person.person_key for person in result.rows}
    assert keys == {
        person_key(C, ("anna", "svensson"), "1970:bolagsverket:s1"),
        person_key(C, ("anna", "svensson"), "1970:esef:e1"),
    }
    assert person_key(C, ("anna", "svensson")) not in keys
    assert person_key(C, ("anna", "svensson"), "1970") not in keys
    # Two DIFFERENT years keep the plain year: the pair only degrades when it has to, so a
    # birth-year discriminator still does not move when a slot comes or goes.
    two_years = fold([
        row("bolagsverket", "s1", birth_year=1970),
        row("bolagsverket", "s2", birth_year=1980),
    ])
    assert {person.person_key for person in two_years.rows} == {
        person_key(C, ("anna", "svensson"), "1970"),
        person_key(C, ("anna", "svensson"), "1980"),
    }


def test_a_merge_rule_joins_the_sets_behind_its_keys_and_re_keys_the_result() -> None:
    first = fold([
        row("bolagsverket", "s1", middles=("maria",)),
        row("esef", "e1", first="hakan", last="oberg"),
    ])
    published = [dataclasses.replace(person, folded_at=None) for person in first.rows]
    keys = sorted(person.person_key for person in first.rows)
    merged = fold(
        [row("bolagsverket", "s1", middles=("maria",)), row("esef", "e1", first="hakan", last="oberg")],
        published=published,
        rules=[PersonRule(C, "r" * 64, "merge", tuple(keys), ())],
    )
    live = [person for person in merged.rows if person.active == 1]
    assert len(live) == 1
    assert live[0].display_name == "Anna Maria Svensson"       # the most complete member
    assert live[0].member_slots == ("s1", "e1")
    assert merged.stale_rules == 0


def test_a_merge_rule_key_that_resolves_to_nothing_is_ignored_and_counted() -> None:
    result = fold(
        [row("bolagsverket", "s1")],
        rules=[PersonRule(C, "r" * 64, "merge", ("f" * 64, "e" * 64), ())],
    )
    assert len(result.rows) == 1
    assert result.stale_rules == 1


def test_a_split_rule_moves_its_slots_into_a_set_of_their_own() -> None:
    result = fold(
        [row("bolagsverket", "s1"), row("esef", "e1")],
        rules=[PersonRule(C, "r" * 64, "split", (), ("e1",))],
    )
    assert len(result.rows) == 2
    assert {person.member_slots for person in result.rows} == {("s1",), ("e1",)}
    assert len({person.person_key for person in result.rows}) == 2
    assert result.stale_rules == 0


def test_a_split_rule_whose_slots_come_from_two_sets_joins_them() -> None:
    """The rule's slots form ONE new set even when they sat under two different persons:
    exactly those slots are pulled out and joined and the rest of both sets stays behind,
    which is how a reviewer moves two observations into one person without also merging
    everything else the two sets held."""
    result = fold(
        [
            row("bolagsverket", "s1"),
            row("esef", "e1"),
            row("esef", "e2", first="hakan", last="oberg"),
        ],
        rules=[PersonRule(C, "r" * 64, "split", (), ("e1", "e2"))],
    )
    assert {person.member_slots for person in result.rows} == {("s1",), ("e1", "e2")}
    assert len({person.person_key for person in result.rows}) == 2
    assert result.stale_rules == 0


def test_a_split_rule_whose_slots_are_gone_is_counted_stale() -> None:
    result = fold([row("bolagsverket", "s1")], rules=[PersonRule(C, "r" * 64, "split", (), ("gone",))])
    assert len(result.rows) == 1 and result.stale_rules == 1


def test_rules_apply_in_kind_order_merge_then_split() -> None:
    """A split rule sees the sets a merge rule already joined, never the other way round --
    otherwise a reviewer who merged two people and then split one member out would get the
    member back in the joined set."""
    first = fold([row("bolagsverket", "s1"), row("esef", "e1", first="hakan", last="oberg")])
    published = [dataclasses.replace(person, folded_at=None) for person in first.rows]
    keys = sorted(person.person_key for person in first.rows)
    result = fold(
        [row("bolagsverket", "s1"), row("esef", "e1", first="hakan", last="oberg")],
        published=published,
        rules=[
            PersonRule(C, "a" * 64, "merge", tuple(keys), ()),
            PersonRule(C, "b" * 64, "split", (), ("e1",)),
        ],
    )
    live = sorted(
        (person for person in result.rows if person.active == 1), key=lambda p: p.member_slots
    )
    assert [person.member_slots for person in live] == [("e1",), ("s1",)]
    assert result.stale_rules == 0


def test_a_rule_for_another_company_is_refused() -> None:
    """`PersonRule` carries a company_id and the fold checks it: a mis-scoped rule SELECT
    would otherwise silently regroup another company's people."""
    with pytest.raises(ValueError):
        fold(
            [row("bolagsverket", "s1")],
            rules=[PersonRule(OTHER, "r" * 64, "hide", ("f" * 64,), ())],
        )


def test_an_unknown_rule_kind_is_refused_and_named() -> None:
    """A kind the fold does not implement is a bad row in the rule table -- a bug, not a
    rule that resolved to nothing -- so it fails loudly instead of disappearing."""
    with pytest.raises(ValueError, match="unmerge"):
        fold(
            [row("bolagsverket", "s1")],
            rules=[PersonRule(C, "r" * 64, "unmerge", (), ("s1",))],
        )


def test_bad_input_is_refused() -> None:
    with pytest.raises(ValueError):
        fold([row("bolagsverket", "s1", parse_status="partial")])
    with pytest.raises(ValueError):
        fold([row("reviewer_draft", "d1")])
    with pytest.raises(ValueError):
        fold([row("bolagsverket", "s1", company_id=OTHER)])


# --- part 2: the person row, roles, data, lifecycle -------------------------------------

from dagster_v3.defs.se_company.person.fold import (  # noqa: E402  (kept beside part 1's import)
    change_kind_for,
    merge_member_data,
    role_block,
    role_years_for,
)


def published(result, key: str | None = None):
    """The row of `key`, or the only row."""
    if key is None:
        assert len(result.rows) == 1
        return result.rows[0]
    return next(person for person in result.rows if person.person_key == key)


def refold(result, rows, rules=(), precedence=None, *, run="run-2", year=2026, matches=()):
    """Feed a fold's output back in as the published set, the way the batch does."""
    previous = [dataclasses.replace(person, folded_at=None) for person in result.rows]
    return fold(rows, published=previous, rules=rules, precedence=precedence, run=run,
                year=year, matches=matches)


def test_the_person_row_takes_its_spelling_from_the_highest_precedence_member() -> None:
    result = fold([
        row("esef", "e1", display="HÅKAN ÖBERG", first="hakan", last="oberg"),
        row("bolagsverket", "s1", display="Håkan Öberg", first="hakan", last="oberg"),
    ])
    person = published(result)
    assert (person.display_name, person.text_source) == ("Håkan Öberg", "bolagsverket")
    assert (person.first_name, person.last_name) == ("Hakan", "Oberg")


def test_a_precedence_tie_takes_the_most_complete_spelling() -> None:
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
    ])
    assert published(result).display_name == "Anna Maria Svensson"


def test_a_hide_rule_follows_the_person_when_a_fuller_spelling_re_keys_the_set() -> None:
    """A hide names the key as it was; a later member with a longer name moves the set to
    a new canonical key, and the hide resolves through the previous members instead of
    going stale (otherwise a Remove would silently reverse)."""
    first = fold([row("bolagsverket", "s1")])
    old_key = first.rows[0].person_key
    hide = PersonRule(C, "h" * 64, "hide", (old_key,), ())
    hidden = fold([row("bolagsverket", "s1")], published=first.rows, rules=[hide])
    assert hidden.rows[0].active == 0 and hidden.rows[0].inactive_reason == "hidden"
    grown = fold(
        [row("bolagsverket", "s1"), row("esef", "e1", middles=("maria",))],
        published=hidden.rows, rules=[hide],
    )
    new = [person for person in grown.rows if person.person_key != old_key]
    assert len(new) == 1 and new[0].active == 0 and new[0].inactive_reason == "hidden"
    assert grown.stale_rules == 0


def test_a_company_precedence_row_overrides_the_global_order() -> None:
    rows = [
        row("esef", "e1", display="HAKAN OBERG", first="hakan", last="oberg"),
        row("bolagsverket", "s1", display="Håkan Öberg", first="hakan", last="oberg"),
    ]
    assert published(fold(rows, precedence={"esef": 5000})).text_source == "esef"


def test_members_are_ordered_by_precedence_then_slot_and_the_arrays_are_parallel() -> None:
    result = fold([
        row("esef", "e2", data='{"confidence":"0.9"}'),
        row("esef", "e1"),
        row("wikidata", "q1", birth_year=1970, wikidata_id="Q42"),
        row("bolagsverket", "s1"),
    ])
    person = published(result)
    assert person.member_sources == ("bolagsverket", "wikidata", "esef", "esef")
    assert person.member_slots == ("s1", "q1", "e1", "e2")
    assert person.slots == person.member_slots
    assert person.sources == ("bolagsverket", "wikidata", "esef")
    assert person.member_birth_years == (None, 1970, None, None)
    assert person.member_wikidata_ids == ("", "Q42", "", "")
    assert person.member_names == ("Anna Svensson",) * 4
    assert person.member_data == ("{}", "{}", "{}", '{"confidence":"0.9"}')
    assert person.normalized_ids == tuple(
        f"{source}-{slot}".ljust(64, "0")
        for source, slot in zip(person.member_sources, person.member_slots, strict=True)
    )


def test_birth_year_and_qid_come_from_whichever_member_carries_one() -> None:
    person = published(fold([
        row("bolagsverket", "s1"),
        row("wikidata", "q1", birth_year=1970, wikidata_id="Q42"),
    ]))
    assert (person.birth_year, person.wikidata_id) == (1970, "Q42")


def test_roles_are_a_union_of_pairs_with_their_sources_sorted_by_year_then_code() -> None:
    person = published(fold([
        row("bolagsverket", "s1", role_code="board_member", role_year=2024),
        row("bolagsverket", "s2", role_code="board_chair", role_year=2024),
        row("esef", "e1", role_code="board_member", role_year=2024),
        row("bolagsverket", "s3", role_code="board_member", role_year=2023),
    ]))
    assert person.role_codes == ("board_member", "board_chair", "board_member")
    assert person.role_years == (2023, 2024, 2024)
    assert person.role_sources == (("bolagsverket",), ("bolagsverket",), ("bolagsverket", "esef"))
    assert (person.first_year, person.last_year) == (2023, 2024)
    assert set(person.current_roles) == {"board_chair", "board_member"}


def test_a_wikidata_span_expands_year_by_year_and_an_open_span_reaches_the_current_year() -> None:
    closed = row(
        "wikidata", "q1", role_code="founder",
        role_from=date(2019, 5, 1), role_to=date(2021, 3, 1),
    )
    assert role_years_for(closed, 2026) == (2019, 2020, 2021)
    open_span = row("wikidata", "q2", role_code="owner", role_from=date(2024, 1, 1))
    assert role_years_for(open_span, 2026) == (2024, 2025, 2026)
    person = published(fold([open_span]))
    assert person.current_roles == ("owner",) and person.last_year == 2026


def test_a_fiscal_year_wins_over_a_span_and_a_year_less_role_is_current() -> None:
    """ESEF delivers both a document fiscal year and an effective span; spec 4.3 says the
    document's fiscal year is that row's role year. A role with no year information at all
    is taken as held now (controller ruling 2026-09-10: 290 of Wikidata's 466 roles on prod
    carry no date; dropping them would hide most Wikidata roles), so it makes the pair
    (code, current_year); a row with only an end date makes (code, end year)."""
    esef = row("esef", "e1", role_code="auditor", role_year=2023, role_from=date(2020, 1, 1))
    assert role_years_for(esef, 2026) == (2023,)
    dateless = row("wikidata", "q3", role_code="executive")
    assert role_years_for(dateless, 2026) == (2026,)
    end_only = row("wikidata", "q4", role_code="owner", role_to=date(2021, 6, 1))
    assert role_years_for(end_only, 2026) == (2021,)
    person = published(fold([dateless]))
    assert person.role_codes == ("executive",) and person.first_year == 2026
    assert person.current_roles == ("executive",) and person.member_slots == ("q3",)


def test_a_roleless_member_still_counts_as_a_member() -> None:
    person = published(fold([
        row("bolagsverket", "s1", role_code=None, role_year=2024),
        row("bolagsverket", "s2", role_code="board_member", role_year=2024),
    ]))
    assert person.member_slots == ("s1", "s2")
    assert person.role_codes == ("board_member",) and person.role_years == (2024,)


def test_role_block_keeps_a_code_the_normalizer_could_not_map() -> None:
    """Owner ruling 2026-08-28: an unmapped label publishes as itself. The fold keeps it."""
    block = role_block([row("bolagsverket", "s1", role_code="styrelseledarmot", role_year=2024)], 2026)
    assert block.role_codes == ("styrelseledarmot",)


def test_data_merges_key_by_key_by_precedence_with_nested_objects_and_replaced_arrays() -> None:
    members = [
        row("bolagsverket", "s1", data='{"kind":"board","extra":{"a":"1","b":"2"},"tags":["x"]}'),
        row("esef", "e1", data='{"kind":"llm","confidence":"0.9","extra":{"b":"9","c":"3"},"tags":["y","z"]}'),
    ]
    merged = json.loads(merge_member_data(members, None))
    assert merged["kind"] == "board"                       # bolagsverket 900 > esef 400
    assert merged["confidence"] == "0.9"                   # only ESEF has it
    assert merged["extra"] == {"a": "1", "b": "2", "c": "3"}   # one level down, higher wins
    assert merged["tags"] == ["x"]                         # arrays replace, never concatenate
    assert merge_member_data([row("bolagsverket", "s1")], None) == "{}"
    assert merge_member_data([row("bolagsverket", "s1", data="not json")], None) == "{}"


def test_reviewer_data_keys_win_outright() -> None:
    members = [
        row("bolagsverket", "s1", data='{"extra":{"a":"1","b":"2"}}'),
        row("reviewer", "r00000000000000001", data='{"extra":{"b":"9"}}'),
    ]
    merged = json.loads(merge_member_data(members, None))
    assert merged["extra"] == {"b": "9"}                   # taken whole, not merged


def test_the_row_data_is_the_merged_object_and_the_key_order_is_stable() -> None:
    person = published(fold([
        row("esef", "e1", data='{"b":"2","a":"1"}'),
        row("bolagsverket", "s1", data='{"c":"3"}'),
    ]))
    assert person.data == '{"a":"1","b":"2","c":"3"}'


def test_a_first_fold_creates_and_an_identical_second_fold_changes_nothing() -> None:
    rows = [row("bolagsverket", "s1", role_code="board_member", role_year=2024)]
    first = fold(rows)
    assert (first.created, first.updated, first.unchanged) == (1, 0, 0)
    assert [entry.change_kind for entry in first.history] == ["created"]
    assert first.history[0].row.person_key == first.rows[0].person_key
    again = refold(first, rows)
    assert (again.created, again.updated, again.unchanged) == (0, 0, 1)
    assert again.history == ()
    assert again.rows[0].person_key == first.rows[0].person_key


def test_a_new_member_updates_the_person_and_history_keeps_the_previous_image() -> None:
    first = fold([row("bolagsverket", "s1")])
    second = refold(first, [row("bolagsverket", "s1"), row("esef", "e1")])
    assert (second.updated, second.unchanged) == (1, 0)
    entry = second.history[0]
    assert entry.change_kind == "updated"
    assert entry.row.sources == ("bolagsverket",)          # the PREVIOUS image
    assert second.rows[0].sources == ("bolagsverket", "esef")


def test_a_key_that_loses_every_observation_is_withdrawn_once_then_left_alone() -> None:
    first = fold([row("bolagsverket", "s1")])
    gone = refold(first, [])
    withdrawn = published(gone)
    assert (withdrawn.active, withdrawn.inactive_reason) == (0, "withdrawn")
    assert withdrawn.sources == ("bolagsverket",)          # its blocks are kept
    assert [entry.change_kind for entry in gone.history] == ["withdrawn"]
    assert gone.history[0].row.active == 1                 # the previous, live image
    again = refold(gone, [], run="run-3")
    assert again.history == () and again.unchanged == 1 and again.withdrawn == 0
    assert published(again).inactive_reason == "withdrawn"


def test_a_withdrawn_person_who_comes_back_is_reactivated() -> None:
    rows = [row("bolagsverket", "s1")]
    gone = refold(fold(rows), [])
    back = refold(gone, rows, run="run-3")
    person = published(back)
    assert (person.active, person.inactive_reason) == (1, "")
    assert [entry.change_kind for entry in back.history] == ["reactivated"]
    assert back.reactivated == 1


def test_a_hide_rule_marks_the_set_inactive_and_a_reset_reactivates_it() -> None:
    rows = [row("bolagsverket", "s1")]
    first = fold(rows)
    key = first.rows[0].person_key
    hidden = refold(first, rows, rules=[PersonRule(C, "h" * 64, "hide", (key,), ())])
    person = published(hidden)
    assert (person.active, person.inactive_reason) == (0, "hidden")
    assert [entry.change_kind for entry in hidden.history] == ["hidden"]
    assert hidden.persons == 0 and hidden.hidden == 1
    reset = refold(hidden, rows, run="run-3")               # the batch stopped passing the rule
    assert (published(reset).active, published(reset).inactive_reason) == (1, "")
    assert [entry.change_kind for entry in reset.history] == ["reactivated"]


def test_a_hide_rule_for_a_key_no_set_carries_is_counted_stale() -> None:
    result = fold([row("bolagsverket", "s1")], rules=[PersonRule(C, "h" * 64, "hide", ("f" * 64,), ())])
    assert published(result).active == 1 and result.stale_rules == 1


def test_change_kind_for_names_every_transition() -> None:
    live = fold([row("bolagsverket", "s1")]).rows[0]
    hidden = dataclasses.replace(live, active=0, inactive_reason="hidden")
    withdrawn = dataclasses.replace(live, active=0, inactive_reason="withdrawn")
    assert change_kind_for(live, dataclasses.replace(live, display_name="Other")) == "updated"
    assert change_kind_for(live, hidden) == "hidden"
    assert change_kind_for(live, withdrawn) == "withdrawn"
    assert change_kind_for(withdrawn, live) == "reactivated"
    assert change_kind_for(hidden, live) == "reactivated"
    assert set(tables.CHANGE_KINDS) == {"created", "updated", "hidden", "withdrawn", "reactivated"}


def test_as_tuple_follows_main_columns_and_history_tuple_appends_the_change_block() -> None:
    from datetime import UTC, datetime

    folded_at = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
    person = fold([row("bolagsverket", "s1", role_code="board_member", role_year=2024)]).rows[0]
    values = person.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS) == 29
    by_name = dict(zip(tables.MAIN_COLUMNS, values, strict=True))
    assert by_name["folded_at"] == folded_at and by_name["fold_version"] == FOLD_VERSION
    assert by_name["source_run_id"] == "run-1" and by_name["data"] == "{}"
    for column in ("sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS, "role_codes",
                   "role_years", "current_roles", "role_sources"):
        assert isinstance(by_name[column], list), column
    assert by_name["role_sources"] == [["bolagsverket"]]
    for column in ("company_id", "person_key", "display_name", "first_name", "last_name",
                   "text_source", "inactive_reason", "data", "fold_version", "source_run_id"):
        assert by_name[column] is not None, column
    history = HistoryEntry(person, "created").row.history_tuple(
        changed_at=folded_at, change_kind="created", fold_run_id="run-1"
    )
    assert len(history) == len(tables.HISTORY_COLUMNS) == 32
    assert history[-3:] == (folded_at, "created", "run-1")
    assert dict(zip(tables.HISTORY_COLUMNS, history, strict=True))["folded_at"] == folded_at


# --- part 3: the LLM's scored pairs (spec 2026-09-11 section 4) -------------------------

RATSIT_ERIK = row("ratsit", "r1", display="Erik Bo Bengtsson", first="erik",
                  middles=("bo",), last="bengtsson", birth_year=1966)
ESEF_BO = row("esef", "e1", display="Bo Bengtsson", first="bo", last="bengtsson")


def match(a=RATSIT_ERIK, b=ESEF_BO, *, confidence=0.93, reason="call name") -> MatchPair:
    return MatchPair(
        members_a=(a.normalized_id,), members_b=(b.normalized_id,), confidence=confidence,
        reason=reason, name_a=a.display_name, name_b=b.display_name,
        model="deepseek-v4-flash", prompt_version="se-person-match-v1",
    )


def test_a_call_name_pair_becomes_one_person_with_the_ratsit_spelling() -> None:
    """The gap of spec section 1: the K3 identity keeps `erik bo bengtsson` and
    `bo bengtsson` apart, and a scored pair joins them."""
    apart = fold([RATSIT_ERIK, ESEF_BO])
    assert len(apart.rows) == 2

    result = fold([RATSIT_ERIK, ESEF_BO], matches=[match()])
    person = published(result)
    assert result.persons == 1
    # Identity follows the most complete member; the spelling follows the name precedence,
    # and ratsit (1000) outranks esef (400), so both say the full name here.
    assert person.person_key == person_key(C, ("erik", "bo", "bengtsson"))
    assert person.display_name == "Erik Bo Bengtsson" and person.text_source == "ratsit"
    assert person.sources == ("ratsit", "esef")
    assert person.birth_year == 1966
    assert json.loads(person.data)["llm_match"] == {
        "pairs": [{"a": "Erik Bo Bengtsson", "b": "Bo Bengtsson",
                   "confidence": 0.93, "reason": "call name"}],
        "model": "deepseek-v4-flash",
        "prompt_version": "se-person-match-v1",
    }
    assert person.fold_version == FOLD_VERSION == "se-person-fold-v2"


def test_a_pair_across_two_birth_years_never_joins() -> None:
    """The second lock (spec section 4): the stored row already says confidence 0 for this
    case, and the fold refuses it again even at 0.99."""
    other_year = row("esef", "e1", display="Bo Bengtsson", first="bo", last="bengtsson",
                     birth_year=1971)
    result = fold([RATSIT_ERIK, other_year],
                  matches=[match(b=other_year, confidence=0.99)])
    assert len(result.rows) == 2
    for person in result.rows:
        assert "llm_match" not in json.loads(person.data)


def test_a_match_below_the_threshold_does_nothing() -> None:
    result = fold([RATSIT_ERIK, ESEF_BO], matches=[match(confidence=0.79)])
    assert len(result.rows) == 2
    assert MATCH_THRESHOLD == 0.8
    exact = fold([RATSIT_ERIK, ESEF_BO], matches=[match(confidence=0.8)])
    assert len(exact.rows) == 1              # at the threshold, not above it


def test_a_split_rule_on_the_ratsit_slot_undoes_the_merge() -> None:
    """apply_rules runs on the LLM-joined sets, so a reviewer keeps the last word (spec 4);
    and the person left behind no longer carries llm_match, because the pair is no longer
    inside one set."""
    rule = PersonRule(C, "rule-1", "split", (), ("r1",))
    result = fold([RATSIT_ERIK, ESEF_BO], rules=[rule], matches=[match()])
    assert names(result) == ["Bo Bengtsson", "Erik Bo Bengtsson"]
    assert result.stale_rules == 0
    for person in result.rows:
        assert "llm_match" not in json.loads(person.data)


def test_the_same_match_on_a_second_fold_changes_nothing() -> None:
    first = fold([RATSIT_ERIK, ESEF_BO], matches=[match()])
    again = refold(first, [RATSIT_ERIK, ESEF_BO], matches=[match()])
    assert (again.created, again.updated, again.unchanged) == (0, 0, 1)
    assert again.history == ()
    assert again.rows[0].person_key == first.rows[0].person_key


def test_the_merge_is_an_update_whose_history_keeps_the_previous_image() -> None:
    """`data` is in _COMPARED, so the added key alone would already mark the row changed --
    here the re-key does it, exactly as a name-driven re-key does."""
    before = fold([RATSIT_ERIK, ESEF_BO])
    after = refold(before, [RATSIT_ERIK, ESEF_BO], matches=[match()])
    assert len(before.rows) == 2
    assert sorted(entry.change_kind for entry in after.history) == ["updated", "withdrawn"]
    surviving = [person for person in after.rows if person.active == 1]
    assert len(surviving) == 1 and len(surviving[0].member_slots) == 2
    # The joined set keeps the fuller name's key, which the Ratsit person already had, so
    # the ESEF person's key is the one that goes.
    assert surviving[0].person_key == person_key(C, ("erik", "bo", "bengtsson"))
    withdrawn = [person for person in after.rows if person.inactive_reason == "withdrawn"]
    assert len(withdrawn) == 1 and withdrawn[0].person_key == person_key(C, ("bo", "bengtsson"))


def test_llm_match_is_fold_owned_and_overwrites_whatever_a_member_carried() -> None:
    """Member `data` never contains the key; if a hand-written row does, the fold's value
    wins, because it is written AFTER merge_member_data."""
    liar = row("esef", "e1", display="Bo Bengtsson", first="bo", last="bengtsson",
               data='{"llm_match":"not mine","section":"signatures"}')
    person = published(fold([RATSIT_ERIK, liar], matches=[match(b=liar)]))
    data = json.loads(person.data)
    assert data["section"] == "signatures"
    assert data["llm_match"]["model"] == "deepseek-v4-flash"


def test_the_birth_year_split_sends_a_paired_row_to_its_partner_not_the_smallest_year() -> None:
    """A year-less row can enter a set through an LLM pair ALONE: it matches no bucket by
    name, so a split that only knows `_matches` drops it on the smallest year -- the wrong
    person -- and `pairs_within` then finds nothing, so the row does not even record the
    match that moved it (fix wave F1)."""
    r1 = row("ratsit", "r1", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson", birth_year=1966)
    r2 = row("ratsit", "r2", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson")
    r3 = row("ratsit", "r3", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson", birth_year=1980)
    b1 = row("bolagsverket", "b1", display="Bo Bengtsson", first="bo", last="bengtsson")
    pair = match(a=r3, b=b1, confidence=0.95, reason="call name")

    # One closure (r2 joins both years by name, b1 joins r3 by the pair), split into two.
    assert len(identity_sets([r1, r2, r3, b1], [pair])) == 2
    result = fold([r1, r2, r3, b1], matches=[pair])
    assert result.sets_split_by_birth_year == 1
    by_slot = {
        slot: person for person in result.rows for slot in person.member_slots
    }
    assert by_slot["b1"] is by_slot["r3"]                  # the partner, not the 1966 person
    assert by_slot["b1"].birth_year == 1980
    assert sorted(by_slot["b1"].member_slots) == ["b1", "r3"]
    assert json.loads(by_slot["b1"].data)["llm_match"]["pairs"] == [
        {"a": "Erik Bo Bengtsson", "b": "Bo Bengtsson", "confidence": 0.95,
         "reason": "call name"}
    ]
    # The 1966 person keeps the year-less row that reached it by NAME, and no llm_match.
    assert sorted(by_slot["r1"].member_slots) == ["r1", "r2"]
    assert by_slot["r1"].birth_year == 1966
    assert "llm_match" not in json.loads(by_slot["r1"].data)


def test_a_paired_row_still_falls_back_to_the_name_relation_and_then_to_the_smallest_year() -> None:
    """The pair relation is tried FIRST, not instead: a year-less row with no pair inside the
    set still attaches by name, and one that reaches neither still lands deterministically."""
    r1 = row("ratsit", "r1", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson", birth_year=1966)
    r2 = row("ratsit", "r2", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson")
    r3 = row("ratsit", "r3", display="Erik Bo Bengtsson", first="erik", middles=("bo",),
             last="bengtsson", birth_year=1980)
    # A pair naming a member that is not in this set changes nothing.
    outsider = row("esef", "e9", display="Bo Bengtsson", first="bo", last="bengtsson")
    result = fold([r1, r2, r3], matches=[match(a=r3, b=outsider)])
    by_slot = {slot: person for person in result.rows for slot in person.member_slots}
    assert sorted(by_slot["r2"].member_slots) == ["r1", "r2"]      # the smallest year, as before
    assert all("llm_match" not in json.loads(person.data) for person in result.rows)


def test_pairs_within_needs_both_sides_and_admits_only_year_compatible_pairs() -> None:
    members = [RATSIT_ERIK, ESEF_BO]
    assert pairs_within(members, [match()]) == (match(),)
    assert pairs_within([RATSIT_ERIK], [match()]) == ()
    other_year = row("esef", "e2", display="Bo Bengtsson", first="bo", last="bengtsson",
                     birth_year=1971)
    assert pairs_within([RATSIT_ERIK, other_year], [match(b=other_year)]) == ()
