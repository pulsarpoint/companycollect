"""The store's ONE read rule, pinned on both sides.

The rule lives twice by necessity -- once as SQL for the two ClickHouse consumers, once as
Python for the demand scan that has to reason about outcomes in memory. Both halves are
generated from the same constants and both are pinned here, because a divergence between
them is invisible at runtime: the SQL would serve one coordinate and the demand scan would
believe another, and neither would raise.
"""
import re
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.sweden_company import shared_address_geocoding
from dagster_v3.defs.sweden_company.geocode_store import (
    CURRENT_OUTCOME_CHOICE_RANK_SQL,
    GEOCODED_STATUSES,
    IS_ADOPTED_SQL,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    NEWEST_PER_FAMILY_RANK_SQL,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    RANK_INPUT_COLUMNS,
    RESOLVER_ONLY_FILTER_SQL,
    SERVING_COLUMNS,
    STORE_COLUMNS,
    STORE_KEY_COLUMNS,
    VALID_STATUSES,
    StoredOutcome,
    build_current_geocodes_sql,
    build_current_resolver_geocodes_sql,
    choice_rank,
    current_adopted_outcome,
    current_outcome,
    current_outcomes_by_address,
    current_resolver_outcome,
    current_resolver_outcomes_by_address,
    is_adopted,
    is_geocoded,
)

POLICY = "se-address-resolution-policy-v5"
OLD_POLICY = "se-address-resolution-policy-v4"
MD5_A, MD5_B = "aaaaaaaa", "bbbbbbbb"
T1 = datetime(2026, 8, 1, tzinfo=UTC)
T2 = datetime(2026, 8, 8, tzinfo=UTC)
T3 = datetime(2026, 8, 15, tzinfo=UTC)
ADDRESS = "f" * 64
ADOPTED = LEGACY_ADOPTED_POLICY_VERSION


def _outcome(policy: str, md5: str, status: str, matched_at: datetime,
             address_id: str = ADDRESS) -> StoredOutcome:
    return StoredOutcome(address_id=address_id, policy_version=policy, reference_md5=md5,
                         match_status=status, matched_at=matched_at)


def test_the_store_columns_are_the_serving_columns_plus_the_two_version_columns() -> None:
    assert STORE_KEY_COLUMNS == ("address_id", "policy_version", "reference_md5")
    assert STORE_COLUMNS[:3] == STORE_KEY_COLUMNS
    assert SERVING_COLUMNS == tuple(
        column for column in STORE_COLUMNS if column not in ("policy_version", "reference_md5"))
    # The serving contract is not re-typed here: it IS the shipped export list.
    assert SERVING_COLUMNS == shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS
    # Everything the two ranks read must be projectable even when the caller did not ask
    # for it -- SERVING_COLUMNS omits both version columns, and the choice rank needs them.
    assert set(RANK_INPUT_COLUMNS) == {
        "address_id", "policy_version", "reference_md5", "match_status", "matched_at"}
    assert set(RANK_INPUT_COLUMNS) <= set(STORE_COLUMNS)


def test_the_module_agrees_with_the_canonicalization_module_on_names() -> None:
    """Two literals, one meaning -- geocode_store spells them itself to stay import-light."""
    from dagster_v3.defs.sweden_company import address_canonicalization
    from dagster_v3.defs.sweden_company import geocode_store

    assert geocode_store.CLICKHOUSE_DATABASE == address_canonicalization.CLICKHOUSE_DATABASE
    assert geocode_store.ENRICHMENT_SCHEMA == address_canonicalization.ENRICHMENT_SCHEMA


def test_the_taxonomy_has_one_home() -> None:
    """address_resolution_promotion imported these; re-declaring them anywhere would let a
    status be geocoded on one side of the pipeline and not on the other."""
    from dagster_v3.defs.sweden_company import address_resolution_promotion

    assert address_resolution_promotion.GEOCODED_STATUSES is GEOCODED_STATUSES
    assert address_resolution_promotion.VALID_STATUSES is VALID_STATUSES
    assert set(GEOCODED_STATUSES) < set(VALID_STATUSES)
    assert len(VALID_STATUSES) == 11
    assert LEGACY_ADOPTED_POLICY_VERSION not in VALID_STATUSES
    assert LEGACY_ADOPTED_MATCH_METHOD == "legacy_adopted"


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_is_geocoded_agrees_with_the_geocoded_tuple(status: str) -> None:
    assert is_geocoded(status) == (status in GEOCODED_STATUSES)


def test_the_two_rank_expressions_spell_their_components_in_order() -> None:
    """Parsed out of the expressions, not substring-matched.

    Reordering `servable` and `matched_at` in the choice rank is the mutation that matters:
    it would make a newer resolver `ambiguous` outrank an adopted exact -- exactly the
    regression the import exists to prevent, and one no downstream assertion would catch.
    """
    assert NEWEST_PER_FAMILY_RANK_SQL == "tuple(matched_at, reference_md5, policy_version)"
    components = [line.strip().rstrip(",")
                  for line in CURRENT_OUTCOME_CHOICE_RANK_SQL.splitlines()[1:]]
    assert components == [
        f"toUInt8(is_adopted = 1 OR match_status IN ({', '.join(repr(s) for s in GEOCODED_STATUSES)}))",
        "matched_at",
        "1 - is_adopted",
        "reference_md5",
        "policy_version)",
    ]
    assert IS_ADOPTED_SQL == f"toUInt8(policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}')"
    assert RESOLVER_ONLY_FILTER_SQL == f"policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'"


def test_the_read_runs_both_stages_and_keeps_one_row_per_identity() -> None:
    sql = build_current_geocodes_sql()
    # Stage 1: newest per (identity, matcher family).
    assert f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}\n" in sql
    assert f"{IS_ADOPTED_SQL} AS is_adopted" in sql
    assert f"ORDER BY address_id, is_adopted, {NEWEST_PER_FAMILY_RANK_SQL} DESC" in sql
    assert "LIMIT 1 BY address_id, is_adopted" in sql
    # Stage 2: choose between the at most two survivors.
    assert ") AS candidates\n" in sql
    assert f"ORDER BY address_id, {CURRENT_OUTCOME_CHOICE_RANK_SQL} DESC" in sql
    assert sql.rstrip().endswith("LIMIT 1 BY address_id")
    assert sql.index("LIMIT 1 BY address_id, is_adopted") < sql.index(") AS candidates")
    # FINAL is deliberately absent -- see the module docstring.
    assert "FINAL" not in sql
    # The outer projection IS the requested column list, in order, and nothing else.
    assert re.findall(r"^    (\w+),?$", sql[: sql.index("\nFROM (")], re.MULTILINE) == list(STORE_COLUMNS)


def test_the_read_projects_the_rank_inputs_even_when_they_were_not_requested() -> None:
    """SERVING_COLUMNS has no policy_version and no reference_md5, and the derived
    `_current` table asks for exactly those 26 columns. If the inner SELECT projected only
    what was asked for, the outer ORDER BY would reference columns that do not exist and the
    derivation would fail at run time on the host, not here."""
    sql = build_current_geocodes_sql(columns=SERVING_COLUMNS)
    inner = sql[sql.index("FROM (") : sql.index(") AS candidates")]
    for column in RANK_INPUT_COLUMNS:
        assert re.search(rf"^        {column},?$", inner, re.MULTILINE), column
    outer = sql[: sql.index("\nFROM (")]
    assert "policy_version" not in outer and "reference_md5" not in outer


def test_the_read_filters_before_ranking() -> None:
    sql = build_current_geocodes_sql(
        columns=("address_id", "match_status"),
        address_filter_sql="address_id IN (SELECT address_id FROM corpscout.se_company_address_links_current)")
    # The filter sits in the INNER query: it prunes on the sorting key's leading column, so
    # a page-sized read touches parts, not all 2.09M identities. Filtering the ranked result
    # would be correct and would pay for the whole store on every page.
    assert sql.index("WHERE address_id IN (") < sql.index("ORDER BY address_id, is_adopted")


def test_the_resolver_only_read_is_stage_one_over_the_resolver_family() -> None:
    """What the demand scan loads. It is NOT the served answer: an identity whose served
    answer is an adopted exact still has a resolver `ambiguous`, and that ambiguous is what
    decides whether the identity is due for a rematch."""
    sql = build_current_resolver_geocodes_sql(columns=("address_id", "match_status"))
    assert f"WHERE {RESOLVER_ONLY_FILTER_SQL}" in sql
    assert f"ORDER BY address_id, {NEWEST_PER_FAMILY_RANK_SQL} DESC" in sql
    assert sql.rstrip().endswith("LIMIT 1 BY address_id")
    # One stage only -- no candidates subquery, no servable component.
    assert "candidates" not in sql and "is_adopted" not in sql
    filtered = build_current_resolver_geocodes_sql(address_filter_sql="address_id = 'x'")
    assert f"WHERE address_id = 'x'\n  AND {RESOLVER_ONLY_FILTER_SQL}" in filtered


@pytest.mark.parametrize(
    ("name", "outcomes", "expected"),
    [
        ("one outcome wins by default", [_outcome(POLICY, MD5_A, "ambiguous", T1)], (POLICY, MD5_A)),
        # Rule (a) within the resolver family: newest wins, geocoded or not. A reference
        # update that turns an exact into an ambiguous IS honoured -- the identity then sits
        # in the retry pool, which is where it belongs.
        ("a newer resolver outcome replaces an older one",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_exact", T2)],
         (POLICY, MD5_B)),
        ("a newer resolver ambiguous replaces an older resolver exact",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (POLICY, MD5_B)),
        # Rule (b): the adopted row survives a newer resolver non-answer.
        ("an adopted exact outranks a newer resolver ambiguous",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (ADOPTED, MD5_A)),
        ("an adopted exact outranks a newer resolver unmatched",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "unmatched", T2)],
         (ADOPTED, MD5_A)),
        # ... and yields to a newer resolver answer.
        ("a newer geocoded resolver outcome outranks an adopted row",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_site", T2)],
         (POLICY, MD5_B)),
        ("an older geocoded resolver outcome does NOT outrank an adopted row",
         [_outcome(ADOPTED, MD5_B, "matched_exact", T2), _outcome(POLICY, MD5_A, "matched_exact", T1)],
         (ADOPTED, MD5_B)),
        ("at an exact tie the resolver wins -- 'same-or-newer'",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_A, "matched_exact", T1)],
         (POLICY, MD5_A)),
        # The three-row state that a flat rank cannot order: the adopted row is protected
        # from the newest ambiguous, and among resolver rows the newest still wins -- so the
        # served answer is the adopted exact, not either resolver row.
        ("adopted, a newer resolver exact and a newest resolver ambiguous",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1),
          _outcome(POLICY, MD5_A, "matched_exact", T2),
          _outcome(POLICY, MD5_B, "ambiguous", T3)],
         (ADOPTED, MD5_A)),
        ("same three rows with the adopted row newest",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T3),
          _outcome(POLICY, MD5_A, "matched_exact", T1),
          _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (ADOPTED, MD5_A)),
        # Same instants, different references: stable, not merge-order-dependent.
        ("equal instants in one family break on reference_md5",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_exact", T1)],
         (POLICY, MD5_B)),
    ],
)
def test_current_outcome_ranks_the_way_the_rule_says(
    name: str, outcomes: list[StoredOutcome], expected: tuple[str, str]
) -> None:
    chosen = current_outcome(outcomes)
    assert chosen is not None, name
    assert (chosen.policy_version, chosen.reference_md5) == expected, name
    # Order of arrival never decides.
    reversed_choice = current_outcome(list(reversed(outcomes)))
    assert reversed_choice == chosen, name


def test_the_resolver_view_ignores_adopted_rows_entirely() -> None:
    """What the demand scan reads. An adopted row must never make an identity look matched
    or make it look due for a rematch -- it is not a resolver answer at all."""
    outcomes = [_outcome(ADOPTED, MD5_A, "matched_exact", T3),
                _outcome(POLICY, MD5_A, "ambiguous", T1)]
    resolver = current_resolver_outcome(outcomes)
    assert resolver is not None
    assert resolver.policy_version == POLICY and resolver.match_status == "ambiguous"
    adopted = current_adopted_outcome(outcomes)
    assert adopted is not None and adopted.policy_version == ADOPTED
    assert current_resolver_outcome([_outcome(ADOPTED, MD5_A, "matched_exact", T1)]) is None
    assert current_adopted_outcome([_outcome(POLICY, MD5_A, "ambiguous", T1)]) is None
    assert is_adopted(_outcome(ADOPTED, MD5_A, "matched_exact", T1))
    assert not is_adopted(_outcome(POLICY, MD5_A, "matched_exact", T1))


def test_choice_rank_components_are_the_five_the_rule_names() -> None:
    """Named separately from the table above so a reviewer mutating one component sees a
    direct failure rather than a scenario name."""
    assert choice_rank(_outcome(POLICY, MD5_A, "matched_exact", T1)) == (1, T1, 1, MD5_A, POLICY)
    assert choice_rank(_outcome(POLICY, MD5_A, "ambiguous", T1)) == (0, T1, 1, MD5_A, POLICY)
    assert choice_rank(_outcome(ADOPTED, MD5_A, "matched_exact", T1)) == (1, T1, 0, MD5_A, ADOPTED)
    # An adopted row is servable even if some future import stamps a non-exact status on it.
    assert choice_rank(_outcome(ADOPTED, MD5_A, "unmatched", T1))[0] == 1


def test_outcomes_are_grouped_by_identity() -> None:
    other = "e" * 64
    rows = [
        _outcome(POLICY, MD5_A, "ambiguous", T1),
        _outcome(POLICY, MD5_A, "matched_exact", T1, address_id=other),
        _outcome(POLICY, MD5_B, "matched_exact", T2),
        _outcome(ADOPTED, MD5_A, "matched_exact", T3, address_id=other),
    ]
    grouped = current_outcomes_by_address(rows)
    assert set(grouped) == {ADDRESS, other}
    assert grouped[ADDRESS].reference_md5 == MD5_B
    assert grouped[other].policy_version == ADOPTED
    resolver_only = current_resolver_outcomes_by_address(rows)
    assert set(resolver_only) == {ADDRESS, other}
    assert resolver_only[other].policy_version == POLICY


def test_current_outcome_is_none_for_an_identity_with_no_rows() -> None:
    assert current_outcome([]) is None
    assert current_outcomes_by_address([]) == {}
    assert current_resolver_outcomes_by_address([]) == {}
