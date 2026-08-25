"""The chunked demand load reads the same rows the single query did -- on a real engine.

geocode_demand no longer streams the store's current resolver outcome for all ~2.09M
identities over one execute_iter (that read RESET once and HUNG 150 minutes). It walks the
result in address_id keyset pages instead. The load-bearing claim is that HOW it paginates
changed and WHAT it returns did not: the union of the pages is row-for-row the single-query
resolver read, with nothing dropped or repeated at a page boundary.

A substring test cannot prove that -- `LIMIT 1 BY`, the keyset `address_id >` bound and the
outer page `LIMIT` are ClickHouse semantics DuckDB will not run. So this executes both reads
against the store harness's own fixture (superseded resolver versions in RETRIED/REGRESSED, a
resolver-plus-adopted family in ADOPTED/RECLAIMED/TIED) in a disposable clickhouse-local and
compares them.
"""

import subprocess

import pytest

from dagster_v3.defs.sweden_company import geocode_demand
from dagster_v3.defs.sweden_company.geocode_demand import (
    PREVIOUS_OUTCOME_COLUMNS,
    _outcome_page_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    build_current_resolver_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _render,
)
from tests.test_sweden_geocode_store_clickhouse_local import (
    DEMOTED_STREET,
    FIXTURE_STORE_ROWS,
    REGRESSED,
    RETIRED,
    STORE,
    STORE_COLUMNS,
    _insert,
    _schema_statements,
)

pytestmark = pytest.mark.integration

# All ten stored identities carry a resolver row, so the resolver read returns ten, sorted
# '1'*64 .. '9'*64 then 'b'*64 (CHURNED, 'a'*64, is unstored). At page size three the pages
# are 3+3+3+1 and the cursor after each full page is its last identity -- the 3rd, 6th and
# 9th in that order. A wrong cursor would drop or repeat an identity and the union would stop
# matching the baseline, so these are checked BY the parity assertion, not merely trusted.
PAGE_SIZE = 3
CURSORS = (None, REGRESSED, DEMOTED_STREET, RETIRED)

_BASELINE = "baseline"


def _baseline_sql() -> str:
    """The single-query resolver read this module replaced, wrapped so its order is ours."""
    inner = build_current_resolver_geocodes_sql(columns=PREVIOUS_OUTCOME_COLUMNS)
    projection = ", ".join(PREVIOUS_OUTCOME_COLUMNS)
    return f"SELECT {projection} FROM (\n{inner}\n) AS baseline ORDER BY address_id"


def _page_sql(cursor: str | None) -> str:
    """One keyset page exactly as the loader issues it, with the cursor inlined for the CLI."""
    sql = _outcome_page_sql(has_cursor=cursor is not None)
    if cursor is None:
        return sql
    return _render(sql, {"after_address_id": cursor})


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


def _script() -> str:
    parts = [f"{statement};" for statement in _schema_statements()]
    # A background merge would collapse ReplacingMergeTree parts and could turn the read into
    # an accident; the store harness stops merges for the same reason.
    parts.append(f"SYSTEM STOP MERGES {STORE};")
    parts.append(_insert(STORE, STORE_COLUMNS, FIXTURE_STORE_ROWS))
    parts.append(_marked(_BASELINE, _baseline_sql()))
    for index, cursor in enumerate(CURSORS):
        parts.append(_marked(f"page_{index}", _page_sql(cursor)))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    # PAGE_SIZE reaches the SQL through _outcome_page_sql's `LIMIT {QUERY_BATCH_SIZE}`.
    original = geocode_demand.QUERY_BATCH_SIZE
    geocode_demand.QUERY_BATCH_SIZE = PAGE_SIZE
    try:
        completed = subprocess.run(
            command,
            input=_script(),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    finally:
        geocode_demand.QUERY_BATCH_SIZE = original
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def test_the_pages_union_to_the_single_query_row_set(
    sections: dict[str, list[list[str]]],
) -> None:
    baseline = sections[_BASELINE]
    pages = [sections[f"page_{index}"] for index in range(len(CURSORS))]

    # Page sizes: the keyset walk fills every page but the last (3+3+3+1 over ten identities).
    assert [len(page) for page in pages] == [3, 3, 3, 1]

    union = [row for page in pages for row in page]
    # Row-for-row parity with the read this module replaced -- same rows, same order.
    assert union == baseline
    # And the boundaries themselves: no identity dropped, none repeated across the four pages.
    identities = [row[0] for row in union]
    assert identities == [row[0] for row in baseline]
    assert len(identities) == len(set(identities)) == len(baseline)


def test_the_baseline_read_excludes_the_adopted_family(
    sections: dict[str, list[list[str]]],
) -> None:
    """Guards the fixture, not the pagination: the read is the RESOLVER view, so no row
    carries the imported legacy_adopted_v1 policy. ADOPTED/RECLAIMED/TIED appear through
    their resolver rows only."""
    policy_column = PREVIOUS_OUTCOME_COLUMNS.index("policy_version")
    for row in sections[_BASELINE]:
        assert row[policy_column] != "legacy_adopted_v1"
