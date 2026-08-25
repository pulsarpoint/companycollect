"""The chunked canonical-address load reads the same rows the single query did -- on a real engine.

``address_canonicalization`` no longer streams every current company-address observation
(4.67M on prod) over one unbounded ``execute_iter``; that read is the twin of the demand scan's
that RESET (Errno 104) once and HUNG 150 minutes. It walks the result in
``(company_id, address_key, address_type, source)`` keyset pages instead. The load-bearing
claim is that HOW it paginates changed and WHAT it returns did not: the union of the pages is
row-for-row the single-query read, with nothing dropped or repeated at a page boundary.

A substring test cannot prove that -- the keyset tuple comparison
``(company_id, toString(address_fingerprint), address_type, source) > (...)``, the aliased
ORDER BY and the outer ``LIMIT`` are ClickHouse semantics DuckDB will not run. So this executes
both reads in a disposable clickhouse-local over a fixture that deliberately includes a
FINGERPRINT COLLISION: two observations of one company sharing an ``address_fingerprint`` under
different ``(address_type, source)`` pairs -- the case where a bare ``(company_id, address_key)``
cursor would silently drop the second at a page boundary.
"""

import subprocess

import pytest

from dagster_v3.defs.sweden_company import address_canonicalization
from dagster_v3.defs.sweden_company.address_canonicalization import (
    _CURRENT_COMPANY_ADDRESSES_SELECT,
    _company_addresses_page_sql,
)
from tests.test_se_company_address_clickhouse_local import (
    MIGRATIONS,
    _schema_statements,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _render,
)

pytestmark = pytest.mark.integration

# Page size one so every boundary is exercised, including the one that falls between the two
# collision rows. Reaches the SQL through _company_addresses_page_sql's `LIMIT {QUERY_BATCH_SIZE}`.
PAGE_SIZE = 1

_COMPANY_A = "5560000001"
_COMPANY_B = "5560000002"
_FP_A, _FP_B = "a" * 64, "b" * 64

# The three current rows the read returns, in the read's own
# (company_id, address_key, address_type, source) order. Rows 0 and 1 are the collision:
# same company, same fingerprint (hence same address_key), different (address_type, source).
_R0 = (_COMPANY_A, _FP_A, "postal", "bolagsverket")
_R1 = (_COMPANY_A, _FP_A, "visiting", "scb")
_R2 = (_COMPANY_B, _FP_B, "postal", "bolagsverket")
# The cursor before each page: None, then the previous page's last four-tuple. At page size one
# the collision row (_R1) is reached only because the cursor is the FULL tuple -- a
# (company_id, address_key) cursor would not be strictly greater than _R0 and would drop it.
_CURSORS = (None, _R0, _R1, _R2)

_BASELINE = "baseline"


def _fixture_insert() -> str:
    """Three current observations with a fingerprint collision, plus a has_address=0 row that
    the read's WHERE must exclude (so a naive 'select everything' would fail parity)."""
    seed = "toDateTime64('2026-08-01 00:00:00.000', 3, 'UTC')"

    def row(
        company: str,
        address_type: str,
        source: str,
        fingerprint: str,
        *,
        has_address: int,
    ) -> str:
        return (
            f"('{company}', '{address_type}', '{source}', 'Storgatan 1, 111 22 Stockholm',"
            f" 'Storgatan 1', NULL, '111 22', 'Stockholm', 'SE', 'fixture',"
            f" 'rec-{company}-{source}', 'hash-{company}-{source}', 'uid-{company}-{source}',"
            f" {seed}, {has_address}, '{fingerprint}', '{fingerprint}', {seed}, 1)"
        )

    values = ",\n    ".join(
        (
            row(_COMPANY_A, "postal", "bolagsverket", _FP_A, has_address=1),
            row(_COMPANY_A, "visiting", "scb", _FP_A, has_address=1),
            row(_COMPANY_B, "postal", "bolagsverket", _FP_B, has_address=1),
            # Excluded by the read (has_address = 0); present to prove the WHERE still gates.
            row(_COMPANY_B, "visiting", "scb", "c" * 64, has_address=0),
        )
    )
    return (
        "INSERT INTO corpscout.se_company_addresses_current\n"
        "    (company_id, address_type, source, raw_address, street_address, care_of,"
        " postal_code, post_town, country_code, source_run_id, source_record_id,"
        " source_payload_hash, source_record_uid, updated_from_raw_at, has_address,"
        " address_fingerprint, observation_fingerprint, observed_at, has_observation)\n"
        f"VALUES\n    {values};"
    )


def _baseline_sql() -> str:
    """The single-query read this module replaced, wrapped so its order is the pages' order."""
    return (
        f"SELECT * FROM (\n{_CURRENT_COMPANY_ADDRESSES_SELECT.rstrip()}\n) AS baseline\n"
        "ORDER BY company_id, address_key, address_type, address_source"
    )


def _page_sql(cursor: tuple[str, str, str, str] | None) -> str:
    """One keyset page exactly as the loader issues it, with the cursor inlined for the CLI."""
    sql = _company_addresses_page_sql(has_cursor=cursor is not None)
    if cursor is None:
        return sql
    return _render(
        sql,
        {
            "after_company_id": cursor[0],
            "after_address_key": cursor[1],
            "after_address_type": cursor[2],
            "after_address_source": cursor[3],
        },
    )


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


def _script() -> str:
    parts = [f"{statement};" for statement in _schema_statements(MIGRATIONS)]
    parts.append(_fixture_insert())
    parts.append(_marked(_BASELINE, _baseline_sql()))
    for index, cursor in enumerate(_CURSORS):
        parts.append(_marked(f"page_{index}", _page_sql(cursor)))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    # PAGE_SIZE reaches the SQL through _company_addresses_page_sql's LIMIT {QUERY_BATCH_SIZE}.
    original = address_canonicalization.QUERY_BATCH_SIZE
    address_canonicalization.QUERY_BATCH_SIZE = PAGE_SIZE
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
        address_canonicalization.QUERY_BATCH_SIZE = original
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
    pages = [sections[f"page_{index}"] for index in range(len(_CURSORS))]

    # Page sizes: three single-row pages then an empty page that ends the walk.
    assert [len(page) for page in pages] == [1, 1, 1, 0]

    union = [row for page in pages for row in page]
    # Row-for-row parity with the read this module replaced -- same rows, same order.
    assert union == baseline
    # Exactly the three has_address=1 rows: the WHERE excluded the has_address=0 row.
    assert len(baseline) == 3
    # The boundaries themselves: the fingerprint-collision pair both survived, none repeated.
    keys = [(row[0], row[1], row[2], row[3]) for row in union]
    assert keys == [_R0, _R1, _R2]
    assert len(keys) == len(set(keys))
