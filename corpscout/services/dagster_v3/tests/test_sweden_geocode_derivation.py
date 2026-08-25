"""The serving table is a projection of the store, and something says so out loud.

The derivation is a stage + INSERT-SELECT + EXCHANGE, copied from
defs/company_financials_latest/assets.py:52-70 -- the house's ClickHouse-native replace.

Three layers, deliberately:

1. String tests pin that the derivation IS geocode_store's fragment and that the parity
   check compares content rather than counts.
2. Fake-client tests drive the REAL asset body and the REAL check body, recording the
   statements each issues -- which is what pins the order (stage, fill, validate, swap)
   and the two refusals that stand between an empty read and a replaced serving table.
3. A `clickhouse-local` harness REPLAYS the statements the asset body just produced
   against the migrations' own DDL, on the deployed ClickHouse version. Nothing is
   hand-copied: `_asset_statements()` is the asset's own output, so the harness cannot
   drift from the asset the way a transcribed script would.

What layer 3 exists to prove is the deploy coupling Task 4 measured. Under demand-driven
matching a normal week promotes only the identities that were due -- three, in this
fixture, out of eight. The old rebuild exported that promotion over the whole serving
table and left it holding three rows. Deriving it from the store instead leaves all eight
served, and the five nobody re-decided keep the `matched_at` their stored outcome already
carried: no weekly restamp, which is what shrinks the `se_company_address` change scan.
"""

import re
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import dagster as dg
import pytest

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    DERIVED_PARITY_SQL,
    SwedenDerivedGeocodesConfig,
    build_derived_current_geocodes_sql,
    sweden_address_geocode_store_derived_parity_check,
    sweden_address_geocodes_clickhouse,
    sweden_address_geocodes_derived_parity_check,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_POLICY_VERSION,
    SERVING_COLUMNS,
    STORE_COLUMNS,
    build_current_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
)

TARGET = "corpscout.se_address_geocodes_current"
STORE = "corpscout.se_address_geocodes"


def test_the_derivation_is_the_versioned_read_and_nothing_else() -> None:
    """No second ranking may exist. If this SELECT ever stops being byte-identical to
    geocode_store's fragment, the serving table and the final would disagree about which
    outcome is current, and both would look internally consistent."""
    sql = build_derived_current_geocodes_sql()
    assert sql == build_current_geocodes_sql(columns=SERVING_COLUMNS)
    # The 26 serving columns, in the order the target declares them.
    assert re.findall(
        r"^    (\w+),?$", sql[: sql.index("\nFROM (")], re.MULTILINE
    ) == list(SERVING_COLUMNS)


def test_the_parity_check_compares_content_and_not_only_counts() -> None:
    """A stale EXCHANGE leaves the previous week's table in place with the RIGHT row count
    and the wrong rows. Counts alone would pass; the checksum is what makes the check able
    to fail."""
    assert "cityHash64" in DERIVED_PARITY_SQL
    assert "UNION ALL" in DERIVED_PARITY_SQL
    assert "'derived'" in DERIVED_PARITY_SQL and "'store'" in DERIVED_PARITY_SQL
    assert ") AS store_read" in DERIVED_PARITY_SQL
    assert build_current_geocodes_sql(columns=SERVING_COLUMNS) in DERIVED_PARITY_SQL
    for column in ("address_id", "match_status", "latitude", "longitude", "matched_at"):
        assert DERIVED_PARITY_SQL.count(column) >= 2, column
    # Nullable columns are read through ifNull on BOTH sides, so the two checksums are
    # comparable under either join_use_nulls setting.
    assert DERIVED_PARITY_SQL.count("ifNull(toString(latitude), '')") == 2


class _FakeClickhouseClient:
    """Records every statement; answers system.tables and the asset's SELECTs by shape."""

    def __init__(
        self,
        *,
        existing_tables: set[str] | None = None,
        staged: tuple[int, int] = (8, 8),
        existing: int = 7,
        parity: tuple[tuple[Any, ...], ...] = (),
    ) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.existing_tables = (
            {"se_address_geocodes", "se_address_geocodes_current"}
            if existing_tables is None
            else existing_tables
        )
        self.staged = staged
        self.existing = existing
        self.parity = parity

    def execute(self, sql: str, params: Any = None) -> list[tuple]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            return [
                (table,) for table in params["tables"] if table in self.existing_tables
            ]
        if "uniqExact(address_id) FROM corpscout._tmp_" in sql:
            return [self.staged]
        if sql == f"SELECT count() FROM {TARGET}":
            return [(self.existing,)]
        if "UNION ALL" in sql:
            return list(self.parity)
        if "GROUP BY match_status" in sql:
            return [("matched_exact", 4), ("unmatched", 4)]
        if "latitude IS NOT NULL" in sql:
            return [(6,)]
        return []

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _ in self.executed if "system.tables" not in sql]


class _FakeResource:
    """Stands in for ClickhouseResource: one live connection."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._connection


def _run_derivation(
    client: _FakeClickhouseClient, *, allow_shrink: bool = False
) -> dg.MaterializeResult:
    return sweden_address_geocodes_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(),
        SwedenDerivedGeocodesConfig(allow_shrink=allow_shrink),
        _FakeResource(client),
    )


def test_the_asset_stages_fills_validates_and_swaps_and_never_writes_the_target() -> (
    None
):
    """The whole point of the stage: the serving table is replaced in one atomic step, so
    a reader never sees it half-filled and a failed fill never empties it. A body that
    TRUNCATEd the target and inserted into it directly would satisfy every row-count
    assertion in this file and still take the table away from its readers mid-run."""
    client = _FakeClickhouseClient()

    result = _run_derivation(client)

    create, insert, staged, existing, exchange, drop, statuses, geolocated = (
        client.statements
    )
    stage = re.fullmatch(rf"CREATE TABLE (\S+) AS {re.escape(TARGET)}", create).group(1)
    assert stage.startswith("corpscout._tmp_se_address_geocodes_current_")
    assert insert.startswith(f"INSERT INTO {stage} ({', '.join(SERVING_COLUMNS)})\n")
    assert insert.endswith(build_derived_current_geocodes_sql())
    assert staged == f"SELECT count(), uniqExact(address_id) FROM {stage}"
    # The shrink guard's own read, and it happens BEFORE the swap: a pre-swap count is
    # the only count that can say what this replace would cost.
    assert existing == f"SELECT count() FROM {TARGET}"
    assert exchange == f"EXCHANGE TABLES {stage} AND {TARGET}"
    assert drop == f"DROP TABLE IF EXISTS {stage}"
    assert TARGET in statuses and TARGET in geolocated
    # Nothing anywhere in the run writes the target directly.
    for statement in client.statements:
        assert not statement.startswith(f"INSERT INTO {TARGET}")
        assert not statement.startswith(f"TRUNCATE TABLE {TARGET}")
        assert f"DROP TABLE IF EXISTS {TARGET}" not in statement
    assert result.metadata["rows"] == 8
    assert result.metadata["geolocated"] == 6
    assert result.metadata["exact_match_rate_percent"] == 50.0
    assert result.metadata["table"] == TARGET

    # A second run stages under a different name -- two runs cannot collide on it.
    other = _FakeClickhouseClient()
    _run_derivation(other)
    assert other.statements[0] != create


def test_the_asset_refuses_to_replace_the_serving_table_with_an_empty_read() -> None:
    """An empty store, or a versioned read that silently stopped matching anything, must
    not become an empty serving table -- that is the outage this refusal prevents. The
    stage is still dropped: the refusal must not leave litter behind for the next run."""
    client = _FakeClickhouseClient(staged=(0, 0))

    with pytest.raises(ValueError, match="0 rows"):
        _run_derivation(client)

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS corpscout._tmp_")


def test_the_asset_refuses_a_stage_holding_two_outcomes_for_one_identity() -> None:
    """`LIMIT 1 BY address_id` is what makes the read a serving table rather than a
    history. Losing it duplicates identities, and every downstream join fans out."""
    client = _FakeClickhouseClient(staged=(9, 8))

    with pytest.raises(ValueError, match="9 rows for 8 identities"):
        _run_derivation(client)

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_the_asset_refuses_a_store_that_is_not_the_whole_store() -> None:
    """The silent outage the two refusals above cannot see.

    A store holding one demand week -- the backfill never ran, the run pointed at the
    wrong cluster, rows were pruned -- stages a perfectly well-formed handful of rows,
    one per identity, and would replace every served identity with them. rows != 0
    passes. rows == identities passes. And the parity check passes too, because BOTH of
    its sides read that same store. The store is append-only, so the derived set can
    only grow: a shrink is a defect, never news.
    """
    client = _FakeClickhouseClient(staged=(3, 3), existing=7)

    with pytest.raises(ValueError, match="less than 50%"):
        _run_derivation(client)

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS corpscout._tmp_")


def test_the_shrink_refusal_is_overridable_only_by_explicit_run_config() -> None:
    """`allow_shrink` exists for an operator who has confirmed a shrink is real. It must
    never default on -- a guard that defaults to off is not a guard."""
    assert SwedenDerivedGeocodesConfig().allow_shrink is False

    client = _FakeClickhouseClient(staged=(3, 3), existing=7)
    result = _run_derivation(client, allow_shrink=True)

    assert result.metadata["rows"] == 3
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def _run_parity_check(client: _FakeClickhouseClient) -> dict[str, Any]:
    result = (
        sweden_address_geocodes_derived_parity_check.node_def.compute_fn.decorated_fn(
            _FakeResource(client)
        )
    )
    # AssetCheckResult normalizes its metadata into MetadataValue wrappers.
    return {
        "passed": result.passed,
        **{key: value.value for key, value in result.metadata.items()},
    }


def test_the_parity_check_passes_when_both_sides_agree() -> None:
    client = _FakeClickhouseClient(
        parity=(("derived", 8, 8, 4242), ("store", 8, 8, 4242))
    )

    result = _run_parity_check(client)

    assert result["passed"]
    assert result["derived_rows"] == 8
    assert result["store_rows"] == 8
    assert result["derived_identities"] == result["store_identities"] == 8
    assert result["content_hashes_agree"] is True
    assert client.statements == [DERIVED_PARITY_SQL]


def test_the_parity_check_is_registered_on_both_sides_of_the_transition() -> None:
    """A check defined and never registered runs nowhere and reports nothing -- the one
    failure mode a check cannot report on itself.

    Two hosts, one query. A check runs with the asset it hangs off, and during the
    transition these two assets move independently: a retried or manually materialized
    store leg does not rebuild the serving table, and the tripwire hosted on the serving
    asset would not fire in that run at all.
    """
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    for asset_name in (
        "sweden_address_geocodes_clickhouse",
        "sweden_address_geocode_store_clickhouse",
    ):
        assert (
            dg.AssetCheckKey(
                asset_key=dg.AssetKey(asset_name),
                name="derived_current_matches_the_store",
            )
            in repo.asset_graph.asset_check_keys
        )


def test_both_parity_hosts_run_the_same_query_and_read_it_the_same_way() -> None:
    """Not a second expression of anything: one function, two hosts. A twin that grew
    its own query would drift, and the drift would be invisible -- both would keep
    passing on their own terms."""
    twin = sweden_address_geocode_store_derived_parity_check
    client = _FakeClickhouseClient(
        parity=(("derived", 8, 8, 1111), ("store", 8, 8, 2222))
    )

    result = twin.node_def.compute_fn.decorated_fn(_FakeResource(client))

    assert client.statements == [DERIVED_PARITY_SQL]
    assert result.passed is False
    assert result.metadata["content_hashes_agree"].value is False


def test_the_parity_check_fails_on_a_stale_table_with_the_right_row_count() -> None:
    """The interesting failure is not arithmetic: it is a stage that never swapped. Last
    week's table has a plausible row count and the wrong rows, so only the content hash
    can see it."""
    client = _FakeClickhouseClient(
        parity=(("derived", 8, 8, 1111), ("store", 8, 8, 2222))
    )

    result = _run_parity_check(client)

    assert not result["passed"]
    assert result["content_hashes_agree"] is False
    assert result["derived_rows"] == result["store_rows"] == 8


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# 000275 creates the serving table, 000277 adds its spread column, 000317 creates the
# store. 000277 also alters a legacy results table this pipeline never reads, so the
# statements are filtered by target table rather than applied wholesale.
MIGRATIONS = (
    "000275_corpscout_se_address_geocodes_current.up.sql",
    "000277_corpscout_se_address_geocode_spread.up.sql",
    "000317_corpscout_se_address_geocodes_store.up.sql",
)
NEEDED_TABLES = frozenset({"se_address_geocodes_current", "se_address_geocodes"})
_TABLE_RE = re.compile(
    r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)",
    re.IGNORECASE,
)

POLICY = "se-address-resolution-policy-v5"
WEEK_0_MD5 = "md5-week-0"
WEEK_1_MD5 = "md5-week-1"
T_WEEK_0 = datetime(2026, 8, 18, 3, 0, tzinfo=UTC)
T_ADOPTED = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
T_WEEK_1 = datetime(2026, 8, 25, 3, 0, tzinfo=UTC)
# Five identities nobody re-decided this week, three that were due. address_id is a
# FixedString(64) fingerprint of normalized address text.
SETTLED = tuple(character * 64 for character in "abcde")
PENDING = tuple(character * 64 for character in "123")
ADOPTED_IDENTITY = SETTLED[4]
NEW_IDENTITY = PENDING[2]

_STORE_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "policy_version": f"'{POLICY}'",
    "reference_md5": f"'{WEEK_0_MD5}'",
    "address_identity_run_id": "'identity-run-1'",
    "normalized_match_key": "'se|storgatan|11122|stockholm'",
    "match_status": "'unmatched'",
    "candidate_count": "0",
    "candidate_record_ids": "[]",
    "candidate_record_urls": "[]",
    "match_method": "''",
    "match_confidence": "0.0",
    "latitude": "NULL",
    "longitude": "NULL",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "''",
    "coordinate_method": "NULL",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "0",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "NULL",
    "source_record_url": "NULL",
    "source_url": "NULL",
    "source_object_key": "NULL",
    "source_md5": f"'{WEEK_0_MD5}'",
    "source_snapshot_at": "NULL",
    "source_retrieved_at": "NULL",
    "geocode_run_id": "'run-week-0'",
    "matched_at": _literal(T_WEEK_0),
}


def _store_row(address_id: str, **overrides: str) -> str:
    row = {**_STORE_DEFAULTS, "address_id": f"'{address_id}'", **overrides}
    assert set(row) == set(STORE_COLUMNS), set(row) ^ set(STORE_COLUMNS)
    return "(" + ", ".join(row[column] for column in STORE_COLUMNS) + ")"


def _geocoded(latitude: float, longitude: float, status: str) -> dict[str, str]:
    return {
        "match_status": f"'{status}'",
        "latitude": str(latitude),
        "longitude": str(longitude),
        "candidate_count": "1",
        "candidate_record_ids": "['osm/way/1']",
        "match_method": "'country_street_house_exact_unique'",
        "match_confidence": "1.0",
        "geocode_precision": "'building'",
        "coordinate_method": "'osm_record'",
    }


def _fixture_rows() -> list[str]:
    week_1 = {
        "reference_md5": f"'{WEEK_1_MD5}'",
        "source_md5": f"'{WEEK_1_MD5}'",
        "geocode_run_id": "'run-week-1'",
        "matched_at": _literal(T_WEEK_1),
    }
    return [
        # Week 0: one resolver outcome for all eight identities except the brand-new one.
        _store_row(SETTLED[0], **_geocoded(59.3, 18.0, "matched_exact")),
        _store_row(SETTLED[1], **_geocoded(57.7, 11.9, "matched_street")),
        _store_row(SETTLED[2]),
        _store_row(SETTLED[3], **_geocoded(55.6, 13.0, "matched_exact")),
        _store_row(ADOPTED_IDENTITY, match_status="'ambiguous'", candidate_count="3"),
        _store_row(PENDING[0]),
        _store_row(PENDING[1]),
        # The retired matcher's decision, imported once. The resolver's own newest
        # outcome for this identity is `ambiguous`, and an ambiguous must not take the
        # coordinate away -- stage 2 of the read rule, exercised through the derivation.
        _store_row(
            ADOPTED_IDENTITY,
            policy_version=f"'{LEGACY_ADOPTED_POLICY_VERSION}'",
            geocode_run_id="'run-adoption'",
            matched_at=_literal(T_ADOPTED),
            **_geocoded(58.4, 15.6, "matched_exact"),
        ),
        # Week 1: only the three identities the demand scan selected.
        _store_row(PENDING[0], **week_1, **_geocoded(56.2, 15.3, "matched_exact")),
        _store_row(PENDING[1], **week_1),
        _store_row(
            NEW_IDENTITY, **week_1, **_geocoded(60.6, 16.8, "matched_corrected")
        ),
    ]


def _asset_statements() -> list[str]:
    """The statements the REAL asset body issues, in order. Not a transcription."""
    client = _FakeClickhouseClient()
    _run_derivation(client)
    return client.statements


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


def _script(*, join_use_nulls: int) -> str:
    create, insert, staged, existing, exchange, drop, statuses, geolocated = (
        _asset_statements()
    )
    serving_columns = ", ".join(SERVING_COLUMNS)
    parts = [f"SET join_use_nulls = {join_use_nulls};"]
    parts.extend(f"{statement};" for statement in _schema_statements())
    parts.append(
        f"INSERT INTO {STORE} ({', '.join(STORE_COLUMNS)}) VALUES\n"
        + ",\n".join(_fixture_rows())
        + ";"
    )
    # Last Tuesday's serving table: the week-0 resolver outcome for the seven identities
    # that existed then. This is the table the derivation has to replace.
    parts.append(
        f"INSERT INTO {TARGET} ({serving_columns})\n"
        f"SELECT {serving_columns} FROM {STORE}\n"
        f"WHERE policy_version = '{POLICY}' AND reference_md5 = '{WEEK_0_MD5}';"
    )
    parts.append(
        _marked("before", f"SELECT count(), uniqExact(address_id) FROM {TARGET}")
    )

    parts.append(f"{create};")
    parts.append(f"{insert};")
    parts.append(_marked("staged", staged))
    parts.append(_marked("existing", existing))
    parts.append(f"{exchange};")
    parts.append(f"{drop};")

    parts.append(
        _marked(
            "served",
            f"SELECT address_id, match_status, geocode_run_id,"
            f" toString(matched_at), ifNull(toString(latitude), '')"
            f" FROM {TARGET} ORDER BY address_id",
        )
    )
    parts.append(_marked("status_counts", statuses))
    parts.append(_marked("geolocated", geolocated))
    parts.append(_marked("parity", DERIVED_PARITY_SQL))

    # The stage that never swapped, built to be as hard to see as it really is: eight
    # rows for eight identities, one of them still carrying last week's answer.
    parts.append(f"CREATE TABLE corpscout.never_swapped AS {TARGET};")
    parts.append(
        f"INSERT INTO corpscout.never_swapped ({serving_columns})\n"
        f"SELECT {serving_columns} FROM {TARGET}"
        f" WHERE address_id != '{PENDING[0]}';"
    )
    parts.append(
        f"INSERT INTO corpscout.never_swapped ({serving_columns})\n"
        f"SELECT {serving_columns} FROM {STORE}\n"
        f"WHERE address_id = '{PENDING[0]}' AND reference_md5 = '{WEEK_0_MD5}';"
    )
    parts.append(f"EXCHANGE TABLES corpscout.never_swapped AND {TARGET};")
    parts.append(_marked("stale_parity", DERIVED_PARITY_SQL))

    # ... and the store as it looks when it is not the whole store: one demand week,
    # against the serving table last Tuesday left behind. EXCHANGE rather than DELETE so
    # the state is there the moment the next statement reads it.
    parts.append(f"CREATE TABLE corpscout.store_pruned AS {STORE};")
    parts.append(
        f"INSERT INTO corpscout.store_pruned"
        f" SELECT * FROM {STORE} WHERE reference_md5 = '{WEEK_1_MD5}';"
    )
    parts.append(f"EXCHANGE TABLES corpscout.store_pruned AND {STORE};")
    parts.append(f"CREATE TABLE corpscout.target_week_0 AS {TARGET};")
    parts.append(
        f"INSERT INTO corpscout.target_week_0 ({serving_columns})\n"
        f"SELECT {serving_columns} FROM corpscout.store_pruned\n"
        f"WHERE policy_version = '{POLICY}' AND reference_md5 = '{WEEK_0_MD5}';"
    )
    parts.append(f"EXCHANGE TABLES corpscout.target_week_0 AND {TARGET};")
    parts.append(f"{create};")
    parts.append(f"{insert};")
    parts.append(_marked("shrink_staged", staged))
    parts.append(_marked("shrink_existing", existing))
    parts.append(f"{drop};")
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(join_use_nulls=request.param),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
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


def _parity(rows: list[list[str]]) -> dict[str, tuple[str, ...]]:
    """The two sides of the check, keyed by label: UNION ALL fixes no branch order."""
    return {row[0]: tuple(row[1:]) for row in rows}


@pytest.mark.integration
def test_a_partial_pending_week_leaves_the_serving_table_full(
    sections: dict[str, list[list[str]]],
) -> None:
    """THE deploy coupling, closed.

    Demand-driven matching promoted three identities this week. The rebuild this task
    replaced exported that promotion over the whole serving table and left it holding
    three rows -- every other Swedish company losing its coordinate on an ordinary
    Tuesday. Derived from the store, all eight identities stay served, and the five
    nobody re-decided keep the instant their stored outcome already carried: the weekly
    restamp of 2.09M unchanged rows is gone with it.
    """
    assert sections["before"] == [["7", "7"]]  # seven identities existed last week
    assert sections["staged"] == [["8", "8"]]

    served = {row[0]: row[1:] for row in sections["served"]}
    assert len(served) == 8
    for identity in SETTLED[:4]:
        assert served[identity][1] == "run-week-0"
        assert served[identity][2] == "2026-08-18 03:00:00.000"
    for identity in PENDING:
        assert served[identity][1] == "run-week-1"
        assert served[identity][2] == "2026-08-25 03:00:00.000"
    # The brand-new identity is served for the first time, and the re-tried identity that
    # is still unmatched is served at this week's instant -- a re-decision is a decision.
    assert served[NEW_IDENTITY][0] == "matched_corrected"
    assert served[PENDING[1]][0] == "unmatched"
    # The read rule reaches the serving table whole: the imported adopted exact holds its
    # coordinate against the resolver's newer `ambiguous`.
    assert served[ADOPTED_IDENTITY][0] == "matched_exact"
    assert served[ADOPTED_IDENTITY][3] == "58.4"
    assert served[ADOPTED_IDENTITY][1] == "run-adoption"

    assert sorted(sections["status_counts"]) == [
        ["matched_corrected", "1"],
        ["matched_exact", "4"],
        ["matched_street", "1"],
        ["unmatched", "2"],
    ]
    assert sections["geolocated"] == [["6"]]


@pytest.mark.integration
def test_the_parity_query_runs_and_agrees_with_the_store_it_derived_from(
    sections: dict[str, list[list[str]]],
) -> None:
    """DERIVED_PARITY_SQL executed on the deployed version, not substring-tested. Its
    aliased-subquery-aggregate shape is the one construct in this task ClickHouse has no
    other reason to parse."""
    parity = _parity(sections["parity"])
    assert set(parity) == {"derived", "store"}
    assert parity["derived"] == parity["store"]
    assert parity["derived"][:2] == ("8", "8")


@pytest.mark.integration
def test_a_store_that_is_not_the_whole_store_refuses_instead_of_serving(
    sections: dict[str, list[list[str]]],
) -> None:
    """The silent outage, end to end: real numbers out of ClickHouse, real refusal out of
    the asset body.

    The store here holds one demand week -- the shape a missing backfill, a wrong cluster
    or a pruning leaves behind. The versioned read over it stages three well-formed rows,
    one per identity, over a serving table holding seven. Both of the asset's own
    refusals pass on those numbers; only the shrink guard stops it, and the message it
    raises is the guard's, not theirs.
    """
    assert sections["shrink_staged"] == [["3", "3"]]
    assert sections["shrink_existing"] == [["7"]]

    [[staged_rows, staged_identities]] = sections["shrink_staged"]
    [[existing_rows]] = sections["shrink_existing"]
    client = _FakeClickhouseClient(
        staged=(int(staged_rows), int(staged_identities)),
        existing=int(existing_rows),
    )

    with pytest.raises(ValueError) as raised:
        _run_derivation(client)

    assert "staged row count 3 is less than 50% of the existing 7 rows" in str(
        raised.value
    )
    assert "0 rows" not in str(raised.value)
    assert "one outcome per identity" not in str(raised.value)
    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


@pytest.mark.integration
def test_the_parity_query_catches_a_stage_that_never_swapped(
    sections: dict[str, list[list[str]]],
) -> None:
    """Last week's table, back in place, with the same row count and the same identities.
    Counts agree; the content hash is the only term that can tell them apart."""
    parity = _parity(sections["stale_parity"])
    assert parity["derived"][0] == parity["store"][0]
    assert parity["derived"][1] == parity["store"][1]
    assert parity["derived"][2] != parity["store"][2]
