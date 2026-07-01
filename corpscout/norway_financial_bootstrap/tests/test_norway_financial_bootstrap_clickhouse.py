from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.clickhouse import (
    CANDIDATE_TABLE,
    CANDIDATE_RUN_TABLE,
    CANDIDATE_RUN_SUMMARY_SQL,
    CREATE_CANDIDATE_TABLE_SQL,
    CREATE_CANDIDATE_RUN_TABLE_SQL,
    GET_CANDIDATE_RUN_METADATA_SQL,
    INSERT_CANDIDATE_RUN_METADATA_SQL,
    INSERT_CANDIDATES_SQL,
    NO_COMPANIES_QUERY,
    SOURCE_CANDIDATE_COUNT_SQL,
    candidate_table_has_run,
    financial_candidates_from_clickhouse,
    get_candidate_from_clickhouse,
    prepare_candidate_table,
)


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(self, query_results=None) -> None:
        self.query_results = list(query_results or [])
        self.queries = []
        self.commands = []

    def query(self, sql, parameters=None):
        self.queries.append((sql, parameters))
        return self.query_results.pop(0)

    def command(self, sql, parameters=None):
        self.commands.append((sql, parameters))


def test_financial_candidates_from_clickhouse_reads_only_org_number() -> None:
    client = FakeClickHouseClient([FakeQueryResult([("923609016",), ("811685852",)])])

    assert financial_candidates_from_clickhouse(client) == [
        FinancialCandidate("923609016"),
        FinancialCandidate("811685852"),
    ]

    assert "toString(org_number) as org_number" in NO_COMPANIES_QUERY
    assert "last_submitted_accounts_year is not null" in NO_COMPANIES_QUERY.lower()
    assert "name" not in NO_COMPANIES_QUERY.lower()
    assert "website" not in NO_COMPANIES_QUERY.lower()


def test_candidate_table_sql_uses_deterministic_round_robin_slots() -> None:
    assert CANDIDATE_TABLE == "norway_financial_bootstrap_candidates"
    assert CANDIDATE_RUN_TABLE == "norway_financial_bootstrap_candidate_runs"
    assert "ENGINE = MergeTree" in CREATE_CANDIDATE_TABLE_SQL
    assert "ORDER BY (run_id, slot, slot_index)" in CREATE_CANDIDATE_TABLE_SQL
    assert "ENGINE = ReplacingMergeTree(prepared_at)" in CREATE_CANDIDATE_RUN_TABLE_SQL
    assert "rn % {slot_count:UInt8}" in INSERT_CANDIDATES_SQL
    assert "intDiv(rn, {slot_count:UInt8})" in INSERT_CANDIDATES_SQL
    assert "row_number() OVER (ORDER BY org_number) - 1 AS rn" in INSERT_CANDIDATES_SQL
    assert "SELECT DISTINCT toString(org_number) AS org_number" in INSERT_CANDIDATES_SQL
    assert "SELECT count()" in SOURCE_CANDIDATE_COUNT_SQL
    assert "uniqExact(org_number)" in CANDIDATE_RUN_SUMMARY_SQL
    assert "slot_count, candidate_count" in GET_CANDIDATE_RUN_METADATA_SQL
    assert "candidate_count" in INSERT_CANDIDATE_RUN_METADATA_SQL


def test_prepare_candidate_table_inserts_only_when_run_is_missing() -> None:
    client = FakeClickHouseClient(
        [
            FakeQueryResult([]),
            FakeQueryResult([(0, 0, 0)]),
            FakeQueryResult([(2,)]),
            FakeQueryResult([(2, 2, 2)]),
        ]
    )

    prepared = prepare_candidate_table(client, run_id="run-1", slot_count=4)

    assert prepared.inserted is True
    assert prepared.existing_count == 0
    assert client.commands[0][0] == CREATE_CANDIDATE_TABLE_SQL
    assert client.commands[1][0] == CREATE_CANDIDATE_RUN_TABLE_SQL
    assert client.commands[2][0] == INSERT_CANDIDATES_SQL
    assert client.commands[2][1] == {"run_id": "run-1", "slot_count": 4}
    assert client.commands[3][0] == INSERT_CANDIDATE_RUN_METADATA_SQL
    assert client.commands[3][1] == {
        "run_id": "run-1",
        "slot_count": 4,
        "candidate_count": 2,
    }


def test_prepare_candidate_table_reuses_existing_run() -> None:
    client = FakeClickHouseClient(
        [
            FakeQueryResult([(4, 12)]),
            FakeQueryResult([(12, 12, 4)]),
        ]
    )

    prepared = prepare_candidate_table(client, run_id="run-1", slot_count=4)

    assert prepared.inserted is False
    assert prepared.existing_count == 12
    assert prepared.candidate_count == 12
    assert prepared.stored_slot_count == 4
    assert client.commands == [
        (CREATE_CANDIDATE_TABLE_SQL, None),
        (CREATE_CANDIDATE_RUN_TABLE_SQL, None),
    ]


def test_prepare_candidate_table_fails_on_partial_existing_freeze() -> None:
    client = FakeClickHouseClient(
        [
            FakeQueryResult([]),
            FakeQueryResult([(8, 8, 4)]),
            FakeQueryResult([(12,)]),
        ]
    )

    try:
        prepare_candidate_table(client, run_id="run-1", slot_count=4)
    except RuntimeError as exc:
        assert "stored_candidates=8 expected_candidates=12" in str(exc)
    else:
        raise AssertionError("partial candidate freeze must fail")


def test_prepare_candidate_table_fails_on_slot_count_mismatch() -> None:
    client = FakeClickHouseClient(
        [
            FakeQueryResult([(8, 12)]),
            FakeQueryResult([(12, 12, 8)]),
        ]
    )

    try:
        prepare_candidate_table(client, run_id="run-1", slot_count=4)
    except RuntimeError as exc:
        assert "prepared_slot_count=8 requested_slot_count=4" in str(exc)
    else:
        raise AssertionError("candidate slot-count mismatch must fail")


def test_prepare_candidate_table_fails_on_duplicate_existing_candidates() -> None:
    client = FakeClickHouseClient(
        [
            FakeQueryResult([(4, 12)]),
            FakeQueryResult([(12, 11, 4)]),
        ]
    )

    try:
        prepare_candidate_table(client, run_id="run-1", slot_count=4)
    except RuntimeError as exc:
        assert "duplicate orgs" in str(exc)
    else:
        raise AssertionError("duplicate candidates must fail")


def test_get_candidate_from_clickhouse_reads_slot_index() -> None:
    client = FakeClickHouseClient([FakeQueryResult([("923609016",)])])

    candidate = get_candidate_from_clickhouse(
        client, run_id="run-1", slot_id=2, slot_index=9
    )

    assert candidate == FinancialCandidate("923609016")
    sql, params = client.queries[0]
    assert "WHERE run_id = {run_id:String}" in sql
    assert "slot = {slot:UInt8}" in sql
    assert "slot_index = {slot_index:UInt64}" in sql
    assert params == {"run_id": "run-1", "slot": 2, "slot_index": 9}


def test_get_candidate_from_clickhouse_returns_none_when_missing() -> None:
    client = FakeClickHouseClient([FakeQueryResult([])])

    assert (
        get_candidate_from_clickhouse(client, run_id="run-1", slot_id=0, slot_index=99)
        is None
    )


def test_candidate_table_has_run_checks_existing_count() -> None:
    client = FakeClickHouseClient([FakeQueryResult([(3,)])])

    assert candidate_table_has_run(client, run_id="run-1") == 3
