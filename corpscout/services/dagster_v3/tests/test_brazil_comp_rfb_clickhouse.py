from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from dagster_v3.contact_extraction import (
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
)
from dagster_v3.defs.brazil_companies.rfb import clickhouse, contacts, tables
from tests.test_brazil_comp_rfb_transforms import _build_company_stage


class FakeClickHouseClient:
    """Records statements/inserts/settings; does not actually execute the
    merge SQL. The merge's own correctness (open/close/re-entry semantics) is
    covered against a real DuckDB engine in test_brazil_comp_rfb_history.py --
    this fake only proves clickhouse.py wires the pieces together correctly:
    which statements run, in what order, and with what settings.
    """

    def __init__(
        self,
        *,
        ledger_rows: list[tuple[str, int, int]] | None = None,
        merge_counts: tuple[int, int, int] = (0, 0, 0),
        published_row_count: int | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []
        self.calls: list[tuple[str, object | None, dict | None]] = []
        # (snapshot_year_month, edges_in_snapshot, socios_part_count)
        self.ledger_rows = list(ledger_rows or [])
        self.merge_counts = merge_counts
        # What `SELECT count() FROM <published table>` reports after
        # EXCHANGE TABLES. Defaults to agreeing with merge_counts[0]
        # (spells_total) so every test that does not care about the
        # post-EXCHANGE verification guard keeps passing it by default;
        # pass a different value to simulate the publish and the ledger
        # diverging.
        self.published_row_count = (
            merge_counts[0] if published_row_count is None else published_row_count
        )

    def execute(
        self,
        sql: str,
        params: object | None = None,
        settings: dict | None = None,
    ):
        self.calls.append((sql, params, settings))
        if "system.tables" in sql:
            return [
                (tables.BR_COMPANIES_TABLE_CH,),
                (tables.BR_ESTABLISHMENTS_TABLE_CH,),
                (tables.BR_COMPANY_RELATIONS_TABLE_CH,),
                (tables.BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH,),
                (tables.BR_COMPANY_CONTACTS_TABLE_CH,),
                (tables.BR_COMPANY_DOMAINS_TABLE_CH,),
                (tables.BR_WEBSITES_TABLE_CH,),
            ]
        if "SELECT snapshot_year_month, edges_in_snapshot, socios_part_count" in sql:
            return list(self.ledger_rows)
        if "countIf(first_seen_snapshot" in sql:
            return [self.merge_counts]
        if sql.strip().startswith("SELECT count() FROM"):
            return [(self.published_row_count,)]
        if isinstance(params, list):
            self.inserts.append((sql, params))
            return None
        self.statements.append(sql)
        return None


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def test_clickhouse_exports_replace_companies_and_establishments(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(companies_path)) as connection:
        company_rows = clickhouse.export_brazil_comp_rfb_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        establishment_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_establishments(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )
        assert (
            connection.execute(
                f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
            ).fetchone()[0]
            == company_rows
        )

    assert company_rows == 2
    assert establishment_rows == 2
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 2
    )
    assert len(fake_client.inserts) == 2
    company_insert_sql, company_insert_rows = fake_client.inserts[0]
    establishment_insert_sql, establishment_insert_rows = fake_client.inserts[1]
    assert tables.BR_COMPANIES_TABLE_CH in company_insert_sql
    assert tables.BR_ESTABLISHMENTS_TABLE_CH in establishment_insert_sql
    assert len(company_insert_rows[0]) == len(tables.BR_COMPANIES_EXPORT_COLUMNS)
    assert len(establishment_insert_rows[0]) == len(
        tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS
    )


def test_clickhouse_company_exports_null_dates_outside_date32_range(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    with duckdb.connect(str(companies_path)) as connection:
        connection.execute(
            f"""
            update {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            set status_date = date '1893-07-05',
                activity_start_date = date '1893-07-05'
            where cnpj_basico = '12345678'
            """
        )
        connection.execute(
            f"""
            update {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            set status_date = date '1893-07-05',
                activity_start_date = date '1893-07-05'
            where cnpj = '12345678000190'
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        clickhouse.export_brazil_comp_rfb_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        clickhouse.export_brazil_comp_rfb_clickhouse_establishments(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )

    company_rows = fake_client.inserts[0][1]
    establishment_rows = fake_client.inserts[1][1]
    company_status_date_index = tables.BR_COMPANIES_EXPORT_COLUMNS.index("status_date")
    company_activity_date_index = tables.BR_COMPANIES_EXPORT_COLUMNS.index(
        "activity_start_date"
    )
    establishment_status_date_index = tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS.index(
        "status_date"
    )
    establishment_activity_date_index = tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS.index(
        "activity_start_date"
    )

    assert company_rows[0][company_status_date_index] is None
    assert company_rows[0][company_activity_date_index] is None
    assert establishment_rows[0][establishment_status_date_index] is None
    assert establishment_rows[0][establishment_activity_date_index] is None


def _build_relations_stage(tmp_path: Path, values_sql: str) -> Path:
    relations_path = tmp_path / "br_company_relations.duckdb"
    with duckdb.connect(str(relations_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_RELATIONS_TABLE} as
            select * from (values {values_sql}
            ) as t(country_iso2, source_slug, cnpj_basico, related_entity_kind,
                   related_tax_id, relation_code, relation_since_key, related_name,
                   related_country, age_band, representative_tax_id,
                   representative_name, representative_code, relation_since,
                   resolved_at)
            """
        )
    return relations_path


def _build_manifest_stage(
    tmp_path: Path, *, socios_part_count: int, other_family_rows: int = 2
) -> Path:
    """A manifest.duckdb stage carrying `socios_part_count` socios rows plus a
    couple of another family's rows -- so a count that (incorrectly) ignores
    the `family` filter would fail these tests instead of passing by
    accident."""
    manifest_path = tmp_path / "manifest.duckdb"
    with duckdb.connect(str(manifest_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} (
                family varchar, archive_url varchar, archive_name varchar,
                archive_sha256 varchar, csv_member_name varchar,
                csv_path varchar, source_run_id varchar, retrieved_at timestamp
            )
            """
        )
        rows = [
            (
                "socios",
                f"https://example.org/socios{i}.zip",
                f"socios{i}.zip",
                "deadbeef",
                f"K3241.K03200Y{i}.D40412.SOCIOCSV",
                f"/tmp/socios{i}.csv",
                "run-1",
                None,
            )
            for i in range(socios_part_count)
        ] + [
            (
                "empresas",
                f"https://example.org/empresas{i}.zip",
                f"empresas{i}.zip",
                "deadbeef",
                f"K3241.K03200Y{i}.D40412.EMPRECSV",
                f"/tmp/empresas{i}.csv",
                "run-1",
                None,
            )
            for i in range(other_family_rows)
        ]
        connection.executemany(
            f"insert into {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} "
            "values (?,?,?,?,?,?,?,?)",
            rows,
        )
    return manifest_path


def _build_empty_relations_stage(tmp_path: Path) -> Path:
    """A relations stage with the right schema but zero rows -- MINOR 3
    (empty-snapshot refusal) needs a 0-row snapshot that still reaches the
    export function, which `_build_relations_stage`'s VALUES-list approach
    cannot express."""
    relations_path = tmp_path / "br_company_relations_empty.duckdb"
    with duckdb.connect(str(relations_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_RELATIONS_TABLE} (
                country_iso2 varchar, source_slug varchar, cnpj_basico varchar,
                related_entity_kind varchar, related_tax_id varchar,
                relation_code varchar, relation_since_key varchar,
                related_name varchar, related_country varchar, age_band varchar,
                representative_tax_id varchar, representative_name varchar,
                representative_code varchar, relation_since date,
                resolved_at timestamp
            )
            """
        )
    return relations_path


_TWO_EDGES_VALUES_SQL = """
    ('BR', 'brazil_rfb', '12345678', '1', '11111111000191', '22',
     '20100501', 'PARENT HOLDING LTDA', '', '', '', '', '',
     date '2010-05-01', timestamp '2026-07-29 00:00:00'),
    ('BR', 'brazil_rfb', '12345678', '2', '***123456**', '49',
     '20150310', 'JOAO DA SILVA', '', '', '', '', '',
     date '2015-03-10', timestamp '2026-07-29 00:00:00')
"""


def test_company_relations_export_merges_rather_than_truncating() -> None:
    """truncate=True would destroy the previous month's history -- the single
    behaviour this design removes."""
    import inspect

    source = inspect.getsource(
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations
    )
    assert "truncate=True" not in source
    assert "EXCHANGE TABLES" in source
    assert "assert_snapshot_is_newer" in source
    assert "assert_snapshot_edge_count_is_plausible" in source


def test_company_relations_export_merges_first_snapshot_into_empty_history(
    tmp_path: Path,
) -> None:
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(2, 2, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        counts = clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )

    assert counts == {
        "edges_in_snapshot": 2,
        "spells_opened": 2,
        "spells_closed": 0,
        "spells_total": 2,
    }
    # One EXCHANGE TABLES publishes the merged history; the snapshot stage
    # table is never exchanged, only the merge result is.
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 1
    )
    # Two list-inserts: the snapshot stage population, and the ledger row.
    assert len(fake_client.inserts) == 2
    snapshot_insert_sql, snapshot_insert_rows = fake_client.inserts[0]
    ledger_insert_sql, ledger_insert_rows = fake_client.inserts[1]
    assert len(snapshot_insert_rows[0]) == len(
        tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    )
    related_kind_index = tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index(
        "related_entity_kind"
    )
    shipped_kinds = {row[related_kind_index] for row in snapshot_insert_rows}
    assert shipped_kinds == {"1", "2"}

    assert tables.QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE in ledger_insert_sql
    [ledger_row] = ledger_insert_rows
    assert ledger_row[0] == "2026-07"
    assert ledger_row[2] == "run-1"
    # edges_in_snapshot, spells_opened, spells_closed, spells_total, and the
    # exact socios_part_count read from the manifest built above.
    assert ledger_row[3:] == (2, 2, 0, 2, 10)


def test_company_relations_merge_insert_runs_with_join_use_nulls(
    tmp_path: Path,
) -> None:
    """Without join_use_nulls=1 ClickHouse fills the unmatched side of the
    FULL JOIN with type defaults ('') rather than NULL, which blanks the
    entire spell identity on the very first merge -- verified on a real
    ClickHouse 26.5 run. A bare SELECT cannot carry its own SETTINGS clause,
    so this is the only place the setting can be enforced, and this is the
    only defence against it silently regressing."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(2, 2, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )

    merge_insert_calls = [
        (sql, settings)
        for sql, params, settings in fake_client.calls
        if params is None and "with snapshot_unique as" in sql
    ]
    assert len(merge_insert_calls) == 1
    _, settings = merge_insert_calls[0]
    assert settings == {"join_use_nulls": 1}


def test_company_relations_export_writes_the_ledger_row_before_exchanging_tables(
    tmp_path: Path,
) -> None:
    """IMPORTANT 1 fix: with the OLD ordering (EXCHANGE, then ledger INSERT),
    forcing the ledger insert to raise on a real ClickHouse proved the month
    was published (new spells present, last_seen_snapshot advanced) but the
    ledger still listed only the prior month, with the pre-merge copy already
    dropped by `finally` -- no rollback path, and assert_snapshot_is_newer
    would happily let that same month be re-merged, silently double-counting
    `observations`. Writing the ledger row first inverts the failure mode
    into a strictly better one: if EXCHANGE now fails, the month is recorded
    but not published, so the next run refuses it loudly with "already
    merged" instead of silently corrupting the history. This test pins the
    ordering directly, not just the outcome, so a future edit that moves the
    INSERT back below EXCHANGE TABLES fails here first."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(2, 2, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )

    executed_sql = [sql for sql, _, _ in fake_client.calls]
    ledger_insert_index = next(
        index
        for index, sql in enumerate(executed_sql)
        if "INSERT INTO" in sql
        and tables.QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE in sql
    )
    exchange_index = next(
        index for index, sql in enumerate(executed_sql) if "EXCHANGE TABLES" in sql
    )
    assert ledger_insert_index < exchange_index


def test_company_relations_export_verifies_published_row_count_after_exchange(
    tmp_path: Path,
) -> None:
    """IMPORTANT 2 fix part 1: the ledger row is written before EXCHANGE
    TABLES (see the ordering test above), so a failed EXCHANGE leaves a
    loud, recoverable "recorded but unpublished" state -- but a
    "successful" EXCHANGE (no exception) was previously trusted blindly.
    This pins that the export actually queries the published table's row
    count, and that it does so AFTER EXCHANGE TABLES, not before -- checking
    the count before the swap would be checking the wrong table."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(2, 2, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )

    executed_sql = [sql for sql, _, _ in fake_client.calls]
    exchange_index = next(
        index for index, sql in enumerate(executed_sql) if "EXCHANGE TABLES" in sql
    )
    verify_index = next(
        index
        for index, sql in enumerate(executed_sql)
        if sql.strip().startswith("SELECT count() FROM")
    )
    assert exchange_index < verify_index
    assert tables.QUALIFIED_BR_COMPANY_RELATIONS_TABLE in executed_sql[verify_index]


def test_company_relations_export_raises_if_published_row_count_disagrees_with_ledger(
    tmp_path: Path,
) -> None:
    """IMPORTANT 2 fix part 1: EXCHANGE TABLES is atomic in ClickHouse, so
    this should always agree -- but "should always agree" is exactly the
    kind of claim that was previously trusted without verification. A
    mismatch here means the publish and the ledger have already diverged
    for THIS run, and must fail loudly rather than let a later run build on
    state that does not match what the ledger claims."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(
        ledger_rows=[], merge_counts=(2, 2, 0), published_row_count=1
    )
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="diverged"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )


def test_company_relations_snapshot_stage_ddl_carries_resolved_at() -> None:
    """The stage DDL is built FROM BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    rather than hand-listed, so it cannot silently drop a column (a previous
    version dropped resolved_at this way, which then defaulted every row's
    resolved_at to the DateTime64 epoch)."""
    ddl = clickhouse._snapshot_stage_ddl("corpscout._tmp_snap")
    assert "resolved_at DateTime64(3, 'UTC')" in ddl
    for column in tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS:
        assert column in ddl


def test_company_relations_export_refuses_an_out_of_order_snapshot(
    tmp_path: Path,
) -> None:
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(
        ledger_rows=[("2026-08", 100, 10)], merge_counts=(2, 2, 0)
    )
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="older than"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    # Refused before the snapshot stage is even populated.
    assert fake_client.inserts == []


def test_company_relations_export_refuses_an_implausibly_short_snapshot(
    tmp_path: Path,
) -> None:
    """Simulates a truncated download (RFB ships socios as ~10 ZIP parts):
    the previous merged month had far more edges than this one supplies.
    The manifest's own socios part count is UNCHANGED (10 -> 10) so this
    test isolates the edge-count ratio guard from the exact part-count
    guard -- see test_company_relations_export_refuses_a_decrease_in_socios_part_count
    for that one."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(
        ledger_rows=[("2026-06", 1_000, 10)], merge_counts=(2, 2, 0)
    )
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="truncated download"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    # The snapshot stage table was populated (2 edges is genuinely what
    # export_duckdb_connection_table_to_clickhouse inserted) but the merge
    # itself must never have run.
    assert len(fake_client.inserts) == 1
    merge_insert_calls = [
        sql for sql, params, _ in fake_client.calls if "with snapshot_unique as" in sql
    ]
    assert merge_insert_calls == []


def test_company_relations_export_refuses_a_decrease_in_socios_part_count(
    tmp_path: Path,
) -> None:
    """IMPORTANT 2 fix: MIN_SNAPSHOT_EDGE_RATIO cannot catch a single missing
    socios part (~10% edge drop, comfortably inside its 50% floor) -- this
    exact manifest part-count comparison is what does. Refused before the
    snapshot stage table is even created, since the manifest part count is
    available purely from DuckDB."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=9)
    fake_client = FakeClickHouseClient(
        ledger_rows=[("2026-06", 2, 10)], merge_counts=(2, 2, 0)
    )
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="socios ZIP parts"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    # Refused before the snapshot stage table was even created.
    assert fake_client.inserts == []
    assert not any(
        "CREATE TABLE" in statement for statement in fake_client.statements
    )


def test_company_relations_export_allows_an_increase_in_socios_part_count(
    tmp_path: Path,
) -> None:
    """Only a DECREASE is suspect -- RFB legitimately changing how many parts
    it splits socios into (more parts) must not be refused."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=11)
    fake_client = FakeClickHouseClient(
        ledger_rows=[("2026-06", 2, 10)], merge_counts=(2, 2, 0)
    )
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        counts = clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    assert counts["edges_in_snapshot"] == 2


def test_company_relations_export_refuses_when_merge_produces_no_spells_from_a_non_empty_snapshot(
    tmp_path: Path,
) -> None:
    """Global constraint: refuse to publish on empty input, matching the
    module's other exporters -- a non-empty snapshot that folds to zero
    spells must never replace the published history."""
    relations_path = _build_relations_stage(tmp_path, _TWO_EDGES_VALUES_SQL)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(0, 0, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="refusing to replace"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 0
    )


def test_company_relations_export_refuses_an_empty_snapshot(tmp_path: Path) -> None:
    """MINOR 3 fix: truncate=False bypasses the shared exporter's own
    empty-input refusal (resolved.py's check lives inside the `truncate`
    branch, which this call does not take), and relations.py's own "refuse a
    0-row edge table" guard is unreachable from here -- it lives in another
    file. An empty snapshot into an empty history would otherwise publish an
    empty table and record edges_in_snapshot=0 in the ledger, after which
    assert_snapshot_edge_count_is_plausible's own `<= 0` no-op leaves the
    NEXT month completely unguarded. Make the guarantee local."""
    relations_path = _build_empty_relations_stage(tmp_path)
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[])
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection, pytest.raises(
        ValueError, match="0 edges"
    ):
        clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 0
    )
    # 0 rows in the DuckDB source means export_duckdb_connection_table_to_clickhouse's
    # batch loop never calls INSERT at all -- not the snapshot stage, and
    # never reaching the ledger row either.
    assert fake_client.inserts == []


def test_clickhouse_company_relations_export_nulls_relation_since_outside_date32_range(
    tmp_path: Path,
) -> None:
    """I1: relation_since must get the same Date32 guard as status_date /
    activity_start_date -- the design doc records that this RFB family
    contains dates outside ClickHouse's Date32 range (1900-2299), and one
    bad row must not fail the whole 20-25M-row partition export."""
    relations_path = _build_relations_stage(
        tmp_path,
        """
        ('BR', 'brazil_rfb', '12345678', '1', '11111111000191', '22',
         '18991231', 'PARENT HOLDING LTDA', '', '', '', '', '',
         date '1899-12-31', timestamp '2026-07-29 00:00:00'),
        ('BR', 'brazil_rfb', '12345678', '2', '***123456**', '49',
         '20150310', 'JOAO DA SILVA', '', '', '', '', '',
         date '2015-03-10', timestamp '2026-07-29 00:00:00')
        """,
    )
    manifest_path = _build_manifest_stage(tmp_path, socios_part_count=10)
    fake_client = FakeClickHouseClient(ledger_rows=[], merge_counts=(2, 2, 0))
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        counts = clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_year_month="2026-07",
            source_run_id="run-1",
            snapshot_manifest_database_path=manifest_path,
        )

    assert counts["edges_in_snapshot"] == 2
    relations_insert_rows = fake_client.inserts[0][1]
    relation_since_index = tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index(
        "relation_since"
    )
    relation_since_key_index = (
        tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index("relation_since_key")
    )
    relation_since_by_key = {
        row[relation_since_key_index]: row[relation_since_index]
        for row in relations_insert_rows
    }
    assert relation_since_by_key["18991231"] is None
    assert relation_since_by_key["20150310"] is not None


def test_clickhouse_exports_replace_company_contacts_domains_and_websites(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    contacts_path = tmp_path / "br_contact_info.duckdb"
    websites_path = tmp_path / "br_websites.duckdb"
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(contacts_path)) as connection:
        contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=companies_path,
            source_run_id="run-contacts",
        )
        company_contacts_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_contacts(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )
    with duckdb.connect(str(websites_path)) as connection:
        contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=contacts_path,
        )
        website_rows = clickhouse.export_brazil_comp_rfb_clickhouse_websites(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        company_domains_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_domains(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )

    assert company_contacts_rows == 2
    assert website_rows == 1
    assert company_domains_rows == 1
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 3
    )
    assert len(fake_client.inserts) == 3
    contacts_insert_sql, contacts_insert_rows = fake_client.inserts[0]
    website_insert_sql, website_insert_rows = fake_client.inserts[1]
    domains_insert_sql, domains_insert_rows = fake_client.inserts[2]
    assert tables.BR_COMPANY_CONTACTS_TABLE_CH in contacts_insert_sql
    assert tables.BR_WEBSITES_TABLE_CH in website_insert_sql
    assert tables.BR_COMPANY_DOMAINS_TABLE_CH in domains_insert_sql
    assert len(contacts_insert_rows[0]) == len(COMPANY_CONTACTS_COLUMNS)
    assert len(website_insert_rows[0]) == len(tables.BR_WEBSITES_EXPORT_COLUMNS)
    assert len(domains_insert_rows[0]) == len(COMPANY_DOMAINS_COLUMNS)

    # The CH `confidence` column is Float32 while the DuckDB stage value is a
    # double (exact 0.9) — compare with approx once the value has passed
    # through a (fake) ClickHouse client round-trip.
    confidence_index = COMPANY_DOMAINS_COLUMNS.index("confidence")
    assert domains_insert_rows[0][confidence_index] == pytest.approx(0.9)


def test_company_relations_snapshot_input_is_everything_the_build_produces() -> None:
    """The exporter ships THIS tuple positionally, and a column left out of it is
    never inserted -- ClickHouse fills it with the type default instead. For
    resolved_at (DateTime64) that default is the epoch, on every row, silently.

    So the tuple must be exactly the history shape minus the six columns the
    merge computes, and nothing else may go missing.
    """
    history_only = {
        "first_seen_snapshot",
        "last_seen_snapshot",
        "start_at",
        "end_at",
        "is_current",
        "observations",
    }
    assert set(tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS) == (
        set(tables.BR_COMPANY_RELATIONS_COLUMNS) - history_only
    )
    assert "resolved_at" in tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    assert tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS[0] == "country_iso2"


def test_export_column_tuples_are_the_shared_canonical_tuples() -> None:
    # Identity (not just equality) pin: clickhouse.py's export columns ARE the
    # shared dagster_v3.contact_extraction tuples, so the DuckDB stage order,
    # the ClickHouse export order, and the migration DDL order can never diverge.
    assert tables.BR_COMPANY_CONTACTS_EXPORT_COLUMNS is COMPANY_CONTACTS_COLUMNS
    assert tables.BR_COMPANY_DOMAINS_EXPORT_COLUMNS is COMPANY_DOMAINS_COLUMNS
