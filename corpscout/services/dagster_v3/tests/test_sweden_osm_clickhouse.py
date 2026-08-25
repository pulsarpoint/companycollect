"""Publish path for the Sweden OSM gazetteer into ClickHouse.

Exercises the real DuckDB->ClickHouse export against an in-memory DuckDB and a purpose-built
fake ClickHouse client that keeps per-table row state through the staged CREATE / INSERT /
EXCHANGE / DROP the publish performs -- so the assertions are about the resulting table state
and the computed normalized_match_key, not echoed SQL. The end-to-end EXCHANGE is additionally
verified against a real ClickHouse 26.5 in the task's throwaway-container script.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_address_osm import clickhouse as osm_clickhouse
from dagster_v3.defs.sweden_address_osm import tables

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

ADDRESS_POINT_SOURCE_COLUMNS = (
    "source_record_id",
    "osm_type",
    "osm_id",
    "country_code",
    "street",
    "house_number",
    "unit",
    "postcode",
    "city",
    "place",
    "full_address",
    "normalized_street",
    "normalized_house_number",
    "normalized_postcode",
    "normalized_city",
    "address_match_key",
    "longitude",
    "latitude",
    "coordinate_method",
    "source_record_url",
    "source_tags_json",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
)

STREET_SEGMENT_SOURCE_COLUMNS = (
    "source_record_id",
    "osm_id",
    "street",
    "normalized_street",
    "highway",
    "longitude",
    "latitude",
    "coordinate_method",
    "source_record_url",
    "source_tags_json",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
)


class FakeClickHouseClient:
    """Keeps per-table row state through the staged EXCHANGE publish.

    Handles exactly what export_duckdb_connection_table_to_clickhouse(truncate=True) issues:
    the system.tables existence probe, ``CREATE TABLE stage AS target``, explicit-column
    ``INSERT INTO stage (...) VALUES`` data blocks, ``EXCHANGE TABLES stage AND target``,
    ``DROP TABLE IF EXISTS stage`` and a plain ``SELECT count() FROM db.table``.
    """

    def __init__(self, target_tables: tuple[str, ...]) -> None:
        # Names are stored unqualified-as-`db.table`, lowercased backticks stripped.
        self.rows: dict[str, list[tuple[Any, ...]]] = {t: [] for t in target_tables}
        self.columns: dict[str, tuple[str, ...]] = {t: () for t in target_tables}
        self.operations: list[str] = []

    @staticmethod
    def _unquote(identifier: str) -> str:
        return identifier.replace("`", "").strip()

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        normalized = " ".join(sql.split())
        self.operations.append(normalized)

        if "system.tables" in normalized:
            requested = params["tables"]
            existing = {name.split(".")[-1] for name in self.rows}
            return [(name,) for name in requested if name in existing]

        if normalized.startswith("CREATE TABLE "):
            body = normalized[len("CREATE TABLE ") :]
            stage_raw, target_raw = body.split(" AS ")
            stage = self._unquote(stage_raw)
            target = self._unquote(target_raw)
            self.rows[stage] = []
            self.columns[stage] = self.columns.get(target, ())
            return []

        if normalized.startswith("INSERT INTO "):
            head = normalized[len("INSERT INTO ") :]
            table = self._unquote(head.split("(")[0])
            column_block = head[head.index("(") + 1 : head.index(")")]
            columns = tuple(
                self._unquote(part) for part in column_block.split(",")
            )
            self.columns[table] = columns
            self.rows.setdefault(table, []).extend(params)
            return []

        if normalized.startswith("EXCHANGE TABLES "):
            body = normalized[len("EXCHANGE TABLES ") :]
            left_raw, right_raw = body.split(" AND ")
            left = self._unquote(left_raw)
            right = self._unquote(right_raw)
            self.rows[left], self.rows[right] = self.rows[right], self.rows[left]
            self.columns[left], self.columns[right] = (
                self.columns[right],
                self.columns[left],
            )
            return []

        if normalized.startswith("DROP TABLE IF EXISTS "):
            table = self._unquote(normalized[len("DROP TABLE IF EXISTS ") :])
            self.rows.pop(table, None)
            self.columns.pop(table, None)
            return []

        if normalized.startswith("SELECT count() FROM "):
            table = self._unquote(normalized[len("SELECT count() FROM ") :])
            return [(len(self.rows.get(table, [])),)]

        raise AssertionError(f"unhandled SQL: {normalized}")

    def value(self, table: str, row_index: int, column: str) -> Any:
        return self.rows[table][row_index][self.columns[table].index(column)]

    def remaining_stage_tables(self) -> list[str]:
        return [name for name in self.rows if "._tmp_" in name]


@contextmanager
def _patched_clickhouse(monkeypatch, client: FakeClickHouseClient) -> Iterator[ClickhouseResource]:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    yield resource


def _seed_duckdb() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(f"create schema {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {tables.QUALIFIED_ADDRESS_TABLE} (
            source_record_id varchar, osm_type varchar, osm_id bigint,
            country_code varchar, street varchar, house_number varchar, unit varchar,
            postcode varchar, city varchar, place varchar, full_address varchar,
            normalized_street varchar, normalized_house_number varchar,
            normalized_postcode varchar, normalized_city varchar, address_match_key varchar,
            longitude double, latitude double, coordinate_method varchar,
            source_record_url varchar, source_tags_json varchar, source_url varchar,
            source_object_key varchar, source_md5 varchar,
            source_snapshot_at timestamptz, source_retrieved_at timestamptz
        )
        """
    )
    snapshot = "2026-08-14 05:18:07+00"
    connection.execute(
        f"""
        insert into {tables.QUALIFIED_ADDRESS_TABLE} values
        -- accented street with a postcode: key must strip the accent
        ('way/848991947','way',848991947,'SE','Plåtslagarvägen','26',NULL,'14636',
         'Tullinge',NULL,NULL,'platslagarvagen','26','14636','tullinge',
         '14636|platslagarvagen26',18.0,59.2,'osm_way_centroid',
         'https://www.openstreetmap.org/way/848991947','{{}}','http://s','k','md5',
         '{snapshot}','{snapshot}'),
        -- no postcode: key postcode part is empty, still keyed on street+house
        ('node/1','node',1,'SE','Storgatan','5',NULL,NULL,'Ort',NULL,NULL,
         'storgatan','5','','ort','|storgatan5',17.0,58.0,'osm_node',
         'https://www.openstreetmap.org/node/1','{{}}','http://s','k','md5',
         '{snapshot}','{snapshot}'),
        -- place-only (no street): street falls back to place
        ('node/2','node',2,'SE',NULL,'7',NULL,'98253','By','Norrbyn',NULL,
         'norrbyn','7','98253','by','98253|norrbyn7',20.0,66.0,'osm_node',
         'https://www.openstreetmap.org/node/2','{{}}','http://s','k','md5',
         '{snapshot}','{snapshot}')
        """
    )
    connection.execute(
        f"""
        create table {tables.QUALIFIED_STREET_SEGMENT_TABLE} (
            source_record_id varchar, osm_id bigint, street varchar,
            normalized_street varchar, highway varchar, longitude double, latitude double,
            coordinate_method varchar, source_record_url varchar, source_tags_json varchar,
            source_url varchar, source_object_key varchar, source_md5 varchar,
            source_snapshot_at timestamptz, source_retrieved_at timestamptz
        )
        """
    )
    connection.execute(
        f"""
        insert into {tables.QUALIFIED_STREET_SEGMENT_TABLE} values
        ('way/9',9,'Långkärrsvägen','langkarrsvagen','residential',18.1,59.3,
         'osm_road_segment_midpoint','https://www.openstreetmap.org/way/9','{{}}',
         'http://s','k','md5','{snapshot}','{snapshot}')
        """
    )
    return connection


def test_migrations_declare_every_published_column() -> None:
    address_up = (
        MIGRATIONS_DIR / "000322_corpscout_se_osm_address_points.up.sql"
    ).read_text()
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_osm_address_points" in address_up
    )
    assert "ENGINE = MergeTree" in address_up
    assert (
        "ORDER BY (normalized_postcode, normalized_street, normalized_house_number)"
        in address_up
    )
    for column in osm_clickhouse.ADDRESS_POINT_COLUMNS:
        assert f"    {column} " in address_up, column

    street_up = (
        MIGRATIONS_DIR / "000323_corpscout_se_osm_street_segments.up.sql"
    ).read_text()
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_osm_street_segments" in street_up
    )
    assert "ORDER BY (normalized_street)" in street_up
    for column in osm_clickhouse.STREET_SEGMENT_COLUMNS:
        assert f"    {column} " in street_up, column


def test_match_key_reproduces_store_format_keys() -> None:
    connection = _seed_duckdb()
    key = osm_clickhouse.address_point_match_key_sql()
    produced = dict(
        connection.execute(
            f"select source_record_id, {key} from {tables.QUALIFIED_ADDRESS_TABLE}"
        ).fetchall()
    )
    assert produced["way/848991947"] == "14636|platslagarvagen26"
    assert produced["node/1"] == "|storgatan5"
    assert produced["node/2"] == "98253|norrbyn7"

    street_key = osm_clickhouse.street_segment_match_key_sql()
    [(value,)] = connection.execute(
        f"select {street_key} from {tables.QUALIFIED_STREET_SEGMENT_TABLE}"
    ).fetchall()
    assert value == "langkarrsvagen"


def test_publish_swaps_both_tables_and_computes_keys(monkeypatch) -> None:
    connection = _seed_duckdb()
    client = FakeClickHouseClient(
        (
            f"{osm_clickhouse.CLICKHOUSE_DATABASE}."
            f"{osm_clickhouse.ADDRESS_POINTS_TABLE_CH}",
            f"{osm_clickhouse.CLICKHOUSE_DATABASE}."
            f"{osm_clickhouse.STREET_SEGMENTS_TABLE_CH}",
        )
    )
    published_at = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    with _patched_clickhouse(monkeypatch, client) as clickhouse:
        result = osm_clickhouse.publish_sweden_osm_gazetteer(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            published_at=published_at,
        )

    assert result.address_points == 3
    assert result.street_segments == 1
    # The staged tables were swapped in and every _tmp_ stage was dropped.
    assert client.remaining_stage_tables() == []
    assert any(op.startswith("EXCHANGE TABLES ") for op in client.operations)

    address_table = (
        f"{osm_clickhouse.CLICKHOUSE_DATABASE}."
        f"{osm_clickhouse.ADDRESS_POINTS_TABLE_CH}"
    )
    assert len(client.rows[address_table]) == 3
    keys = {
        client.value(address_table, i, "source_record_id"): client.value(
            address_table, i, "normalized_match_key"
        )
        for i in range(3)
    }
    assert keys["way/848991947"] == "14636|platslagarvagen26"
    assert keys["node/2"] == "98253|norrbyn7"
    # Nullable text mirrors as empty string, never None.
    node1_index = [
        i
        for i in range(3)
        if client.value(address_table, i, "source_record_id") == "node/1"
    ][0]
    assert client.value(address_table, node1_index, "postcode") == ""
    assert client.value(address_table, node1_index, "published_at") == published_at


def test_match_join_sql_survives_driver_param_substitution() -> None:
    """clickhouse-driver substitutes named params with Python percent-formatting, so a
    literal percent (the LIKE wildcards) or a stray %(name)s in a comment would raise. The
    check runs this against the real driver, so guard the escaping here."""
    rendered = osm_clickhouse.GAZETTEER_MATCH_JOIN_SQL % {"sample_limit": 50000}
    assert "LIKE 'way/%'" in rendered
    assert "LIMIT 50000" in rendered


def test_row_count_band_rejects_empty_and_mismatch() -> None:
    assert osm_clickhouse.row_count_is_within_band(
        clickhouse_count=1000, duckdb_count=1000
    )
    assert not osm_clickhouse.row_count_is_within_band(
        clickhouse_count=0, duckdb_count=1000
    )
    assert not osm_clickhouse.row_count_is_within_band(
        clickhouse_count=500, duckdb_count=1000
    )


@pytest.mark.parametrize(
    ("sample_size", "osm_id_present", "postcode_bearing", "key_ok", "expected"),
    [
        (0, 0, 0, 0, True),  # nothing computed against this snapshot yet -> pass
        (100, 95, 60, 60, True),  # healthy
        (100, 50, 60, 60, False),  # osm ids do not resolve back -> fail
        (100, 95, 60, 10, False),  # normalization regression -> fail
        (100, 95, 0, 0, True),  # no postcode-bearing rows to judge -> pass on osm id
    ],
)
def test_gazetteer_join_health_gate(
    sample_size, osm_id_present, postcode_bearing, key_ok, expected
) -> None:
    assert (
        osm_clickhouse.gazetteer_match_join_is_healthy(
            sample_size=sample_size,
            osm_id_present=osm_id_present,
            postcode_bearing=postcode_bearing,
            key_matches_postcode_bearing=key_ok,
        )
        is expected
    )
