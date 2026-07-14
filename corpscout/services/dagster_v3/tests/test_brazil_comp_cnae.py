import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.cnae import tables
from dagster_v3.defs.brazil_companies.cnae.mapping import (
    build_br_cnae_to_nace_rows,
    normalize_cnae_code,
    normalize_nace_code,
)


PULLED_AT = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)


def _write_fixture(tmp_path: Path, rows: list[str]) -> Path:
    fixture_path = tmp_path / "br_cnae_to_nace.csv"
    fixture_path.write_text(
        "\n".join(
            [
                "cnae_version,cnae_code,cnae_description_pt,cnae_description_en,"
                "nace_revision,nace_code,nace_description_en,mapping_source,"
                "source_url",
                *rows,
            ]
        ),
        encoding="utf-8",
    )
    return fixture_path


def test_code_normalizers_keep_stable_lookup_keys() -> None:
    assert normalize_cnae_code("6201-5/01") == "6201501"
    assert normalize_cnae_code(" 62.01-5/01 ") == "6201501"
    assert normalize_nace_code("62.01") == "6201"
    assert normalize_nace_code(" 62.01 ") == "6201"


def test_build_rows_supports_many_to_many_edges(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.02,"
            "Computer consultancy activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6311-9/00,Tratamento de dados e hospedagem,"
            "Data processing and hosting,NACE_REV_2,63.11,"
            '"Data processing, hosting and related activities",'
            "ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    rows = build_br_cnae_to_nace_rows(
        fixture_path=fixture_path,
        source_run_id="run-123",
        valid_nace_targets={
            ("NACE_REV_2", "6201"),
            ("NACE_REV_2", "6202"),
            ("NACE_REV_2", "6311"),
        },
        pulled_at=PULLED_AT,
    )

    assert len(rows) == 3
    assert all(len(row) == len(tables.BR_CNAE_TO_NACE_COLUMNS) for row in rows)
    assert [row[:11] for row in rows] == [
        (
            "CNAE_2_0",
            "6201-5/01",
            "6201501",
            "Desenvolvimento de programas sob encomenda",
            "Custom software development",
            "NACE_REV_2",
            "62.01",
            "6201",
            "Computer programming activities",
            "ibge_concla_isic_bridge",
            "https://example.test/cnae",
        ),
        (
            "CNAE_2_0",
            "6201-5/01",
            "6201501",
            "Desenvolvimento de programas sob encomenda",
            "Custom software development",
            "NACE_REV_2",
            "62.02",
            "6202",
            "Computer consultancy activities",
            "ibge_concla_isic_bridge",
            "https://example.test/cnae",
        ),
        (
            "CNAE_2_0",
            "6311-9/00",
            "6311900",
            "Tratamento de dados e hospedagem",
            "Data processing and hosting",
            "NACE_REV_2",
            "63.11",
            "6311",
            "Data processing, hosting and related activities",
            "ibge_concla_isic_bridge",
            "https://example.test/cnae",
        ),
    ]
    assert rows[0][12] == "run-123"
    assert rows[0][13] == PULLED_AT
    assert {row[11] for row in rows} == {rows[0][11]}
    assert rows[0][11] == hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    assert len(rows[0][11]) == 64
    assert {row[2] for row in rows} == {"6201501", "6311900"}
    assert {row[7] for row in rows} == {"6201", "6202", "6311"}


def test_build_rows_rejects_duplicate_edges(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,62.01-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,6201,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate Brazil CNAE to NACE mapping edge",
    ):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6201")},
            pulled_at=PULLED_AT,
        )


def test_build_rows_rejects_extra_fixture_values(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae,unexpected",
        ],
    )

    with pytest.raises(ValueError, match="Unexpected extra fixture values"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6201")},
            pulled_at=PULLED_AT,
        )


def test_build_rows_rejects_missing_required_values(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing required fixture value: cnae_code",
    ):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6201")},
            pulled_at=PULLED_AT,
        )


def test_build_rows_rejects_missing_required_header_columns(tmp_path: Path) -> None:
    fixture_path = tmp_path / "missing_header.csv"
    fixture_path.write_text(
        "\n".join(
            [
                "cnae_version,cnae_code,cnae_description_pt,cnae_description_en,"
                "nace_revision,nace_description_en,mapping_source,source_url",
                "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
                "Custom software development,NACE_REV_2,"
                "Computer programming activities,ibge_concla_isic_bridge,"
                "https://example.test/cnae",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing required fixture columns: nace_code"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6201")},
            pulled_at=PULLED_AT,
        )


def test_build_rows_rejects_empty_fixture(tmp_path: Path) -> None:
    fixture_path = _write_fixture(tmp_path, [])

    with pytest.raises(
        ValueError,
        match="Brazil CNAE to NACE fixture produced no rows",
    ):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6201")},
            pulled_at=PULLED_AT,
        )


def test_build_rows_rejects_unknown_nace_targets(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(ValueError, match="Unknown NACE target"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture_path,
            source_run_id="run-123",
            valid_nace_targets={("NACE_REV_2", "6202")},
            pulled_at=PULLED_AT,
        )


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object | None]] = []
        self.nace_targets = [
            ("NACE_REV_2", "6201", "Computer programming activities"),
            ("NACE_REV_2", "6202", "Computer consultancy activities"),
            ("NACE_REV_2", "6311", "Data processing, hosting and related activities"),
        ]

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append((sql, params))
        if "FROM `corpscout`.`nace_categories`" in sql:
            return self.nace_targets
        return []


def test_replace_clickhouse_uses_stage_insert_exchange_and_drop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_companies.cnae import mapping

    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.02,"
            "Computer consultancy activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )
    monkeypatch.setattr(
        mapping.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "abc123"})(),
    )
    client = FakeClickHouseClient()

    counts = mapping.replace_br_cnae_to_nace_clickhouse(
        clickhouse_client=client,
        fixture_path=fixture,
        source_run_id="run-1",
        pulled_at=PULLED_AT,
    )

    assert counts == {"rows": 2, "cnae_codes": 1, "nace_targets": 2}
    assert client.statements[0][0] == (
        "SELECT classification_version, normalized_code, description_en "
        "FROM `corpscout`.`nace_categories`"
    )
    assert client.statements[1][0] == (
        "CREATE TABLE `corpscout`.`_tmp_br_cnae_to_nace_abc123` AS "
        "`corpscout`.`br_cnae_to_nace`"
    )
    insert_sql, insert_params = client.statements[2]
    expected_columns = ", ".join(tables.BR_CNAE_TO_NACE_COLUMNS)
    assert insert_sql == (
        "INSERT INTO `corpscout`.`_tmp_br_cnae_to_nace_abc123` "
        f"({expected_columns}) VALUES"
    )
    assert isinstance(insert_params, list)
    assert len(insert_params) == 2
    assert client.statements[3][0] == (
        "EXCHANGE TABLES `corpscout`.`_tmp_br_cnae_to_nace_abc123` "
        "AND `corpscout`.`br_cnae_to_nace`"
    )
    assert client.statements[-1][0] == (
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_br_cnae_to_nace_abc123`"
    )


def test_replace_clickhouse_drops_stage_after_insert_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_companies.cnae import mapping

    class FailingInsertClient(FakeClickHouseClient):
        def execute(
            self,
            sql: str,
            params: object | None = None,
        ) -> list[tuple[object, ...]]:
            self.statements.append((sql, params))
            if "FROM `corpscout`.`nace_categories`" in sql:
                return self.nace_targets
            if sql.startswith("INSERT INTO"):
                raise RuntimeError("insert failed")
            return []

    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )
    monkeypatch.setattr(
        mapping.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "failed"})(),
    )
    client = FailingInsertClient()

    with pytest.raises(RuntimeError, match="insert failed"):
        mapping.replace_br_cnae_to_nace_clickhouse(
            clickhouse_client=client,
            fixture_path=fixture,
            source_run_id="run-1",
            pulled_at=PULLED_AT,
        )

    assert client.statements[-1][0] == (
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_br_cnae_to_nace_failed`"
    )


def test_seed_fixture_builds_non_empty_rows() -> None:
    from dagster_v3.defs.brazil_companies.cnae.assets import BR_CNAE_TO_NACE_FIXTURE

    rows = build_br_cnae_to_nace_rows(
        fixture_path=BR_CNAE_TO_NACE_FIXTURE,
        source_run_id="seed-test",
        pulled_at=PULLED_AT,
        valid_nace_targets={
            ("NACE_REV_2", "6201"),
            ("NACE_REV_2", "6202"),
            ("NACE_REV_2", "6311"),
        },
    )

    assert len(rows) == 3


def test_brazil_comp_cnae_asset_is_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.to_user_string() for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_comp_cnae_to_nace_clickhouse" in keys


def test_brazil_comp_cnae_asset_asserts_clickhouse_target_table_before_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_companies.cnae import assets

    calls: dict[str, object] = {}
    events: list[str] = []
    client = object()

    class FakeConnection:
        def __enter__(self) -> object:
            events.append("connection")
            calls["connection_opened"] = True
            return client

        def __exit__(self, *args: object) -> None:
            return None

    def fake_assert_clickhouse_tables_exist(
        clickhouse: ClickhouseResource,
        *,
        database: str,
        tables: tuple[str, ...],
    ) -> None:
        events.append("assert")
        calls["assert"] = {
            "clickhouse": clickhouse,
            "database": database,
            "tables": tables,
        }

    def fake_replace_br_cnae_to_nace_clickhouse(**kwargs: Any) -> dict[str, int]:
        events.append("replace")
        calls["replace"] = kwargs
        return {"rows": 3, "cnae_codes": 2, "nace_targets": 3}

    monkeypatch.setattr(
        assets,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        assets,
        "replace_br_cnae_to_nace_clickhouse",
        fake_replace_br_cnae_to_nace_clickhouse,
    )

    clickhouse = ClickhouseResource(host="localhost")
    monkeypatch.setattr(
        ClickhouseResource,
        "get_connection",
        lambda self: FakeConnection(),
    )

    result = dg.materialize(
        [assets.brazil_comp_cnae_to_nace_clickhouse],
        resources={"clickhouse": clickhouse},
    )

    assert result.success
    assert events == ["assert", "connection", "replace"]
    assert calls["assert"] == {
        "clickhouse": clickhouse,
        "database": tables.BRAZIL_COMP_CNAE_DATABASE,
        "tables": (tables.BR_CNAE_TO_NACE_TABLE,),
    }
    assert calls["replace"]["clickhouse_client"] is client
    assert calls["replace"]["fixture_path"] == assets.BR_CNAE_TO_NACE_FIXTURE
    assert isinstance(calls["replace"]["source_run_id"], str)
    assert calls["replace"]["source_run_id"]
