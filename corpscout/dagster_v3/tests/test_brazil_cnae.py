import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dagster_v3.defs.brazil_cnae import tables
from dagster_v3.defs.brazil_cnae.mapping import (
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
