from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_DOCUMENTS_TABLE,
)


def test_brazil_fin_cvm_source_duckdb_path_is_partitioned_by_family_and_year(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm.storage import (
        brazil_fin_cvm_source_duckdb_path,
    )

    assert brazil_fin_cvm_source_duckdb_path(
        family="dfp",
        year="2026",
        root=tmp_path,
    ) == (tmp_path / "dfp" / "year=2026" / "source.duckdb")
    assert brazil_fin_cvm_source_duckdb_path(
        family="fre",
        year="2026",
        root=tmp_path,
    ) == (tmp_path / "fre" / "year=2026" / "source.duckdb")


def test_existing_source_duckdb_connection_requires_partition_file(
    tmp_path: Path,
) -> None:
    import pytest

    from dagster_v3.defs.brazil_financial.cvm.storage import (
        brazil_fin_cvm_existing_source_duckdb_connection,
        brazil_fin_cvm_source_duckdb_path,
    )

    db_path = brazil_fin_cvm_source_duckdb_path(
        family="dfp",
        year="2026",
        root=tmp_path,
    )

    with pytest.raises(FileNotFoundError, match="brazil_fin_cvm_dfp_raw_duckdb"):
        with brazil_fin_cvm_existing_source_duckdb_connection(
            family="dfp",
            year="2026",
            root=tmp_path,
        ):
            pass

    assert not db_path.exists()


def test_read_only_partitioned_duckdb_connection_unions_existing_year_files(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm.storage import (
        brazil_fin_cvm_read_only_partitioned_connection,
        brazil_fin_cvm_source_duckdb_path,
    )

    for year, document_id in (("2025", 150000), ("2026", 160000)):
        db_path = brazil_fin_cvm_source_duckdb_path(
            family="dfp",
            year=year,
            root=tmp_path,
        )
        db_path.parent.mkdir(parents=True)
        with duckdb.connect(str(db_path)) as connection:
            connection.execute(f"create schema {BRAZIL_CVM_DUCKDB_SCHEMA}")
            connection.execute(
                f"""
                create table {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE} (
                    dfp_year integer,
                    document_id integer
                )
                """
            )
            connection.execute(
                f"""
                insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE}
                values (?, ?)
                """,
                [int(year), document_id],
            )

    with brazil_fin_cvm_read_only_partitioned_connection(
        family="dfp",
        years=("2024", "2025", "2026"),
        table_names=(DFP_DOCUMENTS_TABLE,),
        root=tmp_path,
    ) as connection:
        rows = connection.execute(
            f"""
            select dfp_year, document_id
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE}
            order by dfp_year
            """
        ).fetchall()

    assert rows == [(2025, 150000), (2026, 160000)]
