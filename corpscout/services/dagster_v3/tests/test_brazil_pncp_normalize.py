import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.normalize import (
    build_contract_candidates,
    load_raw_pages,
)


def _contract(**overrides):
    record = {
        "numeroControlePNCP": "46377800000127-2-002750/2025",
        "anoContrato": 2025,
        "sequencialContrato": 2750,
        "numeroContratoEmpenho": "027/2025",
        "numeroRetificacao": 0,
        "processo": "20250224789",
        "tipoContrato": "Contrato (termo inicial)",
        "categoriaProcesso": "Serviços",
        "objetoContrato": "SERVIÇOS CONTÍNUOS DE LIMPEZA",
        "dataPublicacaoPncp": "2025-06-01T00:00:13",
        "dataAssinatura": "2025-05-30",
        "dataVigenciaInicio": "2025-06-01",
        "dataVigenciaFim": "2027-12-01",
        "dataAtualizacaoGlobal": "2025-06-02T10:11:12",
        "niFornecedor": "57423612000104",
        "tipoPessoa": "PJ",
        "nomeRazaoSocialFornecedor": "RLIMP SERVICOS LTDA",
        "orgaoEntidade": {
            "cnpj": "46377800000127",
            "razaoSocial": "SAO PAULO SECRETARIA DA SEGURANCA PUBLICA",
            "poderId": "E",
            "esferaId": "E",
        },
        "unidadeOrgao": {
            "codigoUnidade": "925000",
            "nomeUnidade": "SSP",
            "ufSigla": "SP",
            "municipioNome": "São Paulo",
        },
        "valorInicial": 94390.8,
        "valorParcela": 94390.8,
        "valorGlobal": 94390.8,
        "valorAcumulado": 94390.8,
        "numeroParcelas": 1,
        "receita": False,
        "emendaParlamentar": False,
        "frutoAdesao": False,
        "temRemanejamento": False,
    }
    record.update(overrides)
    return record


def _loaded(tmp_path: Path, records) -> duckdb.DuckDBPyConnection:
    page = tmp_path / "page-00001.jsonl"
    page.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )
    connection = duckdb.connect(":memory:")
    load_raw_pages(
        connection=connection,
        page_dir=tmp_path,
        source_run_id="run-1",
        source_retrieved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )
    return connection


def _candidates(connection, *columns):
    return connection.execute(
        f"select {', '.join(columns)} "
        f"from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} "
        f"order by source_record_id"
    ).fetchall()


def test_typed_projection_of_a_real_contract(tmp_path: Path) -> None:
    connection = _loaded(tmp_path, [_contract()])
    counts = build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )
    assert counts["candidate_rows"] == 1
    assert counts["eligible_rows"] == 1

    (row,) = _candidates(
        connection,
        "supplier_cnpj",
        "supplier_cnpj_basico",
        "buyer_cnpj",
        "buyer_state_code",
        "data_publicacao_pncp",
        "valor_global",
        "source_url",
    )
    assert row[0] == "57423612000104"
    # The company base is the first 8 digits of the establishment CNPJ.
    assert row[1] == "57423612"
    # Nested objects are read by key, not by inferred struct shape.
    assert row[2] == "46377800000127"
    assert row[3] == "SP"
    # An ISO datetime becomes the date it denotes.
    assert row[4] == datetime(2025, 6, 1).date()
    assert float(row[5]) == 94390.8
    assert row[6] == "https://pncp.gov.br/app/contratos/46377800000127/2025/2750"


def test_natural_persons_are_marked_not_matched(tmp_path: Path) -> None:
    """PF is a natural person and must never be resolved against a company
    register. PF and PJ differ by one character, so a prefix or LIKE match would
    classify every natural person as a company."""
    connection = _loaded(
        tmp_path,
        [
            _contract(tipoPessoa="PJ"),
            _contract(tipoPessoa="PF", niFornecedor="12345678901", sequencialContrato=2),
            _contract(tipoPessoa="", niFornecedor="57423612000104", sequencialContrato=3),
        ],
    )
    counts = build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )

    assert counts["eligible_rows"] == 1
    assert counts["natural_person_rows"] == 1
    # An unstated person type is not silently treated as a company.
    assert counts["candidate_rows"] == 3


def test_a_missing_url_component_yields_no_url_rather_than_a_broken_one(
    tmp_path: Path,
) -> None:
    connection = _loaded(
        tmp_path, [_contract(sequencialContrato=None), _contract(orgaoEntidade={})]
    )
    counts = build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )
    assert counts["rows_without_source_url"] == 2


def test_all_five_value_fields_survive_the_projection(tmp_path: Path) -> None:
    """The API documents none of them, so the choice is made in the view. A
    projection that kept only one would make that choice unrecoverable without
    re-fetching ~6,400 rate-limited pages."""
    connection = _loaded(
        tmp_path,
        [_contract(valorInicial=100.0, valorParcela=50.0, valorGlobal=200.0,
                   valorAcumulado=250.0, numeroParcelas=4)],
    )
    build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )
    (row,) = _candidates(
        connection, "valor_inicial", "valor_parcela", "valor_global",
        "valor_acumulado", "numero_parcelas",
    )
    assert [float(v) for v in row[:4]] == [100.0, 50.0, 200.0, 250.0]
    assert row[4] == 4


def test_candidate_columns_match_the_declared_contract(tmp_path: Path) -> None:
    connection = _loaded(tmp_path, [_contract()])
    build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 26, tzinfo=UTC),
    )
    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DUCKDB_SCHEMA, tables.CANDIDATES_TABLE],
        ).fetchall()
    )
    assert columns == tables.CANDIDATE_COLUMNS
