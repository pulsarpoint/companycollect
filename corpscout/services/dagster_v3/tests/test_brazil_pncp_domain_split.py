"""PNCP's nested domain values, stored as columns rather than JSON blobs.

The ingest stored ``json ->> '$.tipoContrato'``, which extracts the whole nested
object as text, so all 116,226 production rows carry
``{"id":1,"nome":"Contrato (termo inicial)"}`` -- unusable for grouping or
filtering, and it reached the contract page verbatim through the register view's
agreement_type.

It went unnoticed because test_brazil_pncp_normalize's fixture sets
``"tipoContrato": "Contrato (termo inicial)"`` -- a flat string. The live API
sends ``{"id": 1, "nome": ...}``, so the fixture never exercised the shape that
was actually stored. The records here use the live shape.

``unidadeOrgao.codigoIbge`` was dropped entirely, and it is the only field on the
contract endpoint that cannot be derived from what we already keep: the IBGE
municipality code is Brazil's standard geographic key, so without it any join to
population, GDP or regional data goes through fuzzy matching on municipioNome.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.normalize import (
    build_contract_candidates,
    load_raw_pages,
)

# The live shape, verified against
# https://pncp.gov.br/api/pncp/v1/orgaos/00000368000150/contratos/2025/31
LIVE = {
    "numeroControlePNCP": "00000368000150-2-000007/2025",
    "anoContrato": 2025,
    "sequencialContrato": 7,
    "numeroContratoEmpenho": "00091",
    "numeroRetificacao": 0,
    "processo": "003.00004525/2024-15",
    "tipoContrato": {"id": 1, "nome": "Contrato (termo inicial)"},
    "categoriaProcesso": {"id": 2, "nome": "Compras"},
    "objetoContrato": "AQUISIÇÃO DE MATERIAIS DE TIC",
    "dataPublicacaoPncp": "2025-01-31T12:06:43",
    "dataAssinatura": "2025-01-22",
    "dataVigenciaInicio": "2025-01-22",
    "dataVigenciaFim": "2025-04-21",
    "dataAtualizacaoGlobal": "2025-01-31T12:06:43",
    "niFornecedor": "55696882000163",
    "tipoPessoa": "PJ",
    "nomeRazaoSocialFornecedor": "55.696.882 PATRICIA ELISABETE HOSSOTANI",
    "orgaoEntidade": {
        "cnpj": "00000368000150",
        "razaoSocial": "CASA MILITAR DO GABINETE DO GOVERNADOR",
        "esferaId": "E",
        "poderId": "E",
    },
    "unidadeOrgao": {
        "codigoUnidade": "990192",
        "nomeUnidade": "ESP-GABINETE DO GOV CASA MILITAR",
        "municipioNome": "São Paulo",
        "codigoIbge": 3550308,
        "ufSigla": "SP",
        "ufNome": "São Paulo",
    },
    "valorInicial": 193.5,
    "valorGlobal": 193.5,
    "valorParcela": 193.5,
    "numeroParcelas": 1,
    "receita": False,
}


def _row(tmp_path: Path, record: dict) -> dict:
    page = tmp_path / "page-00001.jsonl"
    page.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    connection = duckdb.connect(":memory:")
    load_raw_pages(
        connection=connection,
        page_dir=tmp_path,
        source_run_id="run-1",
        source_retrieved_at=datetime(2026, 7, 30, tzinfo=UTC),
    )
    build_contract_candidates(
        connection=connection,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 30, tzinfo=UTC),
    )
    columns = list(tables.CANDIDATE_COLUMNS)
    values = connection.execute(
        f"select {', '.join(columns)} from "
        f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    ).fetchone()
    assert values is not None
    return dict(zip(columns, values, strict=True))


def test_the_domain_id_and_name_get_their_own_columns(tmp_path: Path) -> None:
    row = _row(tmp_path, LIVE)

    assert row["tipo_contrato_id"] == 1
    assert row["tipo_contrato_name"] == "Contrato (termo inicial)"
    assert row["categoria_processo_id"] == 2
    assert row["categoria_processo_name"] == "Compras"


def test_the_raw_domain_value_is_still_kept_verbatim(tmp_path: Path) -> None:
    """Store every value as the source wrote it (§7a): the nested object is what
    PNCP published for that field, so the parsed pair is added beside it rather
    than replacing it."""
    row = _row(tmp_path, LIVE)

    assert "Contrato (termo inicial)" in row["tipo_contrato"]
    assert '"id"' in row["tipo_contrato"]


def test_the_ibge_municipality_code_is_captured(tmp_path: Path) -> None:
    row = _row(tmp_path, LIVE)

    assert row["buyer_municipality_ibge_code"] == "3550308"


def test_a_flat_domain_string_still_yields_a_name(tmp_path: Path) -> None:
    """Older snapshots -- and this module's own earlier fixture -- carry the
    domain as a plain string. Parsing must not turn that into an empty name."""
    row = _row(tmp_path, {**LIVE, "tipoContrato": "Contrato (termo inicial)"})

    assert row["tipo_contrato_name"] == "Contrato (termo inicial)"
    assert row["tipo_contrato_id"] is None


def test_a_missing_domain_object_lands_empty_not_null(tmp_path: Path) -> None:
    """Non-nullable ClickHouse String columns die on None in the native driver,
    and only real data with a gap triggers it."""
    record = {k: v for k, v in LIVE.items() if k not in ("tipoContrato", "unidadeOrgao")}
    row = _row(tmp_path, record)

    assert row["tipo_contrato_name"] == ""
    assert row["buyer_municipality_ibge_code"] == ""
    # Absent rather than zero: 0 is not a PNCP domain value.
    assert row["tipo_contrato_id"] is None


def test_the_new_columns_are_declared_in_the_export_contract() -> None:
    for column in (
        "tipo_contrato_id",
        "tipo_contrato_name",
        "categoria_processo_id",
        "categoria_processo_name",
        "buyer_municipality_ibge_code",
    ):
        assert column in tables.CANDIDATE_COLUMNS, column
        assert column in tables.CONTRACTS_COLUMNS, column
