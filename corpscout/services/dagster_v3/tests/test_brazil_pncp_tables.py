from pathlib import Path

from dagster_v3.defs.brazil_pncp import tables


def _migration_sql() -> str:
    root = Path(__file__).resolve().parents[3]
    return (
        root / "clickhouse" / "migrations" / "000192_corpscout_br_pncp_partition_by_month.up.sql"
    ).read_text()


def test_migration_covers_every_exported_column() -> None:
    """The migration owns the schema and the export must not drift from it."""
    sql = _migration_sql()

    assert f"CREATE TABLE IF NOT EXISTS corpscout.{tables.CONTRACTS_TABLE}" in sql
    for column in tables.CONTRACTS_COLUMNS:
        assert f"    {column} " in sql, column


def test_amendments_replace_rather_than_accumulate() -> None:
    """Contracts are amended after publication, so a later version of the same
    contract must supersede the earlier one rather than sit beside it."""
    sql = _migration_sql()

    assert "ENGINE = ReplacingMergeTree(data_atualizacao_global)" in sql
    # ORDER BY cannot contain Nullable columns; these three are all String.
    assert "ORDER BY (company_id, numero_controle_pncp, supplier_cnpj)" in sql
    # A monthly asset must be able to REPLACE its month rather than append a
    # second copy of it. allow_nullable_key is off, hence the ifNull.
    assert (
        "PARTITION BY toYYYYMM(ifNull(data_publicacao_pncp, toDate('1970-01-01')))"
        in sql
    )


def test_all_five_value_fields_are_stored() -> None:
    """The API documents none of them, so which is *the* contract value is
    decided in the view against real data. Storing one and dropping four would
    make that decision unrecoverable."""
    for column in (
        "valor_inicial",
        "valor_parcela",
        "valor_global",
        "valor_acumulado",
        "numero_parcelas",
    ):
        assert column in tables.CONTRACTS_COLUMNS


def test_both_cnpj_forms_are_kept() -> None:
    """The 14-digit CNPJ is the establishment that signed; the 8-digit base is
    the company. Rolling up is computable from the establishment, but the
    establishment cannot be recovered from a rollup."""
    assert "supplier_cnpj" in tables.CONTRACTS_COLUMNS
    assert "supplier_cnpj_basico" in tables.CONTRACTS_COLUMNS
    assert "company_id" in tables.CONTRACTS_COLUMNS


def test_page_size_matches_what_the_api_accepts() -> None:
    # Measured 2026-07-26: 1000 is rejected, 500 is the maximum.
    assert tables.MAX_PAGE_SIZE == 500
