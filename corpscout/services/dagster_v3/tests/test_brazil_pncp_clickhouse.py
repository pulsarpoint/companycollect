from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.clickhouse import (
    candidate_stage_ddl,
    contracts_insert_sql,
)


def _insert_sql() -> str:
    return contracts_insert_sql(target_table="stage_target", stage_table="stage_src")


def _executable_sql() -> str:
    """The SQL with comments stripped.

    The comments name headquarters_cnpj precisely to warn against it, so a test
    asserting the column is unused has to look at what runs, not at the prose
    explaining why it does not.
    """
    return "\n".join(
        line for line in _insert_sql().splitlines() if not line.strip().startswith("--")
    )


def test_the_join_uses_the_company_base_not_the_head_office() -> None:
    """headquarters_cnpj is 14 digits, fully populated, and matches whenever a
    matriz signed -- so it passes a spot check while silently dropping all 3.2M
    branch-won contracts. The base reaches the company whichever branch won.
    """
    sql = _executable_sql()

    assert "LEFT ANY JOIN" in sql
    assert "FROM corpscout.br_companies" in sql
    assert "ON c.cnpj_basico = u.supplier_cnpj_basico" in sql
    assert "headquarters_cnpj" not in sql
    # br_establishments is not needed: the base is the first 8 characters of the
    # CNPJ, so the company resolves without a second 71.9M-row lookup.
    assert "br_establishments" not in sql


def test_the_company_register_is_not_read_whole() -> None:
    """ClickHouse materialises the entire right side of a join into a hash
    table. br_companies is 68.6M rows, so an unrestricted join reads the whole
    national register to resolve a few hundred thousand suppliers -- fine in a
    test, ruinous on a real month."""
    sql = _executable_sql()

    assert "SELECT cnpj_basico" in sql
    assert "WHERE cnpj_basico IN (" in sql
    assert "SELECT supplier_cnpj_basico FROM stage_src" in sql


def test_a_company_id_is_only_set_when_the_match_actually_happened() -> None:
    """An id copied from an ineligible or unresolved row would claim a
    verification that never took place."""
    sql = _insert_sql()

    assert "u.match_eligibility = 'eligible' AND c.cnpj_basico != ''" in sql
    assert "'unmatched_company'" in sql
    # A natural person keeps its own status rather than being called unmatched:
    # it is not a company that we failed to find.
    assert "u.match_eligibility != 'eligible', u.match_eligibility" in sql


def test_usd_columns_are_left_for_the_separate_conversion_step() -> None:
    """Currency guidelines: conversion is its own step keyed on the report
    date, never inlined with extraction."""
    sql = _insert_sql()

    assert "CAST(NULL AS Nullable(Decimal(38, 2))) AS valor_global_usd" in sql
    assert "CAST(NULL AS Nullable(Date)) AS fx_rate_date" in sql


def test_insert_projects_the_declared_column_order() -> None:
    sql = _insert_sql()

    assert f"INSERT INTO stage_target ({', '.join(tables.CONTRACTS_COLUMNS)})" in sql
    # Every candidate column is carried through untouched.
    for column in tables.CANDIDATE_COLUMNS:
        assert f"u.{column}" in sql


def test_stage_matches_the_candidate_contract() -> None:
    ddl = candidate_stage_ddl("stage_src")

    for column in tables.CANDIDATE_COLUMNS:
        assert f"{column} " in ddl
    # Dates and money must not arrive as String, or the join target's types
    # would be reached only by implicit coercion.
    assert "data_publicacao_pncp Nullable(Date)" in ddl
    assert "valor_global Nullable(Decimal(38, 2))" in ddl
    assert "data_atualizacao_global DateTime64(3, 'UTC')" in ddl
