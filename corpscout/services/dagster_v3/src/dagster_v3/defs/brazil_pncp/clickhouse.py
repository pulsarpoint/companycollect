"""Export contract candidates to ClickHouse, resolving the winning company.

The join is the part worth being careful about. PNCP names the *establishment*
that signed -- a 14-digit CNPJ -- while ``br_companies`` is keyed on the 8-digit
company base. The obvious-looking alternative, ``br_companies.headquarters_cnpj``,
is also 14 digits and fully populated, and matches whenever a head office signed:
it would pass a spot check and silently drop every one of the 3.2M branch-won
contracts. Matching on the base reaches the company whichever establishment won.

USD conversion is deliberately not done here. It is a separate step keyed on the
contract date, per the currency guidelines; this module only carries the columns
that step already wrote into DuckDB. A partition exported before the conversion
runs lands them NULL, which is the honest reading of "not converted yet".
"""

from __future__ import annotations

from typing import Any

from dagster_v3.defs.brazil_pncp import tables, usd_conversion

_STAGE_COLUMN_TYPES = {
    "ano_contrato": "Nullable(UInt16)",
    "sequencial_contrato": "Nullable(UInt32)",
    "numero_retificacao": "Nullable(UInt16)",
    "numero_parcelas": "Nullable(UInt32)",
    "data_publicacao_pncp": "Nullable(Date)",
    "data_assinatura": "Nullable(Date)",
    "data_vigencia_inicio": "Nullable(Date)",
    "data_vigencia_fim": "Nullable(Date)",
    "data_atualizacao_global": "DateTime64(3, 'UTC')",
    "source_retrieved_at": "DateTime64(3, 'UTC')",
    "resolved_at": "DateTime64(3, 'UTC')",
    "is_revenue_contract": "Nullable(UInt8)",
    "parliamentary_amendment": "Nullable(UInt8)",
    "from_adhesion": "Nullable(UInt8)",
    "has_reallocation": "Nullable(UInt8)",
}


# Read out of DuckDB alongside the candidates. Listed separately from
# CANDIDATE_COLUMNS because a different asset writes them.
FX_COLUMNS = (*tables.USD_VALUE_COLUMNS, *tables.FX_PROVENANCE_COLUMNS)

_STAGE_COLUMN_TYPES.update(
    {
        **{column: "Nullable(Decimal(38, 2))" for column in tables.USD_VALUE_COLUMNS},
        "fx_rate_to_usd": "Nullable(Decimal(24, 10))",
        "fx_rate_date": "Nullable(Date)",
        "fx_source": "String",
    }
)


def _stage_column_type(name: str) -> str:
    if name in _STAGE_COLUMN_TYPES:
        return _STAGE_COLUMN_TYPES[name]
    if name.startswith("valor_"):
        return "Nullable(Decimal(38, 2))"
    return "String"


def candidate_stage_ddl(stage_table: str) -> str:
    """A staging table shaped like the DuckDB candidates, for the join to read."""
    columns = ",\n    ".join(
        f"{name} {_stage_column_type(name)}"
        for name in (*tables.CANDIDATE_COLUMNS, *FX_COLUMNS)
    )
    return f"""
    CREATE TABLE {stage_table}
    (
    {columns}
    )
    ENGINE = MergeTree
    ORDER BY (supplier_cnpj, numero_controle_pncp)
    """


def contracts_insert_sql(*, target_table: str, stage_table: str) -> str:
    """Resolve the company and project into the contracts table's column order."""
    passthrough = ",\n        ".join(f"u.{name}" for name in tables.CANDIDATE_COLUMNS)
    fx_passthrough = ",\n        ".join(f"u.{name}" for name in FX_COLUMNS)
    return f"""
    INSERT INTO {target_table} ({", ".join(tables.CONTRACTS_COLUMNS)})
    SELECT
        -- Only an eligible supplier that actually resolves gets a company id.
        -- An id that exists in the register but was never matched would claim
        -- a verification that did not happen.
        if(
            u.match_eligibility = 'eligible' AND c.cnpj_basico != '',
            c.cnpj_basico,
            ''
        ) AS company_id,
        multiIf(
            u.match_eligibility != 'eligible', u.match_eligibility,
            c.cnpj_basico != '', 'exact',
            'unmatched_company'
        ) AS company_match_status,
        {passthrough},
        -- Written by the separate USD conversion step, keyed on the contract
        -- date. Never inlined with extraction, per the currency guidelines.
        -- NULL here means that step has not run for this partition.
        {fx_passthrough}
    FROM {stage_table} AS u
    -- The 8-digit base, never headquarters_cnpj: that is the head office only,
    -- so it matches when a matriz signed and silently misses every branch.
    --
    -- The right side is restricted to the bases this batch actually references.
    -- ClickHouse materialises the whole right side of a join into a hash table,
    -- and br_companies is 68.6M rows -- joining it unrestricted reads the entire
    -- company register to resolve at most a few hundred thousand suppliers.
    LEFT ANY JOIN
    (
        SELECT cnpj_basico
        FROM corpscout.br_companies
        WHERE cnpj_basico IN (
            SELECT supplier_cnpj_basico FROM {stage_table} WHERE supplier_cnpj_basico != ''
        )
    ) AS c
        ON c.cnpj_basico = u.supplier_cnpj_basico
    """


def insert_contracts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    batch_size: int = 50_000,
) -> dict[str, int]:
    """Append the candidates currently in DuckDB, for the rolling daily window.

    **Appends rather than REPLACE PARTITION, and that is the whole point.** The
    daily job reads a trailing 7 days, which is a slice of a month, not a month.
    Replacing partition 202607 with seven days of it would delete the other
    three weeks -- and because the guard against blanking only fires on an empty
    input, a full-looking batch would have sailed straight through it.

    Correctness rests on the table being
    ``ReplacingMergeTree(data_atualizacao_global)`` keyed on
    ``(company_id, numero_controle_pncp, supplier_cnpj)``: a contract seen again
    -- by both endpoints in one run, or on successive days while it stays in the
    window -- collapses to its newest version. Deduplication is per partition,
    and a contract's publication date never changes, so it always lands in the
    same one.

    The single case that does not collapse is a contract whose ``company_id``
    changes, since that is part of the sort key. In practice that is a supplier
    going unmatched -> matched when ``br_companies`` refreshes, and
    ``br_government_contracts`` already filters ``company_id != ''``, so the
    stale row it leaves behind is invisible downstream rather than
    double-counted.
    """
    def _qualified(name: str) -> str:
        return f"`{tables.CLICKHOUSE_DATABASE}`.`{name}`"

    qualified = _qualified(tables.CONTRACTS_TABLE)
    stage_candidates = _qualified(f"_tmp_{tables.CONTRACTS_TABLE}_daily_src")

    usd_conversion.ensure_usd_columns(duckdb_connection)
    stage_columns = (*tables.CANDIDATE_COLUMNS, *FX_COLUMNS)
    rows = duckdb_connection.execute(
        f"select {', '.join(stage_columns)} "
        f"from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    ).fetchall()

    if not rows:
        # Unlike the monthly export there is nothing to protect here -- an
        # append of nothing removes nothing -- but a window that returns no
        # contracts at all means the fetch failed rather than that Brazil
        # awarded none in a week.
        raise ValueError(
            "Brazil PNCP daily window produced no contracts; refusing to treat "
            "an empty fetch as a quiet week"
        )

    clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")
    clickhouse_client.execute(candidate_stage_ddl(stage_candidates))
    try:
        for start in range(0, len(rows), batch_size):
            clickhouse_client.execute(
                f"INSERT INTO {stage_candidates} "
                f"({', '.join(stage_columns)}) VALUES",
                rows[start : start + batch_size],
            )
        clickhouse_client.execute(
            contracts_insert_sql(target_table=qualified, stage_table=stage_candidates)
        )
        inserted = clickhouse_client.execute(
            f"SELECT count(), countIf(company_match_status = 'exact') "
            f"FROM {stage_candidates}"
        )[0]
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")

    return {"contract_rows": int(inserted[0]), "matched_companies": int(inserted[1])}


def export_contracts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    partition: str,
    batch_size: int = 50_000,
) -> dict[str, int]:
    """Replace one month's partition with the candidates currently in DuckDB.

    REPLACE PARTITION rather than a plain insert, so re-running a month yields
    that month rather than two copies of it. Refuses to blank a partition that
    currently holds rows: an empty fetch is a degraded run, not a month in which
    Brazil awarded no contracts.
    """
    def _qualified(name: str) -> str:
        # Quote the whole identifier, never append to an already-quoted one:
        # "`db`.`t`" + "_src" puts the suffix outside the backticks.
        return f"`{tables.CLICKHOUSE_DATABASE}`.`{name}`"

    qualified = _qualified(tables.CONTRACTS_TABLE)
    stage = _qualified(f"_tmp_{tables.CONTRACTS_TABLE}_{partition}")
    stage_candidates = _qualified(f"_tmp_{tables.CONTRACTS_TABLE}_{partition}_src")

    # The FX columns may predate this partition's build, so make sure they
    # exist before selecting them -- an old DuckDB file would otherwise fail
    # here rather than simply exporting NULLs.
    usd_conversion.ensure_usd_columns(duckdb_connection)
    stage_columns = (*tables.CANDIDATE_COLUMNS, *FX_COLUMNS)
    candidates = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"

    # The buffer must hold the month being exported, and nothing else.
    #
    # normalize.py does `create or replace table contract_candidates`, so this is
    # scratch holding whichever month ran last -- not per-partition input.
    # Reading it unfiltered and trusting it to be `partition` is what silently
    # emptied 36 of 37 months on 2026-07-28: every _duckdb run finished the day
    # before the first _clickhouse run, so all 37 exports read the same leftover
    # 2025-01 rows, and REPLACE PARTITION then dropped each target partition and
    # copied stage's same-named (absent, therefore empty) partition into it.
    # ClickHouse reports that as success. Partition 2024-03 had produced 65,218
    # rows and ended up with none.
    #
    # `if not rows` below cannot catch it -- the read returned 116,226 perfectly
    # valid rows, just for the wrong month -- which is the hazard
    # insert_contracts_clickhouse's docstring already warned about: "a
    # full-looking batch would have sailed straight through it". So compare
    # partitions, not row counts.
    #
    # The expression mirrors the table's own PARTITION BY,
    # toYYYYMM(ifNull(data_publicacao_pncp, '1970-01-01')), so a NULL date (the
    # column is a try_cast) reports as 197001 instead of vanishing: no month's
    # export ever targets 197001, so such a row could never be replaced into
    # visibility later.
    buffered = [
        str(value)
        for (value,) in duckdb_connection.execute(
            f"select distinct "
            f"coalesce(strftime(data_publicacao_pncp, '%Y%m'), '197001') as part "
            f"from {candidates} order by part"
        ).fetchall()
    ]
    unexpected = [value for value in buffered if value != partition]
    if unexpected:
        raise ValueError(
            f"Brazil PNCP was asked to export partition {partition} but the "
            f"DuckDB candidates buffer holds {', '.join(unexpected)}. The buffer "
            f"is overwritten per partition, so the export is running against "
            f"another month's rows -- REPLACE PARTITION {partition} would have "
            f"deleted that partition and written nothing. Re-run this partition's "
            f"normalise step immediately before its export."
        )

    rows = duckdb_connection.execute(
        f"select {', '.join(stage_columns)} from {candidates}"
    ).fetchall()

    if not rows:
        existing = clickhouse_client.execute(
            f"SELECT count() FROM {qualified} WHERE "
            f"toYYYYMM(ifNull(data_publicacao_pncp, toDate('1970-01-01'))) = %(p)s",
            {"p": int(partition)},
        )[0][0]
        if int(existing) > 0:
            raise ValueError(
                f"Brazil PNCP produced no contracts for {partition}, but that "
                f"partition holds {existing} rows -- refusing to blank it"
            )

    clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")
    clickhouse_client.execute(candidate_stage_ddl(stage_candidates))
    clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {qualified}")
    try:
        for start in range(0, len(rows), batch_size):
            clickhouse_client.execute(
                f"INSERT INTO {stage_candidates} "
                f"({', '.join(stage_columns)}) VALUES",
                rows[start : start + batch_size],
            )
        clickhouse_client.execute(
            contracts_insert_sql(target_table=stage, stage_table=stage_candidates)
        )
        matched = clickhouse_client.execute(
            f"SELECT count(), countIf(company_match_status = 'exact') FROM {stage}"
        )[0]
        clickhouse_client.execute(
            f"ALTER TABLE {qualified} REPLACE PARTITION {int(partition)} FROM {stage}"
        )
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")

    return {"contract_rows": int(matched[0]), "matched_companies": int(matched[1])}
