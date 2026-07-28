import uuid
from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.france_decp_procurement import tables

_STAGE_TYPES = {
    "holder_ordinal": "Int32",
    "notification_date": "Nullable(Date)",
    "publication_date": "Nullable(Date)",
    "duration_months": "Nullable(Int32)",
    "offers_received": "Nullable(Int32)",
    "contract_amount_eur": "Nullable(Decimal(38, 2))",
    "contract_amount_usd": "Nullable(Decimal(38, 2))",
    "contract_amount_attributable": "UInt8",
    "modification_amount_eur": "Nullable(Decimal(38, 2))",
    "modification_notification_date": "Nullable(Date)",
    "subcontract_amount_eur": "Nullable(Decimal(38, 2))",
    "source_retrieved_at": "DateTime64(3, 'UTC')",
    "resolved_at": "DateTime64(3, 'UTC')",
}


def candidate_stage_ddl(table: str) -> str:
    columns = ",\n    ".join(
        f"{column} {_STAGE_TYPES.get(column, 'String')}"
        for column in tables.CANDIDATE_COLUMNS
    )
    return f"""
    CREATE TABLE {table}
    (
        {columns}
    )
    ENGINE = MergeTree
    ORDER BY source_record_id
    """


def holders_insert_sql(*, target_table: str, candidate_table: str) -> str:
    passthrough = ",\n        ".join(
        f"d.{column}" for column in tables.CANDIDATE_COLUMNS
    )
    return f"""
    INSERT INTO {target_table} ({", ".join(tables.CONTRACT_HOLDER_COLUMNS)})
    SELECT
        if(c.siren != '', c.siren, '') AS company_id,
        multiIf(
            d.match_eligibility != 'eligible', d.match_eligibility,
            c.siren != '', 'exact',
            'unmatched_company'
        ) AS company_match_status,
        {passthrough}
    FROM {candidate_table} AS d
    LEFT ANY JOIN
    (
        SELECT siren
        FROM corpscout.fr_companies
        WHERE siren IN (
            SELECT holder_siren FROM {candidate_table} WHERE holder_siren != ''
        )
    ) AS c
        ON c.siren = d.holder_siren
    """


def export_decp_contract_holders(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.CONTRACT_HOLDERS_TABLE, "fr_companies"),
    )
    suffix = uuid.uuid4().hex
    candidate_name = f"_tmp_decp_candidates_{suffix}"
    candidates = _qualified(candidate_name)
    target_stage = _qualified(f"_tmp_{tables.CONTRACT_HOLDERS_TABLE}_{suffix}")
    target = _qualified(tables.CONTRACT_HOLDERS_TABLE)
    with clickhouse.get_connection() as client:
        client.execute(candidate_stage_ddl(candidates))
        client.execute(f"CREATE TABLE {target_stage} AS {target}")
        try:
            candidate_rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=tables.DUCKDB_SCHEMA,
                duckdb_table=tables.CANDIDATES_TABLE,
                clickhouse_database=tables.CLICKHOUSE_DATABASE,
                clickhouse_table=candidate_name,
                columns=tables.CANDIDATE_COLUMNS,
                truncate=False,
            )
            client.execute(
                holders_insert_sql(
                    target_table=target_stage, candidate_table=candidates
                )
            )
            rows, matched, unmatched, contracts = client.execute(
                f"""
                SELECT
                    count(),
                    countIf(company_match_status = 'exact'),
                    countIf(company_match_status = 'unmatched_company'),
                    uniqExact(contract_id)
                FROM {target_stage}
                """
            )[0]
            if int(rows) != int(candidate_rows):
                raise ValueError(
                    "DECP publish row mismatch: "
                    f"candidates={candidate_rows} published={rows}"
                )
            client.execute(f"EXCHANGE TABLES {target_stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {target_stage}")
            client.execute(f"DROP TABLE IF EXISTS {candidates}")
    counts = {
        "holder_rows": int(rows),
        "matched_holders": int(matched),
        "unmatched_holders": int(unmatched),
        "contracts": int(contracts),
    }
    if log is not None:
        log("Published DECP contract holders: %s", counts)
    return counts


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
