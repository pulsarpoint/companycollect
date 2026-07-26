import uuid
from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_uhm_procurement import tables


def uhm_awards_insert_sql(*, candidate_table: str, awards_stage: str) -> str:
    return f"""
    INSERT INTO {awards_stage} ({", ".join(tables.AWARDS_COLUMNS)})
    SELECT
        if(
            u.match_eligibility = 'eligible'
            AND c.company_id != ''
            AND length(c.company_id) = 10,
            c.company_id,
            ''
        ) AS company_id,
        multiIf(
            u.match_eligibility != 'eligible',
                u.match_eligibility,
            c.company_id != '' AND length(c.company_id) = 10,
                'exact',
            'unmatched_company'
        ) AS company_match_status,
        u.match_eligibility,
        u.source_slug,
        u.source_run_id,
        u.source_record_id,
        u.source_line_number,
        u.source_procurement_id,
        u.source_lot_id,
        u.publication_date,
        u.title,
        u.agreement_type,
        u.contracted,
        u.buyer_name,
        u.buyer_id_normalized,
        u.supplier_name,
        u.supplier_id_normalized,
        u.cpv_code,
        u.advertising_database,
        u.source_url,
        u.source_object_key,
        u.source_retrieved_at,
        u.resolved_at
    FROM {candidate_table} AS u
    LEFT ANY JOIN corpscout.se_companies AS c
        ON c.company_id = u.supplier_id_normalized
    """


def export_uhm_awards_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Publish every UHM observation and annotate exact ``se_companies`` matches."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.AWARDS_TABLE, "se_companies"),
    )
    candidate_stage = f"_tmp_uhm_candidates_{uuid.uuid4().hex}"
    awards_stage = f"_tmp_{tables.AWARDS_TABLE}_{uuid.uuid4().hex}"
    qualified_candidates = _qualified(candidate_stage)
    qualified_awards_stage = _qualified(awards_stage)
    qualified_awards = _qualified(tables.AWARDS_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(
            f"""
            CREATE TABLE {qualified_candidates}
            (
                source_slug LowCardinality(String),
                source_run_id String,
                source_record_id String,
                source_line_number UInt64,
                source_procurement_id String,
                source_lot_id String,
                publication_date Nullable(Date),
                title String,
                agreement_type LowCardinality(String),
                contracted UInt8,
                buyer_name String,
                buyer_id_normalized String,
                supplier_name String,
                supplier_id_normalized String,
                cpv_code String,
                advertising_database LowCardinality(String),
                source_url String,
                source_object_key String,
                source_retrieved_at DateTime64(3, 'UTC'),
                resolved_at DateTime64(3, 'UTC'),
                match_eligibility LowCardinality(String)
            )
            ENGINE = MergeTree
            ORDER BY source_record_id
            """
        )
        client.execute(f"CREATE TABLE {qualified_awards_stage} AS {qualified_awards}")
        try:
            candidate_rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=tables.DUCKDB_SCHEMA,
                duckdb_table=tables.CANDIDATES_TABLE,
                clickhouse_database=tables.CLICKHOUSE_DATABASE,
                clickhouse_table=candidate_stage,
                columns=tables.CANDIDATE_COLUMNS,
                truncate=False,
            )
            [(eligible_rows, matched_rows, unmatched_company_rows)] = client.execute(
                f"""
                SELECT
                    countIf(u.match_eligibility = 'eligible'),
                    countIf(
                        u.match_eligibility = 'eligible'
                        AND c.company_id != ''
                        AND length(c.company_id) = 10
                    ),
                    countIf(
                        u.match_eligibility = 'eligible'
                        AND c.company_id = ''
                    )
                FROM {qualified_candidates} AS u
                LEFT ANY JOIN corpscout.se_companies AS c
                    ON c.company_id = u.supplier_id_normalized
                """
            )
            client.execute(
                uhm_awards_insert_sql(
                    candidate_table=qualified_candidates,
                    awards_stage=qualified_awards_stage,
                )
            )
            [(awards_rows, exact_rows, distinct_companies)] = client.execute(
                f"""
                SELECT
                    count(),
                    countIf(company_match_status = 'exact'),
                    uniqExactIf(company_id, company_match_status = 'exact')
                FROM {qualified_awards_stage}
                """
            )
            if int(awards_rows) == 0:
                raise ValueError(
                    "UHM exact company matching produced zero awards; "
                    "refusing to replace the published table"
                )
            if int(awards_rows) != int(candidate_rows):
                raise ValueError(
                    "UHM stage mismatch: "
                    f"candidates={candidate_rows} published={awards_rows}"
                )
            if int(exact_rows) != int(matched_rows):
                raise ValueError(
                    "UHM exact-match mismatch: "
                    f"matched={matched_rows} published_exact={exact_rows}"
                )
            client.execute(
                f"EXCHANGE TABLES {qualified_awards_stage} AND {qualified_awards}"
            )
        finally:
            client.execute(f"DROP TABLE IF EXISTS {qualified_awards_stage}")
            client.execute(f"DROP TABLE IF EXISTS {qualified_candidates}")

    counts = {
        "candidate_rows": int(candidate_rows),
        "eligible_rows": int(eligible_rows),
        "matched_rows": int(matched_rows),
        "unmatched_company_rows": int(unmatched_company_rows),
        "awards_rows": int(awards_rows),
        "exact_rows": int(exact_rows),
        "distinct_companies": int(distinct_companies),
    }
    if log is not None:
        log("Published UHM awards: %s", counts)
    return counts


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
