from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.clickhouse.resolved import (
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.company_domain_suggestions import scoring, tables


def publish_country_suggestions(
    duckdb_connection: Any,
    clickhouse_client: Any,
    *,
    discovery_run_id: str,
    started_at: datetime,
    completed_at: datetime | None = None,
    allow_empty: bool = False,
    batch_size: int = 50_000,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    publish_started_at = time.monotonic()
    completed_timestamp = completed_at or datetime.now(UTC)
    schema = tables.DUCKDB_SCHEMA
    _log(log, "Sweden domain suggestion publication validation started")
    suggestion_count = _duckdb_count(
        duckdb_connection, f"{schema}.{tables.SUGGESTIONS_TABLE}"
    )
    evidence_count = _duckdb_count(
        duckdb_connection, f"{schema}.{tables.EVIDENCE_TABLE}"
    )
    company_count = _duckdb_count(duckdb_connection, f"{schema}.companies")
    metrics_row = duckdb_connection.execute(
        f"""
        select candidate_pairs, disqualified_candidates
        from {schema}.run_metrics
        """
    ).fetchone()
    if metrics_row is None:
        raise ValueError("DuckDB suggestion run metrics are missing")
    candidate_pair_count = int(metrics_row[0])
    disqualified_count = int(metrics_row[1])

    if suggestion_count == 0 and not allow_empty:
        raise ValueError(
            "Sweden company-domain scoring produced no suggestions; refusing to "
            "replace the current country snapshot"
        )
    if evidence_count < suggestion_count:
        raise ValueError(
            "Every company-domain suggestion must have retained evidence: "
            f"suggestions={suggestion_count} evidence={evidence_count}"
        )
    duplicate_count = _duckdb_scalar(
        duckdb_connection,
        f"""
        select count(*)
        from (
            select company_id, root_domain
            from {schema}.{tables.SUGGESTIONS_TABLE}
            group by company_id, root_domain
            having count(*) > 1
        )
        """,
    )
    if duplicate_count:
        raise ValueError(
            "Company-domain suggestion output contains duplicate pairs: "
            f"duplicates={duplicate_count}"
        )
    _log(
        log,
        "Sweden domain suggestion publication validation completed: companies=%d "
        "candidate_pairs=%d suggestions=%d evidence=%d",
        company_count,
        candidate_pair_count,
        suggestion_count,
        evidence_count,
    )

    stage_by_table = {
        table: f"_tmp_{table}_{uuid.uuid4().hex}"
        for table in (tables.EVIDENCE_TABLE, tables.SUGGESTIONS_TABLE)
    }
    exchanged: list[str] = []
    primary_error: BaseException | None = None
    try:
        _log(log, "Sweden domain suggestion publication staging started")
        for table, stage in stage_by_table.items():
            clickhouse_client.execute(
                f"CREATE TABLE {tables.CLICKHOUSE_DATABASE}.{stage} AS "
                f"{tables.CLICKHOUSE_DATABASE}.{table}"
            )
            clickhouse_client.execute(
                f"INSERT INTO {tables.CLICKHOUSE_DATABASE}.{stage} "
                f"SELECT * FROM {tables.CLICKHOUSE_DATABASE}.{table} "
                "WHERE country_iso2 != %(country_iso2)s",
                {"country_iso2": tables.COUNTRY_ISO2},
            )
        _log(log, "Sweden domain suggestion publication staging completed")

        _log(
            log,
            "Sweden domain suggestion publication export started: table=%s rows=%d",
            tables.EVIDENCE_TABLE,
            evidence_count,
        )
        export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=schema,
            duckdb_table=tables.EVIDENCE_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=stage_by_table[tables.EVIDENCE_TABLE],
            columns=tables.EVIDENCE_COLUMNS,
            truncate=False,
            batch_size=batch_size,
            log=log,
        )
        _log(
            log,
            "Sweden domain suggestion publication export completed: table=%s rows=%d",
            tables.EVIDENCE_TABLE,
            evidence_count,
        )
        _log(
            log,
            "Sweden domain suggestion publication export started: table=%s rows=%d",
            tables.SUGGESTIONS_TABLE,
            suggestion_count,
        )
        export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=schema,
            duckdb_table=tables.SUGGESTIONS_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=stage_by_table[tables.SUGGESTIONS_TABLE],
            columns=tables.SUGGESTION_COLUMNS,
            truncate=False,
            batch_size=batch_size,
            log=log,
        )
        _log(
            log,
            "Sweden domain suggestion publication export completed: table=%s rows=%d",
            tables.SUGGESTIONS_TABLE,
            suggestion_count,
        )

        _log(log, "Sweden domain suggestion publication stage validation started")
        for table, expected in (
            (tables.EVIDENCE_TABLE, evidence_count),
            (tables.SUGGESTIONS_TABLE, suggestion_count),
        ):
            stage_country_count = _clickhouse_scalar(
                clickhouse_client,
                f"""
                SELECT count()
                FROM {tables.CLICKHOUSE_DATABASE}.{stage_by_table[table]}
                WHERE country_iso2 = %(country_iso2)s
                """,
                {"country_iso2": tables.COUNTRY_ISO2},
            )
            if stage_country_count != expected:
                raise ValueError(
                    f"Country stage row count mismatch for {table}: "
                    f"expected={expected} actual={stage_country_count}"
                )
        _log(log, "Sweden domain suggestion publication stage validation completed")

        _log(log, "Sweden domain suggestion publication exchange started")
        for table in (tables.EVIDENCE_TABLE, tables.SUGGESTIONS_TABLE):
            clickhouse_client.execute(
                f"EXCHANGE TABLES {tables.CLICKHOUSE_DATABASE}.{stage_by_table[table]} "
                f"AND {tables.CLICKHOUSE_DATABASE}.{table}"
            )
            exchanged.append(table)
        _log(log, "Sweden domain suggestion publication exchange completed")

        configuration = {
            "max_companies_per_name_feature": (scoring.MAX_COMPANIES_PER_NAME_FEATURE),
            "max_companies_per_person_feature": (
                scoring.MAX_COMPANIES_PER_PERSON_FEATURE
            ),
            "supporting_signals_generate_candidates": False,
        }
        run_row = (
            tables.COUNTRY_ISO2,
            discovery_run_id,
            tables.SCORING_VERSION,
            company_count,
            candidate_pair_count,
            disqualified_count,
            suggestion_count,
            evidence_count,
            json.dumps(configuration, sort_keys=True, separators=(",", ":")),
            started_at,
            completed_timestamp,
        )
        clickhouse_client.execute(
            f"INSERT INTO {tables.QUALIFIED_RUNS_TABLE} "
            f"({', '.join(tables.RUN_COLUMNS)}) VALUES",
            [run_row],
        )
        _log(log, "Sweden domain suggestion discovery run record inserted")
    except BaseException as exc:
        primary_error = exc
        rollback_failures: list[str] = []
        for table in reversed(exchanged):
            try:
                clickhouse_client.execute(
                    f"EXCHANGE TABLES "
                    f"{tables.CLICKHOUSE_DATABASE}.{stage_by_table[table]} "
                    f"AND {tables.CLICKHOUSE_DATABASE}.{table}"
                )
            except Exception:
                rollback_failures.append(table)
        if rollback_failures:
            raise RuntimeError(
                "Failed to roll back company-domain suggestion snapshot tables: "
                + ", ".join(rollback_failures)
            ) from exc
        raise
    finally:
        for stage in reversed(tuple(stage_by_table.values())):
            try:
                clickhouse_client.execute(
                    f"DROP TABLE IF EXISTS {tables.CLICKHOUSE_DATABASE}.{stage}"
                )
            except Exception:
                if primary_error is None:
                    raise

    counts: dict[str, int | float] = {
        "companies": company_count,
        "candidate_pairs": candidate_pair_count,
        "disqualified_candidates": disqualified_count,
        "suggestions": suggestion_count,
        "evidence": evidence_count,
        "publish_elapsed_seconds": round(time.monotonic() - publish_started_at, 3),
    }
    _log(log, "Sweden domain suggestion publication completed: counts=%s", counts)
    return counts


def _duckdb_count(connection: Any, table: str, where: str = "") -> int:
    predicate = f" where {where}" if where else ""
    return _duckdb_scalar(connection, f"select count(*) from {table}{predicate}")


def _duckdb_scalar(connection: Any, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    return int(row[0]) if row is not None else 0


def _clickhouse_scalar(
    clickhouse_client: Any,
    sql: str,
    params: dict[str, object],
) -> int:
    rows = clickhouse_client.execute(sql, params)
    return int(rows[0][0]) if rows else 0


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)
