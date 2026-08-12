import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_v3.defs.company_domain_suggestions import tables


DBT_CHUNK_COUNT = 1


def complete_sweden_dbt_discovery_run(
    clickhouse_client: Any,
    *,
    discovery_run_id: str,
    completed_at: datetime,
    allow_empty: bool = False,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    """Validate one deterministic identifier/address run and activate its outputs."""
    params = {
        "country_iso2": tables.COUNTRY_ISO2,
        "discovery_run_id": discovery_run_id,
    }
    company_count = _scalar(
        clickhouse_client,
        "SELECT count() FROM corpscout.se_companies FINAL WHERE company_id != ''",
        params,
    )
    identifier_candidate_pair_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_IDENTIFIER_CANDIDATES_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    address_candidate_pair_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    candidate_pair_count = (
        identifier_candidate_pair_count + address_candidate_pair_count
    )
    identifier_directory_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_IDENTIFIER_CANDIDATES_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
          AND match_status = 'directory'
        """,
        params,
    )
    address_directory_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
          AND match_status = 'directory'
        """,
        params,
    )
    address_identifier_conflict_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE} AS address
        INNER JOIN {tables.QUALIFIED_DBT_IDENTIFIER_CANDIDATES_TABLE} AS identifier
            ON identifier.country_iso2 = address.country_iso2
           AND identifier.discovery_run_id = address.discovery_run_id
           AND identifier.chunk_id = address.chunk_id
           AND identifier.company_id = address.company_id
        WHERE address.country_iso2 = %(country_iso2)s
          AND address.discovery_run_id = %(discovery_run_id)s
          AND address.match_status = 'unique'
          AND identifier.match_status = 'unique'
          AND identifier.root_domain != address.root_domain
        """,
        params,
    )
    disqualified_count = (
        identifier_directory_count
        + address_directory_count
        + address_identifier_conflict_count
    )
    matched_company_count = _scalar(
        clickhouse_client,
        f"""
        SELECT uniqExact(company_id)
        FROM
        (
            SELECT company_id
            FROM {tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
            UNION ALL
            SELECT company_id
            FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
        )
        """,
        params,
    )
    ambiguous_company_count = _scalar(
        clickhouse_client,
        f"""
        SELECT uniqExact(company_id)
        FROM
        (
            SELECT company_id
            FROM {tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
              AND match_status = 'ambiguous'
            UNION ALL
            SELECT company_id
            FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
              AND match_status = 'ambiguous'
        ) AS ambiguous
        WHERE company_id NOT IN
        (
            SELECT company_id
            FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
        )
        """,
        params,
    )
    address_nace_suggestion_count = _scalar(
        clickhouse_client,
        f"""
        SELECT uniqExactIf(
            company_id,
            has(candidate_sources, 'address') AND identifier_score = 0
        )
        FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    address_nace_confirmation_count = _scalar(
        clickhouse_client,
        f"""
        SELECT uniqExactIf(
            company_id,
            has(candidate_sources, 'address') AND identifier_score > 0
        )
        FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    directory_only_company_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM
        (
            SELECT company_id
            FROM
            (
                SELECT company_id, match_status
                FROM {tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE}
                WHERE country_iso2 = %(country_iso2)s
                  AND discovery_run_id = %(discovery_run_id)s
                UNION ALL
                SELECT company_id, match_status
                FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}
                WHERE country_iso2 = %(country_iso2)s
                  AND discovery_run_id = %(discovery_run_id)s
            ) AS candidates
            WHERE company_id NOT IN
            (
                SELECT company_id
                FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
                WHERE country_iso2 = %(country_iso2)s
                  AND discovery_run_id = %(discovery_run_id)s
            )
            GROUP BY company_id
            HAVING countIf(match_status != 'directory') = 0
        )
        """,
        params,
    )
    unmatched_company_count = max(company_count - matched_company_count, 0)
    suggestion_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    evidence_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_EVIDENCE_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )
    duplicate_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM
        (
            SELECT company_id, root_domain
            FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
            WHERE country_iso2 = %(country_iso2)s
              AND discovery_run_id = %(discovery_run_id)s
            GROUP BY company_id, root_domain
            HAVING count() > 1
        )
        """,
        params,
    )
    if suggestion_count == 0 and not allow_empty:
        raise ValueError("dbt company-domain suggestion run produced no suggestions")
    if evidence_count < suggestion_count:
        raise ValueError(
            "dbt company-domain suggestion evidence is incomplete: "
            f"suggestions={suggestion_count} evidence={evidence_count}"
        )
    if duplicate_count != 0:
        raise ValueError(
            "dbt company-domain suggestion output contains duplicate pairs: "
            f"duplicates={duplicate_count}"
        )

    started_at = _datetime_scalar(
        clickhouse_client,
        f"""
        SELECT min(suggested_at)
        FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}
        WHERE country_iso2 = %(country_iso2)s
          AND discovery_run_id = %(discovery_run_id)s
        """,
        params,
    ) or completed_at
    legacy_suggestion_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_SUGGESTIONS_TABLE}
        WHERE country_iso2 = %(country_iso2)s
        """,
        params,
    )
    overlapping_suggestion_count = _scalar(
        clickhouse_client,
        f"""
        SELECT count()
        FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE} AS dbt
        INNER JOIN {tables.QUALIFIED_SUGGESTIONS_TABLE} AS legacy
            ON legacy.country_iso2 = dbt.country_iso2
           AND legacy.company_id = dbt.company_id
           AND legacy.root_domain = dbt.root_domain
        WHERE dbt.country_iso2 = %(country_iso2)s
          AND dbt.discovery_run_id = %(discovery_run_id)s
        """,
        params,
    )

    configuration = {
        "chunk_count": DBT_CHUNK_COUNT,
        "country_iso2": tables.COUNTRY_ISO2,
        "dbt_project": "company_domain_suggestions",
        "deterministic_identifier_types": ["vat", "lei"],
        "deterministic_joint_signals": ["normalized_address", "nace_exact"],
        "address_score": 35,
        "industry_score": 25,
        "max_domains_per_address": tables.MAX_DOMAINS_PER_ADDRESS,
        "max_identifiers_per_domain": tables.MAX_IDENTIFIERS_PER_DOMAIN,
        "identifier_precedence": True,
        "supporting_signals_generate_candidates": False,
    }
    clickhouse_client.execute(
        f"""
        INSERT INTO {tables.QUALIFIED_DBT_RUNS_TABLE}
        (
            country_iso2,
            discovery_run_id,
            scoring_version,
            chunk_count,
            company_count,
            matched_company_count,
            ambiguous_company_count,
            directory_only_company_count,
            unmatched_company_count,
            candidate_pair_count,
            disqualified_candidate_count,
            suggestion_count,
            evidence_count,
            legacy_suggestion_count,
            overlapping_suggestion_count,
            configuration_json,
            started_at,
            completed_at
        ) VALUES
        """,
        [
            (
                tables.COUNTRY_ISO2,
                discovery_run_id,
                tables.DBT_SCORING_VERSION,
                DBT_CHUNK_COUNT,
                company_count,
                matched_company_count,
                ambiguous_company_count,
                directory_only_company_count,
                unmatched_company_count,
                candidate_pair_count,
                disqualified_count,
                suggestion_count,
                evidence_count,
                legacy_suggestion_count,
                overlapping_suggestion_count,
                json.dumps(configuration, sort_keys=True, separators=(",", ":")),
                started_at,
                completed_at,
            )
        ],
    )
    counts: dict[str, int | float] = {
        "companies": company_count,
        "matched_companies": matched_company_count,
        "ambiguous_companies": ambiguous_company_count,
        "directory_only_companies": directory_only_company_count,
        "unmatched_companies": unmatched_company_count,
        "unresolved_companies": (
            ambiguous_company_count
            + directory_only_company_count
            + unmatched_company_count
        ),
        "candidate_pairs": candidate_pair_count,
        "disqualified_candidates": disqualified_count,
        "identifier_candidate_pairs": identifier_candidate_pair_count,
        "address_nace_candidate_pairs": address_candidate_pair_count,
        "address_nace_suggestions": address_nace_suggestion_count,
        "address_nace_confirmations": address_nace_confirmation_count,
        "address_identifier_conflicts": address_identifier_conflict_count,
        "suggestions": suggestion_count,
        "evidence": evidence_count,
        "legacy_suggestions": legacy_suggestion_count,
        "overlapping_suggestions": overlapping_suggestion_count,
        "legacy_overlap_percentage": _percentage(
            overlapping_suggestion_count,
            legacy_suggestion_count,
        ),
        "dbt_overlap_percentage": _percentage(
            overlapping_suggestion_count,
            suggestion_count,
        ),
    }
    if log is not None:
        log("Completed Sweden deterministic company-domain matching run: counts=%s", counts)
    return counts


def _scalar(
    clickhouse_client: Any,
    sql: str,
    params: dict[str, object],
) -> int:
    rows = clickhouse_client.execute(sql, params)
    return int(rows[0][0]) if rows else 0


def _datetime_scalar(
    clickhouse_client: Any,
    sql: str,
    params: dict[str, object],
) -> datetime | None:
    rows = clickhouse_client.execute(sql, params)
    if not rows or rows[0][0] is None:
        return None
    value = rows[0][0]
    if not isinstance(value, datetime):
        raise TypeError(f"Expected ClickHouse datetime, received {type(value).__name__}")
    return value


def _percentage(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 3)
