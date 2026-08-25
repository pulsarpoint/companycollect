import re
import time
import uuid
from collections.abc import Callable
from typing import Any

from dagster_v3.defs.company_serving import tables


def publish_company_serving_country(
    client: Any,
    *,
    country_code: str,
    source_run_id: str,
    allow_empty: bool = False,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    if re.fullmatch(r"[A-Z]{2}", country_code) is None:
        raise ValueError(f"Invalid country code: {country_code!r}")
    if country_code != tables.COUNTRY_CODE:
        raise ValueError("The first company-serving publication supports Sweden only")

    started_at = time.monotonic()
    suffix = uuid.uuid4().hex
    stages = {
        contract.name: _qualified(f"_tmp_{contract.name}_{suffix}")
        for contract in tables.CURRENT_TABLES
    }
    backups = {
        contract.name: _qualified(f"_tmp_{contract.name}_backup_{suffix}")
        for contract in tables.CURRENT_TABLES
        if contract.partitioned
    }
    exchanged_unpartitioned: list[tables.CurrentTable] = []
    replaced_partitioned: list[tables.CurrentTable] = []
    counts: dict[str, int | float] = {}
    primary_error: BaseException | None = None

    try:
        _log(log, "Company serving stage build started: country=%s", country_code)
        for contract in tables.CURRENT_TABLES:
            stage = stages[contract.name]
            client.execute(f"CREATE TABLE {stage} AS {contract.qualified_name}")
            columns = ", ".join(contract.columns)
            if contract is tables.DOMAINS:
                _insert_company_domain_stage(
                    client,
                    stage=stage,
                    country_code=country_code,
                )
            else:
                country_filter = (
                    " WHERE country_code = %(country_code)s"
                    if contract.partitioned
                    else ""
                )
                client.execute(
                    f"INSERT INTO {stage} ({columns}) "
                    f"SELECT {columns} FROM {contract.qualified_build_model}"
                    f"{country_filter}",
                    {"country_code": country_code},
                )
            row_count = _scalar(client, f"SELECT count() FROM {stage}")
            counts[f"{contract.name}_rows"] = row_count
            _validate_stage(
                client,
                contract=contract,
                stage=stage,
                row_count=row_count,
                allow_empty=allow_empty,
            )
            _log(
                log,
                "Company serving stage validated: table=%s rows=%d",
                contract.name,
                row_count,
            )

        _validate_anchor_completeness(client, stages[tables.EXTERNAL_IDENTIFIERS.name])
        _validate_sections(client, stages[tables.PRESENCE.name])
        _validate_source_links(client, stages[tables.SOURCE_LINKS.name])
        _validate_presence_counts(client, stages=stages, country_code=country_code)
        _log(log, "Company serving cross-table validation completed")

        _append_changed_observations(
            client,
            stages=stages,
            country_code=country_code,
            source_run_id=source_run_id,
            log=log,
        )

        # Presence is last in CURRENT_TABLES. A request cannot discover a new
        # section until every table backing that section is already published.
        for contract in tables.CURRENT_TABLES:
            target = contract.qualified_name
            stage = stages[contract.name]
            if contract is tables.DOMAINS:
                columns = ", ".join(contract.columns)
                client.execute(
                    f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {stage}"
                )
            elif contract.partitioned:
                backup = backups[contract.name]
                client.execute(f"CREATE TABLE {backup} AS {target}")
                client.execute(
                    f"ALTER TABLE {backup} REPLACE PARTITION '{country_code}' FROM {target}"
                )
                client.execute(
                    f"ALTER TABLE {target} REPLACE PARTITION '{country_code}' FROM {stage}"
                )
                replaced_partitioned.append(contract)
            else:
                client.execute(f"EXCHANGE TABLES {stage} AND {target}")
                exchanged_unpartitioned.append(contract)
            _log(log, "Company serving table published: table=%s", contract.name)
    except BaseException as exc:
        primary_error = exc
        rollback_failures: list[str] = []
        for contract in reversed(exchanged_unpartitioned):
            try:
                client.execute(
                    f"EXCHANGE TABLES {stages[contract.name]} AND {contract.qualified_name}"
                )
            except Exception:
                rollback_failures.append(contract.name)
        for contract in reversed(replaced_partitioned):
            try:
                client.execute(
                    f"ALTER TABLE {contract.qualified_name} REPLACE PARTITION "
                    f"'{country_code}' FROM {backups[contract.name]}"
                )
            except Exception:
                rollback_failures.append(contract.name)
        if rollback_failures:
            raise RuntimeError(
                "Company-serving publication rollback failed for: "
                + ", ".join(rollback_failures)
            ) from exc
        raise
    finally:
        for temporary in (*stages.values(), *backups.values()):
            try:
                client.execute(f"DROP TABLE IF EXISTS {temporary}")
            except Exception:
                if primary_error is None:
                    raise

    counts["publish_elapsed_seconds"] = round(time.monotonic() - started_at, 3)
    _log(
        log,
        "Company serving publication completed: country=%s metrics=%s",
        country_code,
        counts,
    )
    return counts


def _insert_company_domain_stage(
    client: Any,
    *,
    stage: str,
    country_code: str,
) -> None:
    """Stage fresh source evidence with the latest human review state.

    dbt can finish before the serving publisher starts. Reading review fields
    again here prevents a decision made in that interval from being replaced
    by the older dbt snapshot. The published rows retain the build's source
    evidence and become newer ReplacingMergeTree versions.
    """
    client.execute(
        f"""INSERT INTO {stage} ({", ".join(tables.DOMAINS.columns)})
SELECT
    staged.country_code,
    staged.company_id,
    staged.root_domain,
    staged.website_url,
    staged.website_host,
    staged.source_names,
    staged.source_confidences,
    staged.source_record_ids,
    staged.source_urls,
    staged.confidence_bases,
    staged.suggested_confidence,
    staged.suggested_primary,
    staged.evidence_fingerprint,
    if(current.root_domain != '', current.review_status, staged.review_status),
    if(current.root_domain != '', current.review_note, staged.review_note),
    if(current.root_domain != '', current.reviewed_by, staged.reviewed_by),
    if(current.root_domain != '', current.reviewed_at, staged.reviewed_at),
    if(
        current.root_domain != '',
        current.reviewed_evidence_fingerprint,
        staged.reviewed_evidence_fingerprint
    ),
    staged.is_active,
    staged.first_seen_at,
    staged.last_seen_at,
    now64(3, 'UTC')
FROM {tables.DOMAINS.qualified_build_model} AS staged
LEFT JOIN {tables.DOMAINS.qualified_name} AS current FINAL
    ON current.country_code = staged.country_code
   AND current.company_id = staged.company_id
   AND current.root_domain = staged.root_domain
WHERE staged.country_code = %(country_code)s""",
        {"country_code": country_code},
    )


def _validate_stage(
    client: Any,
    *,
    contract: tables.CurrentTable,
    stage: str,
    row_count: int,
    allow_empty: bool,
) -> None:
    if contract.required and row_count == 0 and not allow_empty:
        raise ValueError(
            f"Refusing to publish empty required serving table {contract.name}"
        )
    key_sql = ", ".join(contract.key_columns)
    duplicates = _scalar(
        client,
        f"SELECT count() FROM (SELECT {key_sql} FROM {stage} "
        f"GROUP BY {key_sql} HAVING count() > 1)",
    )
    if duplicates:
        raise ValueError(
            f"Serving stage {contract.name} has {duplicates} duplicate keys"
        )
    orphans = _scalar(
        client,
        f"SELECT count() FROM {stage} AS serving "
        "LEFT JOIN corpscout.se_companies AS company FINAL "
        "ON company.company_id = serving.company_id "
        "WHERE ifNull(company.company_id, '') = ''",
    )
    if orphans:
        raise ValueError(
            f"Serving stage {contract.name} has {orphans} rows without se_companies anchor"
        )


def _validate_anchor_completeness(client: Any, identifier_stage: str) -> None:
    anchors = _scalar(client, "SELECT count() FROM corpscout.se_companies FINAL")
    served = _scalar(
        client,
        f"SELECT countDistinct(company_id) FROM {identifier_stage} "
        "WHERE identifier_scheme = 'national_registry'",
    )
    if anchors != served:
        raise ValueError(
            f"Company anchor reconciliation failed: se_companies={anchors} serving={served}"
        )


def _validate_sections(client: Any, presence_stage: str) -> None:
    allowed = ", ".join(f"'{value}'" for value in tables.VALID_SECTIONS)
    invalid = _scalar(
        client,
        f"SELECT count() FROM {presence_stage} "
        f"WHERE section NOT IN ({allowed}) OR item_count = 0",
    )
    if invalid:
        raise ValueError(f"Section-presence stage contains {invalid} invalid rows")


def _validate_source_links(client: Any, link_stage: str) -> None:
    missing = _scalar(
        client,
        f"SELECT count() FROM {link_stage} "
        "WHERE source_record_uid = '' OR record_kind = '' "
        "OR source_slug = '' OR source_record_key = '' OR payload_sha256 = ''",
    )
    if missing:
        raise ValueError(f"Section evidence stage has {missing} missing source records")


def _validate_presence_counts(
    client: Any,
    *,
    stages: dict[str, str],
    country_code: str,
) -> None:
    expected_queries = {
        "gleif": (
            "SELECT countDistinct(tuple(company_id, item_key)) FROM ("
            f"SELECT company_id, concat('entity:', lei) AS item_key FROM {stages[tables.GLEIF.name]} "
            "UNION ALL "
            f"SELECT company_id, concat('relationship:', relationship_id) AS item_key FROM {stages[tables.GLEIF_RELATIONSHIPS.name]}"
            ")"
        ),
        "wikidata": (
            "SELECT countDistinct(tuple(company_id, wikidata_id)) "
            f"FROM {stages[tables.WIKIDATA.name]}"
        ),
        "management": (
            "SELECT countDistinct(tuple(company_id, management_id)) "
            f"FROM {stages[tables.MANAGEMENT.name]}"
        ),
        "descriptions": (
            "SELECT countDistinct(tuple(company_id, description_id)) "
            f"FROM {stages[tables.DESCRIPTIONS.name]}"
        ),
        "domains": (
            "SELECT countDistinct(tuple(company_id, item_key)) FROM ("
            f"SELECT company_id, concat('domain:', root_domain) AS item_key FROM {stages[tables.DOMAINS.name]} "
            "UNION ALL "
            f"SELECT company_id, concat('contact:', contact_id) AS item_key FROM {stages[tables.CONTACTS.name]}"
            ")"
        ),
        "contracts": (
            "SELECT countDistinct(tuple(company_id, contract_ref)) "
            f"FROM {stages[tables.CONTRACTS.name]}"
        ),
        "financials": (
            "SELECT countDistinct(financials.company_id) "
            "FROM corpscout.se_company_financials_latest AS financials "
            f"INNER JOIN (SELECT DISTINCT company_id FROM {stages[tables.EXTERNAL_IDENTIFIERS.name]}) AS anchors "
            "ON anchors.company_id = financials.company_id"
        ),
        "industries": (
            "SELECT countDistinct(tuple(company_id, classification_code)) "
            f"FROM {stages[tables.INDUSTRIES.name]}"
        ),
        # The display table this counted is retired (migration 000314), so this reconciles
        # against the source the section-presence model now reads, with the same filter and
        # the same key. Anchored like financials and sources: se_company_addresses_current
        # is not a serving stage, so the anchor join is spelled out here.
        "addresses": (
            "SELECT countDistinct(tuple(addresses.company_id, addresses.address_fingerprint)) "
            "FROM corpscout.se_company_addresses_current AS addresses "
            f"INNER JOIN (SELECT DISTINCT company_id FROM {stages[tables.EXTERNAL_IDENTIFIERS.name]}) AS anchors "
            "ON anchors.company_id = addresses.company_id "
            "WHERE addresses.has_address = 1 AND addresses.has_observation = 1"
        ),
        "sources": (
            "SELECT countDistinct(tuple(company_id, source_record_uid)) "
            f"FROM {stages[tables.SOURCE_LINKS.name]} "
            f"WHERE country_code = '{country_code}'"
        ),
        "technology": (
            "SELECT countDistinct(tuple(company_id, root_domain)) "
            f"FROM {stages[tables.DOMAINS.name]}"
        ),
    }
    presence_stage = stages[tables.PRESENCE.name]
    for section, expected_query in expected_queries.items():
        expected_rows = client.execute(expected_query)
        expected = sum(int(row[0]) for row in expected_rows)
        actual = _scalar(
            client,
            f"SELECT coalesce(sum(item_count), 0) FROM {presence_stage} "
            f"WHERE section = '{section}'",
        )
        if actual != expected:
            raise ValueError(
                f"Section-presence reconciliation failed for {section}: "
                f"expected={expected} actual={actual}"
            )


def _append_changed_observations(
    client: Any,
    *,
    stages: dict[str, str],
    country_code: str,
    source_run_id: str,
    log: Callable[..., object] | None,
) -> None:
    for contract in tables.CURRENT_TABLES:
        observation_table = tables.HISTORY_TABLES.get(contract.name)
        if observation_table is None:
            continue
        current = contract.qualified_name
        stage = stages[contract.name]
        observational_columns = {
            "resolved_at",
            "first_seen_date",
            "last_seen_date",
            "first_seen_at",
            "last_seen_at",
        }
        state_columns = tuple(
            column for column in contract.columns if column not in observational_columns
        )
        staged_hash = _fingerprint("staged", state_columns)
        current_hash = _fingerprint("current", state_columns)
        key_match = " AND ".join(
            f"isNotDistinctFrom(current.{key}, staged.{key})"
            for key in contract.key_columns
        )
        current_names = ", ".join(contract.columns)
        staged_values = ", ".join(f"staged.{column}" for column in contract.columns)
        current_values = ", ".join(f"current.{column}" for column in contract.columns)
        observation_columns = (
            f"{current_names}, state_fingerprint, observation_fingerprint, "
            "has_observation, source_run_id, observed_at"
        )
        staged_observation = (
            f"lower(hex(SHA256(concat({staged_hash}, '|1|', %(source_run_id)s))))"
        )
        removed_observation = (
            f"lower(hex(SHA256(concat({current_hash}, '|0|', %(source_run_id)s))))"
        )
        client.execute(
            f"INSERT INTO corpscout.{observation_table} ({observation_columns}) "
            f"SELECT {staged_values}, {staged_hash}, {staged_observation}, "
            "toUInt8(1), %(source_run_id)s, now64(3, 'UTC') "
            f"FROM {stage} AS staged LEFT JOIN {current} AS current ON {key_match} "
            f"WHERE ifNull(toString(current.{contract.key_columns[0]}), '') = '' "
            f"OR {staged_hash} != {current_hash} "
            "UNION ALL "
            f"SELECT {current_values}, {current_hash}, {removed_observation}, "
            "toUInt8(0), %(source_run_id)s, now64(3, 'UTC') "
            f"FROM {current} AS current LEFT JOIN {stage} AS staged ON {key_match} "
            f"WHERE current.country_code = %(country_code)s "
            f"AND ifNull(toString(staged.{contract.key_columns[0]}), '') = ''",
            {"country_code": country_code, "source_run_id": source_run_id},
        )
        _log(log, "Company serving history compared: table=%s", contract.name)


def _fingerprint(alias: str, columns: tuple[str, ...]) -> str:
    values = ", ".join(
        f"ifNull(toString({alias}.{column}), '<NULL>')" for column in columns
    )
    return f"lower(hex(SHA256(arrayStringConcat([{values}], '\\x1f'))))"


def _scalar(client: Any, sql: str) -> int:
    rows = client.execute(sql)
    return int(rows[0][0]) if rows else 0


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"


def _log(log: Callable[..., object] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)
