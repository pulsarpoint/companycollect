import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
LV_COMPANIES_ENRICHED_VIEW = "lv_companies_enriched"
LV_COMPANIES_EXPORT_VIEW = "lv_companies_export"
LV_COMPANY_ADDRESSES_EXPORT_VIEW = "lv_company_addresses_export"


@dataclass(frozen=True, slots=True)
class LatviaUrClickHousePublishResult:
    company_rows: int
    address_candidates: int
    address_observations_inserted: int
    first_address_observations: int
    source_address_changes: int
    enrichment_only_changes: int
    empty_address_candidates: int


def publish_latvia_ur_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    observed_at: datetime,
    log: Callable[..., object] | None = None,
) -> LatviaUrClickHousePublishResult:
    """Publish one Latvia register snapshot and append new address states.

    Address observations land before company facts. On the first cutover this
    preserves the legacy address until its authoritative history row exists; on
    retries the current-view fingerprint makes the append idempotent.
    """
    create_latvia_ur_export_views(
        duckdb_connection=duckdb_connection,
        source_run_id=source_run_id,
        observed_at=observed_at,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(
            tables.LV_COMPANIES_TABLE,
            tables.LV_COMPANY_ADDRESSES_TABLE,
        ),
    )

    if log is not None:
        log(
            "Publishing Latvia UR ClickHouse snapshot: companies_table=%s "
            "addresses_table=%s observed_at=%s",
            tables.QUALIFIED_LV_COMPANIES_TABLE,
            tables.QUALIFIED_LV_COMPANY_ADDRESSES_TABLE,
            observed_at.isoformat(),
        )

    empty_address_candidates = int(
        duckdb_connection.execute(
            f"""
            select count(*)
            from {DLT_DATASET_NAME}.{LV_COMPANY_ADDRESSES_EXPORT_VIEW}
            where has_address = 0
            """
        ).fetchone()[0]
    )

    with clickhouse.get_connection() as client:
        address_counts = _append_changed_address_observations(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            log=log,
        )
        company_rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=LV_COMPANIES_EXPORT_VIEW,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=tables.LV_COMPANIES_TABLE,
            columns=tables.LV_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
            log=log,
        )

    result = LatviaUrClickHousePublishResult(
        company_rows=company_rows,
        address_candidates=address_counts.candidates,
        address_observations_inserted=address_counts.inserted,
        first_address_observations=address_counts.first_observations,
        source_address_changes=address_counts.source_changes,
        enrichment_only_changes=address_counts.enrichment_changes,
        empty_address_candidates=empty_address_candidates,
    )
    if log is not None:
        log(
            "Finished Latvia UR ClickHouse publish: company_rows=%d "
            "address_candidates=%d address_observations_inserted=%d "
            "first_address_observations=%d source_address_changes=%d "
            "enrichment_only_changes=%d empty_address_candidates=%d",
            result.company_rows,
            result.address_candidates,
            result.address_observations_inserted,
            result.first_address_observations,
            result.source_address_changes,
            result.enrichment_only_changes,
            result.empty_address_candidates,
        )
    return result


def create_latvia_ur_export_views(
    *,
    duckdb_connection: Any,
    source_run_id: str,
    observed_at: datetime,
) -> None:
    if source_run_id == "":
        raise ValueError("source_run_id must not be empty")
    if observed_at.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")

    run_id_sql = _duckdb_string_literal(source_run_id)
    observed_at_sql = _duckdb_string_literal(
        observed_at.astimezone(UTC).isoformat(timespec="milliseconds")
    )
    entity_columns = ",\n            ".join(
        f"{run_id_sql} as source_run_id" if column == "source_run_id" else f"e.{column}"
        for column in tables.LATVIA_UR_ENTITIES_COLUMNS
    )
    duckdb_connection.execute(
        f"""
        create or replace view {DLT_DATASET_NAME}.{LV_COMPANIES_ENRICHED_VIEW} as
        select
            {entity_columns},
            a.activity_text_original,
            b.full_address as vzd_address_text,
            b.postal_code as vzd_address_postal_code,
            b.status as vzd_address_status,
            c.name as address_city_name,
            coalesce(m_atvk.name, m_parent.name) as address_municipality_name,
            b.latitude as address_latitude,
            b.longitude as address_longitude
        from {DLT_DATASET_NAME}.{ENTITIES_TABLE} e
        left join (
            select
                regcode,
                any_value(activity_text_original) as activity_text_original
            from {DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE}
            group by regcode
        ) a on a.regcode = e.regcode
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}
            where status = 'EKS'
        ) b on b.address_code = e.address_id
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_CITIES_TABLE}
            where status = 'EKS'
        ) c on c.address_code = coalesce(
            nullif(e.city_code, '0'),
            nullif(e.region_code, '0')
        )
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}
            where status = 'EKS'
        ) m_atvk on m_atvk.atvk_code = e.atvk_code
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}
            where status = 'EKS'
        ) m_parent on m_parent.address_code = c.parent_address_code
        """
    )

    company_columns = ",\n            ".join(tables.LV_COMPANIES_COLUMNS)
    duckdb_connection.execute(
        f"""
        create or replace view {DLT_DATASET_NAME}.{LV_COMPANIES_EXPORT_VIEW} as
        select
            {company_columns}
        from {DLT_DATASET_NAME}.{LV_COMPANIES_ENRICHED_VIEW}
        """
    )

    address_fingerprint = _duckdb_fingerprint(tables.LV_COMPANY_ADDRESS_SOURCE_COLUMNS)
    observation_fingerprint = _duckdb_fingerprint(
        (
            *tables.LV_COMPANY_ADDRESS_SOURCE_COLUMNS,
            *tables.LATVIA_VZD_ADDRESS_COLUMNS,
        )
    )
    has_address = " or ".join(
        f"coalesce(trim(cast({column} as varchar)), '') <> ''"
        for column in tables.LV_COMPANY_ADDRESS_SOURCE_COLUMNS
    )
    duckdb_connection.execute(
        f"""
        create or replace view {DLT_DATASET_NAME}.{LV_COMPANY_ADDRESSES_EXPORT_VIEW} as
        select
            country_iso2,
            source_slug,
            source_run_id,
            source_url,
            regcode,
            {", ".join(tables.LV_COMPANY_ADDRESS_SOURCE_COLUMNS)},
            {", ".join(tables.LATVIA_VZD_ADDRESS_COLUMNS)},
            cast(({has_address}) as utinyint) as has_address,
            {address_fingerprint} as address_fingerprint,
            {observation_fingerprint} as observation_fingerprint,
            cast({observed_at_sql} as timestamptz) as observed_at
        from {DLT_DATASET_NAME}.{LV_COMPANIES_ENRICHED_VIEW}
        """
    )


@dataclass(frozen=True, slots=True)
class _AddressChangeCounts:
    candidates: int
    inserted: int
    first_observations: int
    source_changes: int
    enrichment_changes: int


def _append_changed_address_observations(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    log: Callable[..., object] | None,
) -> _AddressChangeCounts:
    stage_table = f"_tmp_{tables.LV_COMPANY_ADDRESSES_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.LATVIA_UR_DATABASE}.{stage_table}"
    primary_error: BaseException | None = None

    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage} AS "
            f"{tables.QUALIFIED_LV_COMPANY_ADDRESSES_TABLE}"
        )
        candidate_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=LV_COMPANY_ADDRESSES_EXPORT_VIEW,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=stage_table,
            columns=tables.LV_COMPANY_ADDRESSES_COLUMNS,
            truncate=False,
            log=log,
        )
        inserted, first, source_changes, enrichment_changes = _address_change_counts(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
        )
        if inserted > 0:
            _insert_address_changes(
                clickhouse_client=clickhouse_client,
                qualified_stage=qualified_stage,
            )
        return _AddressChangeCounts(
            candidates=candidate_count,
            inserted=inserted,
            first_observations=first,
            source_changes=source_changes,
            enrichment_changes=enrichment_changes,
        )
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
        except Exception as cleanup_error:
            if primary_error is None:
                raise
            primary_error.add_note(
                f"Failed to drop Latvia address stage {qualified_stage}: {cleanup_error}"
            )


def _address_change_counts(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
) -> tuple[int, int, int, int]:
    rows = clickhouse_client.execute(
        f"""
        SELECT
            countIf(
                c.has_observation = 0
                OR c.observation_fingerprint != s.observation_fingerprint
            ) AS address_observations_inserted,
            countIf(c.has_observation = 0) AS first_address_observations,
            countIf(
                c.has_observation = 1
                AND c.address_fingerprint != s.address_fingerprint
            ) AS source_address_changes,
            countIf(
                c.has_observation = 1
                AND c.address_fingerprint = s.address_fingerprint
                AND c.observation_fingerprint != s.observation_fingerprint
            ) AS enrichment_only_changes
        FROM {qualified_stage} AS s
        LEFT JOIN corpscout.{tables.LV_COMPANY_ADDRESSES_CURRENT_VIEW} AS c
            ON c.regcode = s.regcode
        """
    )
    if not rows:
        return (0, 0, 0, 0)
    return tuple(int(value) for value in rows[0])


def _insert_address_changes(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
) -> None:
    columns = ", ".join(tables.LV_COMPANY_ADDRESSES_COLUMNS)
    clickhouse_client.execute(
        f"""
        INSERT INTO corpscout.{tables.LV_COMPANY_ADDRESSES_TABLE} ({columns})
        SELECT {", ".join(f"s.{column}" for column in tables.LV_COMPANY_ADDRESSES_COLUMNS)}
        FROM {qualified_stage} AS s
        LEFT JOIN corpscout.{tables.LV_COMPANY_ADDRESSES_CURRENT_VIEW} AS c
            ON c.regcode = s.regcode
        WHERE c.has_observation = 0
           OR c.observation_fingerprint != s.observation_fingerprint
        """
    )


def _duckdb_fingerprint(columns: Sequence[str]) -> str:
    normalized_columns = ", ".join(
        f"coalesce(cast({column} as varchar), '')" for column in columns
    )
    return f"sha256(concat_ws(chr(31), {normalized_columns}))"


def _duckdb_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
