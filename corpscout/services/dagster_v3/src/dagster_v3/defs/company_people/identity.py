"""Country-scoped person identities built from source observations.

The resolver deliberately stops at a country boundary. Public source identifiers
are accepted as exact evidence. Without one, observations are provisionally
grouped only when the same normalized name appears for the same company and is
not duplicated inside one statement/signatory block. A duplicated name in one
block remains a singleton so two namesakes are never silently collapsed.

Observation and person IDs are deterministic SHA-256-derived UUID values. They
remain stable across full refreshes without using a person's name as a global
identifier: observation IDs include source provenance, while provisional person
IDs are scoped by country and company. The match table records why every
observation was assigned, so a later review layer can replace a provisional
decision without losing the raw evidence.
"""

import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

COUNTRY_PEOPLE_DATABASE = "corpscout"
GROUP_NAME = "country_people"
RESOLVER_VERSION = 1

COUNTRY_PERSON_TABLE = "country_person"
COUNTRY_PERSON_OBSERVATION_TABLE = "country_person_observation"
COUNTRY_PERSON_IDENTIFIER_TABLE = "country_person_identifier"
COUNTRY_PERSON_MATCH_TABLE = "country_person_match"
COUNTRY_PERSON_CORRECTION_TABLE = "country_person_correction"

COUNTRY_PERSON_COLUMNS = (
    "country_iso2",
    "person_id",
    "preferred_name",
    "preferred_name_normalized",
    "resolution_status",
    "resolution_method",
    "merged_into_person_id",
    "first_observed_year",
    "last_observed_year",
    "observation_count",
    "company_count",
    "resolved_at",
)

COUNTRY_PERSON_OBSERVATION_COLUMNS = (
    "country_iso2",
    "observation_id",
    "source",
    "source_record_id",
    "source_person_key",
    "company_id",
    "company_name",
    "observed_first_name",
    "observed_last_name",
    "observed_full_name",
    "observed_name_normalized",
    "role_original",
    "role_kind",
    "signatory_kind",
    "fiscal_year",
    "identifier_kind",
    "identifier_value",
    "source_statement_key",
    "resolved_at",
)

COUNTRY_PERSON_IDENTIFIER_COLUMNS = (
    "country_iso2",
    "identifier_id",
    "person_id",
    "source",
    "identifier_kind",
    "identifier_value",
    "observation_id",
    "is_public",
    "resolved_at",
)

COUNTRY_PERSON_MATCH_COLUMNS = (
    "country_iso2",
    "observation_id",
    "person_id",
    "match_method",
    "match_status",
    "confidence",
    "resolver_version",
    "decided_at",
)

_TARGET_TABLES = (
    COUNTRY_PERSON_OBSERVATION_TABLE,
    COUNTRY_PERSON_MATCH_TABLE,
    COUNTRY_PERSON_IDENTIFIER_TABLE,
    COUNTRY_PERSON_TABLE,
)

_ASSET_TABLES = {
    "country_person_observations_clickhouse": COUNTRY_PERSON_OBSERVATION_TABLE,
    "country_person_matches_clickhouse": COUNTRY_PERSON_MATCH_TABLE,
    "country_person_identifiers_clickhouse": COUNTRY_PERSON_IDENTIFIER_TABLE,
    "country_people_clickhouse": COUNTRY_PERSON_TABLE,
}

_QUALITY_COLUMNS = (
    "observation_count",
    "match_count",
    "identifier_count",
    "person_count",
    "country_count",
    "verified_person_count",
    "reviewed_person_count",
    "provisional_person_count",
    "unresolved_person_count",
)


def _qualified(table: str) -> str:
    return f"`{COUNTRY_PEOPLE_DATABASE}`.`{table}`"


def _insert_columns(columns: tuple[str, ...]) -> str:
    return ",\n    ".join(columns)


def build_country_person_observation_insert_sql(qualified_stage_table: str) -> str:
    """Preserve every Swedish officer occurrence with a stable source identity."""
    return f"""INSERT INTO {qualified_stage_table} (
    {_insert_columns(COUNTRY_PERSON_OBSERVATION_COLUMNS)}
)
SELECT
    'SE' AS country_iso2,
    reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
        'country_person_observation|SE|se_xbrl_signatures|',
        o.statement_key, '|', o.signatory_kind, '|', toString(o.person_seq)
    ))), 1, 32))) AS observation_id,
    'se_xbrl_signatures' AS source,
    o.statement_key AS source_record_id,
    concat(o.signatory_kind, '|', toString(o.person_seq)) AS source_person_key,
    o.company_id AS company_id,
    any(coalesce(c.legal_name, '')) AS company_name,
    o.first_name AS observed_first_name,
    o.last_name AS observed_last_name,
    trim(concat(o.first_name, ' ', o.last_name)) AS observed_full_name,
    lowerUTF8(trim(concat(o.first_name, ' ', o.last_name))) AS observed_name_normalized,
    o.role_original AS role_original,
    o.role_kind AS role_kind,
    o.signatory_kind AS signatory_kind,
    o.fiscal_year AS fiscal_year,
    '' AS identifier_kind,
    '' AS identifier_value,
    o.statement_key AS source_statement_key,
    %(resolved_at)s AS resolved_at
FROM corpscout.se_company_officers AS o
LEFT JOIN corpscout.se_companies AS c ON c.registration_number = o.company_id
WHERE trim(concat(o.first_name, ' ', o.last_name)) != ''
GROUP BY
    o.statement_key,
    o.signatory_kind,
    o.person_seq,
    o.company_id,
    o.first_name,
    o.last_name,
    o.role_original,
    o.role_kind,
    o.fiscal_year"""


def build_country_person_match_insert_sql(
    qualified_stage_table: str,
    qualified_observation_stage: str,
    qualified_correction_table: str,
) -> str:
    """Apply reviewed decisions after deriving conservative automatic matches."""
    return f"""INSERT INTO {qualified_stage_table} (
    {_insert_columns(COUNTRY_PERSON_MATCH_COLUMNS)}
)
WITH observation_corrections AS (
    SELECT
        country_iso2,
        observation_id AS corrected_observation_id,
        argMax(to_person_id, (created_at, correction_id)) AS reviewed_person_id,
        argMax(correction_kind, (created_at, correction_id)) AS correction_kind,
        toUInt8(1) AS has_correction
    FROM {qualified_correction_table}
    WHERE observation_id IS NOT NULL
    GROUP BY country_iso2, observation_id
),
person_corrections AS (
    SELECT
        country_iso2,
        from_person_id,
        argMax(to_person_id, (created_at, correction_id)) AS reviewed_person_id,
        toUInt8(1) AS has_correction
    FROM {qualified_correction_table}
    WHERE observation_id IS NULL
    GROUP BY country_iso2, from_person_id
),
candidates AS (
    SELECT
        o.*,
        count() OVER (
            PARTITION BY o.country_iso2, o.company_id, o.observed_name_normalized,
                o.source_statement_key, o.signatory_kind
        ) AS same_name_in_statement_block,
        c.reviewed_person_id AS observation_person_id,
        c.correction_kind AS observation_correction_kind,
        c.has_correction AS has_observation_correction
    FROM {qualified_observation_stage} AS o
    LEFT JOIN observation_corrections AS c
        ON c.country_iso2 = o.country_iso2
       AND c.corrected_observation_id = o.observation_id
),
base_matches AS (
    SELECT
        *,
        multiIf(
            has_observation_correction = 1, observation_person_id,
            reinterpretAsUUID(unhex(substring(hex(SHA256(
                multiIf(
                    identifier_kind != '' AND identifier_value != '',
                        concat('country_person|identifier|', country_iso2, '|', identifier_kind, '|', identifier_value),
                    same_name_in_statement_block = 1,
                        concat('country_person|company_name|', country_iso2, '|', company_id, '|', observed_name_normalized),
                    concat('country_person|observation|', country_iso2, '|', toString(observation_id))
                )
            )), 1, 32)))
        ) AS base_person_id
    FROM candidates
)
SELECT
    b.country_iso2,
    b.observation_id,
    if(
        p.has_correction = 1 AND p.reviewed_person_id != p.from_person_id,
        p.reviewed_person_id,
        b.base_person_id
    ) AS person_id,
    multiIf(
        p.has_correction = 1 AND p.reviewed_person_id != p.from_person_id,
            'reviewed_merge',
        b.has_observation_correction = 1,
            concat('reviewed_', b.observation_correction_kind),
        b.identifier_kind != '' AND b.identifier_value != '', 'source_identifier',
        b.same_name_in_statement_block = 1, 'same_company_name',
        'singleton'
    ) AS match_method,
    multiIf(
        p.has_correction = 1 AND p.reviewed_person_id != p.from_person_id,
            'reviewed',
        b.has_observation_correction = 1, 'reviewed',
        b.identifier_kind != '' AND b.identifier_value != '', 'accepted',
        b.same_name_in_statement_block = 1, 'provisional',
        'unresolved'
    ) AS match_status,
    toUInt8(multiIf(
        p.has_correction = 1 AND p.reviewed_person_id != p.from_person_id, 100,
        b.has_observation_correction = 1, 100,
        b.identifier_kind != '' AND b.identifier_value != '', 100,
        b.same_name_in_statement_block = 1, 70,
        0
    )) AS confidence,
    toUInt16({RESOLVER_VERSION}) AS resolver_version,
    %(resolved_at)s AS decided_at
FROM base_matches AS b
LEFT JOIN person_corrections AS p
    ON p.country_iso2 = b.country_iso2
   AND p.from_person_id = b.base_person_id"""


def build_country_person_identifier_insert_sql(
    qualified_stage_table: str,
    qualified_observation_stage: str,
    qualified_match_stage: str,
) -> str:
    """Publish only source identifiers explicitly present in observations."""
    return f"""INSERT INTO {qualified_stage_table} (
    {_insert_columns(COUNTRY_PERSON_IDENTIFIER_COLUMNS)}
)
SELECT
    o.country_iso2,
    reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
        'country_person_identifier|', o.country_iso2, '|', o.source, '|',
        o.identifier_kind, '|', o.identifier_value
    ))), 1, 32))) AS identifier_id,
    m.person_id,
    o.source,
    o.identifier_kind,
    o.identifier_value,
    o.observation_id,
    toUInt8(1) AS is_public,
    %(resolved_at)s AS resolved_at
FROM {qualified_observation_stage} AS o
INNER JOIN {qualified_match_stage} AS m
    ON m.country_iso2 = o.country_iso2
   AND m.observation_id = o.observation_id
WHERE o.identifier_kind != '' AND o.identifier_value != ''"""


def build_country_person_insert_sql(
    qualified_stage_table: str,
    qualified_observation_stage: str,
    qualified_match_stage: str,
    qualified_correction_table: str,
    qualified_current_person_table: str,
) -> str:
    """Build active profiles and retain merged IDs as redirect tombstones."""
    return f"""INSERT INTO {qualified_stage_table} (
    {_insert_columns(COUNTRY_PERSON_COLUMNS)}
)
SELECT
    o.country_iso2,
    m.person_id,
    argMax(o.observed_full_name, (o.fiscal_year, o.source_statement_key)) AS preferred_name,
    argMax(o.observed_name_normalized, (o.fiscal_year, o.source_statement_key)) AS preferred_name_normalized,
    multiIf(
        countIf(startsWith(m.match_method, 'reviewed_')) > 0, 'reviewed',
        countIf(m.match_method = 'source_identifier') > 0, 'verified',
        countIf(m.match_method = 'same_company_name') > 0, 'provisional',
        'unresolved'
    ) AS resolution_status,
    multiIf(
        countIf(startsWith(m.match_method, 'reviewed_')) > 0, 'reviewed_correction',
        countIf(m.match_method = 'source_identifier') > 0, 'source_identifier',
        countIf(m.match_method = 'same_company_name') > 0, 'same_company_name',
        'singleton'
    ) AS resolution_method,
    CAST(NULL, 'Nullable(UUID)') AS merged_into_person_id,
    toInt32(minIf(o.fiscal_year, o.fiscal_year > 0)) AS first_observed_year,
    toInt32(max(o.fiscal_year)) AS last_observed_year,
    toUInt32(count()) AS observation_count,
    toUInt32(uniqExact(o.company_id)) AS company_count,
    %(resolved_at)s AS resolved_at
FROM {qualified_observation_stage} AS o
INNER JOIN {qualified_match_stage} AS m
    ON m.country_iso2 = o.country_iso2
   AND m.observation_id = o.observation_id
GROUP BY o.country_iso2, m.person_id
UNION ALL
SELECT
    p.country_iso2,
    p.person_id,
    p.preferred_name,
    p.preferred_name_normalized,
    'merged' AS resolution_status,
    'reviewed_merge' AS resolution_method,
    c.reviewed_person_id AS merged_into_person_id,
    p.first_observed_year,
    p.last_observed_year,
    toUInt32(0) AS observation_count,
    toUInt32(0) AS company_count,
    %(resolved_at)s AS resolved_at
FROM {qualified_current_person_table} AS p
INNER JOIN (
    SELECT
        country_iso2,
        from_person_id,
        argMax(to_person_id, (created_at, correction_id)) AS reviewed_person_id
    FROM {qualified_correction_table}
    WHERE observation_id IS NULL
    GROUP BY country_iso2, from_person_id
    HAVING reviewed_person_id != from_person_id
) AS c
    ON c.country_iso2 = p.country_iso2
   AND c.from_person_id = p.person_id"""


def _quality_sql(stages: dict[str, str]) -> str:
    return f"""SELECT
    (SELECT count() FROM {stages[COUNTRY_PERSON_OBSERVATION_TABLE]}) AS observation_count,
    (SELECT count() FROM {stages[COUNTRY_PERSON_MATCH_TABLE]}) AS match_count,
    (SELECT count() FROM {stages[COUNTRY_PERSON_IDENTIFIER_TABLE]}) AS identifier_count,
    (SELECT count() FROM {stages[COUNTRY_PERSON_TABLE]}) AS person_count,
    (SELECT uniqExact(country_iso2) FROM {stages[COUNTRY_PERSON_TABLE]}) AS country_count,
    (SELECT countIf(resolution_status = 'verified') FROM {stages[COUNTRY_PERSON_TABLE]}) AS verified_person_count,
    (SELECT countIf(resolution_status = 'reviewed') FROM {stages[COUNTRY_PERSON_TABLE]}) AS reviewed_person_count,
    (SELECT countIf(resolution_status = 'provisional') FROM {stages[COUNTRY_PERSON_TABLE]}) AS provisional_person_count,
    (SELECT countIf(resolution_status = 'unresolved') FROM {stages[COUNTRY_PERSON_TABLE]}) AS unresolved_person_count"""


def _validate_quality(quality: dict[str, int]) -> None:
    if quality["observation_count"] == 0:
        raise ValueError("country people resolver produced no observations")
    if quality["person_count"] == 0:
        raise ValueError("country people resolver produced no people")
    if quality["match_count"] != quality["observation_count"]:
        raise ValueError(
            "country people resolver must assign exactly one match row per observation: "
            f"observations={quality['observation_count']} matches={quality['match_count']}"
        )


def replace_country_people_identity_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str]:
    """Rebuild all four identity tables and restore exchanged tables on failure."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=COUNTRY_PEOPLE_DATABASE,
        tables=(
            *_TARGET_TABLES,
            COUNTRY_PERSON_CORRECTION_TABLE,
            "se_company_officers",
            "se_companies",
        ),
    )
    stage_names = {
        table: f"_tmp_{table}_{uuid.uuid4().hex}" for table in _TARGET_TABLES
    }
    stages = {table: _qualified(stage) for table, stage in stage_names.items()}
    targets = {table: _qualified(table) for table in _TARGET_TABLES}

    if log is not None:
        log(
            "Building country-scoped people identity tables: source_run_id=%s resolver_version=%s",
            source_run_id,
            RESOLVER_VERSION,
        )

    with clickhouse.get_connection() as client:
        created: list[str] = []
        exchanged: list[str] = []
        primary_error: Exception | None = None
        try:
            for table in _TARGET_TABLES:
                client.execute(f"CREATE TABLE {stages[table]} AS {targets[table]}")
                created.append(table)

            params = {"resolved_at": resolved_at}
            client.execute(
                build_country_person_observation_insert_sql(
                    stages[COUNTRY_PERSON_OBSERVATION_TABLE]
                ),
                params,
            )
            client.execute(
                build_country_person_match_insert_sql(
                    stages[COUNTRY_PERSON_MATCH_TABLE],
                    stages[COUNTRY_PERSON_OBSERVATION_TABLE],
                    _qualified(COUNTRY_PERSON_CORRECTION_TABLE),
                ),
                params,
            )
            client.execute(
                build_country_person_identifier_insert_sql(
                    stages[COUNTRY_PERSON_IDENTIFIER_TABLE],
                    stages[COUNTRY_PERSON_OBSERVATION_TABLE],
                    stages[COUNTRY_PERSON_MATCH_TABLE],
                ),
                params,
            )
            client.execute(
                build_country_person_insert_sql(
                    stages[COUNTRY_PERSON_TABLE],
                    stages[COUNTRY_PERSON_OBSERVATION_TABLE],
                    stages[COUNTRY_PERSON_MATCH_TABLE],
                    _qualified(COUNTRY_PERSON_CORRECTION_TABLE),
                    targets[COUNTRY_PERSON_TABLE],
                ),
                params,
            )
            quality_row = client.execute(_quality_sql(stages))[0]
            quality = {
                column: int(value)
                for column, value in zip(_QUALITY_COLUMNS, quality_row, strict=True)
            }
            _validate_quality(quality)

            staged_counts = {
                COUNTRY_PERSON_OBSERVATION_TABLE: quality["observation_count"],
                COUNTRY_PERSON_MATCH_TABLE: quality["match_count"],
                COUNTRY_PERSON_IDENTIFIER_TABLE: quality["identifier_count"],
                COUNTRY_PERSON_TABLE: quality["person_count"],
            }
            for table in _TARGET_TABLES:
                guard_against_clickhouse_table_shrink(
                    qualified_table=f"{COUNTRY_PEOPLE_DATABASE}.{table}",
                    existing_row_count=clickhouse_table_row_count(
                        client, targets[table]
                    ),
                    staged_row_count=staged_counts[table],
                    allow_shrink=allow_shrink,
                )

            for table in _TARGET_TABLES:
                client.execute(f"EXCHANGE TABLES {stages[table]} AND {targets[table]}")
                exchanged.append(table)
        except Exception as exc:
            primary_error = exc
            rollback_errors: list[Exception] = []
            for table in reversed(exchanged):
                try:
                    client.execute(
                        f"EXCHANGE TABLES {stages[table]} AND {targets[table]}"
                    )
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)
            if rollback_errors:
                raise RuntimeError(
                    "country people table refresh failed and could not fully roll back"
                ) from exc
            raise
        finally:
            for table in created:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {stages[table]}")
                except Exception:
                    if primary_error is None:
                        raise

    metadata: dict[str, int | str] = {
        **quality,
        "source_run_id": source_run_id,
        "resolver_version": RESOLVER_VERSION,
    }
    if log is not None:
        log(
            "Finished country people identity: observations=%s people=%s reviewed=%s provisional=%s unresolved=%s",
            quality["observation_count"],
            quality["person_count"],
            quality["reviewed_person_count"],
            quality["provisional_person_count"],
            quality["unresolved_person_count"],
        )
    return metadata


class CountryPeopleIdentityConfig(dg.Config):
    allow_shrink: bool = False


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            "country_person_observations_clickhouse",
            deps=[dg.AssetKey("se_company_officers_clickhouse")],
            group_name=GROUP_NAME,
            kinds={"clickhouse"},
            metadata={
                "table": f"{COUNTRY_PEOPLE_DATABASE}.{COUNTRY_PERSON_OBSERVATION_TABLE}"
            },
            description="Country-scoped source observations with deterministic provenance IDs.",
        ),
        dg.AssetSpec(
            "country_person_matches_clickhouse",
            deps=[dg.AssetKey("country_person_observations_clickhouse")],
            group_name=GROUP_NAME,
            kinds={"clickhouse"},
            metadata={
                "table": f"{COUNTRY_PEOPLE_DATABASE}.{COUNTRY_PERSON_MATCH_TABLE}"
            },
            description="Explicit evidence and confidence for every observation-to-person assignment.",
        ),
        dg.AssetSpec(
            "country_person_identifiers_clickhouse",
            deps=[
                dg.AssetKey("country_person_observations_clickhouse"),
                dg.AssetKey("country_person_matches_clickhouse"),
            ],
            group_name=GROUP_NAME,
            kinds={"clickhouse"},
            metadata={
                "table": f"{COUNTRY_PEOPLE_DATABASE}.{COUNTRY_PERSON_IDENTIFIER_TABLE}"
            },
            description="Public source person identifiers, empty for sources that publish none.",
        ),
        dg.AssetSpec(
            "country_people_clickhouse",
            deps=[
                dg.AssetKey("country_person_observations_clickhouse"),
                dg.AssetKey("country_person_matches_clickhouse"),
            ],
            group_name=GROUP_NAME,
            kinds={"clickhouse"},
            metadata={"table": f"{COUNTRY_PEOPLE_DATABASE}.{COUNTRY_PERSON_TABLE}"},
            description="Country-scoped person profiles resolved from preserved source evidence.",
        ),
    ],
    can_subset=False,
)
def country_people_identity_clickhouse(
    context: dg.AssetExecutionContext,
    config: CountryPeopleIdentityConfig,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.MaterializeResult]:
    metadata = replace_country_people_identity_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    for asset_key, table in _ASSET_TABLES.items():
        row_count_key = {
            COUNTRY_PERSON_OBSERVATION_TABLE: "observation_count",
            COUNTRY_PERSON_MATCH_TABLE: "match_count",
            COUNTRY_PERSON_IDENTIFIER_TABLE: "identifier_count",
            COUNTRY_PERSON_TABLE: "person_count",
        }[table]
        yield dg.MaterializeResult(
            asset_key=asset_key,
            metadata={
                **metadata,
                "table": f"{COUNTRY_PEOPLE_DATABASE}.{table}",
                "row_count": metadata[row_count_key],
            },
        )


country_people_identity_job = dg.define_asset_job(
    "country_people_identity_job",
    selection=dg.AssetSelection.assets(*_ASSET_TABLES),
)


def country_person_correction_cursor(
    clickhouse: ClickhouseResource,
) -> str:
    """Return a cursor that advances for every appended ledger row."""
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""SELECT
                count(),
                if(
                    count() = 0,
                    '',
                    toString(argMax(correction_id, (created_at, correction_id)))
                )
            FROM {_qualified(COUNTRY_PERSON_CORRECTION_TABLE)}"""
        )
    if not rows or int(rows[0][0]) == 0:
        return ""
    return f"{int(rows[0][0])}:{rows[0][1]}"


@dg.sensor(
    name="country_person_correction_sensor",
    job=country_people_identity_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    required_resource_keys={"clickhouse"},
)
def country_person_correction_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    correction_cursor = country_person_correction_cursor(context.resources.clickhouse)
    if correction_cursor == "":
        return dg.SkipReason("No reviewed country-person corrections exist")
    if correction_cursor == context.cursor:
        return dg.SkipReason("No new reviewed country-person corrections")
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(run_key=f"country-person-correction:{correction_cursor}")
        ],
        cursor=correction_cursor,
    )


country_people_identity_schedule = dg.ScheduleDefinition(
    name="country_people_identity_schedule",
    job=country_people_identity_job,
    cron_schedule="0 8 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[country_people_identity_clickhouse],
    jobs=[country_people_identity_job],
    schedules=[country_people_identity_schedule],
    sensors=[country_person_correction_sensor],
)
