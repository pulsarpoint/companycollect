"""Build Swedish company-person roles from source-owned static mappings."""

import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_people.draft import normalized_company_ids
from dagster_v3.defs.esef_filings.roles import (
    ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
    ESEF_ROLELESS_ROLE_CATEGORIES,
)
from dagster_v3.defs.sweden_financial.roles import (
    BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
    BOLAGSVERKET_ROLELESS_ROLE_KINDS,
)
from dagster_v3.defs.wikidata.roles import (
    WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
    WIKIDATA_ROLELESS_PROPERTIES,
)

DATABASE = "corpscout"
GROUP_NAME = "company_people"

PERSON_DRAFT_TABLE = "se_company_person_draft"
PERSON_TABLE = "se_company_person"
ROLE_TYPE_TABLE = "company_person_role_type"
ROLE_DRAFT_TABLE = "se_company_person_role_draft"
ROLE_TABLE = "se_company_person_role"

QUALIFIED_ROLE_DRAFT_TABLE = f"{DATABASE}.{ROLE_DRAFT_TABLE}"
QUALIFIED_ROLE_TABLE = f"{DATABASE}.{ROLE_TABLE}"

ROLE_DRAFT_COLUMNS = (
    "role_draft_id",
    "person_draft_id",
    "company_id",
    "source",
    "source_record_uid",
    "person_role_hash",
    "source_role_code",
    "source_role_name",
    "role_code",
    "fiscal_year",
    "source_observed_at",
    "source_run_id",
    "created_at",
)

ROLE_COLUMNS = (
    "role_id",
    "person_id",
    "company_id",
    "role_code",
    "role_draft_ids",
    "person_draft_ids",
    "sources",
    "fiscal_years",
    "first_observed_at",
    "last_observed_at",
    "is_current",
    "source_run_id",
    "created_at",
    "updated_at",
)

SOURCE_ROLE_MAPPINGS: Mapping[str, Mapping[str, str]] = {
    "bolagsverket": BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
    "esef": ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
    "wikidata": WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
}

SOURCE_ROLELESS_CODES: Mapping[str, frozenset[str]] = {
    "bolagsverket": BOLAGSVERKET_ROLELESS_ROLE_KINDS,
    "esef": ESEF_ROLELESS_ROLE_CATEGORIES,
    "wikidata": WIKIDATA_ROLELESS_PROPERTIES,
}

_ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def _qualified(table: str) -> str:
    return f"`{DATABASE}`.`{table}`"


def _columns_sql(columns: Sequence[str]) -> str:
    return ",\n    ".join(columns)


def _company_filter(column: str, company_ids: Sequence[str]) -> str:
    normalized = normalized_company_ids(company_ids)
    if not normalized:
        return "1"
    values = ", ".join(f"'{company_id}'" for company_id in normalized)
    return f"{column} IN ({values})"


def source_role_code(
    source: str,
    source_value: Mapping[str, object],
) -> str:
    """Return the normalized native role code stored by a source."""
    field_by_source = {
        "bolagsverket": "role_kind",
        "esef": "role_category",
        "wikidata": "role_property",
    }
    if source not in field_by_source:
        return ""
    return str(source_value.get(field_by_source[source], "")).strip()


def canonical_role_code(
    source: str,
    source_value: Mapping[str, object],
) -> str | None:
    """Return a mapped role, or ``None`` for roleless or unmapped evidence."""
    native_role_code = source_role_code(source, source_value)
    if native_role_code in SOURCE_ROLELESS_CODES.get(source, frozenset()):
        return None
    return SOURCE_ROLE_MAPPINGS.get(source, {}).get(native_role_code)


def _role_mapping_values_sql() -> str:
    rows = [
        f"('{source}', '{source_role_code}', '{role_code}')"
        for source, mapping in sorted(SOURCE_ROLE_MAPPINGS.items())
        for source_role_code, role_code in sorted(mapping.items())
    ]
    return ",\n        ".join(rows)


def _roleless_filter_sql(source_column: str, role_code_column: str) -> str:
    clauses = [
        f"({source_column} = '{source}' AND {role_code_column} IN ({values}))"
        for source, codes in sorted(SOURCE_ROLELESS_CODES.items())
        if codes
        for values in [", ".join(f"'{code}'" for code in sorted(codes))]
    ]
    if not clauses:
        return "1"
    return "NOT (" + " OR ".join(clauses) + ")"


def _source_roles_sql(company_ids: Sequence[str]) -> str:
    company_filter = _company_filter("drafts.company_id", company_ids)
    return f"""role_mapping AS (
    SELECT source, source_role_code, role_code
    FROM VALUES(
        'source String, source_role_code String, role_code String',
        {_role_mapping_values_sql()}
    )
),
source_roles AS (
    SELECT
        drafts.draft_id AS person_draft_id,
        drafts.company_id,
        toString(drafts.source) AS source,
        drafts.source_record_uid,
        drafts.person_role_hash,
        multiIf(
            drafts.source = 'bolagsverket', JSONExtractString(drafts.source_value_json, 'role_kind'),
            drafts.source = 'esef', JSONExtractString(drafts.source_value_json, 'role_category'),
            drafts.source = 'wikidata', JSONExtractString(drafts.source_value_json, 'role_property'),
            ''
        ) AS source_role_code,
        multiIf(
            drafts.source = 'bolagsverket', JSONExtractString(drafts.source_value_json, 'role_original'),
            drafts.source = 'esef', JSONExtractString(drafts.source_value_json, 'role'),
            drafts.source = 'wikidata', JSONExtractString(drafts.source_value_json, 'role_label'),
            ''
        ) AS source_role_name,
        drafts.fiscal_year,
        drafts.source_observed_at
    FROM corpscout.se_company_person_draft AS drafts FINAL
    WHERE {company_filter}
)"""


def build_unmapped_source_roles_sql(company_ids: Sequence[str] = ()) -> str:
    source_roles_sql = _source_roles_sql(company_ids)
    roleless_filter = _roleless_filter_sql("roles.source", "roles.source_role_code")
    return f"""WITH {source_roles_sql}
SELECT
    roles.source,
    roles.source_role_code,
    count() AS observation_count,
    arraySlice(groupUniqArray(roles.source_role_name), 1, 10) AS source_role_names
FROM source_roles AS roles
LEFT JOIN role_mapping AS mapping
    ON mapping.source = roles.source
   AND mapping.source_role_code = roles.source_role_code
WHERE {roleless_filter}
  AND mapping.role_code = ''
GROUP BY roles.source, roles.source_role_code
ORDER BY observation_count DESC, roles.source, roles.source_role_code"""


def build_inactive_canonical_roles_sql() -> str:
    return f"""WITH role_mapping AS (
    SELECT source, source_role_code, role_code
    FROM VALUES(
        'source String, source_role_code String, role_code String',
        {_role_mapping_values_sql()}
    )
)
SELECT DISTINCT mapping.role_code
FROM role_mapping AS mapping
WHERE mapping.role_code NOT IN (
    SELECT role_code
    FROM corpscout.company_person_role_type FINAL
    WHERE is_active = 1
)
ORDER BY mapping.role_code"""


def build_role_draft_insert_sql(
    qualified_stage_table: str,
    company_ids: Sequence[str] = (),
) -> str:
    source_roles_sql = _source_roles_sql(company_ids)
    roleless_filter = _roleless_filter_sql("roles.source", "roles.source_role_code")
    columns = _columns_sql(ROLE_DRAFT_COLUMNS)
    existing_filter = _company_filter("company_id", company_ids)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH {source_roles_sql},
mapped_roles AS (
    SELECT roles.*, mapping.role_code
    FROM source_roles AS roles
    INNER JOIN role_mapping AS mapping
        ON mapping.source = roles.source
       AND mapping.source_role_code = roles.source_role_code
    WHERE {roleless_filter}
),
candidates AS (
    SELECT
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'se-company-person-role-observation-v1\n',
            toString(person_draft_id), '\n',
            source_role_code, '\n',
            role_code
        ))), 1, 32))) AS role_draft_id,
        *
    FROM mapped_roles
)
SELECT
    role_draft_id,
    person_draft_id,
    company_id,
    source,
    source_record_uid,
    person_role_hash,
    source_role_code,
    if(source_role_name = '', source_role_code, source_role_name),
    role_code,
    fiscal_year,
    source_observed_at,
    %(source_run_id)s,
    %(created_at)s
FROM candidates
WHERE role_draft_id NOT IN (
    SELECT role_draft_id
    FROM corpscout.se_company_person_role_draft FINAL
    WHERE {existing_filter}
)"""


def build_roleless_observation_count_sql(company_ids: Sequence[str] = ()) -> str:
    source_roles_sql = _source_roles_sql(company_ids)
    roleless_filter = _roleless_filter_sql("source", "source_role_code")
    return f"""WITH {source_roles_sql}
SELECT count()
FROM source_roles
WHERE NOT ({roleless_filter})"""


def _role_draft_quality_sql(qualified_stage_table: str) -> str:
    return f"""SELECT
    count() AS inserted_role_draft_count,
    uniqExact(company_id) AS inserted_company_count,
    countIf(source = 'bolagsverket') AS bolagsverket_role_count,
    countIf(source = 'esef') AS esef_role_count,
    countIf(source = 'wikidata') AS wikidata_role_count,
    uniqExact(role_code) AS canonical_role_count
FROM {qualified_stage_table}"""


def _publish_role_drafts_sql(qualified_stage_table: str) -> str:
    columns = _columns_sql(ROLE_DRAFT_COLUMNS)
    return f"""INSERT INTO {_qualified(ROLE_DRAFT_TABLE)} (
    {columns}
)
SELECT
    {columns}
FROM {qualified_stage_table}"""


def collect_se_company_person_role_drafts(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    created_at: datetime,
    company_ids: Sequence[str],
    log: Callable[..., object] | None,
) -> dict[str, int | str | list[str]]:
    """Append statically mapped roles for unseen person draft observations."""
    company_scope = normalized_company_ids(company_ids)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=(PERSON_DRAFT_TABLE, ROLE_TYPE_TABLE, ROLE_DRAFT_TABLE),
    )

    stage_table = f"_tmp_{ROLE_DRAFT_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = _qualified(stage_table)
    qualified_target_table = _qualified(ROLE_DRAFT_TABLE)

    with clickhouse.get_connection() as client:
        inactive_roles = [
            str(row[0]) for row in client.execute(build_inactive_canonical_roles_sql())
        ]
        if inactive_roles:
            raise ValueError(
                "Source role mappings reference missing or inactive canonical roles: "
                + ", ".join(inactive_roles)
            )

        unmapped_roles = client.execute(build_unmapped_source_roles_sql(company_scope))
        if unmapped_roles:
            details = "; ".join(
                f"{source}:{source_role_code or '<empty>'} "
                f"({int(count)} observations; examples={list(names)})"
                for source, source_role_code, count, names in unmapped_roles
            )
            raise ValueError(
                "Unmapped source roles must be added to the source-owned static "
                f"mapping before role materialization: {details}"
            )

        roleless_observation_count = int(
            client.execute(build_roleless_observation_count_sql(company_scope))[0][0]
        )
        stage_created = False
        primary_error: Exception | None = None
        try:
            client.execute(
                f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
            )
            stage_created = True
            client.execute(
                build_role_draft_insert_sql(qualified_stage_table, company_scope),
                {"source_run_id": source_run_id, "created_at": created_at},
            )
            quality = client.execute(_role_draft_quality_sql(qualified_stage_table))[0]
            client.execute(_publish_role_drafts_sql(qualified_stage_table))
            total_role_draft_count = int(
                client.execute(f"SELECT count() FROM {qualified_target_table} FINAL")[
                    0
                ][0]
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            if stage_created:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
                except Exception:
                    if primary_error is None:
                        raise

    metadata: dict[str, int | str | list[str]] = {
        "inserted_role_draft_count": int(quality[0]),
        "inserted_company_count": int(quality[1]),
        "bolagsverket_role_count": int(quality[2]),
        "esef_role_count": int(quality[3]),
        "wikidata_role_count": int(quality[4]),
        "canonical_role_count": int(quality[5]),
        "roleless_observation_count": roleless_observation_count,
        "unmapped_role_count": 0,
        "total_role_draft_count": total_role_draft_count,
        "source_run_id": source_run_id,
        "company_scope": list(company_scope),
    }
    if log is not None:
        log(
            "Collected Sweden company-person role drafts: inserted=%s "
            "roleless=%s total=%s",
            metadata["inserted_role_draft_count"],
            metadata["roleless_observation_count"],
            metadata["total_role_draft_count"],
        )
    return metadata


def build_role_assignments_insert_sql(
    qualified_stage_table: str,
    company_ids: Sequence[str] = (),
) -> str:
    company_filter = _company_filter("people.company_id", company_ids)
    columns = _columns_sql(ROLE_COLUMNS)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH latest_role_drafts AS (
    SELECT
        drafts.person_draft_id,
        argMax(
            drafts.role_draft_id,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS role_draft_id,
        argMax(
            drafts.company_id,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS company_id,
        argMax(
            drafts.source,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS source,
        argMax(
            drafts.role_code,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS role_code,
        argMax(
            drafts.fiscal_year,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS fiscal_year,
        argMax(
            drafts.source_observed_at,
            (drafts.created_at, toString(drafts.role_draft_id))
        ) AS source_observed_at
    FROM corpscout.se_company_person_role_draft AS drafts FINAL
    GROUP BY drafts.person_draft_id
),
person_evidence AS (
    SELECT
        people.person_id,
        people.company_id,
        arrayJoin(people.draft_ids) AS person_draft_id
    FROM corpscout.se_company_person AS people FINAL
    WHERE {company_filter}
),
assignments AS (
    SELECT
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'se-company-person-role-v1\n',
            evidence.company_id, '\n',
            toString(evidence.person_id), '\n',
            roles.role_code
        ))), 1, 32))) AS role_id,
        evidence.person_id,
        evidence.company_id,
        roles.role_code,
        arraySort(groupUniqArray(roles.role_draft_id)) AS role_draft_ids,
        arraySort(groupUniqArray(evidence.person_draft_id)) AS person_draft_ids,
        arraySort(groupUniqArray(toString(roles.source))) AS sources,
        arraySort(groupUniqArrayIf(
            assumeNotNull(roles.fiscal_year),
            roles.fiscal_year IS NOT NULL
        )) AS fiscal_years,
        min(roles.source_observed_at) AS first_observed_at,
        max(roles.source_observed_at) AS last_observed_at
    FROM person_evidence AS evidence
    INNER JOIN latest_role_drafts AS roles
        ON roles.person_draft_id = evidence.person_draft_id
       AND roles.company_id = evidence.company_id
    GROUP BY evidence.person_id, evidence.company_id, roles.role_code
)
SELECT
    assignments.role_id,
    assignments.person_id,
    assignments.company_id,
    assignments.role_code,
    assignments.role_draft_ids,
    assignments.person_draft_ids,
    assignments.sources,
    assignments.fiscal_years,
    assignments.first_observed_at,
    assignments.last_observed_at,
    toUInt8(1),
    %(source_run_id)s,
    if(
        toString(existing.role_id) = '{_ZERO_UUID}',
        %(updated_at)s,
        existing.created_at
    ),
    %(updated_at)s
FROM assignments
LEFT JOIN corpscout.se_company_person_role AS existing FINAL
    ON existing.company_id = assignments.company_id
   AND existing.person_id = assignments.person_id
   AND existing.role_id = assignments.role_id"""


def _role_assignment_quality_sql(
    qualified_stage_table: str,
    company_ids: Sequence[str],
) -> str:
    company_filter = _company_filter("existing.company_id", company_ids)
    return f"""SELECT
    count() AS staged_role_count,
    uniqExact(staged.company_id) AS staged_company_count,
    uniqExact(staged.person_id) AS staged_person_count,
    uniqExact(staged.role_code) AS canonical_role_count,
    countIf(toString(existing.role_id) = '{_ZERO_UUID}') AS inserted_role_count,
    countIf(
        toString(existing.role_id) != '{_ZERO_UUID}'
        AND (
            existing.role_draft_set_hash != staged.role_draft_set_hash
            OR existing.is_current = 0
        )
    ) AS updated_role_count,
    countIf(
        existing.is_current = 1
        AND existing.role_draft_set_hash = staged.role_draft_set_hash
    ) AS skipped_role_count,
    (
        SELECT count()
        FROM corpscout.se_company_person_role AS existing FINAL
        LEFT JOIN {qualified_stage_table} AS current
            ON current.company_id = existing.company_id
           AND current.person_id = existing.person_id
           AND current.role_id = existing.role_id
        WHERE existing.is_current = 1
          AND {company_filter}
          AND toString(current.role_id) = '{_ZERO_UUID}'
    ) AS deactivated_role_count
FROM {qualified_stage_table} AS staged
LEFT JOIN corpscout.se_company_person_role AS existing FINAL
    ON existing.company_id = staged.company_id
   AND existing.person_id = staged.person_id
   AND existing.role_id = staged.role_id"""


def _publish_role_assignments_sql(
    qualified_stage_table: str,
    company_ids: Sequence[str],
) -> str:
    columns = _columns_sql(ROLE_COLUMNS)
    company_filter = _company_filter("existing.company_id", company_ids)
    return f"""INSERT INTO {_qualified(ROLE_TABLE)} (
    {columns}
)
SELECT
    existing.role_id,
    existing.person_id,
    existing.company_id,
    existing.role_code,
    existing.role_draft_ids,
    existing.person_draft_ids,
    existing.sources,
    existing.fiscal_years,
    existing.first_observed_at,
    existing.last_observed_at,
    toUInt8(0),
    %(source_run_id)s,
    existing.created_at,
    %(updated_at)s
FROM corpscout.se_company_person_role AS existing FINAL
LEFT JOIN {qualified_stage_table} AS current
    ON current.company_id = existing.company_id
   AND current.person_id = existing.person_id
   AND current.role_id = existing.role_id
WHERE existing.is_current = 1
  AND {company_filter}
  AND toString(current.role_id) = '{_ZERO_UUID}'

UNION ALL

SELECT
    staged.role_id,
    staged.person_id,
    staged.company_id,
    staged.role_code,
    staged.role_draft_ids,
    staged.person_draft_ids,
    staged.sources,
    staged.fiscal_years,
    staged.first_observed_at,
    staged.last_observed_at,
    staged.is_current,
    staged.source_run_id,
    staged.created_at,
    staged.updated_at
FROM {qualified_stage_table} AS staged
LEFT JOIN corpscout.se_company_person_role AS existing FINAL
    ON existing.company_id = staged.company_id
   AND existing.person_id = staged.person_id
   AND existing.role_id = staged.role_id
WHERE toString(existing.role_id) = '{_ZERO_UUID}'
   OR existing.is_current = 0
   OR existing.role_draft_set_hash != staged.role_draft_set_hash"""


def materialize_se_company_person_roles(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    updated_at: datetime,
    company_ids: Sequence[str],
    log: Callable[..., object] | None,
) -> dict[str, int | str | list[str]]:
    """Publish current roles by joining normalized people to role drafts."""
    company_scope = normalized_company_ids(company_ids)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=(PERSON_TABLE, ROLE_DRAFT_TABLE, ROLE_TABLE),
    )

    stage_table = f"_tmp_{ROLE_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = _qualified(stage_table)
    qualified_target_table = _qualified(ROLE_TABLE)

    with clickhouse.get_connection() as client:
        stage_created = False
        primary_error: Exception | None = None
        try:
            client.execute(
                f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
            )
            stage_created = True
            client.execute(
                build_role_assignments_insert_sql(
                    qualified_stage_table,
                    company_scope,
                ),
                {"source_run_id": source_run_id, "updated_at": updated_at},
            )
            quality = client.execute(
                _role_assignment_quality_sql(qualified_stage_table, company_scope)
            )[0]
            client.execute(
                _publish_role_assignments_sql(qualified_stage_table, company_scope),
                {"source_run_id": source_run_id, "updated_at": updated_at},
            )
            total_current_role_count = int(
                client.execute(
                    f"SELECT count() FROM {qualified_target_table} FINAL "
                    "WHERE is_current = 1"
                )[0][0]
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            if stage_created:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
                except Exception:
                    if primary_error is None:
                        raise

    metadata: dict[str, int | str | list[str]] = {
        "staged_role_count": int(quality[0]),
        "staged_company_count": int(quality[1]),
        "staged_person_count": int(quality[2]),
        "canonical_role_count": int(quality[3]),
        "inserted_role_count": int(quality[4]),
        "updated_role_count": int(quality[5]),
        "skipped_role_count": int(quality[6]),
        "deactivated_role_count": int(quality[7]),
        "total_current_role_count": total_current_role_count,
        "source_run_id": source_run_id,
        "company_scope": list(company_scope),
    }
    if log is not None:
        log(
            "Published Sweden company-person roles: staged=%s inserted=%s "
            "updated=%s skipped=%s deactivated=%s total=%s",
            metadata["staged_role_count"],
            metadata["inserted_role_count"],
            metadata["updated_role_count"],
            metadata["skipped_role_count"],
            metadata["deactivated_role_count"],
            metadata["total_current_role_count"],
        )
    return metadata


class SECompanyPersonRoleConfig(dg.Config):
    company_ids: list[str] = Field(default_factory=list)


@dg.asset(
    name="se_company_person_role_draft_clickhouse",
    deps=[dg.AssetKey("se_company_person_draft_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": QUALIFIED_ROLE_DRAFT_TABLE},
    description=(
        "Classifies source person roles with the static mapping owned by each "
        "source and appends immutable Swedish role draft observations."
    ),
)
def se_company_person_role_draft_clickhouse(
    context: dg.AssetExecutionContext,
    config: SECompanyPersonRoleConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = collect_se_company_person_role_drafts(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        created_at=datetime.now(UTC),
        company_ids=config.company_ids,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={**metadata, "table": QUALIFIED_ROLE_DRAFT_TABLE}
    )


@dg.asset(
    name="se_company_person_role_clickhouse",
    deps=[
        dg.AssetKey("se_company_person_clickhouse"),
        dg.AssetKey("se_company_person_role_draft_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": QUALIFIED_ROLE_TABLE},
    description=(
        "Publishes application-facing Swedish person-role assignments linked "
        "to normalized people and exact role draft evidence."
    ),
)
def se_company_person_role_clickhouse(
    context: dg.AssetExecutionContext,
    config: SECompanyPersonRoleConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = materialize_se_company_person_roles(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        updated_at=datetime.now(UTC),
        company_ids=config.company_ids,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata={**metadata, "table": QUALIFIED_ROLE_TABLE})


se_company_person_role_job = dg.define_asset_job(
    "se_company_person_role_job",
    selection=dg.AssetSelection.assets(
        "se_company_person_role_draft_clickhouse",
        "se_company_person_role_clickhouse",
    ),
)

se_company_person_role_publish_job = dg.define_asset_job(
    "se_company_person_role_publish_job",
    selection=dg.AssetSelection.assets("se_company_person_role_clickhouse"),
)


defs = dg.Definitions(
    assets=[
        se_company_person_role_draft_clickhouse,
        se_company_person_role_clickhouse,
    ],
    jobs=[se_company_person_role_job, se_company_person_role_publish_job],
)
