"""Append immutable Sweden company-person source observations to ClickHouse.

This asset is deliberately not a person resolver. It copies one observation
from each Bolagsverket, ESEF, or Wikidata source row into the draft inbox and
retains the original source values as JSON. Matching people, merging sources,
normalizing roles, and writing application-facing profiles belong to the later
LLM normalization boundary.

The deterministic ``draft_id`` contains the source entity and the source-owned
person/profile hashes. Consequently, rerunning the asset skips a semantic source
version that is already present while an actual person or role change appends a
new immutable observation.
"""

import re
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

DATABASE = "corpscout"
GROUP_NAME = "company_people"

PERSON_DRAFT_TABLE = "se_company_person_draft"
QUALIFIED_PERSON_DRAFT_TABLE = f"{DATABASE}.{PERSON_DRAFT_TABLE}"

PERSON_DRAFT_COLUMNS = (
    "draft_id",
    "company_id",
    "source",
    "source_entity_id",
    "source_record_uid",
    "person_profile_hash",
    "person_role_hash",
    "source_value_json",
    "fiscal_year",
    "source_observed_at",
    "source_run_id",
    "created_at",
)

_REQUIRED_SOURCE_TABLES = (
    PERSON_DRAFT_TABLE,
    "se_financial_report_signatories",
    "esef_document_people",
    "se_companies",
    "company_identifier",
    "wikidata_company_identifiers",
    "wikidata_company_people",
    "wikidata_persons",
)

_COMPANY_ID_PATTERN = re.compile(r"\d{10}")


def _qualified(table: str) -> str:
    return f"`{DATABASE}`.`{table}`"


def _insert_columns(columns: Sequence[str]) -> str:
    return ",\n    ".join(columns)


def normalized_company_ids(company_ids: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted(set(company_id.strip() for company_id in company_ids)))
    if any(
        _COMPANY_ID_PATTERN.fullmatch(company_id) is None for company_id in normalized
    ):
        raise ValueError("Sweden company IDs must contain exactly 10 digits")
    return normalized


def _company_filter(column: str, company_ids: Sequence[str]) -> str:
    normalized = normalized_company_ids(company_ids)
    if not normalized:
        return "1"
    values = ", ".join(f"'{company_id}'" for company_id in normalized)
    return f"{column} IN ({values})"


def _source_observations_sql(company_ids: Sequence[str]) -> str:
    signatory_filter = _company_filter("signatories.company_id", company_ids)
    esef_filter = _company_filter("people.company_id", company_ids)
    company_filter = _company_filter("companies.registration_number", company_ids)

    return f"""swedish_companies AS (
    SELECT registration_number AS company_id
    FROM corpscout.se_companies AS companies FINAL
    WHERE {company_filter}
),
company_leis AS (
    SELECT
        identifiers.company_id,
        upperUTF8(identifiers.issuer_id) AS lei
    FROM corpscout.company_identifier AS identifiers
    INNER JOIN swedish_companies AS companies
        ON companies.company_id = identifiers.company_id
    WHERE identifiers.country_code = 'SE'
      AND identifiers.issuer_scheme = 'lei'
      AND identifiers.is_current = 1
    GROUP BY identifiers.company_id, lei
),
company_wikidata_ids AS (
    SELECT company_id, wikidata_id
    FROM (
        SELECT
            companies.company_id,
            identifiers.wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        INNER JOIN swedish_companies AS companies
            ON companies.company_id = replaceRegexpAll(
                identifiers.identifier_value,
                '[^0-9]',
                ''
            )
        WHERE identifiers.identifier_type = 'se_orgnr'

        UNION ALL

        SELECT
            leis.company_id,
            identifiers.wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        INNER JOIN company_leis AS leis
            ON leis.lei = upperUTF8(identifiers.identifier_value)
        WHERE identifiers.identifier_type = 'lei'
    )
    GROUP BY company_id, wikidata_id
),
source_observations AS (
    SELECT
        signatories.company_id,
        'bolagsverket' AS source,
        toString(signatories.signatory_uid) AS source_entity_id,
        signatories.source_record_uid,
        signatories.person_profile_hash,
        signatories.person_role_hash,
        toJSONString(CAST(tuple(
            signatories.company_id,
            signatories.fiscal_year,
            signatories.statement_key,
            signatories.source_record_uid,
            toString(signatories.signatory_kind),
            signatories.person_seq,
            toString(signatories.signatory_uid),
            signatories.first_name,
            signatories.last_name,
            signatories.role_original,
            toString(signatories.role_kind),
            signatories.resolved_at
        ) AS Tuple(
            company_id String,
            fiscal_year Int32,
            statement_key String,
            source_record_uid String,
            signatory_kind String,
            person_seq UInt16,
            signatory_uid String,
            first_name String,
            last_name String,
            role_original String,
            role_kind String,
            resolved_at DateTime64(3, \'UTC\')
        ))) AS source_value_json,
        if(
            signatories.fiscal_year > 0,
            toNullable(toUInt16(signatories.fiscal_year)),
            CAST(NULL, 'Nullable(UInt16)')
        ) AS fiscal_year,
        signatories.resolved_at AS source_observed_at
    FROM corpscout.se_financial_report_signatories AS signatories
    WHERE {signatory_filter}
      AND trim(concat(signatories.first_name, ' ', signatories.last_name)) != ''

    UNION ALL

    SELECT
        people.company_id,
        'esef' AS source,
        people.source_document_id AS source_entity_id,
        toString(people.source_record_uid) AS source_record_uid,
        people.person_profile_hash,
        people.person_role_hash,
        toJSONString(CAST(tuple(
            toString(people.candidate_uid),
            toString(people.source_record_uid),
            people.source_document_id,
            toString(people.country_code),
            people.company_id,
            people.fiscal_year,
            people.name,
            people.role,
            toString(people.role_category),
            people.organization,
            toString(people.status),
            people.effective_from,
            people.effective_to,
            people.confidence,
            people.evidence_ids,
            toString(people.model_provider),
            people.model_name,
            people.prompt_version,
            people.source_run_id,
            people.extracted_at
        ) AS Tuple(
            candidate_uid String,
            source_record_uid String,
            source_document_id String,
            country_code String,
            company_id String,
            fiscal_year UInt16,
            name String,
            role String,
            role_category String,
            organization String,
            status String,
            effective_from Nullable(Date32),
            effective_to Nullable(Date32),
            confidence Float32,
            evidence_ids Array(String),
            model_provider String,
            model_name String,
            prompt_version String,
            source_run_id String,
            extracted_at DateTime64(3, \'UTC\')
        ))) AS source_value_json,
        toNullable(people.fiscal_year) AS fiscal_year,
        people.extracted_at AS source_observed_at
    FROM corpscout.esef_document_people AS people FINAL
    WHERE people.country_code = 'SE'
      AND people.company_id != ''
      AND trim(people.name) != ''
      AND {esef_filter}

    UNION ALL

    SELECT
        company_ids.company_id,
        'wikidata' AS source,
        links.source_record_id AS source_entity_id,
        persons.source_record_uid,
        persons.person_profile_hash,
        links.person_role_hash,
        toJSONString(CAST(tuple(
            links.company_wikidata_id,
            links.person_wikidata_id,
            toString(links.role_property),
            toString(links.role_label),
            links.start_date,
            links.end_date,
            links.is_current,
            toString(links.source_system),
            links.source_run_id,
            links.source_record_id,
            toString(links.source_payload_hash),
            links.retrieved_at,
            links.resolved_at,
            persons.source_record_uid,
            persons.name,
            persons.description,
            persons.birth_year,
            persons.image_url,
            persons.wikidata_url,
            toString(persons.source_system),
            persons.source_run_id,
            persons.source_record_id,
            toString(persons.source_payload_hash),
            persons.retrieved_at,
            persons.resolved_at
        ) AS Tuple(
            company_wikidata_id String,
            person_wikidata_id String,
            role_property String,
            role_label String,
            start_date Nullable(Date),
            end_date Nullable(Date),
            is_current UInt8,
            link_source_system String,
            link_source_run_id String,
            link_source_record_id String,
            link_source_payload_hash String,
            link_retrieved_at DateTime64(3, \'UTC\'),
            link_resolved_at DateTime64(3, \'UTC\'),
            person_source_record_uid String,
            name String,
            description Nullable(String),
            birth_year Nullable(UInt16),
            image_url Nullable(String),
            wikidata_url Nullable(String),
            person_source_system String,
            person_source_run_id String,
            person_source_record_id String,
            person_source_payload_hash String,
            person_retrieved_at DateTime64(3, \'UTC\'),
            person_resolved_at DateTime64(3, \'UTC\')
        ))) AS source_value_json,
        CAST(NULL, 'Nullable(UInt16)') AS fiscal_year,
        greatest(links.resolved_at, persons.resolved_at) AS source_observed_at
    FROM company_wikidata_ids AS company_ids
    INNER JOIN corpscout.wikidata_company_people AS links FINAL
        ON links.company_wikidata_id = company_ids.wikidata_id
    INNER JOIN corpscout.wikidata_persons AS persons FINAL
        ON persons.person_wikidata_id = links.person_wikidata_id
    WHERE trim(persons.name) != ''
)"""


def build_person_draft_insert_sql(
    qualified_stage_table: str,
    company_ids: Sequence[str] = (),
) -> str:
    source_sql = _source_observations_sql(company_ids)
    columns = _insert_columns(PERSON_DRAFT_COLUMNS)
    existing_filter = _company_filter("company_id", company_ids)

    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH {source_sql},
candidates AS (
    SELECT
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'se-company-person-source-observation-v1\n',
            company_id, '\n',
            source, '\n',
            source_entity_id, '\n',
            toString(person_profile_hash), '\n',
            toString(person_role_hash)
        ))), 1, 32))) AS draft_id,
        *
    FROM source_observations
)
SELECT
    draft_id,
    company_id,
    source,
    source_entity_id,
    source_record_uid,
    person_profile_hash,
    person_role_hash,
    source_value_json,
    fiscal_year,
    source_observed_at,
    %(source_run_id)s AS source_run_id,
    %(created_at)s AS created_at
FROM candidates
WHERE draft_id NOT IN (
    SELECT draft_id
    FROM corpscout.se_company_person_draft FINAL
    WHERE {existing_filter}
)"""


def _publish_stage_sql(qualified_stage_table: str) -> str:
    columns = _insert_columns(PERSON_DRAFT_COLUMNS)
    return f"""INSERT INTO {_qualified(PERSON_DRAFT_TABLE)} (
    {columns}
)
SELECT
    {columns}
FROM {qualified_stage_table}"""


def _quality_sql(qualified_stage_table: str) -> str:
    return f"""SELECT
    count() AS inserted_observation_count,
    uniqExact(company_id) AS inserted_company_count,
    countIf(source = 'bolagsverket') AS bolagsverket_observation_count,
    countIf(source = 'esef') AS esef_observation_count,
    countIf(source = 'wikidata') AS wikidata_observation_count
FROM {qualified_stage_table}"""


def collect_se_company_person_drafts(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    created_at: datetime,
    company_ids: Sequence[str] = (),
    log: Callable[..., object] | None = None,
) -> dict[str, int | str | list[str]]:
    """Append unseen semantic source observations to the Sweden draft inbox."""
    company_scope = normalized_company_ids(company_ids)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=_REQUIRED_SOURCE_TABLES,
    )

    stage_table = f"_tmp_{PERSON_DRAFT_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = _qualified(stage_table)
    qualified_target_table = _qualified(PERSON_DRAFT_TABLE)

    if log is not None:
        log(
            "Collecting Sweden company-person source observations: "
            "source_run_id=%s company_scope=%s",
            source_run_id,
            list(company_scope) if company_scope else "all",
        )

    with clickhouse.get_connection() as client:
        stage_created = False
        primary_error: Exception | None = None
        try:
            client.execute(
                f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
            )
            stage_created = True
            client.execute(
                build_person_draft_insert_sql(
                    qualified_stage_table,
                    company_scope,
                ),
                {"source_run_id": source_run_id, "created_at": created_at},
            )
            quality = client.execute(_quality_sql(qualified_stage_table))[0]
            client.execute(_publish_stage_sql(qualified_stage_table))

            total_observation_count = int(
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
        "inserted_observation_count": int(quality[0]),
        "inserted_company_count": int(quality[1]),
        "bolagsverket_observation_count": int(quality[2]),
        "esef_observation_count": int(quality[3]),
        "wikidata_observation_count": int(quality[4]),
        "total_observation_count": total_observation_count,
        "source_run_id": source_run_id,
        "company_scope": list(company_scope),
    }
    if log is not None:
        log(
            "Collected Sweden company-person source observations: inserted=%s total=%s",
            metadata["inserted_observation_count"],
            metadata["total_observation_count"],
        )
    return metadata


class SECompanyPersonDraftConfig(dg.Config):
    company_ids: list[str] = []


_SOURCE_ASSET_DEPS = (
    dg.AssetKey("se_financial_report_signatories_clickhouse"),
    dg.AssetKey("esef_document_people_clickhouse"),
    dg.AssetKey("sweden_company_companies_clickhouse"),
    dg.AssetKey("company_identifier_clickhouse"),
    dg.AssetKey("wikidata_company_identifiers"),
    dg.AssetKey("wikidata_company_people"),
    dg.AssetKey("wikidata_persons"),
)


@dg.asset(
    name="se_company_person_draft_clickhouse",
    deps=_SOURCE_ASSET_DEPS,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": QUALIFIED_PERSON_DRAFT_TABLE},
    description=(
        "Appends immutable Bolagsverket, ESEF, and Wikidata source observations "
        "to the Sweden company-person draft inbox without matching or normalization."
    ),
)
def se_company_person_draft_clickhouse(
    context: dg.AssetExecutionContext,
    config: SECompanyPersonDraftConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = collect_se_company_person_drafts(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        created_at=datetime.now(UTC),
        company_ids=config.company_ids,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={**metadata, "table": QUALIFIED_PERSON_DRAFT_TABLE}
    )


se_company_person_draft_job = dg.define_asset_job(
    "se_company_person_draft_job",
    selection=dg.AssetSelection.assets("se_company_person_draft_clickhouse"),
)


defs = dg.Definitions(
    assets=[se_company_person_draft_clickhouse],
    jobs=[se_company_person_draft_job],
)
