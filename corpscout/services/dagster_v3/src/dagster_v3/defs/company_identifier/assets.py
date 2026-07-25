import uuid
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_identifier import tables
from dagster_v3.defs.company_identifier.rules import (
    COUNTRY_IDENTITY_RULES,
    CountryIdentityRule,
)

COMPANY_IDENTIFIER_UPSTREAM_ASSET_KEYS = (
    "gleif_reference_clickhouse",
    "sweden_company_companies_clickhouse",
)

_QUALITY_COLUMNS = (
    "row_count",
    "issuer_count",
    "company_count",
    "identity_key_count",
    "invalid_rows",
)


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def build_company_identifier_insert_sql(
    stage_table: str,
    rule: CountryIdentityRule,
) -> str:
    """Resolve GLEIF records to one country's register.

    The register is joined deduplicated because country company tables are
    ReplacingMergeTree: an undeduplicated join multiplies rows for any
    company_id with unmerged parts and trips the grain check intermittently.

    A row is emitted only when the normalized identifier exists in the register.
    The register is the ground truth, so an issuer that does not resolve is
    dropped rather than stored as an unverified guess. That lookup is the whole
    validation -- there is one confidence level because there is one proof.
    """
    columns = ", ".join(tables.COMPANY_IDENTIFIER_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
WITH
register_current AS
(
    SELECT company_id
    FROM corpscout.{rule.register_table}
    GROUP BY company_id
),
gleif_country AS
(
    SELECT
        upperUTF8(trimBoth(lei)) AS lei,
        argMax(ifNull(registered_at_id, ''), (resolved_at, source_run_id))
            AS registered_at_id,
        argMax(ifNull(registered_as, ''), (resolved_at, source_run_id))
            AS registered_as_raw,
        argMax(ifNull(entity_status, ''), (resolved_at, source_run_id))
            AS entity_status,
        argMax(ifNull(registration_status, ''), (resolved_at, source_run_id))
            AS registration_status,
        argMax(ifNull(successor_entity_lei, ''), (resolved_at, source_run_id))
            AS successor_issuer_id,
        argMax(ifNull(jurisdiction, ''), (resolved_at, source_run_id))
            AS jurisdiction,
        min(toDate(resolved_at)) AS first_seen_date,
        max(toDate(resolved_at)) AS last_seen_date
    FROM corpscout.gleif_lei_records
    WHERE trimBoth(lei) != ''
    GROUP BY lei
),
gleif_normalized AS
(
    SELECT
        *,
        replaceRegexpAll(registered_as_raw, '[^0-9]', '') AS company_id_normalized
    FROM gleif_country
    WHERE upperUTF8(jurisdiction) = '{rule.country_code}'
)
SELECT
    '{rule.issuer_scheme}' AS issuer_scheme,
    g.lei AS issuer_id,
    '{rule.country_code}' AS country_code,
    r.company_id AS company_id,
    'gleif_registered_as' AS match_method,
    'register_verified' AS match_confidence,
    g.registered_at_id AS registration_authority_id,
    g.registered_as_raw AS registered_as_raw,
    g.company_id_normalized AS company_id_normalized,
    g.entity_status AS entity_status,
    g.registration_status AS registration_status,
    toUInt8(g.successor_issuer_id = '') AS is_current,
    g.successor_issuer_id AS successor_issuer_id,
    g.first_seen_date AS first_seen_date,
    g.last_seen_date AS last_seen_date,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM gleif_normalized AS g
INNER JOIN register_current AS r
    ON r.company_id = g.company_id_normalized
WHERE length(g.company_id_normalized) = {rule.identifier_length}"""


def _quality_sql(stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(issuer_id) AS issuer_count,
    uniqExact(company_id) AS company_count,
    uniqExact((issuer_scheme, issuer_id, country_code, company_id))
        AS identity_key_count,
    countIf(
        issuer_scheme = ''
        OR issuer_id = ''
        OR country_code = ''
        OR company_id = ''
        OR match_method = ''
        OR match_confidence = ''
    ) AS invalid_rows
FROM {stage_table}"""


def _validate_quality(
    quality: dict[str, object],
    rule: CountryIdentityRule,
) -> None:
    row_count = int(quality["row_count"])
    identity_key_count = int(quality["identity_key_count"])
    invalid_rows = int(quality["invalid_rows"])

    if row_count == 0:
        raise ValueError(
            f"{rule.country_code} company identifier resolution produced no rows"
        )
    if row_count < rule.min_expected_rows:
        raise ValueError(
            f"{rule.country_code} company identifier rows below the expected "
            f"floor: rows={row_count} floor={rule.min_expected_rows}"
        )
    if identity_key_count != row_count:
        raise ValueError(
            f"{rule.country_code} company identifier grain mismatch: "
            f"rows={row_count} unique_keys={identity_key_count}"
        )
    if invalid_rows != 0:
        raise ValueError(
            f"{rule.country_code} company identifier rows are invalid: "
            f"{invalid_rows}"
        )


def replace_company_identifier_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    rule: CountryIdentityRule,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild one country's issuer to company resolution."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.COMPANY_IDENTIFIER_TABLE,
            "gleif_lei_records",
            rule.register_table,
        ),
    )
    stage_name = (
        f"_tmp_{tables.COMPANY_IDENTIFIER_TABLE}_"
        f"{rule.country_code.lower()}_{uuid.uuid4().hex}"
    )
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.COMPANY_IDENTIFIER_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_company_identifier_insert_sql(qualified_stage, rule),
                {"source_run_id": source_run_id, "resolved_at": resolved_at},
            )
            row = client.execute(_quality_sql(qualified_stage))[0]
            quality = {
                column: value
                for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
            }
            _validate_quality(quality, rule)
            client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise

    return {
        **quality,
        "country_code": rule.country_code,
        "issuer_scheme": rule.issuer_scheme,
        "register_table": rule.register_table,
        "table": tables.QUALIFIED_COMPANY_IDENTIFIER_TABLE,
        "source_run_id": source_run_id,
    }


@dg.asset(
    name="company_identifier_clickhouse",
    deps=[dg.AssetKey(key) for key in COMPANY_IDENTIFIER_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="company_identifier_clickhouse",
    metadata={"table": tables.QUALIFIED_COMPANY_IDENTIFIER_TABLE},
    description=(
        "Resolves GLEIF LEI records to national company identifiers by "
        "validating the normalized registered_as value against the country "
        "register. An issuer that does not resolve produces no row."
    ),
)
def company_identifier_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_company_identifier_clickhouse(
        clickhouse=clickhouse,
        rule=COUNTRY_IDENTITY_RULES["SE"],
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Resolved %s company identifiers: rows=%s issuers=%s companies=%s",
        metadata["country_code"],
        metadata["row_count"],
        metadata["issuer_count"],
        metadata["company_count"],
    )
    return dg.MaterializeResult(metadata=metadata)


company_identifier_job = dg.define_asset_job(
    "company_identifier_job",
    selection=dg.AssetSelection.assets("company_identifier_clickhouse"),
)

defs = dg.Definitions(
    assets=[company_identifier_clickhouse],
    jobs=[company_identifier_job],
)
