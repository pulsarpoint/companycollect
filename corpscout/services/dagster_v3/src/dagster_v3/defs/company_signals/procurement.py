import uuid

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_signals import tables

from dagster_v3.defs.company_signals.rules import (
    COUNTRY_PROCUREMENT_RULES,
    CountryProcurementRule,
)

COUNTRY_CODE = "SE"
SIGNAL_NAME = "government_contract"
UHM_SOURCE = "sweden_uhm_procurement"
TED_SOURCE = "ted_procurement"

def procurement_evidence_insert_sql(
    stage_table: str,
    rule: CountryProcurementRule,
) -> str:
    """Union every source the country declares, then canonicalize duplicates.

    Each source emits the same canonical columns, so this builder neither knows
    nor cares whether a source is a flat awards table or a winners/notices pair.
    """
    columns = ", ".join(tables.GOVERNMENT_CONTRACT_EVIDENCE_COLUMNS)
    # Sources own their CTE text and are inconsistent about a trailing
    # comma, so normalize before joining rather than trusting each one.
    ctes = [
        source.build_cte(rule, source.cte_name).rstrip().rstrip(",")
        for source in rule.sources
    ]
    source_union = "\n        UNION ALL\n".join(
        f"        SELECT * FROM {source.cte_name}" for source in rule.sources
    )
    source_ctes = ",\n".join(ctes)
    return f"""
    INSERT INTO {stage_table} ({columns})
    WITH
{source_ctes},
    source_rows AS
    (
{source_union}
    ),
    cross_source_key_counts AS
    (
        SELECT
            dedup_key,
            uniqExact(source_slug) AS source_count,
            count() AS row_count
        FROM source_rows
        WHERE dedup_key != ''
        GROUP BY dedup_key
        HAVING source_count > 1
    ),
    unambiguous_cross_source_keys AS
    (
        SELECT dedup_key
        FROM cross_source_key_counts
        WHERE row_count = source_count
    ),
    canonicalized AS
    (
        SELECT
            *,
            if(
                dedup_key IN (SELECT dedup_key FROM unambiguous_cross_source_keys),
                concat('cross:', dedup_key),
                evidence_id
            ) AS canonical_evidence_id
        FROM source_rows
    )
    SELECT
        country_code,
        company_id,
        canonical_evidence_id AS evidence_id,
        arraySort(groupUniqArray(source_slug)) AS source_slugs,
        arraySort(groupUniqArray(source_reference)) AS source_references,
        arraySort(groupUniqArrayIf(source_url, source_url != '')) AS source_urls,
        max(publication_date) AS publication_date,
        any(buyer_name) AS buyer_name,
        any(title) AS title,
        anyIf(agreement_type, agreement_type != '') AS agreement_type,
        max(source_updated_at) AS source_updated_at,
        now64(3) AS resolved_at
    FROM canonicalized
    GROUP BY country_code, company_id, canonical_evidence_id
    """


def build_country_contract_asset(
    rule: CountryProcurementRule,
) -> dg.AssetsDefinition:
    """One asset per country, because their upstreams genuinely differ.

    Sweden reads its national register alongside TED; Norway has no
    ingested national source and reads TED alone. Dagster declares deps per
    asset, so a single partitioned asset would make Norway falsely depend on
    Swedish UHM data and stall whenever that source is stale.
    """
    coverage_slugs = ", ".join(f"'{slug}'" for slug in rule.source_slugs)
    caveat = rule.coverage_caveat.replace("'", "''")

    @dg.asset(
        name=rule.asset_name,
        deps=[dg.AssetKey(key) for key in rule.upstream_asset_keys],
        group_name=tables.GROUP_NAME,
        kinds={"clickhouse", "sql"},
        metadata={
            "tables": [
                f"{tables.CLICKHOUSE_DATABASE}.{tables.GOVERNMENT_CONTRACT_EVIDENCE_TABLE}",
                f"{tables.CLICKHOUSE_DATABASE}.{tables.GOVERNMENT_CONTRACT_SUMMARY_TABLE}",
                f"{tables.CLICKHOUSE_DATABASE}.{tables.SIGNAL_COVERAGE_TABLE}",
            ]
        },
        description=(
            f"Government-contract evidence for {rule.country_code}: deduplicates "
            f"{' and '.join(rule.source_slugs)} winner evidence, then publishes "
            "one count/latest-date summary per matched company plus a coverage "
            "row stating what this country's sources do and do not cover."
        ),
    )
    def _country_contract_signals(
            context: dg.AssetExecutionContext,
            clickhouse: ClickhouseResource,
        ) -> dg.MaterializeResult:
        required_tables = (
            tables.GOVERNMENT_CONTRACT_EVIDENCE_TABLE,
            tables.GOVERNMENT_CONTRACT_SUMMARY_TABLE,
            tables.SIGNAL_COVERAGE_TABLE,
            *rule.required_clickhouse_tables,
        )
        assert_clickhouse_tables_exist(
            clickhouse,
            database=tables.CLICKHOUSE_DATABASE,
            tables=required_tables,
        )
        stages = {
            table: f"_tmp_{table}_{uuid.uuid4().hex}"
            for table in (
                tables.GOVERNMENT_CONTRACT_EVIDENCE_TABLE,
                tables.GOVERNMENT_CONTRACT_SUMMARY_TABLE,
                tables.SIGNAL_COVERAGE_TABLE,
            )
        }
        qualified = {table: _qualified(table) for table in stages}
        qualified_stages = {table: _qualified(stage) for table, stage in stages.items()}

        with clickhouse.get_connection() as client:
            [(uhm_rows,)] = client.execute(
                "SELECT count() FROM corpscout.se_uhm_procurement_awards"
            )
            [(ted_rows,)] = client.execute(
                """
                SELECT count()
                FROM corpscout.ted_notice_winners
                WHERE country_iso2 = 'SE'
                  AND length(winner_national_id) = 10
                """
            )
            if int(uhm_rows) + int(ted_rows) == 0:
                raise ValueError(
                    "Both Sweden UHM and TED procurement inputs are empty; "
                    "refusing to replace company summaries"
                )

            for table in stages:
                client.execute(
                    f"CREATE TABLE {qualified_stages[table]} AS {qualified[table]}"
                )
            exchanged: list[str] = []
            primary_error: Exception | None = None
            try:
                evidence_stage = qualified_stages[tables.GOVERNMENT_CONTRACT_EVIDENCE_TABLE]
                client.execute(procurement_evidence_insert_sql(evidence_stage, rule))

                summary_stage = qualified_stages[tables.GOVERNMENT_CONTRACT_SUMMARY_TABLE]
                client.execute(
                    f"""
                    INSERT INTO {summary_stage}
                        ({", ".join(tables.GOVERNMENT_CONTRACT_SUMMARY_COLUMNS)})
                    SELECT
                        country_code,
                        company_id,
                        toUInt32(count()) AS public_award_count,
                        max(publication_date) AS public_award_last_date,
                        arraySort(arrayDistinct(arrayFlatten(groupArray(source_slugs))))
                            AS source_slugs,
                        max(source_updated_at) AS source_updated_at,
                        now64(3) AS resolved_at
                    FROM {evidence_stage}
                    WHERE country_code = '{rule.country_code}'
                    GROUP BY country_code, company_id
                    """
                )

                coverage_stage = qualified_stages[tables.SIGNAL_COVERAGE_TABLE]
                client.execute(
                    f"""
                    INSERT INTO {coverage_stage}
                        ({", ".join(tables.SIGNAL_COVERAGE_COLUMNS)})
                    SELECT
                        '{rule.country_code}' AS country_code,
                        '{SIGNAL_NAME}' AS signal_name,
                        'partial' AS coverage_status,
                        min(publication_date) AS coverage_from,
                        max(publication_date) AS coverage_to,
                        [{coverage_slugs}] AS source_slugs,
                        max(source_updated_at) AS source_updated_at,
                        now64(3) AS resolved_at,
                        '{caveat}'
                            AS caveat
                    FROM {evidence_stage}
                    WHERE country_code = '{rule.country_code}'
                    """
                )

                [(evidence_rows, distinct_companies)] = client.execute(
                    f"""
                    SELECT count(), uniqExact(company_id)
                    FROM {evidence_stage}
                    WHERE country_code = '{rule.country_code}'
                    """
                )
                [(summary_rows,)] = client.execute(
                    f"""
                    SELECT count()
                    FROM {summary_stage}
                    WHERE country_code = '{rule.country_code}'
                    """
                )
                if int(evidence_rows) == 0 or int(summary_rows) == 0:
                    # Refuse to blank a partition that currently holds rows --
                    # that is a degraded refresh. An empty result for a country
                    # that has none yet is not a failure: Norway legitimately
                    # produces nothing until its TED partitions are backfilled,
                    # and failing there would leave the asset permanently red
                    # for a state that is simply "no data yet".
                    existing_rows = client.execute(
                        f"""
                        SELECT count()
                        FROM {qualified[tables.GOVERNMENT_CONTRACT_EVIDENCE_TABLE]}
                        WHERE country_code = '{rule.country_code}'
                        """
                    )[0][0]
                    if int(existing_rows) > 0:
                        raise ValueError(
                            f"{rule.country_code} government-contract signal "
                            f"produced no evidence, but the existing partition "
                            f"holds {existing_rows} rows -- refusing to blank it"
                        )
                    context.log.warning(
                        "%s has no government-contract evidence yet; publishing "
                        "an empty partition and a coverage row that says so",
                        rule.country_code,
                    )
                if int(summary_rows) != int(distinct_companies):
                    raise ValueError(
                        "Sweden procurement summary grain mismatch: "
                        f"summaries={summary_rows} companies={distinct_companies}"
                    )

                for table in stages:
                    client.execute(
                        f"ALTER TABLE {qualified[table]} REPLACE PARTITION '{rule.country_code}' "
                        f"FROM {qualified_stages[table]}"
                    )
                    exchanged.append(table)
            except Exception as exc:
                primary_error = exc
                for table in reversed(exchanged):
                    client.execute(
                        f"ALTER TABLE {qualified[table]} REPLACE PARTITION '{rule.country_code}' "
                        f"FROM {qualified_stages[table]}"
                    )
                raise
            finally:
                for table in reversed(tuple(stages)):
                    try:
                        client.execute(f"DROP TABLE IF EXISTS {qualified_stages[table]}")
                    except Exception:
                        if primary_error is None:
                            raise

        return dg.MaterializeResult(
            metadata={
                "uhm_source_rows": int(uhm_rows),
                "ted_source_rows": int(ted_rows),
                "evidence_rows": int(evidence_rows),
                "summary_rows": int(summary_rows),
                "distinct_companies": int(distinct_companies),
            }
        )


    return _country_contract_signals


COUNTRY_CONTRACT_ASSETS = [
    build_country_contract_asset(rule)
    for rule in COUNTRY_PROCUREMENT_RULES.values()
]

# One job per country, so a country can be refreshed on its own and a failing
# country cannot hold back the others.
COUNTRY_CONTRACT_JOBS = [
    dg.define_asset_job(
        f"{rule.country_code.lower()}_government_contract_signals_job",
        selection=dg.AssetSelection.assets(rule.asset_name),
    )
    for rule in COUNTRY_PROCUREMENT_RULES.values()
]

defs = dg.Definitions(
    assets=COUNTRY_CONTRACT_ASSETS,
    jobs=COUNTRY_CONTRACT_JOBS,
)


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
