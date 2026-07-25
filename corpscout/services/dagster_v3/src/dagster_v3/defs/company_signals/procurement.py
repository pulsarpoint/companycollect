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

def _national_source_cte(rule: CountryProcurementRule) -> str:
    """The country's own procurement register, when one is ingested."""
    return f"""    uhm_base AS
    (
        SELECT
            '{rule.country_code}' AS country_code,
            u.company_id AS company_id,
            concat(
                'uhm:',
                lower(hex(MD5(concat(
                    u.company_id, '|', u.source_procurement_id, '|', u.source_lot_id,
                    '|', ifNull(toString(u.publication_date), ''), '|',
                    lowerUTF8(replaceRegexpAll(trim(u.title), '\\\\s+', ' '))
                ))))
            ) AS evidence_id,
            '{UHM_SOURCE}' AS source_slug,
            concat(
                u.source_procurement_id,
                if(u.source_lot_id = '', '', concat(':', u.source_lot_id))
            ) AS source_reference,
            u.publication_date AS publication_date,
            u.buyer_name AS buyer_name,
            u.title AS title,
            any(u.agreement_type) AS agreement_type,
            max(u.source_retrieved_at) AS source_updated_at,
            if(
                u.publication_date IS NULL OR u.buyer_name = '' OR u.title = '',
                '',
                lower(hex(MD5(concat(
                    u.company_id, '|',
                    lowerUTF8(replaceRegexpAll(trim(u.buyer_name), '\\\\s+', ' ')), '|',
                    toString(u.publication_date), '|',
                    lowerUTF8(replaceRegexpAll(trim(u.title), '\\\\s+', ' '))
                ))))
            ) AS dedup_key
        FROM corpscout.{rule.national_source.awards_table} AS u
        WHERE u.company_match_status = 'exact'
          AND u.company_id != ''
        GROUP BY
            u.company_id,
            u.source_procurement_id,
            u.source_lot_id,
            u.publication_date,
            u.buyer_name,
            u.title
    ),
"""


def procurement_evidence_insert_sql(
    stage_table: str,
    rule: CountryProcurementRule,
) -> str:
    """Build the deterministic UHM/TED evidence merge for Sweden."""
    columns = ", ".join(tables.GOVERNMENT_CONTRACT_EVIDENCE_COLUMNS)
    ted_countries = ", ".join(f"'{c}'" for c in rule.ted_winner_countries)
    # A country with no ingested national register contributes TED alone. The
    # cross-source dedup below then finds no UHM rows and becomes a harmless
    # no-op rather than needing its own branch.
    uhm_cte = _national_source_cte(rule) if rule.national_source else ""
    source_union = (
        "        SELECT * FROM uhm_base\n        UNION ALL\n        SELECT * FROM ted_base"
        if rule.national_source
        else "        SELECT * FROM ted_base"
    )
    return f"""
    INSERT INTO {stage_table} ({columns})
    WITH
{uhm_cte}    ted_base AS
    (
        SELECT
            '{rule.country_code}' AS country_code,
            c.{rule.company_id_column} AS company_id,
            concat(
                'ted:', w.publication_number, ':', w.lot_id, ':',
                w.tender_id, ':', toString(w.winner_ordinal)
            ) AS evidence_id,
            '{TED_SOURCE}' AS source_slug,
            concat(
                w.publication_number, ':', w.lot_id, ':',
                w.tender_id, ':', toString(w.winner_ordinal)
            ) AS source_reference,
            w.publication_date AS publication_date,
            any(n.buyer_name) AS buyer_name,
            any(n.notice_title) AS title,
            '' AS agreement_type,
            max(greatest(w.resolved_at, n.resolved_at)) AS source_updated_at,
            if(
                w.publication_date IS NULL
                    OR any(n.buyer_name) = ''
                    OR any(n.notice_title) = '',
                '',
                lower(hex(MD5(concat(
                    c.{rule.company_id_column}, '|',
                    lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\\\s+', ' ')), '|',
                    toString(w.publication_date), '|',
                    lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\\\s+', ' '))
                ))))
            ) AS dedup_key
        FROM corpscout.ted_notice_winners AS w
        INNER JOIN corpscout.ted_notices AS n
            ON n.country_iso2 = w.country_iso2
           AND n.publication_number = w.publication_number
        INNER JOIN corpscout.{rule.companies_table} AS c
            ON c.{rule.company_id_column} = w.winner_national_id
        WHERE w.country_iso2 = '{rule.country_code}'
          AND upper(w.winner_country) IN ({ted_countries})
          AND length(w.winner_national_id) = {rule.identifier_length}
          AND length(c.{rule.company_id_column}) = {rule.identifier_length}
        GROUP BY
            c.{rule.company_id_column},
            w.publication_number,
            w.lot_id,
            w.tender_id,
            w.winner_ordinal,
            w.publication_date
    ),
    source_rows AS
    (
{source_union}
    ),
    cross_source_key_counts AS
    (
        SELECT
            dedup_key,
            countIf(source_slug = '{UHM_SOURCE}') AS uhm_rows,
            countIf(source_slug = '{TED_SOURCE}') AS ted_rows
        FROM source_rows
        WHERE dedup_key != ''
        GROUP BY dedup_key
        HAVING uhm_rows > 0 AND ted_rows > 0
    ),
    unambiguous_cross_source_keys AS
    (
        SELECT dedup_key
        FROM cross_source_key_counts
        WHERE uhm_rows = 1 AND ted_rows = 1
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
