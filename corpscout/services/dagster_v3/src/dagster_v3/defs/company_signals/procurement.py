"""Per-country coverage rows for the government-contract signal.

The contracts themselves are no longer materialized. Each country has a view --
``se_government_contracts``, ``fi_government_contracts`` -- merging its own
sources, plus a summary view over it. The migration owns all of it, so nothing
here copies rows and nothing goes stale.

There is deliberately no cross-country contracts view. Countries publish
genuinely different things, and one object spanning them is either unmanageably
wide or lossy -- the lossy version is what the old materialized evidence table
was, and it is how contract value went missing for every country at once.

What remains is coverage: one row per country stating what its sources do and do
not include. That cannot be derived, because the useful part is prose ("Doffin
is not ingested, so contracts below the EU thresholds are absent entirely"). The
dates around it are read from the country's view so they stay honest.
"""

import uuid

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_signals import tables

from dagster_v3.defs.company_signals.rules import (
    COUNTRY_PROCUREMENT_RULES,
    CountryProcurementRule,
)

SIGNAL_NAME = "government_contract"


def build_country_contract_asset(
    rule: CountryProcurementRule,
) -> dg.AssetsDefinition:
    """One asset per country, because their upstreams genuinely differ.

    Sweden reads its national register alongside TED, Norway has no ingested
    national source and reads TED alone. Dagster declares deps per asset, so a
    single partitioned asset would make Norway falsely depend on Swedish UHM
    data and stall whenever that source is stale.
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
                f"{tables.CLICKHOUSE_DATABASE}.{tables.SIGNAL_COVERAGE_TABLE}"
            ],
            "views_read": [
                f"{tables.CLICKHOUSE_DATABASE}.{rule.contracts_view}"
            ],
        },
        description=(
            f"Coverage row for {rule.country_code} government contracts: the "
            f"date span actually present in {rule.contracts_view} together with "
            "a stated caveat about what "
            f"{' and '.join(rule.source_slugs)} do not cover. The contracts "
            "themselves are a view and are never copied."
        ),
    )
    def _country_contract_signals(
        context: dg.AssetExecutionContext,
        clickhouse: ClickhouseResource,
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=tables.CLICKHOUSE_DATABASE,
            tables=(
                tables.SIGNAL_COVERAGE_TABLE,
                rule.contracts_view,
                *rule.required_clickhouse_tables,
            ),
        )
        coverage = _qualified(tables.SIGNAL_COVERAGE_TABLE)
        stage = _qualified(f"_tmp_{tables.SIGNAL_COVERAGE_TABLE}_{uuid.uuid4().hex}")
        view = _qualified(rule.contracts_view)

        with clickhouse.get_connection() as client:
            [(contract_rows, companies)] = client.execute(
                f"SELECT count(), uniqExact(company_id) FROM {view}"
            )
            if int(contract_rows) == 0:
                # Not a failure. Norway legitimately produces nothing until its
                # TED partitions are backfilled, and a country with no rows yet
                # is exactly what a coverage row should be able to say.
                context.log.warning(
                    "%s has no government contracts yet; publishing a coverage "
                    "row that says so",
                    rule.country_code,
                )

            client.execute(f"CREATE TABLE {stage} AS {coverage}")
            try:
                client.execute(
                    f"""
                    INSERT INTO {stage}
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
                        '{caveat}' AS caveat
                    FROM {view}
                    """
                )
                client.execute(
                    f"ALTER TABLE {coverage} REPLACE PARTITION "
                    f"'{rule.country_code}' FROM {stage}"
                )
            finally:
                client.execute(f"DROP TABLE IF EXISTS {stage}")

        return dg.MaterializeResult(
            metadata={
                "contract_rows": int(contract_rows),
                "distinct_companies": int(companies),
                "sources": list(rule.source_slugs),
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
