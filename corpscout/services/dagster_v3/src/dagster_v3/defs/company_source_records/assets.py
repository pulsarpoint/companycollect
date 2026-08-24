import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_source_records import tables
from dagster_v3.defs.company_source_records.sql import (
    esef_document_observation_sql,
    esef_source_record_sql,
    finland_financial_source_record_sql,
    sweden_financial_source_record_sql,
    sweden_registry_source_record_sql,
    wikidata_source_record_sql,
)


def publish_company_source_records(
    *,
    clickhouse: ClickhouseResource,
    statements: tuple[str, ...],
    source_slugs: tuple[str, ...],
) -> dict[str, int]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.DATABASE,
        tables=(
            tables.SOURCE_RECORDS_TABLE,
            tables.SOURCE_RECORD_ORIGINS_TABLE,
            tables.SOURCE_RECORD_LINKS_TABLE,
            tables.DESCRIPTION_OBSERVATIONS_TABLE,
        ),
    )
    with clickhouse.get_connection() as client:
        for statement in statements:
            client.execute(statement)
        source_rows = client.execute(
            f"SELECT count(), uniqExact(source_record_uid) "
            f"FROM {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE} FINAL "
            "WHERE source_slug IN %(source_slugs)s",
            {"source_slugs": source_slugs},
        )[0]
        linked_rows = client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE} FINAL "
            "WHERE source_record_uid IN ("
            f"SELECT source_record_uid FROM {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE} FINAL "
            "WHERE source_slug IN %(source_slugs)s)",
            {"source_slugs": source_slugs},
        )[0][0]
        invalid_link_count = client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE} FINAL "
            "WHERE country_code = '' OR company_id = '' OR match_method = '' "
            "OR match_confidence < 0 OR match_confidence > 1"
        )[0][0]
        orphan_count = client.execute(
            "SELECT count() FROM ("
            "SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE} FINAL "
            "UNION ALL SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE} FINAL "
            "UNION ALL SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE} FINAL"
            ") AS references ANTI JOIN "
            f"{tables.QUALIFIED_SOURCE_RECORDS_TABLE} AS records FINAL "
            "USING (source_record_uid)"
        )[0][0]
        if invalid_link_count != 0:
            raise ValueError(
                f"Company source records contain {invalid_link_count} invalid company links"
            )
        if orphan_count != 0:
            raise ValueError(
                f"Company source records contain {orphan_count} orphan references"
            )
    return {
        "origin_count": int(source_rows[0]),
        "source_record_count": int(source_rows[1]),
        "company_link_count": int(linked_rows),
    }


def publish_esef_document_observations(
    *, clickhouse: ClickhouseResource
) -> dict[str, int]:
    observation_tables = (
        tables.DESCRIPTION_OBSERVATIONS_TABLE,
        tables.ESEF_DOCUMENT_PEOPLE_TABLE,
        tables.ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE,
        tables.ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.DATABASE,
        tables=(tables.SOURCE_RECORDS_TABLE, *observation_tables),
    )
    with clickhouse.get_connection() as client:
        for statement in esef_document_observation_sql():
            client.execute(statement)
        orphan_count = client.execute(
            "SELECT count() FROM ("
            "SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE} FINAL "
            "WHERE extraction_method = 'llm_extraction' "
            "UNION ALL SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE} FINAL "
            "UNION ALL SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE} FINAL "
            "UNION ALL SELECT source_record_uid FROM "
            f"{tables.QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE} FINAL"
            ") AS observations ANTI JOIN "
            f"{tables.QUALIFIED_SOURCE_RECORDS_TABLE} AS records FINAL "
            "USING (source_record_uid)",
        )[0][0]
        if orphan_count != 0:
            raise ValueError(
                "ESEF document observations contain "
                f"{orphan_count} missing source-record references"
            )
        counts = {
            table: int(
                client.execute(f"SELECT count() FROM {tables.DATABASE}.{table} FINAL")[
                    0
                ][0]
            )
            for table in observation_tables
        }
    return counts


@dg.asset(
    name="sweden_registry_company_source_records_clickhouse",
    deps=[dg.AssetKey("sweden_company_companies_clickhouse")],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance"},
    metadata={"tables": [tables.QUALIFIED_SOURCE_RECORDS_TABLE]},
)
def sweden_registry_company_source_records_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_company_source_records(
            clickhouse=clickhouse,
            statements=sweden_registry_source_record_sql(),
            source_slugs=("sweden_bolagsverket", "sweden_scb"),
        )
    )


@dg.asset(
    name="sweden_financial_company_source_records_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("sweden_financial_backfill_reports_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetKey("sweden_financial_current_reports_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance", "xbrl"},
    metadata={"tables": [tables.QUALIFIED_SOURCE_RECORDS_TABLE]},
)
def sweden_financial_company_source_records_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_company_source_records(
            clickhouse=clickhouse,
            statements=sweden_financial_source_record_sql(),
            source_slugs=("sweden_financial",),
        )
    )


@dg.asset(
    name="finland_financial_company_source_records_clickhouse",
    deps=[dg.AssetKey("fi_financial_statements_ch")],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance", "xbrl"},
    metadata={"tables": [tables.QUALIFIED_SOURCE_RECORDS_TABLE]},
)
def finland_financial_company_source_records_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_company_source_records(
            clickhouse=clickhouse,
            statements=finland_financial_source_record_sql(),
            source_slugs=("finland_prh_xbrl",),
        )
    )


@dg.asset(
    name="esef_company_source_records_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetKey("esef_filings_clickhouse"),
        dg.AssetKey("esef_entity_registry_map_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance", "xbrl"},
    metadata={"tables": [tables.QUALIFIED_SOURCE_RECORDS_TABLE]},
)
def esef_company_source_records_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_company_source_records(
            clickhouse=clickhouse,
            statements=esef_source_record_sql(),
            source_slugs=("esef_filings",),
        )
    )


@dg.asset(
    name="esef_document_observations_clickhouse",
    deps=[
        dg.AssetKey("esef_document_company_information_clickhouse"),
        dg.AssetKey("esef_company_source_records_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance", "llm", "xbrl"},
    metadata={
        "tables": [
            tables.QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE,
            tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE,
            tables.QUALIFIED_ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE,
            tables.QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE,
        ]
    },
)
def esef_document_observations_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_esef_document_observations(clickhouse=clickhouse)
    )


@dg.asset(
    name="wikidata_company_source_records_clickhouse",
    deps=[dg.AssetKey("wikidata_snapshot_complete")],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql", "provenance", "wikidata"},
    metadata={"tables": [tables.QUALIFIED_SOURCE_RECORDS_TABLE]},
)
def wikidata_company_source_records_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=publish_company_source_records(
            clickhouse=clickhouse,
            statements=wikidata_source_record_sql(),
            source_slugs=("wikidata",),
        )
    )


defs = dg.Definitions(
    assets=[
        sweden_registry_company_source_records_clickhouse,
        sweden_financial_company_source_records_clickhouse,
        finland_financial_company_source_records_clickhouse,
        esef_company_source_records_clickhouse,
        esef_document_observations_clickhouse,
        wikidata_company_source_records_clickhouse,
    ]
)
