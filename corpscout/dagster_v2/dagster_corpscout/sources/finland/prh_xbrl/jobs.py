"""Jobs: the partitioned window pull and the on-demand company pull.

The on-demand pull is the secondary path from the design: it writes the same
deterministic object keys and the same ReplacingMergeTree tables as the window
pull, so the two paths converge idempotently — it never matters which path
fetched a statement.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml

pull_window_job = dg.define_asset_job(
    name="finland_prh_xbrl_pull_window",
    selection=[raw_xml_documents],
)


class CompanyPullConfig(dg.Config):
    business_id: str


@dg.op
def pull_company_statements(
    context: dg.OpExecutionContext,
    config: CompanyPullConfig,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> None:
    rustfs.ensure_bucket(spec.BUCKET)
    parsed_at = datetime.now(timezone.utc)

    rows_by_table: dict[str, list[dict]] = {
        tables.STATEMENT_DOCUMENTS_TABLE: [],
        tables.CONTEXTS_TABLE: [],
        tables.UNITS_TABLE: [],
        tables.FACTS_TABLE: [],
    }
    downloaded = 0
    with PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT) as client:
        for statement in client.iter_company_financials(config.business_id):
            body, source_url = client.download_financial_xml(
                statement.business_id, statement.financial_date
            )
            object_key = spec.document_object_key(statement.business_id, statement.financial_date)
            rustfs.put_bytes(spec.BUCKET, object_key, body)
            downloaded += 1
            parsed = parse_statement_xml(
                business_id=statement.business_id,
                financial_date=statement.financial_date,
                registration_date=statement.registration_date,
                source_url=source_url,
                xml_object_key=object_key,
                source_run_id=context.run.run_id,
                body=body,
                parsed_at=parsed_at,
            )
            for table, rows in parsed.rows_by_table.items():
                rows_by_table[table].extend(rows)
            for warning in parsed.warnings:
                context.log.warning(warning)

    counts = load_rows(clickhouse, rows_by_table)
    context.log.info(
        "company pull complete business_id=%s statements=%d facts=%d",
        config.business_id,
        downloaded,
        counts[tables.FACTS_TABLE],
    )


@dg.job(name="finland_prh_xbrl_pull_company")
def pull_company_job():
    pull_company_statements()
