from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import (
    financial_metrics,
    raw_xml_documents,
    source_system,
    statement_tables,
)
from dagster_corpscout.sources.finland.prh_xbrl.checks import statement_documents_have_facts
from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_company_job, pull_window_job
from dagster_corpscout.sources.finland.prh_xbrl.schedules import pull_window_schedule

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents, statement_tables, financial_metrics),
    asset_checks=(statement_documents_have_facts,),
    jobs=(pull_window_job, pull_company_job),
    schedules=(pull_window_schedule,),
)

__all__ = ["source_bundle"]
