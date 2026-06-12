import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="statement_tables",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "parsed"},
    deps=[raw_xml_documents],
    automation_condition=dg.AutomationCondition.eager(),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def statement_tables(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    raise NotImplementedError("Parse raw objects into ClickHouse before registering.")
