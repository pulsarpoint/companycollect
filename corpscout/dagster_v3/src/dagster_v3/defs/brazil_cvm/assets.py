import dagster as dg

from dagster_v3.defs.brazil_cvm.source import (
    BRAZIL_CVM_DFP_END_YEAR,
    BRAZIL_CVM_DFP_START_YEAR,
    BRAZIL_CVM_GROUP_NAME,
    BrazilCvmDfpResource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_CVM_DFP_RAW_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        str(year)
        for year in range(BRAZIL_CVM_DFP_START_YEAR, BRAZIL_CVM_DFP_END_YEAR + 1)
    ]
)


@dg.asset(
    group_name=BRAZIL_CVM_GROUP_NAME,
    kinds={"python", "s3", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_CVM_DFP_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Downloads Brazil CVM DFP yearly ZIP archives for 2010-2026 into object storage.",
)
def brazil_cvm_dfp_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_cvm_dfp: BrazilCvmDfpResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_cvm_dfp.sync_year_archive(
        year=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


brazil_cvm_dfp_raw_backfill_job = dg.define_asset_job(
    "brazil_cvm_dfp_raw_backfill_job",
    selection=dg.AssetSelection.assets("brazil_cvm_dfp_raw_archives_s3"),
)


defs = dg.Definitions(
    assets=[brazil_cvm_dfp_raw_archives_s3],
    jobs=[brazil_cvm_dfp_raw_backfill_job],
    resources={"brazil_cvm_dfp": BrazilCvmDfpResource()},
)
