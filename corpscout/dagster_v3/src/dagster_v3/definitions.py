from pathlib import Path

import dagster as dg
from dagster_dlt import DagsterDltResource

from dagster_v3.defs.clickhouse.resources import clickhouse_resource_from_env


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
        dg.Definitions(
            resources={
                "clickhouse": clickhouse_resource_from_env(),
                "dlt": DagsterDltResource(),
            },
        ),
    )
