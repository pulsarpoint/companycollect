import os
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource

from dagster_v3.defs.translator_load.resource import TranslatorResource


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
        dg.Definitions(
            resources={
                "clickhouse": ClickhouseResource(
                    host=dg.EnvVar("CLICKHOUSE_HOST"),
                    port=_int_env("CLICKHOUSE_NATIVE_PORT", 9000),
                    user=dg.EnvVar("CLICKHOUSE_USER"),
                    password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
                    database=dg.EnvVar("CLICKHOUSE_DATABASE"),
                    secure=_bool_env("CLICKHOUSE_SECURE", False),
                ),
                "dlt": DagsterDltResource(),
                "translator": TranslatorResource(
                    base_url=os.getenv(
                        "TRANSLATOR_API_URL",
                        "http://localhost:8080",
                    )
                ),
            },
        ),
    )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
