#!/usr/bin/env python3
"""Migrate Brave outcomes after migration 428 and after old Brave workers finish."""

import argparse
import json
import os

from clickhouse_driver import Client
from dotenv import load_dotenv

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.company_domains.migrate_results import migrate_results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Import and verify results; retain Postgres and S3 data.",
    )
    args = parser.parse_args()
    if not args.execute:
        parser.error("stop old Brave workers, apply migration 428, then pass --execute")
    load_dotenv()
    client = Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ["PROCESSING_CLICKHOUSE_USER"],
        password=os.environ["PROCESSING_CLICKHOUSE_PASSWORD"],
        database=os.getenv("CLICKHOUSE_DATABASE", "corpscout"),
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"},
    )
    try:
        with ProcessingResource(
            postgres_url=os.environ["PROCESSING_PG_URL"]
        ).get_store() as store:
            print(json.dumps(migrate_results(store, client, batch_size=1000)))
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
