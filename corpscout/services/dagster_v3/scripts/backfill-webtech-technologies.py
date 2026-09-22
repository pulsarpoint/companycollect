#!/usr/bin/env python3
"""Index existing RustFS Webtech detections after ClickHouse migration 432."""

import json
import os

import click
from clickhouse_driver import Client
from dotenv import load_dotenv

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.backfill import backfill_technology_results


@click.command(help=__doc__)
@click.option(
    "--execute",
    is_flag=True,
    help="Write detection rows; otherwise show the pending scope.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=0),
    default=0,
    help="Maximum reports to visit; 0 means all.",
)
@click.option("--workers", type=click.IntRange(1, 32), default=8)
@click.option("--batch-size", type=click.IntRange(1, 1000), default=250)
def main(execute: bool, limit: int, workers: int, batch_size: int) -> None:
    load_dotenv()
    client = Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="corpscout",
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"},
    )
    try:
        if execute:
            result = backfill_technology_results(
                client,
                ObjectStoreResource(),
                batch_size=batch_size,
                workers=workers,
                limit=limit,
            )
        else:
            rows = client.execute(
                "SELECT count(), sum(technology_count) "
                "FROM corpscout.webtech_domain_scan_results FINAL WHERE technology_count > 0"
            )
            result = {
                "reports_with_detections": rows[0][0],
                "expected_technology_rows": rows[0][1],
            }
        click.echo(json.dumps(result))
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
