"""Publication order and temporary explicit legacy/shadow dual writes."""

import os
from pathlib import Path

from clickhouse_driver import Client

from dagster_v3.defs.webtech.technologies import WEBTECH_TECHNOLOGY_COLUMNS

PAGE_KEY = (
    "root_domain",
    "website_origin",
    "page_url",
    "crawl_id",
    "detector_version",
    "scan_id",
)


def destinations() -> tuple[tuple[str, bool], ...]:
    # A shared file can hold already-loaded workers during the short cutover.
    if Path(
        os.getenv("WEBTECH_PAUSE_FILE", "/tmp/corpscout-webtech-writes-paused")
    ).exists():
        raise RuntimeError(
            "Webtech indexing is paused for schema cutover; retry from the durable manifest"
        )
    mode = os.getenv("WEBTECH_WRITE_MODE", "canonical")
    if mode == "canonical":
        return (("", True),)
    if mode == "dual":
        return (("", False), ("_v2", True))
    raise ValueError(f"Unsupported WEBTECH_WRITE_MODE: {mode}")


def validate_report_identities(
    client: Client, table: str, columns: tuple[str, ...], rows: list[tuple[object, ...]]
) -> None:
    if not rows:
        return
    key_indexes = [columns.index(column) for column in PAGE_KEY]
    hash_index = columns.index("report_sha256")
    expected = {}
    for row in rows:
        key = tuple(row[index] for index in key_indexes)
        report_hash = row[hash_index]
        if key in expected and expected[key] != report_hash:
            raise ValueError(f"Conflicting Webtech report for page/scan: {key}")
        expected[key] = report_hash
    for offset in range(0, len(expected), 1000):
        keys = tuple(expected)[offset : offset + 1000]
        existing = client.execute(
            f"SELECT DISTINCT {', '.join(PAGE_KEY)}, report_sha256 FROM corpscout.{table} "
            f"WHERE ({', '.join(PAGE_KEY)}) IN %(keys)s",
            {"keys": keys},
        )
        for *key, report_hash in existing:
            if expected[tuple(key)] != report_hash:
                raise ValueError(
                    f"Conflicting stored Webtech report for page/scan: {key}"
                )


def insert_rows(
    client: Client,
    table: str,
    columns: tuple[str, ...],
    rows: list[tuple[object, ...]],
    *,
    page_schema: bool,
) -> None:
    selected = columns if page_schema else tuple(column for column in columns if column not in {"website_origin", "page_url", "task_id", "input_id"})
    for offset in range(0, len(rows), 50_000):
        batch = rows[offset : offset + 50_000]
        if not page_schema:
            batch = [tuple(row[columns.index(column)] for column in selected) for row in batch]
        client.execute(
            f"INSERT INTO corpscout.{table} ({', '.join(selected)}) VALUES", batch,
            settings={"async_insert": 0},
        )


def write_detection_rows(client: Client, rows: list[tuple[object, ...]]) -> None:
    for suffix, page_schema in destinations():
        table = "webtech_domain_technologies" + suffix
        if page_schema:
            validate_report_identities(client, table, WEBTECH_TECHNOLOGY_COLUMNS, rows)
        insert_rows(
            client, table, WEBTECH_TECHNOLOGY_COLUMNS, rows, page_schema=page_schema
        )


def publish_scan_rows(
    client: Client,
    scans: list[tuple[object, ...]],
    detections: list[tuple[object, ...]],
) -> None:
    from dagster_v3.defs.webtech.storage import WEBTECH_RESULT_COLUMNS

    summaries = {}
    for row in scans:
        item = dict(zip(WEBTECH_RESULT_COLUMNS, row, strict=True))
        key = tuple(item[column] for column in (*PAGE_KEY, "report_sha256"))
        if key in summaries:
            raise ValueError(f"Duplicate Webtech publication marker: {key}")
        summaries[key] = item["technology_count"]
    names = {key: set() for key in summaries}
    for row in detections:
        item = dict(zip(WEBTECH_TECHNOLOGY_COLUMNS, row, strict=True))
        key = tuple(item[column] for column in (*PAGE_KEY, "report_sha256"))
        if key not in names or item["detected_name"] in names[key]:
            raise ValueError(f"Unowned or duplicate Webtech detection: {key}")
        names[key].add(item["detected_name"])
    if any(len(names[key]) != count for key, count in summaries.items()):
        raise ValueError("Webtech detection set does not match publication marker")

    # Validate all destinations before publishing any summary.
    targets = destinations()
    for suffix, page_schema in targets:
        if page_schema:
            validate_report_identities(
                client,
                "webtech_domain_scan_results" + suffix,
                WEBTECH_RESULT_COLUMNS,
                scans,
            )
            validate_report_identities(
                client,
                "webtech_domain_technologies" + suffix,
                WEBTECH_TECHNOLOGY_COLUMNS,
                detections,
            )
    for suffix, page_schema in targets:
        insert_rows(
            client,
            "webtech_domain_technologies" + suffix,
            WEBTECH_TECHNOLOGY_COLUMNS,
            detections,
            page_schema=page_schema,
        )
        insert_rows(
            client,
            "webtech_domain_scan_results" + suffix,
            WEBTECH_RESULT_COLUMNS,
            scans,
            page_schema=page_schema,
        )
