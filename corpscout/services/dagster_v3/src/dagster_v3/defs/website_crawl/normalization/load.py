"""Bounded catalog reads, exact archive retrieval and scan-last publication."""

import re
from uuid import uuid4

import dagster as dg
import duckdb
from pydantic import ConfigDict, Field, field_validator, model_validator

from dagster_v3.defs.clickhouse.resolved import (
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.website_crawl.normalization.parser import (
    decode_archive,
    parse_attempt,
)
from dagster_v3.defs.website_crawl.normalization.tables import (
    COLUMNS,
    PARSER_VERSION,
    SCHEMAS,
    duckdb_schema,
)
from dagster_v3.defs.website_crawl.results import RESULTS_BY_TYPE

SOURCE_COLUMNS = (
    "domain",
    "request_id",
    "attempt",
    "input_revision",
    "work_key",
    "run_id",
    "state",
    "crawl_status",
    "successful",
    "started_at",
    "finished_at",
    "ingested_at",
    "website_url",
    "site_info",
    "page_observations",
    "pages",
    "error",
    "s3_path",
    "s3_state",
)


class NormalizationConfig(dg.Config):
    model_config = ConfigDict(extra="forbid")
    domains: list[str] = Field(default_factory=list, max_length=100)
    crawl_types: list[str] = Field(
        default_factory=lambda: ["full", "jobs", "site_info"], min_length=1
    )
    request_id: str | None = Field(
        default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"
    )
    attempt: int | None = Field(default=None, ge=1, le=4294967295)
    batch_size: int = Field(default=25, ge=1, le=100)
    force: bool = Field(default=False, strict=True)

    @field_validator("domains")
    @classmethod
    def valid_domains(cls, values):
        if any(
            len(v) > 253 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", v)
            for v in values
        ):
            raise ValueError("Use normalized domain names")
        return sorted(set(values))

    @field_validator("crawl_types")
    @classmethod
    def valid_crawl_types(cls, values):
        if any(value not in RESULTS_BY_TYPE for value in values):
            raise ValueError("Unknown crawl type")
        return list(dict.fromkeys(values))

    @model_validator(mode="after")
    def bounded_replay(self):
        if self.force and not self.domains and self.request_id is None:
            raise ValueError("Force replay requires domains or a request_id")
        if self.attempt is not None and self.request_id is None:
            raise ValueError("An attempt filter requires request_id")
        return self


def assert_schema(client) -> None:
    for name, schema in SCHEMAS.items():
        actual = [
            (r[0], r[1])
            for r in client.execute(f"DESCRIBE TABLE corpscout.website_crawl_{name}")
        ]
        expected = [tuple(line.split(" ", 1)) for line in schema.splitlines()]
        if actual != expected:
            raise ValueError(
                f"Normalization schema mismatch for {name}; apply migration 452"
            )


def pending_attempts(client, config: NormalizationConfig) -> list[dict]:
    # The catalog is small compared with archived HTML. Only outstanding bounded rows leave ClickHouse.
    filters = []
    parameters = dict(version=PARSER_VERSION, limit=config.batch_size)
    if config.domains:
        filters.append("domain IN %(domains)s")
        parameters["domains"] = config.domains
    if config.request_id is not None:
        filters.append("request_id=%(request_id)s")
        parameters["request_id"] = config.request_id
    if config.attempt is not None:
        filters.append("attempt=%(attempt)s")
        parameters["attempt"] = config.attempt
    where = " WHERE " + " AND ".join(filters) if filters else ""
    source = " UNION ALL ".join(
        f"SELECT '{kind}' AS crawl_type,{','.join(SOURCE_COLUMNS)} FROM {RESULTS_BY_TYPE[kind]} FINAL{where}"
        for kind in dict.fromkeys(config.crawl_types)
    )
    anti = (
        ""
        if config.force
        else """LEFT ANTI JOIN
        (SELECT domain,crawl_type,request_id,attempt,source_ingested_at
         FROM corpscout.website_crawl_scans FINAL WHERE parser_version=%(version)s) AS n
        ON r.domain=n.domain AND r.crawl_type=n.crawl_type AND r.request_id=n.request_id
           AND r.attempt=n.attempt AND r.ingested_at=n.source_ingested_at"""
    )
    values, columns = client.execute(
        f"""SELECT r.* FROM ({source}) AS r {anti}
        ORDER BY r.ingested_at,r.domain,r.crawl_type,r.request_id,r.attempt LIMIT %(limit)s""",
        parameters,
        with_column_types=True,
    )
    return [dict(zip((c[0] for c in columns), row, strict=True)) for row in values]


def read_archive(client, source: dict) -> dict | None:
    if source["s3_state"] != "uploaded":
        return None
    path = source["s3_path"]
    expected = f"crawls/company-crawls/{source['request_id']}/attempts/{source['attempt']:04d}/result.json.gz"
    if path != expected or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", source["request_id"]
    ):
        raise ValueError("Archive path does not match the catalog request and attempt")
    # The named collection is rooted at crawls/company-crawls/ and owns credentials.
    results = client.execute(
        """SELECT result_json FROM s3(company_crawl_results,
        filename=%(key)s, format='JSONAsString', structure='result_json String', compression_method='gzip')
        LIMIT 2 SETTINGS max_result_bytes=134217728, result_overflow_mode='throw', s3_throw_on_zero_files_match=1""",
        {"key": path.removeprefix("crawls/company-crawls/")},
    )
    if len(results) != 1:
        raise ValueError("Expected exactly one JSON result in the saved archive")
    return decode_archive(results[0][0])


def publish_attempt(
    client, source: dict, payload: dict | None, run_id: str
) -> dict[str, int]:
    """Caller holds the normalization advisory lock for selection through publication."""
    identity = {
        key: source[key] for key in ("domain", "crawl_type", "request_id", "attempt")
    }
    revision = (
        client.execute(
            """SELECT max(normalization_revision) FROM corpscout.website_crawl_scans
        WHERE domain=%(domain)s AND crawl_type=%(crawl_type)s AND request_id=%(request_id)s AND attempt=%(attempt)s""",
            identity,
        )[0][0]
        + 1
    )
    normalization_id = uuid4()
    rows = parse_attempt(source, payload, normalization_id, revision, run_id)
    counts = {table: len(items) for table, items in rows.items()}
    with duckdb.connect(":memory:") as stage:
        # Validate every table's native shape before the first warehouse write.
        for table, columns in COLUMNS.items():
            stage.execute(f'CREATE TABLE "{table}" ({duckdb_schema(table)})')
            if rows[table]:
                stage.executemany(
                    f'INSERT INTO "{table}" VALUES ({",".join("?" for _ in columns)})',
                    [[row[column] for column in columns] for row in rows[table]],
                )
        for table in [t for t in COLUMNS if t != "scans"] + ["scans"]:
            if rows[table]:
                export_duckdb_connection_table_to_clickhouse(
                    duckdb_connection=stage,
                    clickhouse_client=client,
                    duckdb_schema="main",
                    duckdb_table=table,
                    clickhouse_database="corpscout",
                    clickhouse_table="website_crawl_" + table,
                    columns=COLUMNS[table],
                    truncate=False,
                )
            # The scans row is the publication marker and is inserted only after all detail checks.
            count = client.execute(
                f"""SELECT count() FROM corpscout.website_crawl_{table} FINAL
                WHERE domain=%(domain)s AND crawl_type=%(crawl_type)s AND request_id=%(request_id)s
                    AND attempt=%(attempt)s AND normalization_id=%(normalization_id)s""",
                {**identity, "normalization_id": normalization_id},
            )[0][0]
            if count != counts[table]:
                raise ValueError(
                    f"Published row count mismatch for {table}: {count} vs {counts[table]}"
                )
    return counts
