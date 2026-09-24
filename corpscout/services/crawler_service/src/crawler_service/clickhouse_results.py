"""Import lossless result sections and configure direct ClickHouse reads of crawl S3 objects."""

import argparse
import gzip
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from dotenv import dotenv_values

from crawler_service.discovery import normalize_url
from crawler_service.domain_evidence import external_domain_evidence
from crawler_service.models import OBJECTIVES
from crawler_service.service_results import S3Settings

RECORD_SECTIONS = (*OBJECTIVES, "explicit_negatives")
OBJECT_SECTIONS = (
    "site_info",
    "company_overview",
    "site_profile",
    "coverage",
    "technology_classification",
    "catalog",
    "config",
    "model_usage",
    "crawl",
    "entities",
    "objectives",
    "discovery",
    "missing_information",
)
ARRAY_SECTIONS = (
    "page_observations",
    "pages",
    "documents",
    "unvisited_links",
    "technology_summary",
    "external_links",
    "errors",
)


def connect(environment: dict[str, str]) -> Client:
    value = environment.get("CLICKHOUSE_NATIVE_URL") or environment.get(
        "CLICKHOUSE_MIGRATE_URL"
    )
    if not value:
        raise ValueError("Set CLICKHOUSE_NATIVE_URL or CLICKHOUSE_MIGRATE_URL")
    parsed = urlsplit(value)
    if parsed.scheme not in {"clickhouse", "clickhouses"} or parsed.hostname is None:
        raise ValueError("Expected a native ClickHouse connection URL")
    options = parse_qs(parsed.query)
    secure = parsed.scheme == "clickhouses" or options.get("secure", ["false"])[
        0
    ].lower() in {"true", "1"}
    return Client(
        host=parsed.hostname,
        port=parsed.port or (9440 if secure else 9000),
        user=options.get("username", [unquote(parsed.username or "default")])[0],
        password=options.get("password", [unquote(parsed.password or "")])[0],
        database="corpscout",
        secure=secure,
        connect_timeout=10,
        send_receive_timeout=120,
    )


def result_row(payload: dict, *, source_path: str, request_id: str = "") -> dict:
    """Keep record arrays intact, including evidence, null fields and section absence."""
    if not isinstance(payload, dict):
        raise ValueError("Expected a single JSON result object")
    schema = payload.get("schema_version")
    if not isinstance(schema, str):
        raise ValueError("Result schema_version must be a string")
    if schema in {
        "company-crawl-result/1.0",
        "company-crawl-result/1.1",
        "company-crawl-result/1.2",
    }:
        kind, identity = "crawl", payload.get("crawl")
    elif schema == "company-crawl/1.0":
        kind, identity = "crawl", payload
    elif schema == "company-crawl-error/1.0":
        kind, identity = "error", payload
    elif schema == "company-research-result/1.0" or schema in {
        "1.4",
        "1.5",
        "1.6",
        "1.7",
        "1.8",
        "1.9",
        "1.10",
        "1.11",
    }:
        kind, identity = "analysis", payload
    else:
        raise ValueError("Unsupported crawl/research result schema")
    if not isinstance(identity, dict):
        raise ValueError("Result identity must be a JSON object")
    target = (
        identity.get("input_url")
        or identity.get("target_url")
        or identity.get("url")
        or identity.get("site_url")
    )
    if not isinstance(target, str) or not target:
        raise ValueError("Result has no website URL")
    website_url = normalize_url(target)
    hostname = urlsplit(website_url).hostname
    assert hostname is not None
    domain = (
        hostname.rstrip(".").encode("idna").decode("ascii").lower().removeprefix("www.")
    )
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    remainder = dict(payload)
    row = {
        "domain": domain,
        "website_url": website_url,
        "result_id": hashlib.sha256(canonical.encode()).hexdigest(),
        "result_kind": kind,
        "schema_version": schema,
        "request_id": request_id or payload.get("request_id") or "",
        "run_id": identity.get("run_id") or "",
        "revision_id": payload.get("revision_id") or "",
        "status": identity.get("processing_status")
        or identity.get("status")
        or ("failed" if kind == "error" else "unknown"),
        "source_path": source_path,
    }
    for name, value in (
        ("started_at", identity.get("started_at")),
        ("finished_at", identity.get("finished_at") or identity.get("created_at")),
    ):
        if value is None:
            row[name] = None
        else:
            if not isinstance(value, str):
                raise ValueError(f"{name} must be an ISO timestamp")
            timestamp = datetime.fromisoformat(value)
            if timestamp.tzinfo is None:
                raise ValueError(f"{name} must include a time zone")
            row[name] = timestamp.astimezone(UTC)
    records = remainder.pop("records", {})
    if not isinstance(records, dict):
        raise ValueError("records must be a JSON object")
    for name in RECORD_SECTIONS:
        value = records.get(name)
        if value is not None and not isinstance(value, list):
            raise ValueError(f"records.{name} must be an array or null")
        row[name] = (
            None
            if value is None
            else json.dumps(value, ensure_ascii=False, allow_nan=False)
        )
    unknown_records = {k: v for k, v in records.items() if k not in RECORD_SECTIONS}
    if unknown_records:
        remainder["records"] = unknown_records
    for names, expected in ((OBJECT_SECTIONS, dict), (ARRAY_SECTIONS, list)):
        for name in names:
            value = remainder.pop(name, None)
            if name == "external_links":
                value = external_domain_evidence(payload)
            if kind == "crawl" and name in {"site_info", "config", "pages", "errors"}:
                value = identity.get(name)
            if kind == "crawl" and name == "crawl":
                value = identity
            if (
                kind == "crawl"
                and name == "page_observations"
                and "documents" in payload
            ):
                documents = payload["documents"]
                if not isinstance(documents, list):
                    raise ValueError("documents must be an array")
                observations = []
                for document in documents:
                    if not isinstance(document, dict):
                        raise ValueError("documents must contain JSON objects")
                    captured_input = document.get("input", {})
                    if not isinstance(captured_input, dict):
                        raise ValueError("document input must be a JSON object")
                    observed = captured_input.get("observations")
                    if observed is not None:
                        if not isinstance(observed, dict):
                            raise ValueError("page observations must be a JSON object")
                        observations.append(observed)
                value = (
                    observations
                    if observations or schema == "company-crawl-result/1.2"
                    else None
                )
            if name == "model_usage" and value is None:
                value = remainder.pop("usage", None) or identity.get("usage")
            if name == "catalog" and value is None:
                value = remainder.pop("technology_catalog", None)
            if value is not None and not isinstance(value, expected):
                raise ValueError(f"{name} must be a JSON {expected.__name__} or null")
            row[name] = (
                None
                if value is None
                else json.dumps(value, ensure_ascii=False, allow_nan=False)
            )
    row["metadata"] = json.dumps(remainder, ensure_ascii=False, allow_nan=False)
    return row


def insert_result(client: Client, row: dict) -> None:
    # Columns come only from result_row, never from caller-supplied JSON keys.
    columns = ", ".join(row)
    client.execute(
        f"INSERT INTO corpscout.website_crawl_results ({columns}) VALUES",
        [list(row.values())],
    )


def configure_s3(
    client: Client, settings: S3Settings, environment: dict[str, str]
) -> None:
    if not environment.get("AWS_ACCESS_KEY_ID") or not environment.get(
        "AWS_SECRET_ACCESS_KEY"
    ):
        raise ValueError(
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for the ClickHouse S3 reader"
        )
    endpoint = settings.endpoint_url or f"https://s3.{settings.region}.amazonaws.com"
    url = (
        "/".join(
            part
            for part in (endpoint.rstrip("/"), settings.bucket, settings.prefix)
            if part
        )
        + "/"
    )
    if client.execute(
        "SELECT count() FROM system.named_collections WHERE name = 'company_crawl_results'"
    )[0][0]:
        raise ValueError(
            "company_crawl_results already exists; change its settings explicitly with ALTER NAMED COLLECTION"
        )
    client.execute(
        "CREATE NAMED COLLECTION company_crawl_results AS "
        "url = %(url)s NOT OVERRIDABLE, "
        "access_key_id = %(access_key)s NOT OVERRIDABLE, "
        "secret_access_key = %(secret_key)s NOT OVERRIDABLE, "
        "session_token = %(token)s NOT OVERRIDABLE",
        {
            "url": url,
            "access_key": environment["AWS_ACCESS_KEY_ID"],
            "secret_key": environment["AWS_SECRET_ACCESS_KEY"],
            "token": environment.get("AWS_SESSION_TOKEN", ""),
        },
        settings={"log_queries": 0, "log_query_threads": 0},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", action="append", type=Path, default=[])
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser(
        "configure-s3",
        help="Provision the server-side S3 named collection before migration 420",
    )
    setup.add_argument("--bucket", required=True)
    setup.add_argument("--endpoint-url")
    setup.add_argument("--prefix", default="company-crawls")
    setup.add_argument("--region", default="us-east-1")
    local = commands.add_parser(
        "import-file", help="Import saved crawl or LLM-analysis JSON"
    )
    local.add_argument("files", nargs="+", type=Path)
    remote = commands.add_parser(
        "import-s3", help="Import objects through the ClickHouse S3 mapping"
    )
    remote.add_argument(
        "--path",
        action="append",
        required=True,
        help="Exact _path from website_crawl_results_s3_archive (repeatable)",
    )
    args = parser.parse_args()
    environment = {}
    for path in args.env_file:
        environment.update(
            {
                key: value
                for key, value in dotenv_values(path).items()
                if value is not None
            }
        )
    environment.update(os.environ)
    client = None
    try:
        client = connect(environment)
        if args.command == "configure-s3":
            configure_s3(
                client,
                S3Settings(
                    bucket=args.bucket,
                    prefix=args.prefix,
                    endpoint_url=args.endpoint_url,
                    region=args.region,
                ),
                environment,
            )
            print("Created company_crawl_results named collection")
        elif args.command == "import-file":
            for path in args.files:
                content = (
                    gzip.decompress(path.read_bytes())
                    if path.suffix == ".gz"
                    else path.read_bytes()
                )
                row = result_row(json.loads(content), source_path=str(path.resolve()))
                insert_result(client, row)
                print(
                    json.dumps(
                        {
                            "domain": row["domain"],
                            "result_id": row["result_id"],
                            "result_kind": row["result_kind"],
                        }
                    )
                )
        else:
            for path in args.path:
                rows = client.execute(
                    "SELECT result_json, _path, request_id FROM corpscout.website_crawl_results_s3_archive WHERE _path = %(path)s",
                    {"path": path},
                )
                if len(rows) != 1:
                    raise ValueError(
                        "Expected exactly one JSON object at the requested S3 path"
                    )
                content, source_path, request_id = rows[0]
                row = result_row(
                    json.loads(content), source_path=source_path, request_id=request_id
                )
                insert_result(client, row)
                print(
                    json.dumps(
                        {
                            "domain": row["domain"],
                            "result_id": row["result_id"],
                            "result_kind": row["result_kind"],
                        }
                    )
                )
    except (ClickHouseError, OSError, ValueError) as error:
        # DB errors can include credential-bearing SQL or source data.
        parser.exit(1, f"ClickHouse result operation failed ({type(error).__name__})\n")
    finally:
        if client is not None:
            client.disconnect()


if __name__ == "__main__":
    main()
