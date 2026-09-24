"""Freeze source-independent, normalized page selections before scanning."""

import hashlib
import json
import re
from typing import Self
from uuid import UUID
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.resources import ObjectStoreResource

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from clickhouse_driver import Client
from pydantic import Field, field_validator, model_validator

from dagster_v3.domains import root_domain
from dagster_v3.defs.common.clickhouse_queue import (
    ClickHouseInputQueue,
    validate_relation,
)
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.webtech.pages import page_identity
from dagster_v3.defs.website_crawl.se_domains import SE_DOMAIN_TABLE, SeDomainFilters

INPUT_RELATION = "corpscout.webtech_scan_input"
PROCESSOR_VERSION = "webtech-input-v1"
INPUT_COLUMNS = (
    "task_id",
    "input_id",
    "root_domain",
    "website_origin",
    "page_url",
    "source_name",
    "source_record_id",
    "source_run_id",
)


class WebtechInputConfig(dg.Config):
    task_id: str | None = None
    submission_id: str | None = None
    queue_scope: str = Field(default="workspace", min_length=1)
    targets: list[str] = Field(
        default_factory=list, description="Explicit hostnames or HTTP(S) page URLs."
    )
    source_relation: str | None = None
    target_column: str = "root_domain"
    source_record_id_column: str | None = None
    source_name: str | None = None
    filters: dict[str, list[str]] = Field(default_factory=dict)
    se_domain_filters: SeDomainFilters | None = None
    excluded_targets: list[str] = Field(default_factory=list)
    source_final: bool = False
    select_all: bool = False
    max_rows: int | None = Field(
        default=None,
        ge=1,
        le=1_000_000,
        description="Maximum source rows selected, before URL normalization.",
    )
    harmonic_rank_limit: int | None = Field(
        default=None,
        ge=1,
        description="Optional cc_harmonic_rank cap for Common Crawl imports only.",
    )

    @field_validator("task_id", "submission_id")
    @classmethod
    def valid_task(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @model_validator(mode="after")
    def selection(self) -> Self:
        if not self.queue_scope.strip():
            raise ValueError("queue_scope must not be blank")
        if bool(self.targets) == (self.source_relation is not None):
            raise ValueError("provide targets or source_relation, not both")
        for column in (self.target_column, self.source_record_id_column, *self.filters):
            if (
                column is not None
                and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None
            ):
                raise ValueError("column names must be simple SQL identifiers")
        if any(not values for values in self.filters.values()):
            raise ValueError("filter value lists must not be empty")
        if self.source_name is not None and not self.source_name.strip():
            raise ValueError("source_name must not be empty")
        if self.se_domain_filters is not None and (
            self.source_relation != SE_DOMAIN_TABLE
            or not self.source_final
            or self.target_column != "root_domain"
            or self.source_record_id_column not in (None, "root_domain")
        ):
            raise ValueError(
                "SE domain filters require current se_company_domain root_domain inputs"
            )
        if self.source_relation is not None:
            relation = validate_relation(self.source_relation)
            if not relation.startswith("corpscout.") or relation == INPUT_RELATION:
                raise ValueError("source must be a different corpscout relation")
            if not (
                self.filters
                or self.max_rows
                or self.select_all
                or self.harmonic_rank_limit
            ):
                raise ValueError("provide filters, max_rows or select_all=true")
        elif (
            self.filters
            or self.excluded_targets
            or self.se_domain_filters is not None
            or self.harmonic_rank_limit
            or self.source_final
            or self.select_all
        ):
            raise ValueError("table selection options require source_relation")
        if (
            self.harmonic_rank_limit is not None
            and self.source_relation != "corpscout.commoncrawl_domain_graph_signals"
        ):
            raise ValueError(
                "harmonic_rank_limit is only for the Common Crawl graph source"
            )
        return self


def normalized_target(value: str) -> tuple[str, str, str, str]:
    value = value.strip()
    origin, page = page_identity(value if "://" in value else f"https://{value}")
    domain = root_domain(page)
    if not domain:
        raise ValueError(f"Target must have a registrable domain: {value!r}")
    identity = hashlib.sha256(
        json.dumps([domain, origin, page], separators=(",", ":")).encode()
    ).hexdigest()
    return identity, domain, origin, page


def source_query(config: WebtechInputConfig) -> tuple[str, dict]:
    params = {}
    predicates = []
    for index, (column, values) in enumerate(sorted(config.filters.items())):
        predicates.append(f"toString(`{column}`) IN %(filter_{index})s")
        params[f"filter_{index}"] = tuple(sorted(set(values)))
    if config.excluded_targets:
        predicates.append(
            f"toString(`{config.target_column}`) NOT IN %(excluded_targets)s"
        )
        params["excluded_targets"] = tuple(sorted(set(config.excluded_targets)))
    if config.se_domain_filters is not None:
        se_predicates, se_params = config.se_domain_filters.predicates()
        predicates.extend(se_predicates)
        params.update(se_params)
    if config.harmonic_rank_limit is not None:
        predicates.append("cc_harmonic_rank BETWEEN 1 AND %(rank_limit)s")
        params["rank_limit"] = config.harmonic_rank_limit
    record = config.source_record_id_column or config.target_column
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    limit = " LIMIT %(limit)s" if config.max_rows else ""
    if config.max_rows:
        params["limit"] = config.max_rows
    return (
        f"SELECT DISTINCT toString(`{config.target_column}`), toString(`{record}`) "
        f"FROM {config.source_relation}{' FINAL' if config.source_final else ''}{where} "
        f"ORDER BY 1, 2{limit}",
        params,
    )


def insert_input_batch(
    client: Client, rows: list[tuple], *, task_id: str, query_id: str
) -> None:
    """After fencing a retry, insert only missing identities under the task lock."""
    unique = {row[1]: row for row in rows}
    existing = client.execute(
        f"SELECT input_id,root_domain,website_origin,page_url FROM {INPUT_RELATION} "
        "WHERE task_id=%(task)s AND input_id IN %(ids)s",
        {"task": task_id, "ids": tuple(unique)},
    )
    for identity, root, origin, page in existing:
        if tuple(unique[identity][2:5]) != (root, origin, page):
            raise ValueError("Conflicting payload for an existing page identity")
    present = {row[0] for row in existing}
    missing = [row for identity, row in unique.items() if identity not in present]
    if missing:
        [(total,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": task_id},
        )
        if total + len(missing) > 1_000_000:
            raise ValueError(
                "A Webtech draft supports at most one million distinct pages"
            )
        client.execute(
            f"INSERT INTO {INPUT_RELATION} ({','.join(INPUT_COLUMNS)}) VALUES",
            missing,
            query_id=query_id,
            settings={"async_insert": 0},
        )


def load_draft(
    *,
    config: WebtechInputConfig,
    submission_id: str,
    run_id: str,
    clickhouse: ClickhouseResource,
    store: ProcessingStore,
    object_store: ObjectStoreResource,
) -> dict:
    selection = config.model_dump(
        exclude={
            "task_id",
            "submission_id",
            "queue_scope",
            "targets",
            "se_domain_filters",
            "excluded_targets",
        }
    )
    # Omit new empty options so receipts created before these fields keep their fingerprint.
    if config.se_domain_filters is not None:
        selection["se_domain_filters"] = config.se_domain_filters.model_dump()
    if config.excluded_targets:
        selection["excluded_targets"] = sorted(set(config.excluded_targets))
    selection["filters"] = {
        key: sorted(set(value)) for key, value in config.filters.items()
    }
    selection["manual_count"] = len(set(config.targets))
    selection["manual_sha256"] = hashlib.sha256(
        json.dumps(sorted(set(config.targets))).encode()
    ).hexdigest()
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    receipt = draft_queue.submission(store, submission_id)
    if receipt is not None:
        task_id = str(receipt["task_id"])
        task = store.task(task_id)
        if (
            task["queue_scope"] != config.queue_scope
            or task["processor"] != PROCESSOR_VERSION
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id already belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=PROCESSOR_VERSION,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                # Start won the race. Re-resolve to the new default draft.
                continue
            latest = draft_queue.submission(store, submission_id)
            if latest is not None and (
                str(latest["task_id"]) != task_id
                or latest["selection_fingerprint"] != fingerprint
            ):
                raise ValueError(
                    "submission_id already belongs to a different selection or task"
                )
            if latest is not None and latest["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": latest["input_count"],
                    "total": store.task(task_id)["total"],
                }
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=config.source_name or config.source_relation or "manual",
                selection=selection,
                fingerprint=fingerprint,
            )
            try:
                with (
                    TemporaryDirectory(prefix="webtech-input-") as temporary,
                    clickhouse.get_connection() as client,
                ):
                    path = Path(temporary) / "inputs.jsonl"
                    query_id = "webtech-submission:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    if receipt["manifest_uri"] is None:
                        seen = set()
                        source = (
                            client.execute_iter(
                                *source_query(config), settings={"max_block_size": 5000}
                            )
                            if config.source_relation
                            else (
                                (value, value)
                                for value in sorted(set(config.targets))[
                                    : config.max_rows
                                ]
                            )
                        )
                        with path.open("w", encoding="utf-8") as output:
                            for value, record_id in source:
                                identity, domain, origin, page = normalized_target(
                                    value
                                )
                                seen.add(identity)
                                if len(seen) > 1_000_000:
                                    raise ValueError(
                                        "A submission supports at most one million distinct pages"
                                    )
                                row = (
                                    task_id,
                                    identity,
                                    domain,
                                    origin,
                                    page,
                                    config.source_name
                                    or config.source_relation
                                    or "manual",
                                    record_id,
                                    run_id,
                                )
                                output.write(json.dumps(row) + "\n")
                        with path.open("rb") as source_file:
                            digest = hashlib.file_digest(
                                source_file, "sha256"
                            ).hexdigest()
                        key = f"queue-inputs/webtech/{task_id}/{submission_id}/{digest}.jsonl"
                        object_store.ensure_bucket()
                        object_store.upload_file(key, path)
                        uri = f"s3://{object_store.bucket}/{key}"
                        draft_queue.save_manifest(store, submission_id, uri)
                    else:
                        uri = receipt["manifest_uri"]
                        parsed = urlsplit(uri)
                        key = parsed.path.lstrip("/")
                        if (
                            parsed.scheme != "s3"
                            or parsed.netloc != object_store.bucket
                            or not key.startswith(
                                f"queue-inputs/webtech/{task_id}/{submission_id}/"
                            )
                        ):
                            raise ValueError("Unexpected input manifest location")
                        object_store.download_file(key, path)
                        with path.open("rb") as source_file:
                            if (
                                hashlib.file_digest(source_file, "sha256").hexdigest()
                                != Path(key).stem
                            ):
                                raise ValueError("Input manifest checksum mismatch")
                    with path.open(encoding="utf-8") as source_file:
                        identities = {json.loads(line)[1] for line in source_file}
                    existing_ids = {
                        row[0]
                        for row in client.execute(
                            f"SELECT input_id FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                            {"task": task_id},
                        )
                    }
                    if len(identities | existing_ids) > 1_000_000:
                        raise ValueError(
                            "A Webtech draft supports at most one million distinct pages"
                        )
                    seen = set()
                    batch = []
                    with path.open(encoding="utf-8") as source_file:
                        for line in source_file:
                            row = tuple(json.loads(line))
                            if row[0] != task_id or row[1:5] != normalized_target(
                                row[4]
                            ):
                                raise ValueError("Input manifest identity mismatch")
                            if row[1] in seen:
                                continue
                            seen.add(row[1])
                            batch.append(row)
                            if len(batch) == 5000:
                                insert_input_batch(
                                    client, batch, task_id=task_id, query_id=query_id
                                )
                                batch = []
                        if batch:
                            insert_input_batch(
                                client, batch, task_id=task_id, query_id=query_id
                            )
                    total = ClickHouseInputQueue(
                        clickhouse, INPUT_RELATION, selection_task_id=task_id
                    ).inspect()["total"]
                    draft_queue.finish_submission(
                        store,
                        submission_id=submission_id,
                        task_id=task_id,
                        count=len(seen),
                        total=total,
                    )
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": len(seen),
                        "total": total,
                    }
            except BaseException:
                # The retry fences its stable ClickHouse query before reconciliation.
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="webtech",
    kinds={"clickhouse", "postgres", "s3"},
    pool="webtech_scan_input",
    metadata={"dagster/table_name": INPUT_RELATION},
    description="Append normalized pages to the open scoped draft. Does not freeze, skip recent pages or start scans.",
)
def webtech_scan_input(
    context: dg.AssetExecutionContext,
    config: WebtechInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    webtech_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    submission_id = str(
        UUID(
            config.submission_id
            or context.run.tags.get("processing/submission_id")
            or context.run.root_run_id
            or context.run.run_id
        )
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_draft(
            config=config,
            submission_id=submission_id,
            run_id=context.run.run_id,
            clickhouse=clickhouse,
            store=store,
            object_store=webtech_object_store,
        )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    return dg.MaterializeResult(metadata=metadata)


webtech_scan_input_job = dg.define_asset_job(
    "webtech_scan_input_job", selection=dg.AssetSelection.assets(webtech_scan_input)
)
defs = dg.Definitions(assets=[webtech_scan_input], jobs=[webtech_scan_input_job])
