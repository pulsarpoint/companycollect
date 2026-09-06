"""Immutable raw checkpoints and all-or-nothing ClickHouse publication."""

import gzip
import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikipedia.source import (
    ARTICLE_COLUMNS,
    ARTICLE_TABLE,
    WikipediaClient,
    WikipediaSnapshotConfig,
    article_row,
    wikipedia_sitelinks,
)

RAW_BATCH_ARTICLES = 25
RAW_TARGET_BYTES = 16 * 1024 * 1024
INSERT_BATCH_ROWS = 1000


def snapshot_prefix(source_run_id: str) -> str:
    WikipediaSnapshotConfig(source_run_id=source_run_id)
    return f"partition_date={source_run_id}/source_run_id={source_run_id}/"


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _put_object(store: ObjectStoreResource, key: str, body: bytes) -> dict[str, Any]:
    store.write_bytes(key, body)
    return {"key": key, "sha256": _sha(body), "bytes": len(body)}


def _verified_bytes(
    store: ObjectStoreResource, descriptor: dict[str, Any], prefix: str
) -> bytes:
    if not descriptor["key"].startswith(prefix) or ".." in descriptor["key"]:
        raise ValueError("Snapshot object key is outside its source snapshot")
    body = store.read_bytes(descriptor["key"])
    if _sha(body) != descriptor["sha256"] or len(body) != descriptor["bytes"]:
        raise ValueError(f"Snapshot object checksum mismatch: {descriptor['key']}")
    return body


def read_company_inventory(
    store: ObjectStoreResource, source_run_id: str
) -> tuple[list[str], str]:
    body = store.read_bytes(snapshot_prefix(source_run_id) + "companies.json")
    inventory = json.loads(body)
    qids = inventory["company_qids"]
    if inventory["version"] != 1 or inventory["source_run_id"] != source_run_id:
        raise ValueError("Company inventory belongs to a different source snapshot")
    if (
        not qids
        or qids != sorted(set(qids))
        or any(re.fullmatch(r"Q[1-9][0-9]*", qid) is None for qid in qids)
    ):
        raise ValueError(
            "Company inventory must contain unique, sorted QIDs and cannot be empty"
        )
    return qids, _sha(body)


def freeze_company_inventory(
    store: ObjectStoreResource, clickhouse_client: Any, source_run_id: str
) -> None:
    """One SQL snapshot pins all QIDs; retries never reselect a changing serving table."""
    key = snapshot_prefix(source_run_id) + "companies.json"
    if store.exists(key):
        read_company_inventory(store, source_run_id)
        return
    rows = clickhouse_client.execute(
        "SELECT DISTINCT wikidata_id, source_run_id FROM corpscout.wikidata_companies ORDER BY wikidata_id"
    )
    if not rows or {row[1] for row in rows} != {source_run_id}:
        raise ValueError(
            "Wikidata companies do not match the requested completed snapshot; cannot freeze inventory"
        )
    store.write_bytes(
        key,
        _json_bytes(
            {
                "version": 1,
                "source_run_id": source_run_id,
                "company_qids": [row[0] for row in rows],
            }
        ),
    )


def _discover_targets(
    store: ObjectStoreResource,
    client: WikipediaClient,
    source_run_id: str,
    log: Callable,
) -> tuple[list[dict], list[dict]]:
    qids, _ = read_company_inventory(store, source_run_id)
    prefix = snapshot_prefix(source_run_id)
    targets, objects = [], []
    for offset in range(0, len(qids), 50):
        batch = qids[offset : offset + 50]
        key = f"{prefix}discovery/batch={offset // 50:06d}.json.gz"
        if not store.exists(key):
            payload = client.entities(batch)
            store.write_bytes(key, gzip.compress(_json_bytes(payload), mtime=0))
        body = store.read_bytes(key)
        entities = json.loads(gzip.decompress(body))["entities"]
        if set(entities) != set(batch):
            raise ValueError(
                "Cached sitelink discovery does not match the frozen company inventory"
            )
        for qid in batch:
            targets.extend(wikipedia_sitelinks(qid, entities[qid]))
        objects.append({"key": key, "sha256": _sha(body), "bytes": len(body)})
        if offset % 1000 == 0 or offset + 50 >= len(qids):
            log(
                "Wikipedia sitelink discovery: %s/%s companies, %s articles",
                min(offset + 50, len(qids)),
                len(qids),
                len(targets),
            )
    return targets, objects


def _raw_records(
    store: ObjectStoreResource, objects: list[dict], prefix: str
) -> Iterator[dict]:
    for descriptor in objects:
        body = _verified_bytes(store, descriptor, prefix)
        lines = gzip.decompress(body).splitlines()
        if len(lines) != descriptor["records"]:
            raise ValueError("Raw article object record count mismatch")
        for line in lines:
            yield json.loads(line)


def _download_batch(
    store: ObjectStoreResource,
    client: WikipediaClient,
    *,
    prefix: str,
    index: int,
    targets: list[dict],
) -> list[dict]:
    checkpoint_key = f"{prefix}checkpoints/batch={index:06d}.json"
    target_hash = _sha(_json_bytes(targets))
    if store.exists(checkpoint_key):
        checkpoint = json.loads(store.read_bytes(checkpoint_key))
        if checkpoint["target_sha256"] != target_hash:
            raise ValueError("Article checkpoint target inventory changed")
        records = list(_raw_records(store, checkpoint["objects"], prefix))
        if [record["target"] for record in records] != targets:
            raise ValueError("Article checkpoint coverage mismatch")
        return checkpoint["objects"]
    objects, lines, byte_count = [], [], 0
    for target in targets:
        line = _json_bytes(client.article(target)) + b"\n"
        if lines and byte_count + len(line) > RAW_TARGET_BYTES:
            descriptor = _put_object(
                store,
                f"{prefix}part={index:06d}-{len(objects):03d}.jsonl.gz",
                gzip.compress(b"".join(lines), mtime=0),
            )
            objects.append({**descriptor, "records": len(lines)})
            lines, byte_count = [], 0
        lines.append(line)
        byte_count += len(line)
    if lines:
        descriptor = _put_object(
            store,
            f"{prefix}part={index:06d}-{len(objects):03d}.jsonl.gz",
            gzip.compress(b"".join(lines), mtime=0),
        )
        objects.append({**descriptor, "records": len(lines)})
    store.write_bytes(
        checkpoint_key, _json_bytes({"target_sha256": target_hash, "objects": objects})
    )
    return objects


def build_article_snapshot(
    store: ObjectStoreResource,
    client: WikipediaClient,
    source_run_id: str,
    log: Callable,
) -> dict[str, Any]:
    """Resume frozen discovery and bounded article batches; mark complete only after full validation."""
    prefix = snapshot_prefix(source_run_id)
    if store.exists(prefix + "manifest.json"):
        manifest = load_manifest(store, source_run_id)
        for _ in iter_snapshot_records(store, manifest):
            pass
        log("Reused complete Wikipedia snapshot: %s rows", manifest["row_count"])
        return manifest
    qids, inventory_hash = read_company_inventory(store, source_run_id)
    targets, discovery = _discover_targets(store, client, source_run_id, log)
    objects = []
    for offset in range(0, len(targets), RAW_BATCH_ARTICLES):
        objects.extend(
            _download_batch(
                store,
                client,
                prefix=prefix,
                index=offset // RAW_BATCH_ARTICLES,
                targets=targets[offset : offset + RAW_BATCH_ARTICLES],
            )
        )
        log(
            "Wikipedia article checkpoints: %s/%s articles",
            min(offset + RAW_BATCH_ARTICLES, len(targets)),
            len(targets),
        )
    languages, outcomes = Counter(), Counter()
    for record in _raw_records(store, objects, prefix):
        outcomes[record["status"]] += 1
        if record["status"] == "ok":
            languages[record["target"]["language_code"]] += 1
    manifest = {
        "version": 1,
        "complete": True,
        "source_run_id": source_run_id,
        "company_inventory_sha256": inventory_hash,
        "company_count": len(qids),
        "sitelink_count": len(targets),
        "row_count": outcomes["ok"],
        "missing_count": outcomes["missing"],
        "language_counts": dict(languages),
        "objects": objects,
        "discovery_objects": discovery,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    for _ in iter_snapshot_records(store, manifest):
        pass
    store.write_bytes(prefix + "manifest.json", _json_bytes(manifest))
    return manifest


def load_manifest(store: ObjectStoreResource, source_run_id: str) -> dict[str, Any]:
    manifest = json.loads(
        store.read_bytes(snapshot_prefix(source_run_id) + "manifest.json")
    )
    if (
        manifest.get("version") != 1
        or manifest.get("complete") is not True
        or manifest.get("source_run_id") != source_run_id
    ):
        raise ValueError(
            "Wikipedia manifest is incomplete or belongs to a different snapshot"
        )
    return manifest


def iter_snapshot_records(
    store: ObjectStoreResource, manifest: dict[str, Any]
) -> Iterator[dict]:
    """Verify hashes, exact discovery coverage, uniqueness, outcomes and counts before publication."""
    source_run_id = manifest["source_run_id"]
    prefix = snapshot_prefix(source_run_id)
    qids, inventory_hash = read_company_inventory(store, source_run_id)
    if (
        manifest.get("version") != 1
        or manifest.get("complete") is not True
        or inventory_hash != manifest["company_inventory_sha256"]
        or len(qids) != manifest["company_count"]
    ):
        raise ValueError("Wikipedia manifest company inventory mismatch")
    discovered_qids, expected = set(), {}
    for descriptor in manifest["discovery_objects"]:
        entities = json.loads(
            gzip.decompress(_verified_bytes(store, descriptor, prefix))
        )["entities"]
        if discovered_qids.intersection(entities):
            raise ValueError("Duplicate companies in sitelink discovery")
        discovered_qids.update(entities)
        for qid, entity in entities.items():
            for target in wikipedia_sitelinks(qid, entity):
                expected[(qid, target["site_id"])] = target
    if discovered_qids != set(qids) or len(expected) != manifest["sitelink_count"]:
        raise ValueError(
            "Wikipedia discovery does not cover the complete company inventory"
        )
    seen, outcomes, languages = set(), Counter(), Counter()
    for record in _raw_records(store, manifest["objects"], prefix):
        key = (record["target"]["wikidata_id"], record["target"]["site_id"])
        if key in seen or expected.get(key) != record["target"]:
            raise ValueError("Duplicate or unexpected Wikipedia article identity")
        seen.add(key)
        status = record["status"]
        if status not in {"ok", "missing"} or (
            status == "missing" and record["http_status"] not in {404, 410}
        ):
            raise ValueError("Unresolved Wikipedia download failure in snapshot")
        outcomes[status] += 1
        if status == "ok":
            languages[record["target"]["language_code"]] += 1
            yield record
    if (
        seen != set(expected)
        or outcomes["ok"] != manifest["row_count"]
        or outcomes["missing"] != manifest["missing_count"]
        or dict(languages) != manifest["language_counts"]
    ):
        raise ValueError("Wikipedia manifest coverage/count mismatch")
    if outcomes["ok"] == 0 and expected:
        raise ValueError(
            "Refusing empty Wikipedia snapshot when sitelinks were discovered"
        )


def publish_articles(
    client: Any,
    store: ObjectStoreResource,
    manifest: dict[str, Any],
    run_id: str,
    log: Callable,
) -> int:
    """The Dagster pool serializes publishers; old backfills cannot replace a newer snapshot."""
    table = f"corpscout.{ARTICLE_TABLE}"
    suffix = re.sub(r"[^a-zA-Z0-9]", "", run_id)
    if not suffix:
        raise ValueError("A unique publication run ID is required")
    stage = f"{table}__stage_{suffix}"
    source_run_id = manifest["source_run_id"]
    latest = client.execute(f"SELECT max(source_run_id) FROM {table}")[0][0]
    if latest and latest > source_run_id:
        raise ValueError(
            f"Refusing to replace newer Wikipedia snapshot {latest} with {source_run_id}"
        )
    client.execute(f"CREATE TABLE {stage} AS {table}")
    try:
        rows, byte_count, inserted = [], 0, 0
        resolved_at = datetime.now(UTC)
        for record in iter_snapshot_records(store, manifest):
            row = article_row(
                record, source_run_id=source_run_id, resolved_at=resolved_at
            )
            rows.append(tuple(row[column] for column in ARTICLE_COLUMNS))
            byte_count += len(row["article_text"].encode("utf-8"))
            if len(rows) >= INSERT_BATCH_ROWS or byte_count >= RAW_TARGET_BYTES:
                client.execute(
                    f"INSERT INTO {stage} ({', '.join(ARTICLE_COLUMNS)}) VALUES", rows
                )
                inserted += len(rows)
                log(
                    "Wikipedia ClickHouse staging: %s/%s rows",
                    inserted,
                    manifest["row_count"],
                )
                rows, byte_count = [], 0
        if rows:
            client.execute(
                f"INSERT INTO {stage} ({', '.join(ARTICLE_COLUMNS)}) VALUES", rows
            )
            inserted += len(rows)
        count, unique = client.execute(
            f"SELECT count(), uniqExact(tuple(wikidata_id, site_id)) FROM {stage}"
        )[0]
        if count != manifest["row_count"] or unique != count or inserted != count:
            raise ValueError(
                "Wikipedia staging row count or uniqueness validation failed"
            )
        latest = client.execute(f"SELECT max(source_run_id) FROM {table}")[0][0]
        if latest and latest > source_run_id:
            raise ValueError("A newer Wikipedia snapshot was published during staging")
        client.execute(f"EXCHANGE TABLES {stage} AND {table}")
        return inserted
    finally:
        client.execute(f"DROP TABLE IF EXISTS {stage}")
