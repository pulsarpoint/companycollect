"""S3 layout + IO for raw RÚZ statement batches.

One download run stores one batch: `statements.ndjson` (one JSON bundle per
statement: raw statement + raw entity + raw reports, exactly as the API
returned them) plus a small `manifest.json`. Statement templates repeat
across thousands of statements, so they are stored once under a shared
`templates/` prefix. The batch directory name encodes the swept id range,
so listings are self-describing without reading any object.
"""

import json
import re
from typing import Any

RAW_BUCKET = "source-slovakia-financials"
BATCH_PREFIX = "slovakia_financials/raw_statements/"
TEMPLATE_PREFIX = "slovakia_financials/templates/"

_BATCH_KEY_RE = re.compile(r"batch-(\d{12})-(\d{12})/statements\.ndjson$")


def statement_batch_key(after_id: int, last_id: int) -> str:
    return f"{BATCH_PREFIX}batch-{after_id:012d}-{last_id:012d}/statements.ndjson"


def manifest_key_for(batch_key: str) -> str:
    return batch_key.rsplit("/", 1)[0] + "/manifest.json"


def parse_batch_id_range(batch_key: str) -> tuple[int, int] | None:
    match = _BATCH_KEY_RE.search(batch_key)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def write_statement_batch(
    object_store: Any,
    *,
    after_id: int,
    last_id: int,
    bundles: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> str:
    batch_key = statement_batch_key(after_id, last_id)
    body = "\n".join(json.dumps(bundle, ensure_ascii=False) for bundle in bundles) + "\n"
    object_store.write_bytes(batch_key, body.encode("utf-8"), bucket=RAW_BUCKET)
    object_store.write_bytes(
        manifest_key_for(batch_key),
        json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        bucket=RAW_BUCKET,
    )
    return batch_key


def list_statement_batch_keys(object_store: Any) -> list[str]:
    return sorted(
        key
        for key in object_store.list_keys(BATCH_PREFIX, bucket=RAW_BUCKET)
        if key.endswith("/statements.ndjson")
    )


def read_statement_batch(object_store: Any, batch_key: str) -> list[dict[str, Any]]:
    body = object_store.read_bytes(batch_key, bucket=RAW_BUCKET).decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def template_key(template_id: int) -> str:
    return f"{TEMPLATE_PREFIX}template-{int(template_id)}.json"


def template_exists(object_store: Any, template_id: int) -> bool:
    return object_store.exists(template_key(template_id), bucket=RAW_BUCKET)


def write_template(object_store: Any, template_id: int, template: dict[str, Any]) -> None:
    object_store.write_bytes(
        template_key(template_id),
        json.dumps(template, ensure_ascii=False).encode("utf-8"),
        bucket=RAW_BUCKET,
    )


def read_template(object_store: Any, template_id: int) -> dict[str, Any]:
    return json.loads(
        object_store.read_bytes(template_key(template_id), bucket=RAW_BUCKET).decode("utf-8")
    )
