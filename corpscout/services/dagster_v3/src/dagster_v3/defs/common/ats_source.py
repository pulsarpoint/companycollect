from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dlt.sources.helpers import requests

from dagster_v3.defs.common.resources import ObjectStoreResource

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_USER_AGENT = "corpscout-dagster-v3-sweden-ats/0.1"


@dataclass(frozen=True)
class BoardDefinition:
    provider_board_id: str
    board_token: str
    display_name: str
    company_id: str
    country_code: str
    board_url: str
    evidence_url: str
    configured_at: datetime
    enabled: bool = True


@dataclass(frozen=True)
class BoardPayload:
    payload: Any
    source_url: str
    job_count: int
    http_status: int = 200


FetchBoard = Callable[[BoardDefinition], BoardPayload]


def get_json(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    response = requests.get(
        url,
        params=params,
        timeout=(DEFAULT_CONNECT_TIMEOUT_SECONDS, timeout_seconds),
        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def sync_board_snapshots(
    *,
    object_store: ObjectStoreResource,
    bucket: str,
    provider: str,
    boards: Sequence[BoardDefinition],
    fetch_board: FetchBoard,
    run_id: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    enabled_boards = [board for board in boards if board.enabled]
    if not enabled_boards:
        raise ValueError(f"{provider} has no enabled reviewed boards")

    object_store.ensure_bucket(bucket)
    stored_boards: list[dict[str, Any]] = []
    for board in enabled_boards:
        result = fetch_board(board)
        if result.job_count < 0:
            raise ValueError(
                f"{provider} board {board.provider_board_id} returned a negative job count"
            )
        encoded = json.dumps(
            result.payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        object_key = f"raw/sha256={digest[:2]}/{digest}.json"
        if not object_store.exists(object_key, bucket=bucket):
            object_store.write_bytes(object_key, encoded, bucket=bucket)
        stored_boards.append(
            {
                "provider_board_id": board.provider_board_id,
                "board_token": board.board_token,
                "display_name": board.display_name,
                "company_id": board.company_id,
                "country_code": board.country_code,
                "board_url": board.board_url,
                "evidence_url": board.evidence_url,
                "configured_at": _iso_utc(board.configured_at),
                "enabled": board.enabled,
                "source_url": result.source_url,
                "source_object_key": object_key,
                "retrieved_at": _iso_utc(retrieved_at),
                "http_status": result.http_status,
                "job_count": result.job_count,
            }
        )

    manifest = {
        "provider": provider,
        "source_run_id": run_id,
        "retrieved_at": _iso_utc(retrieved_at),
        "boards": stored_boards,
    }
    manifest_key = snapshot_manifest_key(provider=provider, run_id=run_id)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=bucket,
    )
    manifest["manifest_key"] = manifest_key
    return manifest


def snapshot_manifest_key(*, provider: str, run_id: str) -> str:
    return f"manifests/{provider}/run_id={run_id}/manifest.json"


def read_snapshot_manifest(
    *,
    object_store: ObjectStoreResource,
    bucket: str,
    provider: str,
    run_id: str,
) -> dict[str, Any]:
    key = snapshot_manifest_key(provider=provider, run_id=run_id)
    if not object_store.exists(key, bucket=bucket):
        raise ValueError(
            f"{provider} snapshot manifest {key} does not exist; materialize the "
            "source snapshot asset in the same run"
        )
    payload = json.loads(object_store.read_bytes(key, bucket=bucket).decode("utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("boards"), list):
        raise ValueError(f"{provider} snapshot manifest {key} is malformed")
    return payload


@contextmanager
def local_snapshot_files(
    *,
    object_store: ObjectStoreResource,
    bucket: str,
    manifest: Mapping[str, Any],
) -> Iterator[tuple[dict[str, Any], ...]]:
    with tempfile.TemporaryDirectory(prefix="sweden_ats_snapshot_") as temp:
        temp_path = Path(temp)
        local_boards: list[dict[str, Any]] = []
        for index, raw_board in enumerate(manifest["boards"]):
            board = dict(raw_board)
            local_path = temp_path / f"board-{index}.json"
            object_store.download_file(
                str(board["source_object_key"]), local_path, bucket=bucket
            )
            board["local_path"] = str(local_path)
            board["source_run_id"] = str(manifest["source_run_id"])
            local_boards.append(board)
        yield tuple(local_boards)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
