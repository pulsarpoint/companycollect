"""Parser and importer for PRH YTJ code-list artifacts."""

import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import BinaryIO

from dagster_corpscout.sources.finland_prhytj.normalizer import source_item_hash
from dagster_corpscout.sources.finland_prhytj.tables import CODE_LIST_COLUMNS, CODE_LIST_TABLE


@dataclass(frozen=True)
class CodeListRun:
    run_id: str
    source_export_id: uuid.UUID
    ingested_at: datetime


@dataclass(frozen=True)
class CodeListObject:
    file_key: str
    code_list: str
    language_code: str
    open_stream: Callable[[], BinaryIO]


def parse_code_list_rows(
    *,
    stream: BinaryIO,
    run: CodeListRun,
    file_key: str,
    code_list: str,
    language_code: str,
) -> Iterable[dict]:
    for line_number, raw_line in enumerate(stream, start=1):
        raw_line = raw_line.rstrip(b"\n")
        if raw_line.endswith(b"\r"):
            raw_line = raw_line[:-1]
        line = raw_line.decode("utf-8")
        if not line.strip():
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            raise ValueError(f"malformed PRH YTJ code list row on line {line_number}")
        yield {
            "country_iso2": "FI",
            "source_slug": "prhytj",
            "source_run_id": run.run_id,
            "file_run_id": run.run_id,
            "file_key": file_key,
            "code_list": code_list,
            "language_code": language_code,
            "code": parts[0].strip(),
            "description": parts[1].strip(),
            "source_line_number": line_number,
            "source_payload_hash": source_item_hash(file_key, line),
            "ingested_at": run.ingested_at,
            "source_export_id": run.source_export_id,
        }


def import_code_lists(
    *,
    clickhouse,
    objects: Iterable[CodeListObject],
    run_id: str,
    truncate: bool = True,
    batch_size: int = 1000,
) -> int:
    client = clickhouse.client()
    if truncate:
        clickhouse.truncate_tables(client, [CODE_LIST_TABLE])

    run = CodeListRun(
        run_id=run_id,
        source_export_id=uuid.uuid4(),
        ingested_at=datetime.now(timezone.utc),
    )
    rows: list[dict] = []
    imported = 0

    def flush() -> None:
        if not rows:
            return
        clickhouse.insert_rows(client, CODE_LIST_TABLE, CODE_LIST_COLUMNS, rows)
        rows.clear()

    for item in objects:
        with item.open_stream() as stream:
            for row in parse_code_list_rows(
                stream=stream,
                run=run,
                file_key=item.file_key,
                code_list=item.code_list,
                language_code=item.language_code,
            ):
                rows.append(row)
                imported += 1
                if len(rows) >= batch_size:
                    flush()
    flush()
    return imported


def code_list_objects_from_manifest(manifest: dict, rustfs, bucket: str) -> list[CodeListObject]:
    objects: list[CodeListObject] = []
    for artifact in manifest.get("artifacts", []):
        file_key = artifact.get("key", "")
        if not file_key.startswith("codelist_"):
            continue
        code_list, language_code = _parse_file_key(file_key)
        object_key = artifact["object_key"]
        objects.append(
            CodeListObject(
                file_key=file_key,
                code_list=code_list,
                language_code=language_code or "en",
                open_stream=lambda object_key=object_key: rustfs.open_object(bucket, object_key),
            )
        )
    return objects


def _parse_file_key(file_key: str) -> tuple[str, str]:
    trimmed = file_key.removeprefix("codelist_")
    parts = trimmed.split("_")
    if len(parts) < 2:
        return trimmed, ""
    return "_".join(parts[:-1]), parts[-1]
