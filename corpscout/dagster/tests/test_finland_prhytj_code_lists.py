import io
import uuid
from datetime import datetime, timezone

import pytest

from dagster_corpscout.sources.finland_prhytj.code_lists import (
    CodeListObject,
    CodeListRun,
    code_list_objects_from_manifest,
    import_code_lists,
    parse_code_list_rows,
)
from dagster_corpscout.sources.finland_prhytj.normalizer import source_item_hash
from dagster_corpscout.sources.finland_prhytj.tables import CODE_LIST_COLUMNS, CODE_LIST_TABLE


class FakeClickHouse:
    def __init__(self):
        self.truncated = []
        self.inserts = []
        self.client_object = object()

    def client(self):
        return self.client_object

    def truncate_tables(self, client, tables):
        assert client is self.client_object
        self.truncated.extend(tables)

    def insert_rows(self, client, table, columns, rows):
        assert client is self.client_object
        self.inserts.append((table, list(columns), list(rows)))


def make_run() -> CodeListRun:
    return CodeListRun(
        run_id="20260611T100000Z-abc12345",
        source_export_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        ingested_at=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
    )


def test_parse_code_list_rows_sets_metadata_and_hash():
    rows = list(
        parse_code_list_rows(
            stream=io.BytesIO(b"1\tActive\r\n\n"),
            run=make_run(),
            file_key="codelist_STATUS3_en",
            code_list="STATUS3",
            language_code="en",
        )
    )

    assert rows[0]["code"] == "1"
    assert rows[0]["description"] == "Active"
    assert rows[0]["source_line_number"] == 1
    assert rows[0]["source_payload_hash"] == source_item_hash("codelist_STATUS3_en", "1\tActive")
    assert rows[0]["file_run_id"] == "20260611T100000Z-abc12345"


def test_parse_code_list_rows_rejects_malformed_rows():
    with pytest.raises(ValueError, match="line 1"):
        list(
            parse_code_list_rows(
                stream=io.BytesIO(b"missing-tab\n"),
                run=make_run(),
                file_key="codelist_STATUS3_en",
                code_list="STATUS3",
                language_code="en",
            )
        )


def test_import_code_lists_truncates_and_inserts_rows():
    fake = FakeClickHouse()
    imported = import_code_lists(
        clickhouse=fake,
        objects=[
            CodeListObject(
                file_key="codelist_STATUS3_en",
                code_list="STATUS3",
                language_code="en",
                open_stream=lambda: io.BytesIO(b"1\tActive\n"),
            )
        ],
        run_id="20260611T100000Z-abc12345",
        batch_size=1,
    )

    assert imported == 1
    assert fake.truncated == [CODE_LIST_TABLE]
    assert fake.inserts == [
        (
            CODE_LIST_TABLE,
            CODE_LIST_COLUMNS,
            [fake.inserts[0][2][0]],
        )
    ]
    assert fake.inserts[0][2][0]["code"] == "1"


def test_code_list_objects_from_manifest_parses_code_with_underscore():
    class FakeRustFS:
        def open_object(self, bucket, key):
            assert bucket == "source-finland-prhytj"
            assert key == "runs/x/codelists/REK_KDI.en.tsv"
            return io.BytesIO(b"1\tTrade register\n")

    objects = code_list_objects_from_manifest(
        {
            "artifacts": [
                {
                    "key": "source",
                    "object_key": "runs/x/source.ndjson",
                },
                {
                    "key": "codelist_REK_KDI_en",
                    "object_key": "runs/x/codelists/REK_KDI.en.tsv",
                },
            ]
        },
        FakeRustFS(),
        "source-finland-prhytj",
    )

    assert len(objects) == 1
    assert objects[0].code_list == "REK_KDI"
    assert objects[0].language_code == "en"
    with objects[0].open_stream() as stream:
        assert stream.read() == b"1\tTrade register\n"
