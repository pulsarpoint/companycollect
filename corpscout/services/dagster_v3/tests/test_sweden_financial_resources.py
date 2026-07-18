import zipfile
from datetime import date
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.sweden_financial.parsing import (
    REPORT_XHTML_PREFIX,
    extract_sweden_financial_report_xhtml_catalog,
    parse_sweden_financial_report_xhtml_catalog,
)
from dagster_v3.defs.sweden_financial.resources import (
    SWEDEN_FINANCIAL_RAW_BUCKET,
    SwedenFinancialReportsResource,
    archive_object_key,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def download_file(
        self,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        Path(target_path).write_bytes(self.objects[(bucket, key)])

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return sorted(
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        )


class TrackingDuckDbConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection
        self.report_arrow_batch_sizes: list[int] = []
        self.fact_arrow_batch_sizes: list[int] = []
        self.error_arrow_batch_sizes: list[int] = []
        self.commit_count = 0

    def execute(self, *args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        return self._connection.execute(*args, **kwargs)

    def register(self, view_name: str, python_object: Any) -> duckdb.DuckDBPyConnection:
        if "reports" in view_name:
            self.report_arrow_batch_sizes.append(python_object.num_rows)
        elif "facts" in view_name:
            self.fact_arrow_batch_sizes.append(python_object.num_rows)
        elif "parse_errors" in view_name:
            self.error_arrow_batch_sizes.append(python_object.num_rows)
        return self._connection.register(view_name, python_object)

    def unregister(self, view_name: str) -> duckdb.DuckDBPyConnection:
        return self._connection.unregister(view_name)

    def commit(self) -> None:
        self.commit_count += 1
        self._connection.commit()

    def executemany(
        self,
        query: str,
        rows: list[tuple[Any, ...]],
    ) -> duckdb.DuckDBPyConnection:
        raise AssertionError("Sweden financial parser should use Arrow bulk inserts")


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:2], self._body[2:]]

    @property
    def text(self) -> str:
        return self._body.decode("utf-8")


class FakeSession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        return FakeResponse(self.responses_by_url[url])


class ListingOnlySession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        if stream:
            raise AssertionError(f"unexpected archive download for {url}")
        return FakeResponse(self.responses_by_url[url])


def test_sweden_financial_reports_downloads_missing_archives_to_s3() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url()
    archive_url = f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            listing_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"1bb9cb50"',
                size=6,
            ),
            archive_url: b"zip-bytes",
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
    )

    expected_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    assert object_store.created_buckets == [SWEDEN_FINANCIAL_RAW_BUCKET]
    assert (
        object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, expected_key)]
        == b"zip-bytes"
    )
    assert object_store.uploaded_files == [(SWEDEN_FINANCIAL_RAW_BUCKET, expected_key)]
    assert session.requested_urls == [listing_url, archive_url]
    assert result.metadata["archive_count"] == 1
    assert result.metadata["downloaded_archive_count"] == 1
    assert result.metadata["reused_archive_count"] == 0
    assert result.metadata["downloaded_size_bytes"] == len(b"zip-bytes")
    assert result.metadata["sample_s3_keys"] == [expected_key]


def test_sweden_financial_reports_skips_existing_last_modified_archives() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url()
    existing_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    object_store = FakeObjectStore()
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, existing_key)] = b"already-there"
    session = ListingOnlySession(
        {
            listing_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"1bb9cb50"',
                size=6,
            ),
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
    )

    assert object_store.uploaded_files == []
    assert session.requested_urls == [listing_url]
    assert result.metadata["archive_count"] == 1
    assert result.metadata["downloaded_archive_count"] == 0
    assert result.metadata["reused_archive_count"] == 1


def test_sweden_financial_reports_sync_exposes_changed_and_unchanged_keys() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url(marker="arsredovisningar/2026/")
    archive_url = f"{resource.archive_base_url}/arsredovisningar/2026/01_1.zip"
    unchanged_key = archive_object_key(
        upstream_key="arsredovisningar/2026/02_1.zip",
        source_last_modified="2026-07-01T09:13:53.713Z",
    )
    changed_key = archive_object_key(
        upstream_key="arsredovisningar/2026/01_1.zip",
        source_last_modified="2026-07-04T09:13:53.713Z",
    )
    object_store = FakeObjectStore()
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, unchanged_key)] = b"old"
    session = FakeSession(
        {
            listing_url: _listing_xml_many(
                [
                    {
                        "key": "arsredovisningar/2026/01_1.zip",
                        "last_modified": "2026-07-04T09:13:53.713Z",
                        "etag": '"changed"',
                        "size": 7,
                    },
                    {
                        "key": "arsredovisningar/2026/02_1.zip",
                        "last_modified": "2026-07-01T09:13:53.713Z",
                        "etag": '"same"',
                        "size": 3,
                    },
                ]
            ),
            archive_url: b"changed",
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
        year="2026",
    )

    assert session.requested_urls == [listing_url, archive_url]
    assert result.changed_s3_keys == [changed_key]
    assert result.unchanged_s3_keys == [unchanged_key]
    assert result.metadata["changed_archive_count"] == 1
    assert result.metadata["unchanged_archive_count"] == 1


def test_sweden_financial_reports_follows_listing_pagination() -> None:
    resource = SwedenFinancialReportsResource()
    first_url = resource.listing_url()
    second_url = resource.listing_url(marker="arsredovisningar/2020/08_2.zip")
    first_archive_url = f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    second_archive_url = f"{resource.archive_base_url}/arsredovisningar/2021/01_1.zip"
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            first_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"first"',
                size=6,
                next_marker="arsredovisningar/2020/08_2.zip",
            ),
            second_url: _listing_xml(
                key="arsredovisningar/2021/01_1.zip",
                last_modified="2025-02-10T10:00:00.000Z",
                etag='"second"',
                size=9,
            ),
            first_archive_url: b"first",
            second_archive_url: b"second",
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == [
        first_url,
        second_url,
        first_archive_url,
        second_archive_url,
    ]
    assert result.metadata["archive_count"] == 2
    assert result.metadata["downloaded_archive_count"] == 2


def test_sweden_financial_reports_count_archives_by_year_groups_across_pages() -> None:
    resource = SwedenFinancialReportsResource()
    first_url = resource.listing_url()
    second_url = resource.listing_url(marker="arsredovisningar/2020/08_2.zip")
    session = ListingOnlySession(
        {
            first_url: _listing_xml_many(
                [
                    {
                        "key": "arsredovisningar/2020/08_2.zip",
                        "last_modified": "2025-02-07T09:13:53.713Z",
                        "etag": '"a"',
                        "size": 6,
                    },
                    {
                        "key": "arsredovisningar/2020/09_1.zip",
                        "last_modified": "2025-02-08T09:13:53.713Z",
                        "etag": '"b"',
                        "size": 6,
                    },
                ],
                next_marker="arsredovisningar/2020/08_2.zip",
            ),
            second_url: _listing_xml(
                key="arsredovisningar/2021/01_1.zip",
                last_modified="2025-02-10T10:00:00.000Z",
                etag='"c"',
                size=9,
            ),
        }
    )

    counts = resource.count_archives_by_year(session=session)

    assert session.requested_urls == [first_url, second_url]
    assert counts == {"2020": 2, "2021": 1}


def test_sweden_financial_reports_filters_listing_by_year_partition() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url(marker="arsredovisningar/2020/")
    archive_url = f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            listing_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"1bb9cb50"',
                size=6,
            ),
            archive_url: b"zip-bytes",
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
        year="2020",
    )

    assert "prefix=arsredovisningar%2F" in listing_url
    assert "marker=arsredovisningar%2F2020%2F" in listing_url
    assert session.requested_urls == [listing_url, archive_url]
    assert result.metadata["partition_year"] == "2020"
    assert result.metadata["archive_count"] == 1


def test_sweden_financial_reports_skips_later_year_archives_on_listing_page() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url(marker="arsredovisningar/2020/")
    archive_url = f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            listing_url: _listing_xml_many(
                [
                    {
                        "key": "arsredovisningar/2020/08_2.zip",
                        "last_modified": "2025-02-07T09:13:53.713Z",
                        "etag": '"2020"',
                        "size": 6,
                    },
                    {
                        "key": "arsredovisningar/2025/26_2.zip",
                        "last_modified": "2026-02-07T09:13:53.713Z",
                        "etag": '"2025"',
                        "size": 9,
                    },
                ],
                next_marker="arsredovisningar/2025/26_2.zip",
            ),
            archive_url: b"zip-bytes",
        }
    )

    result = resource.sync_raw_archives(
        object_store=object_store,
        session=session,
        year="2020",
    )

    assert session.requested_urls == [listing_url, archive_url]
    assert result.metadata["archive_count"] == 1


def test_extract_sweden_financial_report_xhtml_catalog_from_raw_archive(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5560000000_2023-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=b"<html><body>annual report</body></html>",
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-1",
            partition_year="2020",
        )
        catalog_rows = connection.execute(
            """
            select
                partition_year,
                company_id,
                report_period_end,
                source_archive_key,
                nested_zip_member,
                xhtml_member,
                xhtml_s3_key,
                size_bytes
            from sweden_financial.report_xhtml_catalog
            """
        ).fetchall()

    assert counts == {
        "partition_year": "2020",
        "source_archive_count": 1,
        "nested_zip_count": 1,
        "skipped_nested_zip_count": 0,
        "report_xhtml_count": 1,
        "downloaded_report_xhtml_count": 1,
        "reused_report_xhtml_count": 0,
        "catalog_row_count": 1,
    }
    assert catalog_rows == [
        (
            "2020",
            "5560000000",
            "2023-12-31",
            raw_archive_key,
            "5560000000_2023-12-31.zip",
            "report.xhtml",
            (
                f"{REPORT_XHTML_PREFIX}year=2020/"
                "company_id=5560000000/"
                "report_period_end=2023-12-31/"
                f"source_archive_hash={sha256(raw_archive_key.encode()).hexdigest()[:16]}/"
                "source_archive=08_2.zip/"
                "nested_zip=5560000000_2023-12-31/"
                "report.xhtml"
            ),
            len(b"<html><body>annual report</body></html>"),
        )
    ]
    assert (
        object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, catalog_rows[0][6])]
        == b"<html><body>annual report</body></html>"
    )


def test_parse_sweden_financial_report_xhtml_catalog_writes_reports_and_facts(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    xhtml_body = b"""<?xml version="1.0"?>
<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:se="http://example.test/se">
<head>
  <link:schemaRef xlink:href="https://example.test/taxonomy.xsd" />
</head>
<body>
  <xbrli:context id="duration">
    <xbrli:entity>
      <xbrli:identifier scheme="https://bolagsverket.se/">5560000000</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2023-01-01</xbrli:startDate>
      <xbrli:endDate>2023-12-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="SEK">
    <xbrli:measure>iso4217:SEK</xbrli:measure>
  </xbrli:unit>
  <ix:nonFraction name="se:Revenue" contextRef="duration" unitRef="SEK" decimals="0" scale="3">123</ix:nonFraction>
  <ix:nonNumeric name="se:CompanyName" contextRef="duration">Example AB</ix:nonNumeric>
</body>
</html>
"""
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5560000000_2023-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=xhtml_body,
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="catalog-run",
            partition_year="2020",
        )
        counts = parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="parse-run",
            partition_year="2020",
            replace_scope="partition",
        )
        report_row = connection.execute(
            """
            select
                country_iso2,
                source_slug,
                source_run_id,
                company_id,
                report_period_start,
                report_period_end,
                fiscal_year,
                source_archive_name,
                contexts_count,
                units_count,
                facts_count,
                taxonomy_entrypoint
            from sweden_financial.reports
            """
        ).fetchone()
        fact_rows = connection.execute(
            """
            select
                fact_ordinal,
                concept_qname,
                concept_namespace,
                concept_local_name,
                context_id,
                unit_id,
                value_kind,
                raw_value,
                amount_original,
                text_value,
                currency
            from sweden_financial.facts
            order by fact_ordinal
            """
        ).fetchall()

    assert counts == {
        "partition_year": "2020",
        "catalog_row_count": 1,
        "reports_parsed_count": 1,
        "reports_failed_count": 0,
        "report_row_count": 1,
        "fact_row_count": 2,
        "parse_error_row_count": 0,
    }
    assert report_row == (
        "SE",
        "sweden_financial",
        "parse-run",
        "5560000000",
        date(2023, 1, 1),
        date(2023, 12, 31),
        2023,
        "08_2.zip",
        1,
        1,
        2,
        "https://example.test/taxonomy.xsd",
    )
    assert fact_rows == [
        (
            1,
            "se:Revenue",
            "http://example.test/se",
            "Revenue",
            "duration",
            "SEK",
            "numeric",
            "123",
            Decimal("123000.0000000000"),
            None,
            "SEK",
        ),
        (
            2,
            "se:CompanyName",
            "http://example.test/se",
            "CompanyName",
            "duration",
            None,
            "text",
            "Example AB",
            None,
            "Example AB",
            None,
        ),
    ]


def test_parse_sweden_financial_report_xhtml_catalog_requires_catalog_table(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        with pytest.raises(
            ValueError,
            match="sweden_financial_backfill_report_xhtml_catalog_duckdb",
        ):
            parse_sweden_financial_report_xhtml_catalog(
                connection=connection,
                object_store=object_store,
                source_run_id="parse-run",
                partition_year="2025",
                replace_scope="partition",
            )
        tables = connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'sweden_financial'
            order by table_name
            """
        ).fetchall()

    assert tables == []


def test_parse_sweden_financial_report_xhtml_catalog_requires_partition_catalog_rows(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        connection.execute("create schema sweden_financial")
        connection.execute(
            """
            create table sweden_financial.report_xhtml_catalog (
                source_run_id varchar,
                partition_year varchar,
                source_archive_key varchar,
                nested_zip_member varchar,
                xhtml_member varchar,
                xhtml_s3_bucket varchar,
                xhtml_s3_key varchar,
                company_id varchar,
                report_period_end varchar,
                source_archive_year varchar,
                source_archive_name varchar,
                size_bytes bigint,
                sha256 varchar
            )
            """
        )
        with pytest.raises(ValueError, match="No Sweden financial XHTML catalog rows"):
            parse_sweden_financial_report_xhtml_catalog(
                connection=connection,
                object_store=object_store,
                source_run_id="parse-run",
                partition_year="2025",
                replace_scope="partition",
            )
        tables = connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'sweden_financial'
            order by table_name
            """
        ).fetchall()

    assert tables == [("report_xhtml_catalog",)]


def test_parse_sweden_financial_report_xhtml_catalog_flushes_insert_batches(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2025/12_31.zip",
        source_last_modified="2026-01-02T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5594386681_2025-12-31.zip",
        xhtml_files={
            "first.xhtml": _sample_ixbrl_body(company_id="5594386681", amount="100"),
            "second.xhtml": _sample_ixbrl_body(company_id="5594386681", amount="200"),
            "third.xhtml": _sample_ixbrl_body(company_id="5594386681", amount="300"),
        },
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="catalog-run",
            partition_year="2025",
        )
        tracking_connection = TrackingDuckDbConnection(connection)
        counts = parse_sweden_financial_report_xhtml_catalog(
            connection=tracking_connection,
            object_store=object_store,
            source_run_id="parse-run",
            partition_year="2025",
            replace_scope="partition",
            insert_batch_size=2,
        )

    assert counts["report_row_count"] == 3
    assert counts["fact_row_count"] == 3
    assert tracking_connection.report_arrow_batch_sizes == [2, 1]
    assert tracking_connection.fact_arrow_batch_sizes == [2, 1]
    assert tracking_connection.commit_count == 2


def test_parse_sweden_financial_report_xhtml_catalog_partition_replace_keeps_other_partitions(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    archive_2024_key = archive_object_key(
        upstream_key="arsredovisningar/2024/01_1.zip",
        source_last_modified="2025-01-02T09:13:53.713Z",
    )
    archive_2025_key = archive_object_key(
        upstream_key="arsredovisningar/2025/01_1.zip",
        source_last_modified="2026-01-02T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, archive_2024_key)] = _outer_zip(
        nested_name="5560000000_2024-12-31.zip",
        xhtml_body=_sample_ixbrl_body(company_id="5560000000", amount="100"),
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, archive_2025_key)] = _outer_zip(
        nested_name="5560000001_2025-12-31.zip",
        xhtml_body=_sample_ixbrl_body(company_id="5560000001", amount="200"),
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="catalog-2024",
            partition_year="2024",
            source_archive_keys=[archive_2024_key],
            replace_scope="partition",
        )
        parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="parse-2024",
            partition_year="2024",
            replace_scope="partition",
        )
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="catalog-2025",
            partition_year="2025",
            source_archive_keys=[archive_2025_key],
            replace_scope="partition",
        )
        parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="parse-2025",
            partition_year="2025",
            replace_scope="partition",
        )
        rows = connection.execute(
            """
            select source_archive_key, company_id
            from sweden_financial.reports
            order by source_archive_key
            """
        ).fetchall()

    assert rows == [
        (archive_2024_key, "5560000000"),
        (archive_2025_key, "5560000001"),
    ]


def test_parse_sweden_financial_report_xhtml_catalog_replaces_changed_archives(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    old_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/01_1.zip",
        source_last_modified="2026-07-01T09:13:53.713Z",
    )
    unchanged_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/02_1.zip",
        source_last_modified="2026-07-01T09:13:53.713Z",
    )
    changed_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/01_1.zip",
        source_last_modified="2026-07-04T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, old_archive_key)] = _outer_zip(
        nested_name="5560000000_2026-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=_sample_ixbrl_body(company_id="5560000000", amount="100"),
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, unchanged_archive_key)] = (
        _outer_zip(
            nested_name="5560000001_2026-12-31.zip",
            xhtml_name="report.xhtml",
            xhtml_body=_sample_ixbrl_body(company_id="5560000001", amount="200"),
        )
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, changed_archive_key)] = (
        _outer_zip(
            nested_name="5560000002_2026-12-31.zip",
            xhtml_name="report.xhtml",
            xhtml_body=_sample_ixbrl_body(company_id="5560000002", amount="300"),
        )
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="initial-catalog",
            partition_year="2026",
            source_archive_keys=[old_archive_key, unchanged_archive_key],
            replace_scope="partition",
        )
        parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="initial-parse",
            partition_year="2026",
            replace_scope="partition",
        )
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="current-catalog",
            partition_year="2026",
            source_archive_keys=[changed_archive_key],
            replace_scope="archive",
        )
        counts = parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="current-parse",
            partition_year="2026",
            replace_scope="archive",
            catalog_source_run_id="current-catalog",
        )
        report_rows = connection.execute(
            """
            select source_archive_name, company_id, source_run_id
            from sweden_financial.reports
            order by source_archive_name, company_id
            """
        ).fetchall()
        fact_rows = connection.execute(
            """
            select reports.source_archive_name, facts.company_id, facts.raw_value
            from sweden_financial.facts as facts
            join sweden_financial.reports as reports using (statement_key)
            order by reports.source_archive_name, facts.company_id
            """
        ).fetchall()

    assert counts["catalog_row_count"] == 1
    assert counts["reports_parsed_count"] == 1
    assert report_rows == [
        ("01_1.zip", "5560000002", "current-parse"),
        ("02_1.zip", "5560000001", "initial-parse"),
    ]
    assert fact_rows == [
        ("01_1.zip", "5560000002", "300"),
        ("02_1.zip", "5560000001", "200"),
    ]


def test_extract_sweden_financial_report_xhtml_catalog_keeps_multiple_xhtml_members(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5566235023_2020-06-30.zip",
        xhtml_files={
            "a64ce432-a97d-4e74-a35f-a77d66523c72.xhtml": b"<html>first</html>",
            "c38e1b06-f6cf-4ad4-bce5-887f0e775455.xhtml": b"<html>second</html>",
        },
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-1",
            partition_year="2020",
        )
        catalog_rows = connection.execute(
            """
            select company_id, report_period_end, nested_zip_member, xhtml_member
            from sweden_financial.report_xhtml_catalog
            order by xhtml_member
            """
        ).fetchall()

    assert counts["nested_zip_count"] == 1
    assert counts["report_xhtml_count"] == 2
    assert counts["downloaded_report_xhtml_count"] == 2
    assert counts["catalog_row_count"] == 2
    assert catalog_rows == [
        (
            "5566235023",
            "2020-06-30",
            "5566235023_2020-06-30.zip",
            "a64ce432-a97d-4e74-a35f-a77d66523c72.xhtml",
        ),
        (
            "5566235023",
            "2020-06-30",
            "5566235023_2020-06-30.zip",
            "c38e1b06-f6cf-4ad4-bce5-887f0e775455.xhtml",
        ),
    ]


def test_extract_sweden_financial_report_xhtml_catalog_skips_nested_zip_without_xhtml(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2025/12_31.zip",
        source_last_modified="2026-01-02T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5594386681_2025-12-31.zip",
        xhtml_files={
            "metadata.xml": b"<metadata />",
            "readme.txt": b"no inline xhtml in this nested archive",
        },
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-1",
            partition_year="2025",
        )
        catalog_count = connection.execute(
            "select count(*) from sweden_financial.report_xhtml_catalog"
        ).fetchone()[0]

    assert counts["nested_zip_count"] == 1
    assert counts["skipped_nested_zip_count"] == 1
    assert counts["report_xhtml_count"] == 0
    assert counts["catalog_row_count"] == 0
    assert catalog_count == 0


def test_extract_sweden_financial_report_xhtml_catalog_logs_archive_progress(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    raw_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, raw_archive_key)] = _outer_zip(
        nested_name="5560000000_2023-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=b"<html><body>annual report</body></html>",
    )
    log_messages: list[str] = []

    def log_info(message: str, *args: object) -> None:
        log_messages.append(message % args)

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-1",
            partition_year="2020",
            log_info=log_info,
        )

    assert any(
        "Starting Sweden financial XHTML catalog extraction" in message
        for message in log_messages
    )
    assert any(
        "Processing Sweden financial archive" in message for message in log_messages
    )
    assert any(
        "Processing Sweden financial nested ZIPs" in message for message in log_messages
    )
    assert any(
        "Processed Sweden financial archive" in message for message in log_messages
    )
    assert any(
        "Finished Sweden financial XHTML catalog extraction" in message
        for message in log_messages
    )


def test_extract_sweden_financial_report_xhtml_catalog_replaces_only_partition_year(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    archive_2020_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    archive_2021_key = archive_object_key(
        upstream_key="arsredovisningar/2021/01_1.zip",
        source_last_modified="2025-02-10T10:00:00.000Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, archive_2020_key)] = _outer_zip(
        nested_name="5560000000_2020-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=b"<html>2020</html>",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, archive_2021_key)] = _outer_zip(
        nested_name="5560000001_2021-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=b"<html>2021</html>",
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-2021",
            partition_year="2021",
        )
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="run-2020",
            partition_year="2020",
        )
        rows = connection.execute(
            """
            select partition_year, company_id, report_period_end
            from sweden_financial.report_xhtml_catalog
            order by partition_year
            """
        ).fetchall()

    assert rows == [
        ("2020", "5560000000", "2020-12-31"),
        ("2021", "5560000001", "2021-12-31"),
    ]


def test_extract_sweden_financial_report_xhtml_catalog_merges_changed_archives(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    old_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/01_1.zip",
        source_last_modified="2026-07-01T09:13:53.713Z",
    )
    unchanged_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/02_1.zip",
        source_last_modified="2026-07-01T09:13:53.713Z",
    )
    changed_archive_key = archive_object_key(
        upstream_key="arsredovisningar/2026/01_1.zip",
        source_last_modified="2026-07-04T09:13:53.713Z",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, old_archive_key)] = _outer_zip(
        nested_name="5560000000_2026-12-31.zip",
        xhtml_name="report.xhtml",
        xhtml_body=b"<html>old changed archive</html>",
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, unchanged_archive_key)] = (
        _outer_zip(
            nested_name="5560000001_2026-12-31.zip",
            xhtml_name="report.xhtml",
            xhtml_body=b"<html>unchanged archive</html>",
        )
    )
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, changed_archive_key)] = (
        _outer_zip(
            nested_name="5560000002_2026-12-31.zip",
            xhtml_name="report.xhtml",
            xhtml_body=b"<html>new changed archive</html>",
        )
    )

    with duckdb.connect(str(tmp_path / "sweden_financial_source.duckdb")) as connection:
        extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="initial",
            partition_year="2026",
            source_archive_keys=[old_archive_key, unchanged_archive_key],
            replace_scope="partition",
        )
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id="current",
            partition_year="2026",
            source_archive_keys=[changed_archive_key],
            replace_scope="archive",
        )
        rows = connection.execute(
            """
            select source_archive_name, company_id
            from sweden_financial.report_xhtml_catalog
            where partition_year = '2026'
            order by source_archive_name, company_id
            """
        ).fetchall()

    assert counts["source_archive_count"] == 1
    assert rows == [
        ("01_1.zip", "5560000002"),
        ("02_1.zip", "5560000001"),
    ]


def _sample_ixbrl_body(*, company_id: str, amount: str) -> bytes:
    return f"""<?xml version="1.0"?>
<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:se="http://example.test/se">
<body>
  <xbrli:context id="duration">
    <xbrli:entity>
      <xbrli:identifier scheme="https://bolagsverket.se/">{company_id}</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2026-01-01</xbrli:startDate>
      <xbrli:endDate>2026-12-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="SEK">
    <xbrli:measure>iso4217:SEK</xbrli:measure>
  </xbrli:unit>
  <ix:nonFraction name="se:Revenue" contextRef="duration" unitRef="SEK" decimals="0">{amount}</ix:nonFraction>
</body>
</html>
""".encode()


def _listing_xml(
    *,
    key: str,
    last_modified: str,
    etag: str,
    size: int,
    next_marker: str | None = None,
) -> bytes:
    next_marker_xml = (
        f"<NextMarker>{next_marker}</NextMarker>" if next_marker is not None else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bulkfil-paketering-zipfiler-prod</Name>
  <Prefix>arsredovisningar/</Prefix>
  <Contents>
    <Key>{key}</Key>
    <LastModified>{last_modified}</LastModified>
    <ETag>{etag}</ETag>
    <Size>{size}</Size>
  </Contents>
  {next_marker_xml}
</ListBucketResult>
""".encode("utf-8")


def _listing_xml_many(
    archives: list[dict[str, object]],
    *,
    next_marker: str | None = None,
) -> bytes:
    contents_xml = "\n".join(
        f"""
  <Contents>
    <Key>{archive["key"]}</Key>
    <LastModified>{archive["last_modified"]}</LastModified>
    <ETag>{archive["etag"]}</ETag>
    <Size>{archive["size"]}</Size>
  </Contents>
"""
        for archive in archives
    )
    next_marker_xml = (
        f"<NextMarker>{next_marker}</NextMarker>" if next_marker is not None else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bulkfil-paketering-zipfiler-prod</Name>
  <Prefix>arsredovisningar/</Prefix>
  {contents_xml}
  {next_marker_xml}
</ListBucketResult>
""".encode("utf-8")


def _outer_zip(
    *,
    nested_name: str,
    xhtml_name: str = "report.xhtml",
    xhtml_body: bytes = b"<html>report</html>",
    xhtml_files: dict[str, bytes] | None = None,
) -> bytes:
    nested_buffer = BytesIO()
    with zipfile.ZipFile(
        nested_buffer, "w", compression=zipfile.ZIP_DEFLATED
    ) as nested:
        files = xhtml_files or {xhtml_name: xhtml_body}
        for name, body in files.items():
            nested.writestr(name, body)

    outer_buffer = BytesIO()
    with zipfile.ZipFile(outer_buffer, "w", compression=zipfile.ZIP_DEFLATED) as outer:
        outer.writestr(nested_name, nested_buffer.getvalue())
    return outer_buffer.getvalue()
