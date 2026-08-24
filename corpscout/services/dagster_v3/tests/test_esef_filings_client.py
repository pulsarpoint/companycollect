"""Tests for the ESEF filings index client, streamed fact download, and the
export-column contract against migration 000149.

All fixtures are committed files under tests/fixtures/esef_filings/ (captured
once via curl against the live filings.xbrl.org API — see that directory).
No test here hits the network.
"""

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.esef_filings import client as esef_client
from dagster_v3.defs.esef_filings import tables

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "esef_filings"

# Same parents[] depth as the working MIGRATIONS_DIR in test_clickhouse_migrations.py
# (this file lives in the same tests/ directory): tests -> dagster_v3 -> services -> corpscout.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = MIGRATIONS_DIR / "000149_corpscout_esef_filings.up.sql"
DOCUMENT_MIGRATION_FILE = (
    MIGRATIONS_DIR / "000243_corpscout_esef_source_documents.up.sql"
)
DISCLOSURE_MIGRATION_FILE = (
    MIGRATIONS_DIR / "000316_corpscout_esef_disclosures.up.sql"
)
CONCEPT_LABEL_MIGRATION_FILE = (
    MIGRATIONS_DIR / "000246_corpscout_esef_document_concept_labels.up.sql"
)
LLM_PROVENANCE_MIGRATION_FILE = (
    MIGRATIONS_DIR / "000247_corpscout_esef_llm_provenance.up.sql"
)
PARTITIONED_PARSING_MIGRATION_FILE = (
    MIGRATIONS_DIR / "000309_corpscout_esef_parsing_v2.up.sql"
)
assert MIGRATION_FILE.exists(), (
    f"migration file not found at {MIGRATION_FILE} — check the parents[] depth "
    "(tests/ -> dagster_v3 -> services -> corpscout -> clickhouse/migrations)"
)


def _fixture_json(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text())


# --------------------------------------------------------------------------
# iter_filings: fake JSON:API index session
# --------------------------------------------------------------------------


class _FakeIndexResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeIndexSession:
    """Serves canned pages keyed by page[number]; asserts the query shape."""

    def __init__(self, pages: dict[int, dict[str, Any]]) -> None:
        self._pages = pages
        self.requested_pages: list[int] = []
        self.requested_params: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
        stream: bool = False,
    ) -> _FakeIndexResponse:
        assert url == esef_client.ESEF_INDEX_URL
        assert params is not None
        assert params["page[size]"] == esef_client.PAGE_SIZE
        assert params["include"] == "entity"
        page_number = params["page[number]"]
        self.requested_pages.append(page_number)
        self.requested_params.append(params)
        return _FakeIndexResponse(self._pages[page_number])


def _three_page_session() -> _FakeIndexSession:
    return _FakeIndexSession(
        {
            1: _fixture_json("index_page1.json"),
            2: _fixture_json("index_page2.json"),
            3: _fixture_json("index_page_empty.json"),
        }
    )


def test_iter_filings_paginates_across_pages_then_stops_on_empty_page() -> None:
    session = _three_page_session()
    client = esef_client.EsefFilingsClient(session=session)

    records = list(client.iter_filings())

    assert session.requested_pages == [1, 2, 3]
    # 3 filings on page1 + 3 on page2; the empty page3 yields nothing.
    assert len(records) == 6
    assert [r.fxo_id for r in records] == [
        "EDRPOU-32033791-2020-12-31-UAIFRS-UA-0",
        "EDRPOU-31729918-2020-12-31-UAIFRS-UA-0",
        "549300CSLHPO6Y1AZN37-2021-12-31-ESEF-SE-1",
        "259400OOMJ31L0SWCY70-2022-12-31-ESEF-PL-0",
        "259400NFU8A8SBP6VC21-2022-12-31-ESEF-PL-0",
        "259400G1XM5GQU4K4G26-2022-12-31-ESEF-PL-0",
    ]


def test_iter_filings_stops_immediately_on_empty_first_page() -> None:
    session = _FakeIndexSession({1: _fixture_json("index_page_empty.json")})

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    assert records == []
    assert session.requested_pages == [1]


def test_iter_filings_filters_and_sorts_by_processed_window() -> None:
    session = _FakeIndexSession({1: _fixture_json("index_page_empty.json")})

    list(
        esef_client.EsefFilingsClient(session=session).iter_filings(
            processed_from=datetime(2026, 7, 1, tzinfo=UTC),
            processed_until=datetime(2026, 8, 1, tzinfo=UTC),
        )
    )

    params = session.requested_params[0]
    assert params["sort"] == "processed,fxo_id"
    assert json.loads(params["filter"]) == [
        {
            "name": "processed",
            "op": "ge",
            "val": "2026-07-01 00:00:00",
        },
        {
            "name": "processed",
            "op": "lt",
            "val": "2026-08-01 00:00:00",
        },
    ]


def _one_real_page_then_empty(fixture_name: str) -> _FakeIndexSession:
    return _FakeIndexSession(
        {1: _fixture_json(fixture_name), 2: _fixture_json("index_page_empty.json")}
    )


def test_iter_filings_joins_entity_by_relationship_id_for_lei_and_name() -> None:
    session = _one_real_page_then_empty("index_page1.json")

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    by_fxo = {r.fxo_id: r for r in records}
    cloetta = by_fxo["549300CSLHPO6Y1AZN37-2021-12-31-ESEF-SE-1"]
    assert cloetta.lei == "549300CSLHPO6Y1AZN37"
    assert cloetta.entity_name == "Cloetta AB"
    assert cloetta.country == "SE"


def test_iter_filings_missing_entity_relationship_id_yields_empty_lei_and_name() -> (
    None
):
    # Synthetic fixture: relationships.entity.data.id points at "missing-999",
    # which is absent from included -- the page-local entity map join must
    # degrade to empty lei/entity_name rather than raising a KeyError.
    payload = {
        "data": [
            {
                "type": "filing",
                "id": "1",
                "attributes": {
                    "fxo_id": "MISSING-ENTITY-2022-12-31-ESEF-XX-0",
                    "country": "XX",
                    "period_end": "2022-12-31",
                    "date_added": "2023-01-01 00:00:00",
                    "processed": "2023-01-02 00:00:00",
                    "json_url": None,
                    "package_url": None,
                    "report_url": None,
                    "viewer_url": None,
                    "sha256": None,
                    "error_count": 0,
                    "warning_count": 0,
                    "inconsistency_count": 0,
                },
                "relationships": {
                    "entity": {"data": {"type": "entity", "id": "missing-999"}}
                },
            }
        ],
        "included": [],
    }
    session = _FakeIndexSession({1: payload, 2: _fixture_json("index_page_empty.json")})

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    assert len(records) == 1
    assert records[0].fxo_id == "MISSING-ENTITY-2022-12-31-ESEF-XX-0"
    assert records[0].lei == ""
    assert records[0].entity_name == ""


def test_iter_filings_absolutizes_relative_urls() -> None:
    session = _one_real_page_then_empty("index_page1.json")

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    cloetta = next(
        r for r in records if r.fxo_id == "549300CSLHPO6Y1AZN37-2021-12-31-ESEF-SE-1"
    )
    assert cloetta.package_url == (
        "https://filings.xbrl.org/549300CSLHPO6Y1AZN37/2021-12-31/ESEF/SE/1/"
        "549300CSLHPO6Y1AZN37-2021-12-31.zip"
    )


def test_iter_filings_missing_json_url_is_none() -> None:
    session = _one_real_page_then_empty("index_page1.json")

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    cloetta = next(
        r for r in records if r.fxo_id == "549300CSLHPO6Y1AZN37-2021-12-31-ESEF-SE-1"
    )
    assert cloetta.json_url is None
    # ...and viewer_url/report_url, also null on this filing, stay None too.
    assert cloetta.viewer_url is None
    assert cloetta.report_url is None


def test_iter_filings_present_json_url_is_absolutized_on_second_page() -> None:
    session = _one_real_page_then_empty("index_page2.json")

    records = list(esef_client.EsefFilingsClient(session=session).iter_filings())

    instal_krakow = next(
        r for r in records if r.fxo_id == "259400OOMJ31L0SWCY70-2022-12-31-ESEF-PL-0"
    )
    assert instal_krakow.json_url == (
        "https://filings.xbrl.org/259400OOMJ31L0SWCY70/2022-12-31/ESEF/PL/0/"
        "259400OOMJ31L0SWCY70-2022-12-31.json"
    )
    assert instal_krakow.lei == "259400OOMJ31L0SWCY70"
    assert instal_krakow.entity_name == "Instal Kraków S.A."


def test_absolute_leaves_already_absolute_urls_unchanged() -> None:
    assert esef_client._absolute("https://example.test/x.json") == (
        "https://example.test/x.json"
    )


def test_absolute_treats_empty_string_as_none() -> None:
    assert esef_client._absolute("") is None
    assert esef_client._absolute(None) is None


# --------------------------------------------------------------------------
# last_reported_total: API's own meta.count, captured off the first page
# (Finding M1 -- powers the crawl-completeness guard in assets.py)
# --------------------------------------------------------------------------


def test_last_reported_total_is_none_before_any_crawl() -> None:
    client = esef_client.EsefFilingsClient(session=_three_page_session())
    assert client.last_reported_total is None


def test_iter_filings_captures_reported_total_from_first_page_meta_count() -> None:
    session = _three_page_session()
    client = esef_client.EsefFilingsClient(session=session)

    list(client.iter_filings())

    # index_page1.json's real meta.count -- same fixture value repeated
    # on index_page2.json (a real API would keep the total stable across
    # pages of the same crawl).
    assert client.last_reported_total == 25061


def test_iter_filings_reported_total_reflects_only_the_first_page() -> None:
    # A page-2 meta.count that differs from page 1's must NOT overwrite
    # last_reported_total -- it is captured once, off the first page only.
    session = _FakeIndexSession(
        {
            1: _fixture_json("index_page1.json"),
            2: {**_fixture_json("index_page2.json"), "meta": {"count": 999}},
            3: _fixture_json("index_page_empty.json"),
        }
    )
    client = esef_client.EsefFilingsClient(session=session)

    list(client.iter_filings())

    assert client.last_reported_total == 25061


def test_iter_filings_reported_total_is_none_when_first_page_meta_has_no_count() -> (
    None
):
    session = _FakeIndexSession(
        {1: {"data": [], "included": [], "meta": {}}},
    )
    client = esef_client.EsefFilingsClient(session=session)

    list(client.iter_filings())

    assert client.last_reported_total is None


def test_iter_filings_reported_total_is_none_when_first_page_has_no_meta_key() -> None:
    session = _FakeIndexSession({1: {"data": [], "included": []}})
    client = esef_client.EsefFilingsClient(session=session)

    list(client.iter_filings())

    assert client.last_reported_total is None


def test_extract_reported_total_tolerates_malformed_shapes() -> None:
    assert esef_client._extract_reported_total({}) is None
    assert esef_client._extract_reported_total({"meta": "not-a-dict"}) is None
    assert esef_client._extract_reported_total({"meta": {}}) is None
    assert esef_client._extract_reported_total({"meta": {"count": "25061"}}) is None
    # bool is technically an int subclass in Python -- must not be accepted.
    assert esef_client._extract_reported_total({"meta": {"count": True}}) is None
    assert esef_client._extract_reported_total({"meta": {"count": 42}}) == 42


# --------------------------------------------------------------------------
# download_json_facts: whole-download retry loop
# --------------------------------------------------------------------------


class _FakeDownloadResponse:
    def __init__(
        self,
        *,
        body: bytes,
        fail: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._body = body
        self._fail = fail
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        if self._fail:
            raise esef_client.dlt_requests.ChunkedEncodingError(
                "connection broken mid-stream"
            )
        yield self._body


class _FlakyDownloadSession:
    def __init__(self, body: bytes, fail_times: int) -> None:
        self._body = body
        self._fail_times = fail_times
        self.calls = 0

    def get(self, url: str, *, timeout: int, stream: bool = False):
        self.calls += 1
        return _FakeDownloadResponse(
            body=self._body, fail=self.calls <= self._fail_times
        )


def test_download_json_facts_writes_real_fixture_bytes(tmp_path: Path) -> None:
    facts_path = FIXTURES_DIR / "facts_sample.json"
    body = facts_path.read_bytes()
    session = _FlakyDownloadSession(body, fail_times=0)
    client = esef_client.EsefFilingsClient(session=session)
    target = tmp_path / "facts.json"

    client.download_json_facts(
        "https://filings.xbrl.org/x/facts.json", target, retry_base_seconds=0
    )

    assert target.read_bytes() == body
    assert session.calls == 1


def test_download_json_facts_retries_transient_failure_then_succeeds(
    tmp_path: Path,
) -> None:
    body = b'{"facts": {}}'
    session = _FlakyDownloadSession(body, fail_times=2)
    client = esef_client.EsefFilingsClient(session=session)
    target = tmp_path / "facts.json"

    client.download_json_facts(
        "https://filings.xbrl.org/x/facts.json", target, retry_base_seconds=0
    )

    assert target.read_bytes() == body
    assert session.calls == 3  # 2 transient failures + 1 success


def test_download_json_facts_raises_after_exhausting_retries(tmp_path: Path) -> None:
    session = _FlakyDownloadSession(b"x", fail_times=99)
    client = esef_client.EsefFilingsClient(session=session)
    target = tmp_path / "facts.json"

    with pytest.raises(esef_client.dlt_requests.ChunkedEncodingError):
        client.download_json_facts(
            "https://filings.xbrl.org/x/facts.json",
            target,
            max_attempts=3,
            retry_base_seconds=0,
        )
    assert session.calls == 3


def test_download_json_facts_truncates_target_on_each_attempt(tmp_path: Path) -> None:
    # A short first attempt must not leave stale bytes behind once retried.
    target = tmp_path / "facts.json"
    target.write_bytes(b"stale-partial-content-from-a-previous-run")

    session = _FlakyDownloadSession(b"ok", fail_times=1)
    client = esef_client.EsefFilingsClient(session=session)

    client.download_json_facts(
        "https://filings.xbrl.org/x/facts.json", target, retry_base_seconds=0
    )

    assert target.read_bytes() == b"ok"


def test_download_json_facts_short_read_vs_content_length_is_retried(
    tmp_path: Path,
) -> None:
    # Server advertises more bytes than it delivers -> treat as retryable.
    class _ShortThenOkSession:
        def __init__(self) -> None:
            self.calls = 0

        def get(self, url: str, *, timeout: int, stream: bool = False):
            self.calls += 1
            if self.calls == 1:
                return _FakeDownloadResponse(
                    body=b"only-ten!!", headers={"Content-Length": "100"}
                )
            return _FakeDownloadResponse(body=b"x" * 100)

    session = _ShortThenOkSession()
    client = esef_client.EsefFilingsClient(session=session)
    target = tmp_path / "facts.json"

    client.download_json_facts(
        "https://filings.xbrl.org/x/facts.json", target, retry_base_seconds=0
    )

    assert session.calls == 2
    assert len(target.read_bytes()) == 100


# --------------------------------------------------------------------------
# Contract test: export-column tuples vs migration 000149's column order
# --------------------------------------------------------------------------


def _migration_table_columns(sql: str, table: str) -> list[str]:
    """Extract a CREATE TABLE's column names in declared order.

    Mirrors the parsing approach in tests/canonical_contact_tables.py
    (_extract_create/_assert_ddl): grab the parenthesized column block up to
    `ENGINE = `, split into lines, strip trailing commas, and take the first
    whitespace-separated token of each line as the column name.
    """
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS corpscout\.{re.escape(table)}\s*\((.*?)\)\s*"
        rf"ENGINE\s*=",
        sql,
        re.DOTALL,
    )
    assert match, f"no CREATE TABLE found for corpscout.{table} in migration SQL"
    body = match.group(1)
    lines = [
        line.strip().rstrip(",") for line in body.strip().splitlines() if line.strip()
    ]
    return [line.split()[0] for line in lines]


def test_export_columns_match_migration_000149_column_order() -> None:
    sql = MIGRATION_FILE.read_text()
    expected_by_table = {
        tables.ESEF_FILINGS_TABLE: tables.ESEF_FILINGS_EXPORT_COLUMNS,
        tables.ESEF_FACTS_TABLE: tables.ESEF_FACTS_EXPORT_COLUMNS,
        tables.ESEF_ENTITY_REGISTRY_MAP_TABLE: tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS,
        tables.ESEF_FINANCIAL_METRICS_TABLE: tables.ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS,
    }

    for table_name, export_columns in expected_by_table.items():
        migration_columns = _migration_table_columns(sql, table_name)
        # resolved_at is CH-defaulted (DEFAULT now64(3)) and deliberately excluded
        # from every *_EXPORT_COLUMNS tuple.
        assert migration_columns[-1] == "resolved_at", (
            f"{table_name}: expected trailing resolved_at column, got "
            f"{migration_columns}"
        )
        assert tuple(migration_columns[:-1]) == export_columns, (
            f"{table_name}: export columns {export_columns} do not match "
            f"migration column order {migration_columns[:-1]}"
        )


def test_document_export_columns_match_migration_000243_column_order() -> None:
    sql = DOCUMENT_MIGRATION_FILE.read_text(encoding="utf-8")
    expected_by_table = {
        tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE: (
            tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS
        ),
        tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE: (
            tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS
        ),
    }

    for table_name, export_columns in expected_by_table.items():
        migration_columns = _migration_table_columns(sql, table_name)
        assert migration_columns[-1] == "resolved_at"
        if table_name == tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE:
            provenance_columns = {
                "llm_request_object_key",
                "llm_request_sha256",
                "llm_response_text",
                "llm_response_sha256",
            }
            assert tuple(
                column for column in export_columns if column not in provenance_columns
            ) == tuple(migration_columns[:-1])
        else:
            assert tuple(migration_columns[:-1]) == export_columns


def test_llm_provenance_migration_adds_request_and_response_columns() -> None:
    sql = LLM_PROVENANCE_MIGRATION_FILE.read_text(encoding="utf-8")

    assert "ALTER TABLE corpscout.esef_document_company_information" in sql
    for column in (
        "llm_request_object_key",
        "llm_request_sha256",
        "llm_response_text",
        "llm_response_sha256",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column} String" in sql


def test_disclosure_export_columns_match_migration_000316_column_order() -> None:
    sql = DISCLOSURE_MIGRATION_FILE.read_text(encoding="utf-8")
    migration_columns = _migration_table_columns(
        sql,
        tables.ESEF_DISCLOSURES_TABLE,
    )

    assert migration_columns[-1] == "resolved_at"
    assert tuple(migration_columns[:-1]) == (
        tables.ESEF_DISCLOSURES_PARTITION_EXPORT_COLUMNS
    )


def test_concept_label_export_columns_match_migration_000246_column_order() -> None:
    sql = CONCEPT_LABEL_MIGRATION_FILE.read_text(encoding="utf-8")
    migration_columns = _migration_table_columns(
        sql,
        tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    )

    assert migration_columns[-1] == "resolved_at"
    assert (
        tuple(
            column for column in migration_columns[:-1] if column != "source_record_uid"
        )
        == tables.ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS
    )


def test_partition_export_columns_match_promoted_table_contracts() -> None:
    sql = PARTITIONED_PARSING_MIGRATION_FILE.read_text(encoding="utf-8")
    expected_by_table = {
        "esef_facts_v2": (tables.ESEF_FACTS_PARTITION_EXPORT_COLUMNS, set()),
        "esef_document_contact_candidates_v2": (
            tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_PARTITION_EXPORT_COLUMNS,
            {"source_record_uid"},
        ),
        "esef_document_concept_labels_v2": (
            tables.ESEF_DOCUMENT_CONCEPT_LABELS_PARTITION_EXPORT_COLUMNS,
            {"source_record_uid"},
        ),
    }

    for table_name, (export_columns, defaulted_columns) in expected_by_table.items():
        migration_columns = _migration_table_columns(sql, table_name)
        assert migration_columns[-1] == "resolved_at"
        explicit_columns = tuple(
            column
            for column in migration_columns[:-1]
            if column not in defaulted_columns
        )
        assert explicit_columns == export_columns
