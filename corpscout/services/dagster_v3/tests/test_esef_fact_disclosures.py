import json
from hashlib import sha256
from pathlib import Path

import duckdb
from dagster import AssetKey

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.disclosure_assets import (
    esef_fact_disclosure_artifacts_s3,
    esef_fact_disclosure_inputs_s3,
    esef_fact_disclosures_duckdb,
    parse_disclosure_inputs,
    publish_disclosure_partition,
    snapshot_disclosure_inputs,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.esef_filings.disclosure_parser import (
    DISCLOSURE_PARSER_NAME,
    DISCLOSURE_PARSER_VERSION,
    disclosure_row,
    parse_esef_disclosure,
    replace_disclosure_partition_rows,
)


def test_disclosure_parser_preserves_readable_text_and_tables() -> None:
    raw_value = """
    <div>
      <h2>MARKNADSVÄRDEN</h2>
      <p>Fastighetsbestånd i sammandrag</p>
      <table>
        <tr><td colspan="2">Sweden</td></tr>
        <tr><th>Market</th><th>Value</th></tr>
        <tr><td>Stockholm</td><td>100</td></tr>
      </table>
      <p>Investor rela<span></span>tions</p>
      <script>ignore this text</script>
    </div>
    """

    disclosure = parse_esef_disclosure(raw_value)

    assert disclosure.plain_text == (
        "MARKNADSVÄRDEN\n\nFastighetsbestånd i sammandrag\n\n"
        "Sweden\nMarket\tValue\nStockholm\t100\n\nInvestor relations"
    )
    assert [block["type"] for block in disclosure.blocks] == [
        "heading",
        "paragraph",
        "table",
        "paragraph",
    ]
    table = disclosure.blocks[2]
    assert table == {
        "type": "table",
        "title": "Sweden",
        "headerRowCount": 1,
        "rows": [
            [
                {"text": "Market", "colSpan": 1, "rowSpan": 1},
                {"text": "Value", "colSpan": 1, "rowSpan": 1},
            ],
            [
                {"text": "Stockholm", "colSpan": 1, "rowSpan": 1},
                {"text": "100", "colSpan": 1, "rowSpan": 1},
            ],
        ],
    }


def test_disclosure_row_is_deterministic_and_source_preserving() -> None:
    source = {
        "source_document_id": "SAMPLE-2024-0",
        "package_sha256": "a" * 64,
        "lei": "549300SAMPLE000000001",
        "country_iso2": "SE",
        "company_id": "5566000000",
        "period_end": "2024-12-31",
        "fiscal_year": 2024,
        "fact_id": "fact-42",
        "concept_qname": "ifrs:DisclosureOfRevenueExplanatory",
        "concept_local_name": "DisclosureOfRevenueExplanatory",
        "language": "sv",
        "raw_value": "<p>Oljor och fetter</p>",
    }

    first = disclosure_row(
        source,
        source_run_id="run-1",
        extracted_at="2026-08-03T08:00:00Z",
    )
    second = disclosure_row(
        source,
        source_run_id="run-2",
        extracted_at="2026-08-03T09:00:00Z",
    )

    assert first["disclosure_id"] == second["disclosure_id"]
    assert (
        first["source_record_uid"]
        == sha256(
            f"company-source-record-v1\nfile\nesef_report_package\n{'a' * 64}".encode()
        ).hexdigest()
    )
    assert first["raw_value_sha256"] == sha256(source["raw_value"].encode()).hexdigest()
    assert first["parser_name"] == DISCLOSURE_PARSER_NAME
    assert first["parser_version"] == DISCLOSURE_PARSER_VERSION
    assert json.loads(first["blocks_json"]) == [
        {"text": "Oljor och fetter", "type": "paragraph"}
    ]
    assert first["plain_text"] == "Oljor och fetter"
    assert first["source_run_id"] == "run-1"


def test_partition_replace_is_idempotent_and_preserves_other_years() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute("create schema esef_filings")
    row_2024 = _stored_row("doc-2024", "fact-1", 2024, "First")
    row_2023 = _stored_row("doc-2023", "fact-2", 2023, "Older")

    replace_disclosure_partition_rows(
        connection,
        source_document_ids=["doc-2024", "doc-2023"],
        rows=[row_2024, row_2023],
    )
    changed_2024 = _stored_row("doc-2024", "fact-1", 2024, "Changed")
    replace_disclosure_partition_rows(
        connection,
        source_document_ids=["doc-2024"],
        rows=[changed_2024],
    )
    replace_disclosure_partition_rows(
        connection,
        source_document_ids=["doc-2024"],
        rows=[changed_2024],
    )

    rows = connection.execute(
        f"select source_document_id, plain_text from {tables.QUALIFIED_FACT_DISCLOSURES_TABLE} "
        "order by source_document_id"
    ).fetchall()
    assert rows == [("doc-2023", "Older"), ("doc-2024", "Changed")]


def test_disclosure_assets_release_duckdb_during_parallel_parsing() -> None:
    input_key = AssetKey("esef_fact_disclosure_inputs_s3")
    artifact_key = AssetKey("esef_fact_disclosure_artifacts_s3")
    output_key = AssetKey("esef_fact_disclosures_duckdb")

    assert esef_fact_disclosure_inputs_s3.asset_deps[input_key] == {
        AssetKey("esef_filing_facts_duckdb"),
        AssetKey("esef_source_documents_duckdb"),
    }
    assert esef_fact_disclosure_inputs_s3.op.pool == "esef_filings_duckdb"
    assert esef_fact_disclosure_artifacts_s3.asset_deps[artifact_key] == {input_key}
    assert esef_fact_disclosure_artifacts_s3.op.pool == "esef_disclosure_parse"
    assert esef_fact_disclosures_duckdb.asset_deps[output_key] == {artifact_key}
    assert esef_fact_disclosures_duckdb.op.pool == "esef_filings_duckdb"


def test_processed_week_pipeline_snapshots_parses_and_publishes(tmp_path: Path) -> None:
    database = duckdb_resource(tmp_path / "esef.duckdb")
    package_sha256 = "b" * 64
    with database.get_connection() as connection:
        connection.execute("create schema esef_filings")
        connection.execute(
            "create table esef_filings.esef_source_documents ("
            "source_document_id varchar, package_sha256 varchar, lei varchar, "
            "country_iso2 varchar, company_id varchar, period_end varchar, "
            "fiscal_year integer, extraction_status varchar, "
            "source_processed_at varchar)"
        )
        connection.execute(
            "insert into esef_filings.esef_source_documents values "
            "('sample-2024', ?, '549300SAMPLE000000001', 'SE', '5566000000', "
            "'2024-12-31', 2024, 'parsed', '2025-04-01T00:00:00Z')",
            [package_sha256],
        )
        connection.execute(
            "create table esef_filings.facts (fxo_id varchar, fact_id varchar, "
            "concept_qname varchar, concept_local_name varchar, language varchar, "
            "raw_value varchar, value_kind varchar)"
        )
        connection.execute(
            "insert into esef_filings.facts values "
            "('sample-2024', 'fact-1', 'ifrs:DisclosureExplanatory', "
            "'DisclosureExplanatory', 'en', "
            "'<h2>BUSINESS</h2><p>Produces oils.</p>', 'text')"
        )
    object_store = _MemoryObjectStore()

    snapshot = snapshot_disclosure_inputs(
        esef_filings_duckdb=database,
        object_store=object_store,
        partition_key="2025-03-30",
        source_run_id="snapshot-run",
    )
    parsed = parse_disclosure_inputs(
        object_store=object_store,
        partition_key="2025-03-30",
        source_run_id="parse-run",
        parse_workers=1,
    )
    published = publish_disclosure_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        partition_key="2025-03-30",
    )

    assert snapshot["input_row_count"] == 1
    assert parsed["output_row_count"] == 1
    assert parsed["block_count"] == 2
    assert published["table"] == tables.QUALIFIED_FACT_DISCLOSURES_TABLE
    with database.get_connection() as connection:
        row = connection.execute(
            "select source_document_id, source_record_uid, source_fact_id, "
            "plain_text, block_count from esef_filings.esef_fact_disclosures"
        ).fetchone()
    assert row == (
        "sample-2024",
        sha256(
            f"company-source-record-v1\nfile\nesef_report_package\n{package_sha256}".encode()
        ).hexdigest(),
        "fact-1",
        "BUSINESS\n\nProduces oils.",
        2,
    )


def _stored_row(
    source_document_id: str,
    fact_id: str,
    fiscal_year: int,
    text: str,
) -> dict[str, object]:
    return disclosure_row(
        {
            "source_document_id": source_document_id,
            "package_sha256": sha256(source_document_id.encode()).hexdigest(),
            "lei": "549300SAMPLE000000001",
            "country_iso2": "SE",
            "company_id": "5566000000",
            "period_end": f"{fiscal_year}-12-31",
            "fiscal_year": fiscal_year,
            "fact_id": fact_id,
            "concept_qname": "ifrs:DisclosureExplanatory",
            "concept_local_name": "DisclosureExplanatory",
            "language": "en",
            "raw_value": f"<p>{text}</p>",
        },
        source_run_id="test-run",
        extracted_at="2026-08-03T08:00:00Z",
    )


class _MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_file(self, key: str, path: Path, *, bucket: str) -> None:
        self.objects[(bucket, key)] = path.read_bytes()

    def download_file(self, key: str, path: Path, *, bucket: str) -> None:
        path.write_bytes(self.objects[(bucket, key)])

    def write_bytes(self, key: str, value: bytes, *, bucket: str) -> None:
        self.objects[(bucket, key)] = value

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        return self.objects[(bucket, key)]
