import json
from hashlib import sha256
from pathlib import Path

import duckdb

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.disclosure_parser import (
    DISCLOSURE_PARSER_NAME,
    DISCLOSURE_PARSER_VERSION,
    disclosure_row,
    parse_esef_disclosure,
)
from dagster_v3.defs.esef_filings.partitioned_assets import (
    build_disclosures_partition_database,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    document_result_object_key,
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
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "lei": "549300SAMPLE000000001",
        "country_iso2": "SE",
        "company_id": "5566000000",
        "period_end": "2024-12-31",
        "fiscal_year": 2024,
        "source_fact_id": "fact-42",
        "source_fact_key": "fact-key-42",
        "concept_qname": "ifrs:DisclosureOfRevenueExplanatory",
        "concept_local_name": "DisclosureOfRevenueExplanatory",
        "language": "sv",
        "segment": "business_profile",
        "selection_reason": "concept:DisclosureOfRevenueExplanatory",
        "report_member": "report.xhtml",
        "period": {"start": "2024-01-01", "end": "2024-12-31"},
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
    assert first["text_sha256"] == sha256(b"Oljor och fetter").hexdigest()
    assert first["parser_name"] == DISCLOSURE_PARSER_NAME
    assert first["parser_version"] == DISCLOSURE_PARSER_VERSION
    assert json.loads(first["blocks_json"]) == [
        {"text": "Oljor och fetter", "type": "paragraph"}
    ]
    assert first["plain_text"] == "Oljor och fetter"
    assert first["source_run_id"] == "run-1"


def test_processed_week_pipeline_parses_artifacts_directly(tmp_path: Path) -> None:
    partition_key = "2025-03-30"
    package_sha256 = "b" * 64
    object_store = _MemoryObjectStore()
    artifact_key = "esef_filings/artifacts/sample.json"
    visible_text = "BOARD OF DIRECTORS\nAnna Andersson — Chair"
    artifact = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "parser": {
            "candidate_extractor_versions": {"corpscout-visible-sections": "1"}
        },
        "concepts": {"ifrs:DisclosureExplanatory": {"base_xsd_type": "string"}},
        "facts": {
            "fact-1": {
                "fact_key": "fact-1",
                "source_fact_id": "fact-1",
                "report_member": "report.xhtml",
                "ordinal": 1,
                "canonical_value": "<h2>BUSINESS</h2><p>Produces oils.</p>",
                "decimals": None,
                "oim_dimensions": {
                    "concept": "ifrs:DisclosureExplanatory",
                    "language": "en",
                },
            }
        },
        "segments": {
            "business_profile": [
                {
                    "fact_key": "fact-1",
                    "selection_reason": "concept:DisclosureExplanatory",
                }
            ]
        },
        "visible_sections": [
            {
                "section_type": "board_composition",
                "report_member": "report.xhtml",
                "heading": "BOARD OF DIRECTORS",
                "text": visible_text,
                "page_id": "pf42",
                "printed_page_number": "42",
                "anchor_xpath": "/html/body/div[1]",
                "anchor_visual_order": 7,
                "extraction_method": "positioned_page",
                "language": "en",
                "original_character_count": len(visible_text),
                "included_character_count": len(visible_text),
                "truncated": False,
                "text_sha256": sha256(visible_text.encode()).hexdigest(),
            }
        ],
    }
    result = {
        "schema_version": 3,
        "processed_week": partition_key,
        "source_run_id": "artifact-run",
        "extracted_at": "2025-04-01T00:00:00Z",
        "source_document_ids": ["sample-2024"],
        "document_rows": [
            {
                "source_document_id": "sample-2024",
                "package_sha256": package_sha256,
                "lei": "549300SAMPLE000000001",
                "country_iso2": "SE",
                "company_id": "5566000000",
                "period_end": "2024-12-31",
                "fiscal_year": 2024,
                "extraction_status": "parsed",
                "parsed_artifact_object_key": artifact_key,
            }
        ],
        "candidate_rows": [],
        "concept_label_rows": [],
        "metadata": {},
    }
    object_store.objects[(ESEF_DOCUMENT_BUCKET, artifact_key)] = json.dumps(
        artifact
    ).encode()
    object_store.objects[
        (ESEF_DOCUMENT_BUCKET, document_result_object_key(partition_key))
    ] = json.dumps(result).encode()
    target_path = tmp_path / "disclosures.duckdb"

    metadata = build_disclosures_partition_database(
        object_store=object_store,
        partition_key=partition_key,
        source_run_id="parse-run",
        parse_workers=1,
        target_path=target_path,
    )

    assert metadata["row_count"] == 2
    assert metadata["block_count"] == 3
    assert metadata["table"] == tables.QUALIFIED_DISCLOSURES_TABLE
    with duckdb.connect(str(target_path), read_only=True) as connection:
        row = connection.execute(
            "select source_document_id, source_record_uid, source_fact_id, "
            "plain_text, block_count from esef_filings.esef_disclosures "
            "where disclosure_kind = 'tagged_fact'"
        ).fetchone()
        visible_row = connection.execute(
            "select section_type, segment, page_id, anchor_visual_order, plain_text "
            "from esef_filings.esef_disclosures "
            "where disclosure_kind = 'visible_section'"
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
    assert visible_row == (
        "board_composition",
        "people_and_audit",
        "pf42",
        7,
        visible_text,
    )


class _MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_file(self, key: str, path: Path, *, bucket: str) -> None:
        self.objects[(bucket, key)] = path.read_bytes()

    def download_file(self, key: str, path: Path, *, bucket: str) -> None:
        path.write_bytes(self.objects[(bucket, key)])

    def exists(self, key: str, *, bucket: str) -> bool:
        return (bucket, key) in self.objects

    def write_bytes(self, key: str, value: bytes, *, bucket: str) -> None:
        self.objects[(bucket, key)] = value

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        return self.objects[(bucket, key)]
