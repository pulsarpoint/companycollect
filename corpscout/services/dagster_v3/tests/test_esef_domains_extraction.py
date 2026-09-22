"""The esef_domains extractor's per-document function: package in, website
candidates + esef_domains rows out. Runs the real extraction in the real
spawned child for one synthetic package."""

import subprocess
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_OK,
    DocumentDomains,
    DomainsDocument,
    domain_id,
    domain_rows,
    extract_package_domains,
    extract_package_domains_bounded,
)
from dagster_v3.defs.esef_filings.tables import ESEF_DOMAINS_EXPORT_COLUMNS

REPORT = """<html xmlns="http://www.w3.org/1999/xhtml"
 xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>
<p>Mer information finns på www.example.com.</p>
<p>Delårsrapporter publiceras på www.example.com/ir.</p>
<p>Läs mer på www.once-only.org om ramverket.</p>
</body></html>"""

DOCUMENT = DomainsDocument(
    source_document_id="doc-1",
    package_sha256="a" * 64,
    lei="5493001KJTIIGC8Y1R12",
    period_end=date(2024, 12, 31),
    fiscal_year=2024,
)


def _package(path: Path, report: str = REPORT) -> Path:
    with zipfile.ZipFile(path, "w") as package:
        package.writestr("pkg/META-INF/taxonomyPackage.xml", "<x/>")
        package.writestr("pkg/reports/report.xhtml", report)
    return path


def test_extract_package_domains_reads_reports_tagged_facts_and_emails(
    tmp_path: Path,
) -> None:
    package = _package(tmp_path / "p.zip")

    candidates, corroborated = extract_package_domains(
        str(package),
        (("WebsitesOfLegalEntity", "https://www.tagged.example.net"),),
        ("example.se",),
        str(tmp_path),
    )

    assert [c.registrable_domain for c in candidates] == [
        "example.com",
        "example.net",
        "once-only.org",
    ]
    assert corroborated == frozenset({"example.com", "example.net", "example.se"})
    tagged = candidates[1]
    assert tagged.suggested_roles == ["company_website"]
    assert tagged.evidence[0].report_member == ""
    assert tagged.evidence[0].extraction_method == "tagged_fact"
    # The extraction directory is cleaned up.
    assert [p for p in tmp_path.iterdir() if p.is_dir()] == []


def test_bounded_extraction_runs_in_a_child_and_reports_ok(tmp_path: Path) -> None:
    package = _package(tmp_path / "p.zip")

    result = extract_package_domains_bounded(str(package), (), (), 120, str(tmp_path))

    assert result.status == STATUS_OK
    assert [c.registrable_domain for c in result.candidates] == [
        "example.com",
        "once-only.org",
    ]
    assert result.corroborated_domains == frozenset({"example.com"})
    assert result.error_message == ""
    assert result.seconds > 0


def test_bounded_extraction_reports_a_bad_package_as_failed(tmp_path: Path) -> None:
    not_a_zip = tmp_path / "p.zip"
    not_a_zip.write_bytes(b"not a zip")

    result = extract_package_domains_bounded(str(not_a_zip), (), (), 120, str(tmp_path))

    assert result.status == STATUS_FAILED
    assert result.candidates == ()
    assert "BadZipFile" in result.error_message


def test_domain_rows_one_row_per_candidate_in_export_column_order(
    tmp_path: Path,
) -> None:
    candidates, corroborated = extract_package_domains(
        str(_package(tmp_path / "p.zip")), (), ("example.se",), str(tmp_path)
    )
    result = DocumentDomains(STATUS_OK, candidates, corroborated, "", 0.5)
    extracted_at = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)

    rows = domain_rows(
        DOCUMENT,
        result,
        extractor_version=ESEF_DOMAINS_EXTRACTOR_VERSION,
        source_run_id="run-1",
        extracted_at=extracted_at,
    )

    assert [row["registrable_domain"] for row in rows] == [
        "example.com",
        "once-only.org",
    ]
    for row in rows:
        assert tuple(row) == ESEF_DOMAINS_EXPORT_COLUMNS
        assert row["source_document_id"] == "doc-1"
        assert row["package_sha256"] == "a" * 64
        assert row["lei"] == DOCUMENT.lei
        assert row["period_end"] == date(2024, 12, 31)
        assert row["fiscal_year"] == 2024
        assert row["extraction_status"] == STATUS_OK
        assert row["error_message"] == ""
        assert row["extractor_version"] == "esef-domains-v2-context"
        assert row["source_run_id"] == "run-1"
        assert row["extracted_at"] == extracted_at
    first = rows[0]
    assert first["domain_id"] == domain_id("doc-1", "example.com")
    assert len(first["domain_id"]) == 64
    assert first["corroborated"] == 1
    assert first["evidence_count"] == 2
    assert first["hosts_json"] == '["www.example.com"]'
    assert '"extraction_method"' in first["evidence_json"]
    assert rows[1]["corroborated"] == 0
    assert rows[1]["roles_json"] == '["external_reference"]'


def test_domain_rows_marker_rows_for_empty_and_failed_documents() -> None:
    extracted_at = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
    empty = domain_rows(
        DOCUMENT,
        DocumentDomains(STATUS_EMPTY, (), frozenset(), "", 0.1),
        extractor_version="esef-domains-v1",
        source_run_id="run-1",
        extracted_at=extracted_at,
    )
    failed = domain_rows(
        DOCUMENT,
        DocumentDomains(STATUS_FAILED, (), frozenset(), "BadZipFile: x", 0.1),
        extractor_version="esef-domains-v1",
        source_run_id="run-1",
        extracted_at=extracted_at,
    )

    assert len(empty) == 1 and len(failed) == 1
    for row in (empty[0], failed[0]):
        assert tuple(row) == ESEF_DOMAINS_EXPORT_COLUMNS
        assert row["registrable_domain"] == ""
        assert row["domain_id"] == domain_id("doc-1", "")
        assert row["hosts_json"] == "[]"
        assert row["roles_json"] == "[]"
        assert row["evidence_json"] == "[]"
        assert row["evidence_count"] == 0
        assert row["corroborated"] == 0
    assert empty[0]["extraction_status"] == STATUS_EMPTY
    assert empty[0]["error_message"] == ""
    assert failed[0]["extraction_status"] == STATUS_FAILED
    assert failed[0]["error_message"] == "BadZipFile: x"


def test_module_stays_light_no_dagster_or_arelle_import() -> None:
    probe = (
        "import sys; import dagster_v3.defs.esef_filings.domains_extraction; "
        "heavy = sorted(m for m in sys.modules if m == 'dagster' or m.startswith('dagster.') "
        "or m == 'arelle' or m.startswith('arelle.') "
        "or m.endswith('segment_parser') or m.endswith('segment_assets')); "
        "print(heavy); sys.exit(1 if heavy else 0)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
