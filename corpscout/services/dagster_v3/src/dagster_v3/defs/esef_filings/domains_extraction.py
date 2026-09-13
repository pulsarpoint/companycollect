"""Per-document domain extraction for the esef_domains extractor (spec
2026-09-13, section 3): one archived ESEF report package in, the website
candidates and the rows for corpscout.esef_domains out.

Light on purpose. `extract_package_domains` runs inside a spawned child
(`dagster_v3.defs.common.child_timeout`), so this module must not import
dagster, arelle or the artifact parser: every import here is paid once per
document (tests/test_esef_domains_extraction.py pins this).
"""

import json
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter

from dagster_v3.defs.common.child_timeout import (
    ChildFailedError,
    ChildTimeoutError,
    run_in_child_with_timeout,
)
from dagster_v3.defs.esef_filings.report_package import (
    extract_report_package,
    report_paths_for,
)
from dagster_v3.defs.esef_filings.website_candidates import (
    EsefWebsiteCandidate,
    TaggedWebsiteValue,
    extract_website_candidates_with_corroboration,
)

# Bump to re-extract every document (the sensor drains the stale set). The
# only version this extractor has; the artifact schema plays no part.
ESEF_DOMAINS_EXTRACTOR_VERSION = "esef-domains-v1"

STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"
STATUS_TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class DomainsDocument:
    """One row of the esef_filings index selected for extraction."""

    source_document_id: str
    package_sha256: str
    lei: str
    period_end: date
    fiscal_year: int


@dataclass(frozen=True)
class DocumentDomains:
    """What one document's extraction produced (picklable: crosses the pool)."""

    status: str
    candidates: tuple[EsefWebsiteCandidate, ...]
    corroborated_domains: frozenset[str]
    error_message: str
    seconds: float


def extract_package_domains(
    package_path: str,
    tagged_values: tuple[tuple[str, str], ...],
    known_email_domains: tuple[str, ...],
    work_root: str,
) -> tuple[tuple[EsefWebsiteCandidate, ...], frozenset[str]]:
    """Open the package, select its report members, extract. Runs in the child.

    `tagged_values` are (concept_local_name, raw_value) pairs read from
    corpscout.esef_facts, which does not record the report member a fact
    came from, so their evidence carries report_member ''.
    """
    work_dir = Path(tempfile.mkdtemp(prefix="esef-domains-", dir=work_root))
    try:
        report_members, _repaired = extract_report_package(Path(package_path), work_dir)
        candidates, corroborated = extract_website_candidates_with_corroboration(
            report_paths_for(work_dir, report_members),
            tagged_values=[
                TaggedWebsiteValue(
                    report_member="", concept_local_name=concept, value=value
                )
                for concept, value in tagged_values
            ],
            known_email_domains=known_email_domains,
        )
        return tuple(candidates), corroborated
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def extract_package_domains_bounded(
    package_path: str,
    tagged_values: tuple[tuple[str, str], ...],
    known_email_domains: tuple[str, ...],
    timeout_seconds: int,
    work_root: str,
) -> DocumentDomains:
    """Pool-worker entry: the extraction in a killable child with a budget.

    Never raises for a document's own problems -- a timeout, a bad package,
    an extraction error -- those become the document's status so the run
    continues; only the child machinery itself can raise.
    """
    started = perf_counter()
    try:
        candidates, corroborated = run_in_child_with_timeout(
            extract_package_domains,
            (package_path, tagged_values, known_email_domains, work_root),
            timeout_seconds=timeout_seconds,
        )
    except ChildTimeoutError as exc:
        return DocumentDomains(
            STATUS_TIMED_OUT, (), frozenset(), str(exc), perf_counter() - started
        )
    except ChildFailedError as exc:
        return DocumentDomains(
            STATUS_FAILED, (), frozenset(), exc.detail, perf_counter() - started
        )
    return DocumentDomains(
        STATUS_OK if candidates else STATUS_EMPTY,
        candidates,
        corroborated,
        "",
        perf_counter() - started,
    )


def domain_id(source_document_id: str, registrable_domain: str) -> str:
    return sha256(f"{source_document_id}\n{registrable_domain}".encode()).hexdigest()


def domain_rows(
    document: DomainsDocument,
    result: DocumentDomains,
    *,
    extractor_version: str,
    source_run_id: str,
    extracted_at: datetime,
) -> list[dict[str, object]]:
    """The esef_domains rows for one document, keyed in export-column order:
    one per registrable domain, or one marker row (registrable_domain '')
    when the document produced none or its extraction failed -- so the
    document is never selected as stale again at this extractor version.
    """

    def row(
        registrable_domain: str,
        hosts: list[str],
        normalized_urls: list[str],
        roles: list[str],
        evidence: list[dict[str, object]],
        corroborated: int,
    ) -> dict[str, object]:
        return {
            "domain_id": domain_id(document.source_document_id, registrable_domain),
            "source_document_id": document.source_document_id,
            "package_sha256": document.package_sha256,
            "lei": document.lei,
            "period_end": document.period_end,
            "fiscal_year": document.fiscal_year,
            "extraction_status": result.status,
            "registrable_domain": registrable_domain,
            "hosts_json": _json_text(hosts),
            "normalized_urls_json": _json_text(normalized_urls),
            "roles_json": _json_text(roles),
            "evidence_json": _json_text(evidence),
            "evidence_count": len(evidence),
            "corroborated": corroborated,
            "error_message": result.error_message,
            "extractor_version": extractor_version,
            "source_run_id": source_run_id,
            "extracted_at": extracted_at,
        }

    if not result.candidates:
        return [row("", [], [], [], [], 0)]
    return [
        row(
            candidate.registrable_domain,
            list(candidate.hosts),
            list(candidate.normalized_urls),
            list(candidate.suggested_roles),
            [asdict(item) for item in candidate.evidence],
            int(candidate.registrable_domain in result.corroborated_domains),
        )
        for candidate in result.candidates
    ]


def _json_text(value: object) -> str:
    # Same shape as segment_assets._json_text (the contact-candidates rows).
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
