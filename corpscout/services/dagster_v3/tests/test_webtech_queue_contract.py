"""Queue contract pieces that need no database: models, identities, buffering."""

from datetime import UTC, datetime

import pytest

from dagster_v3.defs.webtech.models import (
    RemoteScanProgressEvent,
    StoredDomainResultDocument,
    StoredResultReference,
    WEBTECH_DETECTOR_VERSION,
)
from dagster_v3.defs.webtech.storage import _unique_references, _validate_execution_result_identity

EVENT = {
    "sequence": 1,
    "completed_count": 1,
    "total_count": 2,
    "window_count": 1,
    "window_outcome_counts": {"success": 1},
    "window_technology_count": 0,
    "elapsed_seconds": 1.0,
    "domains_per_minute": 60.0,
}
REFERENCE = {
    "root_domain": "novelic.com",
    "harmonic_rank": 0,
    "input_id": "a" * 64,
    "outcome": "success",
    "timeout_stage": None,
    "technology_count": 0,
    "duration_ms": 10,
    "object_key": "webtech/scans/x/pages/input_id=" + "a" * 64 + "/report.json",
    "sha256": "0" * 64,
    "size_bytes": 1,
}


def test_progress_event_accepts_result_references():
    event = RemoteScanProgressEvent.model_validate({**EVENT, "results": [REFERENCE]})
    assert [item.input_id for item in event.results] == ["a" * 64]


def test_progress_event_without_results_still_parses():
    assert RemoteScanProgressEvent.model_validate(EVENT).results == []


def document(scan_id="first-scan", crawl_id="webtech-exec"):
    return StoredDomainResultDocument(
        schema_version=1, scan_id=scan_id, crawl_id=crawl_id, partition_key="envelope-a",
        detector_version=WEBTECH_DETECTOR_VERSION,
        candidate={"root_domain": "novelic.com", "harmonic_rank": 0, "task_id": "t",
                   "input_id": "a" * 64, "page_url": "https://novelic.com/"},
        outcome="success", requested_url="https://novelic.com", final_url="https://novelic.com/",
        final_hostname="novelic.com", http_fallback_used=False, scanned_at=datetime.now(UTC),
        duration_ms=10, error_message="", timeout_stage=None, report=None,
    )


def test_page_from_an_earlier_envelope_of_the_execution_is_accepted():
    _validate_execution_result_identity(
        document(scan_id="an-earlier-scan"),
        reference=StoredResultReference.model_validate(REFERENCE),
        crawl_id="webtech-exec", detector_version=WEBTECH_DETECTOR_VERSION,
    )


@pytest.mark.parametrize("changed", [{"crawl_id": "webtech-other"}, {"input_id": "b" * 64}])
def test_page_from_another_execution_or_input_is_rejected(changed):
    reference = StoredResultReference.model_validate({**REFERENCE, **({"input_id": changed["input_id"]} if "input_id" in changed else {})})
    with pytest.raises(ValueError, match="identity mismatch"):
        _validate_execution_result_identity(
            document(crawl_id=changed.get("crawl_id", "webtech-exec")),
            reference=reference, crawl_id="webtech-exec", detector_version=WEBTECH_DETECTOR_VERSION,
        )


def test_identical_duplicates_collapse():
    ref = StoredResultReference.model_validate(REFERENCE)
    refs = [ref, ref, ref]
    unique = _unique_references(refs)
    assert len(unique) == 1
    assert unique[0] == ref


def test_conflicting_duplicates_raise():
    ref1 = StoredResultReference.model_validate(REFERENCE)
    ref2 = StoredResultReference.model_validate({**REFERENCE, "object_key": "different/key.json"})
    with pytest.raises(ValueError, match="conflicting result references"):
        _unique_references([ref1, ref2])
