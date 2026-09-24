"""Queue contract pieces that need no database: models, identities, buffering."""

from dagster_v3.defs.webtech.models import RemoteScanProgressEvent

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
