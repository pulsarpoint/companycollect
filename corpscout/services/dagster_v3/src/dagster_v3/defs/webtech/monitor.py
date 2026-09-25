"""Remote scan monitoring shared by queue executions."""

import time
from collections.abc import Callable

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.client import (
    UnknownRemoteScanError,
    WebtechApiResource,
    WebtechApiUnavailableError,
)
from dagster_v3.defs.webtech.models import (
    FinalScanManifest,
    FinalScanReference,
    RemoteScanSnapshot,
    StoredResultReference,
    SubmittedScanReference,
)
from dagster_v3.defs.webtech.storage import (
    WebtechS3Destination,
    read_final_manifest,
)

WEBTECH_MONITOR_INTERVAL_SECONDS = 2
WEBTECH_STALL_TIMEOUT_SECONDS = 900.0
WEBTECH_STATUS_LOG_EVERY_POLLS = 30


def monitor_webtech_scan(
    context: dg.AssetExecutionContext,
    submission: SubmittedScanReference,
    webtech_api: WebtechApiResource,
    webtech_object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    poll_interval_seconds: int = WEBTECH_MONITOR_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    stall_timeout_seconds: float = WEBTECH_STALL_TIMEOUT_SECONDS,
    on_results: Callable[[list[StoredResultReference]], None] | None = None,
    on_poll: Callable[[], None] | None = None,
) -> RemoteScanSnapshot:
    """Poll with short requests until one submitted remote scan is terminal.

    A running scan that reports no progress for ``stall_timeout_seconds`` is
    cancelled remotely and the step fails, so a run retry resubmits it and the
    scanner resumes from the results already stored in RustFS.
    """
    latest_event_sequence = 0
    last_logged_state: tuple[str, int, int] | None = None
    polls_since_log = 0
    while True:
        try:
            response = webtech_api.poll(
                submission.scan_id,
                after_event=latest_event_sequence,
                wait_seconds=0,
            )
            for event in response.events:
                if on_results is not None and event.results:
                    on_results(event.results)
                latest_event_sequence = max(latest_event_sequence, event.sequence)
            snapshot = response.scan
        except UnknownRemoteScanError:
            snapshot = webtech_api.submit(submission.manifest)
            latest_event_sequence = 0  # a restarted scanner numbers events from 1 again
            context.log.warning(
                "Webtech scanner lost in-memory state; resubmitted scan_id=%s "
                "partition=%s status=%s completed=%s/%s",
                snapshot.scan_id,
                snapshot.partition_key,
                snapshot.status,
                snapshot.completed_count,
                snapshot.total_count,
            )
        except WebtechApiUnavailableError as error:
            context.log.warning(
                "Webtech scanner status unavailable; retrying in %ss: "
                "scan_id=%s partition=%s error=%s",
                poll_interval_seconds,
                submission.scan_id,
                submission.manifest.partition_key,
                error,
            )
            if on_poll is not None:
                on_poll()
            sleep(poll_interval_seconds)
            continue

        _validate_monitored_snapshot(submission, snapshot)
        state = (
            snapshot.status,
            snapshot.completed_count,
            snapshot.latest_event_sequence,
        )
        polls_since_log += 1
        if (
            state != last_logged_state
            or polls_since_log >= WEBTECH_STATUS_LOG_EVERY_POLLS
        ):
            context.log.info(
                "Webtech scan status: scan_id=%s partition=%s status=%s "
                "completed=%s/%s outcomes=%s technologies=%s "
                "progress_age_seconds=%.1f elapsed_seconds=%.1f "
                "rate_per_minute=%.2f",
                snapshot.scan_id,
                snapshot.partition_key,
                snapshot.status,
                snapshot.completed_count,
                snapshot.total_count,
                snapshot.outcome_counts,
                snapshot.technology_count,
                snapshot.progress_age_seconds,
                snapshot.elapsed_seconds,
                snapshot.domains_per_minute,
            )
            last_logged_state = state
            polls_since_log = 0
        if (
            snapshot.status == "running"
            and snapshot.progress_age_seconds >= stall_timeout_seconds
        ):
            _abandon_stalled_scan(
                context=context,
                webtech_api=webtech_api,
                snapshot=snapshot,
                stall_timeout_seconds=stall_timeout_seconds,
            )
        if snapshot.status == "completed":
            reference = _final_reference(snapshot)
            final_manifest = read_final_manifest(
                object_store=webtech_object_store,
                destination=destination,
                reference=reference,
            )
            _validate_s3_manifest(submission, reference, final_manifest)
            context.log.info(
                "Webtech scan complete and RustFS manifest verified: "
                "scan_id=%s partition=%s uri=%s",
                snapshot.scan_id,
                snapshot.partition_key,
                snapshot.final_manifest_uri,
            )
            return snapshot
        if snapshot.status in {"failed", "cancelled"}:
            raise RuntimeError(
                f"Remote Webtech scan {snapshot.scan_id} ended with "
                f"status={snapshot.status}: {snapshot.error_message}"
            )
        if on_poll is not None:
            on_poll()
        sleep(poll_interval_seconds)


def _abandon_stalled_scan(
    *,
    context: dg.AssetExecutionContext,
    webtech_api: WebtechApiResource,
    snapshot: RemoteScanSnapshot,
    stall_timeout_seconds: float,
) -> None:
    """Cancel a remote scan that stopped progressing, then fail this step."""
    context.log.warning(
        "Webtech scan stalled; cancelling remote scan so a retry can resume it: "
        "scan_id=%s partition=%s completed=%s/%s progress_age_seconds=%.1f "
        "limit_seconds=%.0f",
        snapshot.scan_id,
        snapshot.partition_key,
        snapshot.completed_count,
        snapshot.total_count,
        snapshot.progress_age_seconds,
        stall_timeout_seconds,
    )
    try:
        webtech_api.cancel(snapshot.scan_id)
    except (UnknownRemoteScanError, WebtechApiUnavailableError, RuntimeError) as error:
        context.log.warning(
            "Webtech scan cancel request failed; failing the step anyway: "
            "scan_id=%s error=%s",
            snapshot.scan_id,
            error,
        )
    raise RuntimeError(
        f"Remote Webtech scan {snapshot.scan_id} stalled: no progress for "
        f"{snapshot.progress_age_seconds:.0f}s "
        f"(completed {snapshot.completed_count}/{snapshot.total_count}); "
        "the remote scan was cancelled so a retry can resume from RustFS"
    )


def _validate_monitored_snapshot(
    submission: SubmittedScanReference,
    snapshot: RemoteScanSnapshot,
) -> None:
    if (
        snapshot.scan_id != submission.scan_id
        or snapshot.crawl_id != submission.manifest.crawl_id
        or snapshot.partition_key != submission.manifest.partition_key
        or snapshot.detector_version != submission.manifest.detector_version
        or snapshot.candidate_manifest_uri != submission.manifest.uri
    ):
        raise RuntimeError("Remote Webtech snapshot does not match its submission")


def _final_reference(snapshot: RemoteScanSnapshot) -> FinalScanReference:
    return FinalScanReference(
        scan_id=snapshot.scan_id,
        crawl_id=snapshot.crawl_id,
        partition_key=snapshot.partition_key,
        detector_version=snapshot.detector_version,
        uri=snapshot.final_manifest_uri,
        total_count=snapshot.total_count,
        outcome_counts=snapshot.outcome_counts,
        technology_count=snapshot.technology_count,
        elapsed_seconds=snapshot.elapsed_seconds,
        domains_per_minute=snapshot.domains_per_minute,
    )


def _validate_s3_manifest(
    submission: SubmittedScanReference,
    reference: FinalScanReference,
    manifest: FinalScanManifest,
) -> None:
    if (
        manifest.candidate_manifest_uri != submission.manifest.uri
        or manifest.candidate_manifest_sha256 != submission.manifest.sha256
        or manifest.outcome_counts != reference.outcome_counts
        or manifest.technology_count != reference.technology_count
    ):
        raise RuntimeError(
            "RustFS Webtech final manifest does not match the monitored submission"
        )
