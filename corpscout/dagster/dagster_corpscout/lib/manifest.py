"""Run manifest: the durable artifact ledger written to every run prefix."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Artifact:
    key: str
    object_key: str
    content_sha256: str
    content_length_bytes: int
    records_written: int


def build_manifest(
    run_id: str,
    source: str,
    workflow_id: str,
    artifacts: list[Artifact],
) -> dict:
    return {
        "run_id": run_id,
        "source": source,
        "workflow_id": workflow_id,
        "artifacts": [asdict(artifact) for artifact in artifacts],
    }
