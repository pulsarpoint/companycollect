from dagster_corpscout.lib.manifest import Artifact, build_manifest


def test_build_manifest_shape():
    manifest = build_manifest(
        run_id="20260611T120000Z",
        source="finland_prhytj",
        workflow_id="dagster-run-abc123",
        artifacts=[
            Artifact(
                key="source",
                object_key="runs/20260611T120000Z/source.ndjson",
                content_sha256="deadbeef",
                content_length_bytes=42,
                records_written=2,
            )
        ],
    )
    assert manifest == {
        "run_id": "20260611T120000Z",
        "source": "finland_prhytj",
        "workflow_id": "dagster-run-abc123",
        "artifacts": [
            {
                "key": "source",
                "object_key": "runs/20260611T120000Z/source.ndjson",
                "content_sha256": "deadbeef",
                "content_length_bytes": 42,
                "records_written": 2,
            }
        ],
    }
