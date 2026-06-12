"""Manifest helpers for Finland PRH YTJ assets."""


def artifact_by_key(manifest: dict, key: str) -> dict:
    for artifact in manifest.get("artifacts", []):
        if artifact.get("key") == key:
            return artifact
    raise KeyError(f"manifest artifact not found: {key}")
