from pathlib import Path

import dagster as dg


class ObjectStoreResource(dg.ConfigurableResource):
    """Run-scoped local object storage for the isolated Denmark CVR project."""

    root_path: str = "data/object_store"

    def ensure_bucket(self, bucket: str | None = None) -> None:
        self._object_path("", bucket=bucket).mkdir(parents=True, exist_ok=True)

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        target = self._object_path(key, bucket=bucket)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.write_bytes(key, body.encode("utf-8"), bucket=bucket)

    def _object_path(self, key: str, *, bucket: str | None) -> Path:
        target_bucket = bucket or "objects"
        relative_path = Path(key)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("Object key must be a safe relative path")
        return Path(self.root_path) / target_bucket / relative_path
