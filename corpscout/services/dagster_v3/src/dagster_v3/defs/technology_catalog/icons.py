"""Icon sync into the dedicated technology-icons bucket.

Idempotent and cheap after the first run: every object is keyed by the
technology's stable slug plus the source file's extension, an existing object
with the same byte size is skipped (HEAD only), and overlay icons are fetched
from GitHub only for technologies whose winning layer is the overlay AND whose
icon filename is absent from the local extension bundle — the bundle already
carries almost every icon the overlay references.

This module only ever writes into the technology-icons bucket.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dagster_v3.defs.technology_catalog import tables
from dagster_v3.defs.technology_catalog.catalog import MergedTechnology

ICON_CONTENT_TYPES = {
    ".svg": "image/svg+xml",
    ".png": "image/png",
}


class IconBucketClient(Protocol):
    """The three S3 calls this sync needs, boto3-kwarg shaped."""

    def head_object(self, *, Bucket: str, Key: str) -> Mapping[str, Any]: ...

    def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> Any: ...


@dataclass(frozen=True)
class IconRef:
    object_key: str
    content_type: str


@dataclass(frozen=True)
class IconSyncResult:
    # technology name -> IconRef; technologies with no resolvable icon are
    # absent and publish with icon_object_key=''.
    refs: Mapping[str, IconRef]
    uploaded: int
    skipped: int
    missing: int
    overlay_fetches: int


def icon_ref(slug: str, icon_filename: str) -> IconRef:
    """Bucket key + content type for one icon file.

    The key keeps the SOURCE file's extension so the stored bytes and the
    declared content type always agree.
    """
    extension = Path(icon_filename).suffix.lower()
    content_type = ICON_CONTENT_TYPES.get(extension, "application/octet-stream")
    return IconRef(
        object_key=f"{tables.ICON_KEY_PREFIX}{slug}{extension}",
        content_type=content_type,
    )


def sync_icons(
    technologies: Sequence[MergedTechnology],
    *,
    bundle_icons_dir: Path,
    s3_client: IconBucketClient,
    bucket: str,
    fetch_overlay_icon: Callable[[str], bytes | None],
    extra_icons_dirs: Sequence[Path] = (),
    log: Callable[[str], None] = lambda message: None,
) -> IconSyncResult:
    refs: dict[str, IconRef] = {}
    uploaded = 0
    skipped = 0
    missing = 0
    overlay_fetches = 0
    # One fetch per distinct overlay filename even when several technologies
    # share an icon (None results cached too, so a dropped file costs one 404).
    overlay_cache: dict[str, bytes | None] = {}

    for technology in technologies:
        if not technology.icon_filename:
            missing += 1
            continue

        local_path = bundle_icons_dir / technology.icon_filename
        for extra_dir in extra_icons_dirs:
            if local_path.is_file():
                break
            local_path = extra_dir / technology.icon_filename
        body: bytes | None = None
        size: int | None = None
        if local_path.is_file():
            # Every layer prefers a local file: it avoids thousands of
            # GitHub fetches and icon artwork is effectively immutable.
            size = local_path.stat().st_size
        elif technology.source == tables.OVERLAY_SOURCE:
            if technology.icon_filename not in overlay_cache:
                overlay_cache[technology.icon_filename] = fetch_overlay_icon(
                    technology.icon_filename
                )
                overlay_fetches += 1
            body = overlay_cache[technology.icon_filename]
            if body is None:
                log(
                    f"{technology.technology}: icon {technology.icon_filename!r} "
                    "missing from the overlay tree"
                )
                missing += 1
                continue
            size = len(body)
        else:
            log(
                f"{technology.technology}: icon {technology.icon_filename!r} "
                "missing from the local icon directories"
            )
            missing += 1
            continue

        ref = icon_ref(technology.slug, technology.icon_filename)
        if _existing_object_size(s3_client, bucket, ref.object_key) == size:
            skipped += 1
        else:
            if body is None:
                body = local_path.read_bytes()
            s3_client.put_object(
                Bucket=bucket,
                Key=ref.object_key,
                Body=body,
                ContentType=ref.content_type,
            )
            uploaded += 1
        refs[technology.technology] = ref

    return IconSyncResult(
        refs=refs,
        uploaded=uploaded,
        skipped=skipped,
        missing=missing,
        overlay_fetches=overlay_fetches,
    )


def _existing_object_size(
    s3_client: IconBucketClient, bucket: str, key: str
) -> int | None:
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return int(head["ContentLength"])


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(error.get("Code", ""))
