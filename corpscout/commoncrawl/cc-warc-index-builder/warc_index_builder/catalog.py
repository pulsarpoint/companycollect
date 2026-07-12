"""Local catalog build paths and exclusive build lifecycle."""

import fcntl
import os
import shutil
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from ._identity import decode_sha256, new_identity_digest, update_text


CATALOG_SCHEMA_VERSION = 1


class CatalogBuildLocked(RuntimeError):
    pass


def warc_inventory_sha256(inventory: Sequence[tuple[int, str, int]]) -> str:
    """Hash the complete index-ordered WARC inventory and exact object sizes."""
    if not inventory:
        raise ValueError("WARC inventory must not be empty")
    digest = new_identity_digest("warc-inventory")
    digest.update(len(inventory).to_bytes(4, byteorder="big"))
    filenames: set[str] = set()
    for expected_index, (warc_index, warc_filename, object_bytes) in enumerate(inventory):
        if warc_index != expected_index:
            raise ValueError(
                "WARC inventory must be in contiguous warc_index order starting at 0"
            )
        if not warc_filename.strip():
            raise ValueError("WARC filenames must not be blank")
        if warc_filename in filenames:
            raise ValueError("WARC filenames must be unique")
        if not 1 <= object_bytes <= 0xFFFFFFFFFFFFFFFF:
            raise ValueError("WARC object sizes must be between 1 and uint64 max")
        filenames.add(warc_filename)
        digest.update(warc_index.to_bytes(4, byteorder="big"))
        update_text(digest, warc_filename)
        digest.update(object_bytes.to_bytes(8, byteorder="big"))
    return digest.hexdigest()


def catalog_id(
    *,
    schema_version: int,
    crawl_id: str,
    pages_per_domain: int,
    selection_policy_version: str,
    selection_policy_sha256: str,
    source_schema_sha256: str,
    warc_manifest_sha256: str,
    index_manifest_sha256: str,
    warc_inventory_sha256: str,
) -> str:
    """Hash every logical input that determines one published catalog."""
    if not 1 <= schema_version <= 0xFFFF:
        raise ValueError("schema_version must be between 1 and uint16 max")
    if not 1 <= pages_per_domain <= 0xFFFF:
        raise ValueError("pages_per_domain must be between 1 and uint16 max")
    if not crawl_id or not selection_policy_version:
        raise ValueError("catalog identity strings must not be empty")

    hashes = (
        ("selection_policy_sha256", selection_policy_sha256),
        ("source_schema_sha256", source_schema_sha256),
        ("warc_manifest_sha256", warc_manifest_sha256),
        ("index_manifest_sha256", index_manifest_sha256),
        ("warc_inventory_sha256", warc_inventory_sha256),
    )
    digest = new_identity_digest("catalog")
    digest.update(schema_version.to_bytes(2, byteorder="big"))
    update_text(digest, crawl_id)
    digest.update(pages_per_domain.to_bytes(2, byteorder="big"))
    update_text(digest, selection_policy_version)
    for name, value in hashes:
        digest.update(decode_sha256(value, name))
    return digest.hexdigest()


def require_path_within(base: Path, target: Path) -> None:
    resolved_base = base.resolve()
    resolved_target = target.resolve()
    try:
        resolved_target.relative_to(resolved_base)
    except ValueError as error:
        raise ValueError(f"path {resolved_target} escapes base {resolved_base}") from error


@contextmanager
def catalog_build_lock(catalog_directory: Path) -> Iterator[None]:
    catalog_directory.mkdir(parents=True, exist_ok=True)
    lock_path = catalog_directory / "build.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o644)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CatalogBuildLocked(f"another builder holds {lock_path}") from error
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def prepare_build_directory(base: Path, catalog_directory: Path, *, rebuild: bool) -> Path:
    require_path_within(base, catalog_directory)
    build_directory = catalog_directory / ".build"
    if build_directory.is_symlink():
        raise ValueError(f"build directory must not be a symlink: {build_directory}")
    require_path_within(base, build_directory)
    if rebuild and build_directory.exists():
        if not build_directory.is_dir():
            raise ValueError(f"build path is not a directory: {build_directory}")
        shutil.rmtree(build_directory)
    build_directory.mkdir(parents=True, exist_ok=True)
    return build_directory
