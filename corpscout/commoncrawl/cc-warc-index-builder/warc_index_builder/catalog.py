"""Local catalog build paths and exclusive build lifecycle."""

import fcntl
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class CatalogBuildLocked(RuntimeError):
    pass


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
