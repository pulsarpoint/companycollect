from dataclasses import dataclass
from datetime import date
from pathlib import Path
import shutil

from dagster_v3.defs.brazil_companies.rfb import source


@dataclass(frozen=True)
class BrazilCompRfbCleanupResult:
    target_partition: str
    removed_partition: str | None
    removed_paths: tuple[str, ...]
    missing_paths: tuple[str, ...]
    removed_file_count: int
    removed_bytes: int


def previous_snapshot_year_month(
    partition_key: str,
) -> str:
    current_partition = _month_start(partition_key)
    previous_partition = _previous_month(current_partition)
    return previous_partition.strftime("%Y-%m")


def cleanup_previous_partition_files(
    *,
    partition_key: str,
    data_root: Path,
    download_root: Path,
) -> BrazilCompRfbCleanupResult:
    target_partition = source.validate_snapshot_year_month(partition_key[:7])
    removed_partition = previous_snapshot_year_month(partition_key)

    removed_paths = []
    missing_paths = []
    removed_file_count = 0
    removed_bytes = 0
    for root, target in (
        (data_root, data_root / removed_partition),
        (download_root, download_root / removed_partition),
    ):
        safe_target = assert_safe_cleanup_target(root=root, target=target)
        if not safe_target.exists():
            missing_paths.append(str(safe_target))
            continue

        path_file_count, path_bytes = _path_stats(safe_target)
        if safe_target.is_dir():
            shutil.rmtree(safe_target)
        else:
            safe_target.unlink()

        removed_paths.append(str(safe_target))
        removed_file_count += path_file_count
        removed_bytes += path_bytes

    return BrazilCompRfbCleanupResult(
        target_partition=target_partition,
        removed_partition=removed_partition,
        removed_paths=tuple(removed_paths),
        missing_paths=tuple(missing_paths),
        removed_file_count=removed_file_count,
        removed_bytes=removed_bytes,
    )


def assert_safe_cleanup_target(*, root: Path, target: Path) -> Path:
    resolved_root = root.resolve()
    resolved_target = target.resolve()
    if resolved_target == resolved_root:
        raise ValueError(f"refusing to remove cleanup root: {resolved_root}")
    if resolved_root not in resolved_target.parents:
        raise ValueError(
            "refusing to remove path outside Brazil RFB cleanup root: "
            f"root={resolved_root} target={resolved_target}"
        )
    return resolved_target


def _month_start(partition_key: str) -> date:
    year_month = source.validate_snapshot_year_month(partition_key[:7])
    year, month = year_month.split("-")
    return date(int(year), int(month), 1)


def _previous_month(month_start: date) -> date:
    if month_start.month == 1:
        return date(month_start.year - 1, 12, 1)
    return date(month_start.year, month_start.month - 1, 1)


def _path_stats(path: Path) -> tuple[int, int]:
    if path.is_file():
        return 1, path.stat().st_size

    file_count = 0
    total_bytes = 0
    for child in path.rglob("*"):
        if child.is_file():
            file_count += 1
            total_bytes += child.stat().st_size
    return file_count, total_bytes
