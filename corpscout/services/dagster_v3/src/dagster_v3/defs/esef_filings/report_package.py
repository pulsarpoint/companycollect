"""Opening an archived ESEF report package: the zip's safety limits and the
selection of its report XHTML members.

Split out of ``segment_parser.py`` (which keeps importing these names, so the
arelle parser's behaviour is unchanged) so that light per-document extractors
-- ``domains_extraction.py`` first -- can open a package without importing
arelle or the artifact parser: they run in spawned children and pay every
import per document.
"""

import shutil
import stat
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import zipfile_deflate64 as zipfile

MAX_PACKAGE_FILES = 10_000
MAX_PACKAGE_UNCOMPRESSED_BYTES = 1_500_000_000
MAX_PACKAGE_MEMBER_BYTES = 750_000_000
INLINE_XBRL_NAMESPACE_MARKERS = (
    b"http://www.xbrl.org/2008/inlineXBRL",
    b"http://www.xbrl.org/2013/inlineXBRL",
)


def contains_inline_xbrl_namespace(report_path: Path) -> bool:
    longest_marker = max(map(len, INLINE_XBRL_NAMESPACE_MARKERS))
    previous_tail = b""
    with report_path.open("rb") as report:
        while chunk := report.read(64 * 1024):
            searchable = previous_tail + chunk
            if any(marker in searchable for marker in INLINE_XBRL_NAMESPACE_MARKERS):
                return True
            previous_tail = searchable[-longest_marker:]
    return False


def extract_report_package(
    package_path: Path,
    temp_dir: Path,
) -> tuple[list[str], int]:
    with zipfile.ZipFile(package_path) as package:
        members = package.infolist()
        if len(members) > MAX_PACKAGE_FILES:
            raise ValueError(
                f"ESEF package contains too many files: {len(members)} > {MAX_PACKAGE_FILES}"
            )
        total_size = sum(member.file_size for member in members)
        if total_size > MAX_PACKAGE_UNCOMPRESSED_BYTES:
            raise ValueError(
                "ESEF package expands beyond the configured limit: "
                f"{total_size} > {MAX_PACKAGE_UNCOMPRESSED_BYTES}"
            )

        report_members: list[str] = []
        nonstandard_html_members: list[tuple[str, Path]] = []
        extracted_member_names: set[str] = set()
        repaired_member_count = 0
        for member in members:
            separator_normalized_name = member.filename.replace("\\", "/")
            member_path = PurePosixPath(separator_normalized_name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"ESEF package contains an unsafe path: {member.filename}"
                )
            if member.file_size > MAX_PACKAGE_MEMBER_BYTES:
                raise ValueError(
                    "ESEF package member expands beyond the configured limit: "
                    f"{member.filename}"
                )
            mode = member.external_attr >> 16
            if mode != 0 and stat.S_ISLNK(mode):
                raise ValueError(
                    f"ESEF package contains an unsupported symbolic link: {member.filename}"
                )
            if member.filename != separator_normalized_name:
                repaired_member_count += 1
            if member.is_dir() or separator_normalized_name.endswith("/"):
                continue

            normalized_name = member_path.as_posix()
            if normalized_name in extracted_member_names:
                raise ValueError(
                    "ESEF package contains duplicate paths after separator "
                    f"normalization: {normalized_name}"
                )
            extracted_member_names.add(normalized_name)
            target_path = temp_dir.joinpath(*member_path.parts)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with (
                package.open(member) as source_file,
                target_path.open("wb") as target_file,
            ):
                shutil.copyfileobj(source_file, target_file)
            lowered = f"/{normalized_name.lower()}"
            if "/reports/" in lowered and lowered.endswith((".xhtml", ".html")):
                report_members.append(normalized_name)

            elif lowered.endswith((".xhtml", ".html")):
                nonstandard_html_members.append((normalized_name, target_path))

    if report_members:
        return sorted(report_members), repaired_member_count

    report_members = [
        member_name
        for member_name, member_path in nonstandard_html_members
        if contains_inline_xbrl_namespace(member_path)
    ]

    if not report_members:
        raise ValueError("ESEF report package contains no report XHTML members")
    return sorted(report_members), repaired_member_count


def report_paths_for(
    temp_dir: Path,
    report_members: Iterable[str],
) -> dict[str, Path]:
    """The extracted path of each report member (segment_parser's mapping)."""
    return {
        member: temp_dir.joinpath(*PurePosixPath(member).parts)
        for member in report_members
    }
