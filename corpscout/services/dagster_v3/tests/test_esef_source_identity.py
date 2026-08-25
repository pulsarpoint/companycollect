import pytest

from dagster_v3.defs.esef_filings.source_identity import file_source_record_uid


def test_esef_file_identity_deduplicates_packages_by_content() -> None:
    package_hash = "A" * 64

    assert file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_hash,
    ) == file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_hash.lower(),
    )


@pytest.mark.parametrize("value", ["", "abc", "z" * 64, "a" * 63])
def test_esef_file_identity_rejects_invalid_hashes(value: str) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        file_source_record_uid(
            record_kind="esef_report_package",
            content_sha256=value,
        )
