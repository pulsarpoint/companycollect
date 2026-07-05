import pytest


def test_cleanup_previous_partition_removes_previous_month_folders(tmp_path) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    data_root = tmp_path / "brazil_rfb"
    download_root = tmp_path / "brazil_rfb_downloads"
    data_file = data_root / "2026-05" / "companies.duckdb"
    download_file = download_root / "2026-05" / "Empresas.zip"
    current_file = data_root / "2026-06" / "manifest.duckdb"

    data_file.parent.mkdir(parents=True)
    download_file.parent.mkdir(parents=True)
    current_file.parent.mkdir(parents=True)
    data_file.write_bytes(b"abc")
    download_file.write_bytes(b"abcdef")
    current_file.write_bytes(b"current")

    result = cleanup.cleanup_previous_partition_files(
        partition_key="2026-06-01",
        data_root=data_root,
        download_root=download_root,
    )

    assert result.target_partition == "2026-06"
    assert result.removed_partition == "2026-05"
    assert result.removed_file_count == 2
    assert result.removed_bytes == 9
    assert result.removed_paths == (
        str((data_root / "2026-05").resolve()),
        str((download_root / "2026-05").resolve()),
    )
    assert result.missing_paths == ()
    assert not data_file.exists()
    assert not download_file.exists()
    assert current_file.exists()


def test_cleanup_previous_partition_removes_month_before_first_partition(
    tmp_path,
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    data_root = tmp_path / "brazil_rfb"
    old_file = data_root / "2026-03" / "manifest.duckdb"
    old_file.parent.mkdir(parents=True)
    old_file.write_text("old")

    result = cleanup.cleanup_previous_partition_files(
        partition_key="2026-04-01",
        data_root=data_root,
        download_root=tmp_path / "brazil_rfb_downloads",
    )

    assert result.target_partition == "2026-04"
    assert result.removed_partition == "2026-03"
    assert result.removed_paths == (str((data_root / "2026-03").resolve()),)
    assert result.missing_paths == (
        str((tmp_path / "brazil_rfb_downloads" / "2026-03").resolve()),
    )
    assert result.removed_file_count == 1
    assert result.removed_bytes == 3
    assert not old_file.exists()


def test_cleanup_previous_partition_reports_missing_folders(tmp_path) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    data_root = tmp_path / "brazil_rfb"
    download_root = tmp_path / "brazil_rfb_downloads"

    result = cleanup.cleanup_previous_partition_files(
        partition_key="2026-06-01",
        data_root=data_root,
        download_root=download_root,
    )

    assert result.removed_partition == "2026-05"
    assert result.removed_paths == ()
    assert result.missing_paths == (
        str((data_root / "2026-05").resolve()),
        str((download_root / "2026-05").resolve()),
    )
    assert result.removed_file_count == 0
    assert result.removed_bytes == 0


def test_cleanup_rejects_target_outside_root(tmp_path) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    with pytest.raises(ValueError, match="outside Brazil RFB cleanup root"):
        cleanup.assert_safe_cleanup_target(
            root=tmp_path / "brazil_rfb",
            target=tmp_path / "brazil_rfb_downloads" / "2026-05",
        )


def test_cleanup_rejects_root_as_target(tmp_path) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    root = tmp_path / "brazil_rfb"

    with pytest.raises(ValueError, match="refusing to remove cleanup root"):
        cleanup.assert_safe_cleanup_target(root=root, target=root)


@pytest.mark.parametrize(
    ("partition_key", "previous_partition"),
    [
        ("2026-06-01", "2026-05"),
        ("2026-01-01", "2025-12"),
    ],
)
def test_previous_snapshot_year_month(
    partition_key: str, previous_partition: str
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    assert cleanup.previous_snapshot_year_month(partition_key) == previous_partition


def test_previous_snapshot_year_month_has_no_first_partition_cutoff() -> None:
    from dagster_v3.defs.brazil_companies.rfb import cleanup

    assert cleanup.previous_snapshot_year_month("2026-04-01") == "2026-03"
