import json
from pathlib import Path

import pytest

import warc_index_builder.__main__ as command
from warc_index_builder.catalog import CatalogResult
from warc_index_builder.publication import (
    CatalogDestination,
    CatalogPublication,
    PublishedObject,
)


def _destination() -> CatalogDestination:
    return CatalogDestination(
        bucket="crawls",
        prefix="commoncrawl/catalogs",
        endpoint="http://rustfs:9000",
        region="us-east-1",
        access_key="access",
        secret_key="secret",
    )


def _publication(crawl: str, selection: str) -> CatalogPublication:
    prefix = f"commoncrawl/catalogs/{crawl}/{selection}"
    return CatalogPublication(
        bucket="crawls",
        ready_key=f"{prefix}/ready.json",
        catalog=PublishedObject(f"{prefix}/catalog.duckdb", 12, "a" * 64),
    )


def test_cli_defaults_and_python_314_runtime(tmp_path: Path) -> None:
    options = command.parse_options(
        ["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]
    )

    assert options.base == tmp_path.resolve()
    assert options.pages_per_domain == 25
    assert options.attempts == 5
    assert options.rebuild_catalog is False
    assert options.cleanup_candidates is False


@pytest.mark.parametrize(
    "arguments",
    [
        ["--crawl", "CC-MAIN-2026-25", "--pages-per-domain", "0"],
        ["--crawl", "CC-MAIN-2026-25", "--pages-per-domain", "65536"],
        ["--crawl", "CC-MAIN-2026-25", "--attempts", "no"],
    ],
)
def test_cli_rejects_invalid_positive_options(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        command.parse_options(arguments)


def test_missing_rustfs_catalog_configuration_fails_before_catalog_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = command.parse_options(
        ["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]
    )
    monkeypatch.delenv("COMMONCRAWL_CATALOG_S3_BASE", raising=False)
    monkeypatch.setattr(
        command,
        "read_catalog",
        lambda *_args, **_kwargs: pytest.fail(
            "catalog must not be read before publication configuration validates"
        ),
    )

    with pytest.raises(ValueError, match="COMMONCRAWL_CATALOG_S3_BASE is required"):
        command.run(options)


def test_existing_catalog_returns_without_network_or_candidate_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    options = command.parse_options(
        ["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]
    )
    catalog_path = (
        tmp_path / "CC-MAIN-2026-25" / "warc-index" / "pages25" / "catalog.duckdb"
    )
    monkeypatch.setattr(
        command,
        "read_catalog",
        lambda path, **_identity: CatalogResult(
            path, 100_000, 80_000, 1_000_000, 20_000_000, 1e9, True
        ),
    )
    published: list[Path] = []
    monkeypatch.setattr(
        command,
        "destination_from_environment",
        lambda _environment: _destination(),
    )

    def publish_catalog(
        _destination: CatalogDestination,
        *,
        crawl: str,
        selection: str,
        catalog_path: Path,
    ) -> CatalogPublication:
        published.append(catalog_path)
        return _publication(crawl, selection)

    monkeypatch.setattr(
        command,
        "publish_catalog",
        publish_catalog,
    )

    class ForbiddenClient:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("network must not be opened for a reusable catalog")

    monkeypatch.setattr(command.httpx, "Client", ForbiddenClient)

    assert command.run(options) == 0
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["msg"] for event in events] == [
        "WARC index build started",
        "catalog ready",
        "RustFS catalog ready",
    ]
    assert events[-2]["catalog"] == str(catalog_path)
    assert events[-2]["reused"] is True
    assert events[-1]["ready_key"].endswith("/CC-MAIN-2026-25/pages25/ready.json")
    assert events[-1]["catalog_key"].endswith(
        "/CC-MAIN-2026-25/pages25/catalog.duckdb"
    )
    assert events[-1]["catalog_bytes"] == 12
    assert events[-1]["catalog_size"] == command.binary_size(12)
    assert events[-1]["catalog_sha256"] == "a" * 64
    assert published == [catalog_path]


def test_invalid_existing_catalog_requires_explicit_rebuild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = command.parse_options(
        ["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]
    )
    catalog_path = (
        tmp_path / "CC-MAIN-2026-25" / "warc-index" / "pages25" / "catalog.duckdb"
    )
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_bytes(b"not a DuckDB catalog")

    monkeypatch.setattr(
        command,
        "destination_from_environment",
        lambda _environment: _destination(),
    )

    monkeypatch.setattr(
        command.httpx,
        "Client",
        lambda *_args, **_kwargs: pytest.fail(
            "invalid catalog must stop before network"
        ),
    )

    with pytest.raises(RuntimeError, match="use --rebuild-catalog"):
        command.run(options)
    assert catalog_path.read_bytes() == b"not a DuckDB catalog"


def test_main_logs_one_boundary_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        command,
        "run",
        lambda _options: (_ for _ in ()).throw(RuntimeError("simulated failure")),
    )

    assert command.main(["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]) == 1
    event = json.loads(capsys.readouterr().err)
    assert event["msg"] == "WARC index build failed"
    assert event["error_type"] == "RuntimeError"
    assert event["error"] == "simulated failure"
