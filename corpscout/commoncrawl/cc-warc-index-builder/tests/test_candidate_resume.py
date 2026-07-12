from pathlib import Path

import duckdb
import pytest

from warc_index_builder import catalog
from warc_index_builder.manifests import IndexSource


def _write_source(path: Path, *, include_url_path: bool = True) -> None:
    connection = duckdb.connect()
    try:
        url_path = "url_path VARCHAR," if include_url_path else ""
        connection.execute(
            f"""
            CREATE TABLE source_rows (
                url_host_registered_domain VARCHAR,
                url_host_name VARCHAR,
                url VARCHAR,
                {url_path}
                fetch_status BIGINT,
                content_mime_type VARCHAR,
                warc_filename VARCHAR,
                warc_record_offset BIGINT,
                warc_record_length BIGINT
            )
            """
        )
        columns = (
            "url_host_registered_domain, url_host_name, url, "
            + ("url_path, " if include_url_path else "")
            + "fetch_status, content_mime_type, warc_filename, "
            "warc_record_offset, warc_record_length"
        )
        values = (
            "'example.com', 'example.com', 'https://example.com/', "
            + ("'/', " if include_url_path else "")
            + "200, 'text/html', 'one.warc.gz', 10, 100"
        )
        connection.execute(f"INSERT INTO source_rows ({columns}) VALUES ({values})")
        connection.execute("COPY source_rows TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def test_candidate_with_wrong_source_identity_is_rebuilt(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    output_path = tmp_path / "candidate.parquet"
    _write_source(source_path)
    connection = catalog.open_duckdb(
        None, tmp_path / "temp", threads=1, memory_limit=None
    )
    try:
        first = catalog.build_candidate(
            connection,
            IndexSource(0, str(source_path), str(source_path)),
            output_path,
            pages_per_domain=1,
            attempts=1,
        )
        second = catalog.build_candidate(
            connection,
            IndexSource(1, str(source_path), str(source_path)),
            output_path,
            pages_per_domain=1,
            attempts=1,
        )
        source_indexes = connection.execute(
            "SELECT DISTINCT source_index FROM read_parquet(?)", [str(output_path)]
        ).fetchall()
    finally:
        connection.close()

    assert first.reused is False
    assert second.reused is False
    assert source_indexes == [(1,)]


def test_transient_schema_probe_is_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "source.parquet"
    _write_source(source_path)
    actual = catalog.open_duckdb(None, tmp_path / "temp", threads=1, memory_limit=None)

    class FlakyConnection:
        def __init__(self) -> None:
            self.schema_attempts = 0

        def execute(self, query: str, parameters: object = None):
            if query.startswith("DESCRIBE SELECT * FROM"):
                self.schema_attempts += 1
                if self.schema_attempts == 1:
                    raise duckdb.IOException("simulated HTTP 503")
            return (
                actual.execute(query, parameters)
                if parameters
                else actual.execute(query)
            )

    flaky = FlakyConnection()
    monkeypatch.setattr(catalog.time, "sleep", lambda _seconds: None)
    try:
        result = catalog.build_candidate(
            flaky,  # type: ignore[arg-type]
            IndexSource(0, str(source_path), str(source_path)),
            tmp_path / "candidate.parquet",
            pages_per_domain=1,
            attempts=2,
        )
    finally:
        actual.close()

    assert flaky.schema_attempts >= 2
    assert result.attempts == 2


def test_missing_required_source_column_does_not_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "invalid.parquet"
    _write_source(source_path, include_url_path=False)
    connection = catalog.open_duckdb(
        None, tmp_path / "temp", threads=1, memory_limit=None
    )
    monkeypatch.setattr(
        catalog.time,
        "sleep",
        lambda _seconds: pytest.fail("permanent schema error must not back off"),
    )
    try:
        with pytest.raises(ValueError, match="missing columns: url_path"):
            catalog.build_candidate(
                connection,
                IndexSource(0, str(source_path), str(source_path)),
                tmp_path / "candidate.parquet",
                pages_per_domain=1,
                attempts=5,
            )
    finally:
        connection.close()
