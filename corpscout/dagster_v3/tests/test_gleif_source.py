from datetime import UTC, datetime

from dagster_v3.defs.gleif import source


class _FakeResponse:
    def __init__(
        self,
        *,
        headers: dict[str, str],
        chunks: tuple[bytes, ...] = (b"alpha",),
        history: tuple["_FakeResponse", ...] = (),
    ) -> None:
        self.headers = headers
        self.history = list(history)
        self._chunks = chunks

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        assert chunk_size > 0
        yield from self._chunks


class _FakeSession:
    def __init__(
        self,
        response: _FakeResponse,
        expected_source_url: str = "https://example.test/latest.csv",
    ) -> None:
        self.response = response
        self.expected_source_url = expected_source_url

    def get(self, source_url: str, *, timeout: int, stream: bool) -> _FakeResponse:
        assert source_url == self.expected_source_url
        assert timeout == 30
        assert stream is True
        return self.response


class _FakeS3Client:
    def __init__(self) -> None:
        self.body_was_stream = False
        self.uploaded_body = b""
        self.uploaded_key = ""

    def put_object(self, *, Bucket: str, Key: str, Body) -> None:
        assert Bucket == source.GLEIF_RAW_BUCKET
        self.uploaded_key = Key
        self.body_was_stream = hasattr(Body, "read") and not isinstance(Body, bytes | str)
        self.uploaded_body = Body.read() if hasattr(Body, "read") else Body


class _FakeObjectStore:
    def __init__(self, s3_client: _FakeS3Client) -> None:
        self._s3_client = s3_client

    def client(self) -> _FakeS3Client:
        return self._s3_client


class _FakeManifestObjectStore:
    def __init__(self, objects: dict[str, dict]) -> None:
        self.objects = objects

    def list_keys(self, prefix: str, *, bucket: str) -> list[str]:
        assert bucket == source.GLEIF_RAW_BUCKET
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        assert bucket == source.GLEIF_RAW_BUCKET
        return source.json.dumps(self.objects[key]).encode()


def test_raw_download_config_defaults_to_csv() -> None:
    assert source.GleifRawDownloadConfig().file_format == "csv"


def test_golden_copy_url_supports_full_and_delta() -> None:
    assert source.golden_copy_url(file_kind="lei2", file_format="csv", delta=None) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"
    )
    assert source.golden_copy_url(
        file_kind="repex",
        file_format="csv",
        delta="LastDay",
    ) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/"
        "repex/latest.csv?delta=LastDay"
    )


def test_raw_object_key_includes_load_mode_publish_date_and_run_id() -> None:
    key = source.raw_file_object_key(
        load_mode="delta",
        publish_date="2026-06-21T16:00:00+00:00",
        run_id="run-1",
        file_kind="lei_records",
        delta="LastDay",
        extension="csv.zip",
    )

    assert key == (
        "gleif/raw/load_mode=delta/delta=LastDay/"
        "publish_date=2026-06-21T16-00-00Z/run_id=run-1/"
        "file_kind=lei_records/source.csv.zip"
    )


def test_manifest_object_key_matches_snapshot_prefix() -> None:
    key = source.manifest_object_key(
        load_mode="full",
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
        delta=None,
    )

    assert key == (
        "gleif/raw/load_mode=full/"
        "publish_date=2026-06-20T16-00-00Z/run_id=run-1/manifest.json"
    )


def test_state_object_key_is_stable() -> None:
    assert source.GLEIF_STATE_OBJECT_KEY == "gleif/state/current.json"


def test_manifest_payload_has_operational_metadata_only() -> None:
    manifest = source.build_manifest(
        load_mode="full",
        delta=None,
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
        pulled_at=datetime(2026, 6, 20, 17, 0, tzinfo=UTC),
        files=[
            source.DownloadedFile(
                file_kind="lei_records",
                file_format="csv",
                source_url=(
                    "https://goldencopy.gleif.org/api/v2/"
                    "golden-copies/publishes/lei2/latest.csv"
                ),
                s3_key=(
                    "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
                    "run_id=run-1/file_kind=lei_records/source.csv.zip"
                ),
                size_bytes=123,
                sha256="a" * 64,
                etag='"etag"',
                last_modified="Sat, 20 Jun 2026 17:29:16 GMT",
            )
        ],
    )

    assert manifest["source"] == "gleif"
    assert manifest["load_mode"] == "full"
    assert manifest["delta"] is None
    assert manifest["files"][0]["file_kind"] == "lei_records"
    assert manifest["files"][0]["file_format"] == "csv"
    assert "raw_file_manifest" not in manifest


def test_manifest_for_run_prefers_current_run_over_existing_newer_snapshot() -> None:
    object_store = _FakeManifestObjectStore(
        {
            (
                "gleif/raw/load_mode=delta/delta=LastDay/"
                "publish_date=2026-06-21T16-00-00Z/run_id=run-current/manifest.json"
            ): {
                "load_mode": "delta",
                "publish_date": "2026-06-21T16:00:00+00:00",
                "run_id": "run-current",
            },
            (
                "gleif/raw/load_mode=delta/delta=LastDay/"
                "publish_date=2026-06-22T16-00-00Z/run_id=run-other/manifest.json"
            ): {
                "load_mode": "delta",
                "publish_date": "2026-06-22T16:00:00+00:00",
                "run_id": "run-other",
            },
        }
    )

    assert source.manifest_for_run(object_store, "run-current")["run_id"] == "run-current"


def test_delta_requires_existing_current_state() -> None:
    try:
        source.ensure_bootstrap_state_for_delta(None)
    except ValueError as exc:
        assert "full bootstrap" in str(exc)
    else:
        raise AssertionError("delta should fail before full bootstrap state exists")


def test_delta_accepts_existing_full_state() -> None:
    source.ensure_bootstrap_state_for_delta(
        {
            "last_full_publish_date": "2026-06-20T16:00:00+00:00",
            "last_successful_run_id": "run-full",
        }
    )


def test_retention_keeps_manifests_and_newest_raw_snapshot() -> None:
    keys = [
        (
            "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/"
            "run_id=old/file_kind=lei_records/source.csv.zip"
        ),
        (
            "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/"
            "run_id=old/manifest.json"
        ),
        (
            "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
            "run_id=new/file_kind=lei_records/source.csv.zip"
        ),
        (
            "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
            "run_id=new/manifest.json"
        ),
    ]

    assert source.select_gleif_raw_keys_for_deletion(keys) == [
        (
            "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/"
            "run_id=old/file_kind=lei_records/source.csv.zip"
        )
    ]


def test_download_uses_publish_date_from_redirect_response_headers() -> None:
    redirect_response = _FakeResponse(
        headers={"x-gleif-publish-date": "2026-06-20T16:00:00+00:00"},
        chunks=(),
    )
    response = _FakeResponse(headers={}, chunks=(b"abc",), history=(redirect_response,))
    s3_client = _FakeS3Client()

    downloaded_file, publish_date = source._download_one_file(
        object_store=_FakeObjectStore(s3_client),
        session=_FakeSession(response),
        source_url="https://example.test/latest.csv",
        file_kind="lei_records",
        file_format="csv",
        load_mode="full",
        delta=None,
        run_id="run-1",
        timeout_seconds=30,
    )

    assert publish_date == "2026-06-20T16:00:00+00:00"
    assert downloaded_file.s3_key == s3_client.uploaded_key
    assert "publish_date=2026-06-20T16-00-00Z" in downloaded_file.s3_key
    assert s3_client.uploaded_body == b"abc"


def test_download_streams_temp_file_to_object_store() -> None:
    response = _FakeResponse(
        headers={"x-gleif-publish-date": "2026-06-20T16:00:00+00:00"},
        chunks=(b"abc", b"def"),
    )
    s3_client = _FakeS3Client()

    source._download_one_file(
        object_store=_FakeObjectStore(s3_client),
        session=_FakeSession(response),
        source_url="https://example.test/latest.csv",
        file_kind="lei_records",
        file_format="csv",
        load_mode="full",
        delta=None,
        run_id="run-1",
        timeout_seconds=30,
    )

    assert s3_client.body_was_stream is True
    assert s3_client.uploaded_body == b"abcdef"
