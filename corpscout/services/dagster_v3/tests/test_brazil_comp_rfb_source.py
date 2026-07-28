import hashlib
import io
import json
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError
from requests import HTTPError

from dagster_v3.defs.brazil_companies.rfb import source


def _zip_bytes(member_name: str, body: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, body)
    return output.getvalue()


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        json_payload: dict | None = None,
        status_code: int = 200,
    ) -> None:
        self.content = body
        self.headers = {"Content-Length": str(len(body))}
        self._json_payload = json_payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"{self.status_code} error")
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self.content

    def json(self) -> dict:
        if self._json_payload is None:
            raise AssertionError("json() called on non-json fake response")
        return self._json_payload


class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, bool]] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.calls.append((url, stream))
        return self.responses[url]


class NoDownloadSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        raise AssertionError(f"unexpected HTTP request: {url}")


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []
        self.downloaded_files: list[tuple[str, str, Path]] = []
        self.written_json: list[tuple[str, str]] = []
        self.lifecycle_rules: list[tuple[str, list[dict[str, object]]]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def apply_lifecycle_rules(
        self,
        rules: object,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.lifecycle_rules.append((bucket, list(rules)))  # type: ignore[arg-type]

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def download_file(
        self,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        resolved_target = Path(target_path)
        resolved_target.parent.mkdir(parents=True, exist_ok=True)
        resolved_target.write_bytes(self.objects[(bucket, key)])
        self.downloaded_files.append((bucket, key, resolved_target))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_json.append((bucket, key))
        self.objects[(bucket, key)] = body.encode("utf-8")

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return sorted(
            key
            for (object_bucket, key) in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        )


def test_family_from_archive_name_matches_rfb_patterns() -> None:
    assert (
        source.family_from_archive_name("K3241.K03200Y0.D30612.EMPRECSV.zip")
        == "empresas"
    )
    assert source.family_from_archive_name("Empresas0.zip") == "empresas"
    assert (
        source.family_from_archive_name("K3241.K03200Y0.D30612.ESTABELE.zip")
        == "estabelecimentos"
    )
    assert (
        source.family_from_archive_name("Estabelecimentos9.zip") == "estabelecimentos"
    )
    assert (
        source.family_from_archive_name("K3241.K03200Y0.D30612.SIMPLES.CSV.zip")
        == "simples"
    )
    assert source.family_from_archive_name("Simples.zip") == "simples"
    assert source.family_from_archive_name("F.K03200$Z.D30612.CNAECSV.zip") == "cnaes"
    assert source.family_from_archive_name("Cnaes.zip") == "cnaes"
    assert (
        source.family_from_archive_name("F.K03200$Z.D30612.NATJUCSV.zip") == "naturezas"
    )
    assert source.family_from_archive_name("Naturezas.zip") == "naturezas"
    assert (
        source.family_from_archive_name("F.K03200$Z.D30612.MUNICCSV.zip")
        == "municipios"
    )
    assert source.family_from_archive_name("Municipios.zip") == "municipios"
    assert source.family_from_archive_name("F.K03200$Z.D30612.PAISCSV.zip") == "paises"
    assert source.family_from_archive_name("Paises.zip") == "paises"
    assert (
        source.family_from_archive_name("F.K03200$Z.D30612.QUALSCSV.zip")
        == "qualificacoes"
    )
    assert source.family_from_archive_name("Qualificacoes.zip") == "qualificacoes"
    assert source.family_from_archive_name("F.K03200$Z.D30612.MOTICSV.zip") == "motivos"
    assert source.family_from_archive_name("Motivos.zip") == "motivos"
    assert source.family_from_archive_name("SOCIOCSV.zip") == "socios"
    assert source.family_from_archive_name("Random0.zip") == ""


def test_family_from_archive_name_matches_socios_patterns() -> None:
    assert (
        source.family_from_archive_name("F.K03200$W.SIMPLES.CSV.D30612.SOCIOCSV.zip")
        == "socios"
    )
    assert source.family_from_archive_name("Socios0.zip") == "socios"
    assert source.family_from_archive_name("Socios.zip") == "socios"


def test_discover_snapshot_zip_urls_from_directory_html() -> None:
    html = """
    <html><body>
      <a href="K3241.K03200Y0.D30612.EMPRECSV.zip">empresas</a>
      <a href="K3241.K03200Y0.D30612.ESTABELE.zip">estab</a>
      <a href="ignore.txt">ignore</a>
    </body></html>
    """

    files = source.discover_snapshot_zip_urls(
        html,
        base_url="https://example.test/dados_abertos_cnpj/2026-06/",
        families=("empresas", "estabelecimentos"),
    )

    assert [(item.family, item.url) for item in files] == [
        (
            "empresas",
            "https://example.test/dados_abertos_cnpj/2026-06/K3241.K03200Y0.D30612.EMPRECSV.zip",
        ),
        (
            "estabelecimentos",
            "https://example.test/dados_abertos_cnpj/2026-06/K3241.K03200Y0.D30612.ESTABELE.zip",
        ),
    ]


def test_discover_current_snapshot_zip_urls_from_mirror_html() -> None:
    html = """
    <html><body>
      <a href="Empresas0.zip">empresas 0</a>
      <a href="Empresas1.zip">empresas 1</a>
      <a href="Estabelecimentos0.zip">estab 0</a>
      <a href="Simples.zip">simples</a>
      <a href="Cnaes.zip">cnaes</a>
      <a href="Naturezas.zip">naturezas</a>
      <a href="Municipios.zip">municipios</a>
      <a href="Paises.zip">paises</a>
      <a href="Qualificacoes.zip">qualificacoes</a>
      <a href="Motivos.zip">motivos</a>
      <a href="Socios0.zip">socios</a>
    </body></html>
    """

    files = source.discover_snapshot_zip_urls(
        html,
        base_url="https://example.test/arquivos/2026-05-10/",
        families=source.DEFAULT_FAMILIES,
    )

    assert len(files) == 11
    assert (
        source.BrazilRfbRemoteFile(
            family="empresas",
            url="https://example.test/arquivos/2026-05-10/Empresas0.zip",
            archive_name="Empresas0.zip",
        )
        in files
    )
    assert (
        source.BrazilRfbRemoteFile(
            family="estabelecimentos",
            url="https://example.test/arquivos/2026-05-10/Estabelecimentos0.zip",
            archive_name="Estabelecimentos0.zip",
        )
        in files
    )
    assert (
        source.BrazilRfbRemoteFile(
            family="socios",
            url="https://example.test/arquivos/2026-05-10/Socios0.zip",
            archive_name="Socios0.zip",
        )
        in files
    )


def test_snapshot_year_month_builds_month_directory_url() -> None:
    assert (
        source.build_year_month_base_url(
            snapshot_year_month="2026-05",
            base_url="https://example.test/dados_abertos_cnpj/",
        )
        == "https://example.test/dados_abertos_cnpj/2026-05/"
    )


def test_default_base_url_points_to_fetchable_cnpj_mirror() -> None:
    assert source.DEFAULT_BASE_URL == (
        "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
    )


def test_dated_snapshot_directory_resolves_latest_day_for_month() -> None:
    html = """
    <html><body>
      <a href="2026-04-12/">2026-04-12</a>
      <a href="2026-05-01/">2026-05-01</a>
      <a href="2026-05-10/">2026-05-10</a>
      <a href="2026-06-13/">2026-06-13</a>
    </body></html>
    """

    assert (
        source.resolve_dated_snapshot_directory_url(
            html,
            base_url="https://example.test/arquivos/",
            snapshot_year_month="2026-05",
        )
        == "https://example.test/arquivos/2026-05-10/"
    )


def test_fetch_snapshot_remote_files_falls_back_to_dated_month_directory() -> None:
    base_url = "https://example.test/arquivos/"
    direct_month_url = "https://example.test/arquivos/2026-05/"
    dated_month_url = "https://example.test/arquivos/2026-05-10/"
    root_html = """
    <html><body>
      <a href="2026-05-01/">2026-05-01</a>
      <a href="2026-05-10/">2026-05-10</a>
    </body></html>
    """
    dated_html = """
    <html><body>
      <a href="Empresas0.zip">empresas</a>
      <a href="Cnaes.zip">cnaes</a>
    </body></html>
    """
    session = FakeSession(
        {
            direct_month_url: FakeResponse(b"not found", status_code=404),
            base_url: FakeResponse(root_html.encode("utf-8")),
            dated_month_url: FakeResponse(dated_html.encode("utf-8")),
        }
    )

    files = source.fetch_snapshot_remote_files(
        snapshot_year_month="2026-05",
        base_url=base_url,
        families=("empresas", "cnaes"),
        session=session,
    )

    assert files == [
        source.BrazilRfbRemoteFile(
            family="cnaes",
            url="https://example.test/arquivos/2026-05-10/Cnaes.zip",
            archive_name="Cnaes.zip",
        ),
        source.BrazilRfbRemoteFile(
            family="empresas",
            url="https://example.test/arquivos/2026-05-10/Empresas0.zip",
            archive_name="Empresas0.zip",
        ),
    ]
    assert session.calls == [
        (direct_month_url, False),
        (base_url, False),
        (dated_month_url, False),
    ]


def test_snapshot_year_month_rejects_invalid_format() -> None:
    try:
        source.build_year_month_base_url(
            snapshot_year_month="202605",
            base_url="https://example.test/dados_abertos_cnpj/",
        )
    except ValueError as exc:
        assert "snapshot_year_month must use YYYY-MM format" in str(exc)
    else:
        raise AssertionError("expected invalid snapshot_year_month to fail")


def test_download_extract_and_build_manifest_rows(tmp_path: Path) -> None:
    archive_url = "https://example.test/K3241.K03200Y0.D30612.EMPRECSV.zip"
    session = FakeSession(
        {
            archive_url: FakeResponse(
                _zip_bytes(
                    "K3241.K03200Y0.D30612.EMPRECSV",
                    b"12345678;ACME LTDA;2062;49;1000,00;01;\n",
                )
            )
        }
    )

    rows = source.download_extract_snapshot_files(
        remote_files=[
            source.BrazilRfbRemoteFile(
                family="empresas",
                url=archive_url,
                archive_name="K3241.K03200Y0.D30612.EMPRECSV.zip",
            )
        ],
        download_dir=tmp_path,
        source_run_id="run-1",
        session=session,
    )

    assert len(rows) == 1
    assert rows[0]["family"] == "empresas"
    assert rows[0]["archive_url"] == archive_url
    assert rows[0]["archive_sha256"]
    assert rows[0]["csv_member_name"] == "K3241.K03200Y0.D30612.EMPRECSV"
    assert Path(rows[0]["csv_path"]).exists()
    assert rows[0]["source_run_id"] == "run-1"
    assert session.calls == [(archive_url, True)]


def test_download_extract_logs_file_progress(tmp_path: Path) -> None:
    archive_url = "https://example.test/K3241.K03200Y0.D30612.EMPRECSV.zip"
    session = FakeSession(
        {
            archive_url: FakeResponse(
                _zip_bytes(
                    "K3241.K03200Y0.D30612.EMPRECSV",
                    b"12345678;ACME LTDA;2062;49;1000,00;01;\n",
                )
            )
        }
    )
    messages: list[str] = []

    def log(message: str, *args: object) -> None:
        messages.append(message % args)

    rows = source.download_extract_snapshot_files(
        remote_files=[
            source.BrazilRfbRemoteFile(
                family="empresas",
                url=archive_url,
                archive_name="K3241.K03200Y0.D30612.EMPRECSV.zip",
            )
        ],
        download_dir=tmp_path,
        source_run_id="run-1",
        session=session,
        log=log,
    )

    assert len(rows) == 1
    assert any(
        message.startswith("Downloading empresas archive 1/1") for message in messages
    )
    assert any(
        message.startswith("Downloaded empresas archive 1/1") for message in messages
    )
    assert any(
        message.startswith("Extracted empresas archive 1/1") for message in messages
    )
    assert any(
        message.startswith("Recorded Brazil RFB snapshot manifest row")
        for message in messages
    )


def test_download_extract_normalizes_dirty_latin1_csv_for_duckdb(
    tmp_path: Path,
) -> None:
    archive_url = "https://example.test/K3241.K03200Y0.D30612.EMPRECSV.zip"
    session = FakeSession(
        {
            archive_url: FakeResponse(
                _zip_bytes(
                    "K3241.K03200Y0.D30612.EMPRECSV",
                    b"12345678;CAF\xc9 \x8f LTDA;2062;49;1000,00;01;\n",
                )
            )
        }
    )

    rows = source.download_extract_snapshot_files(
        remote_files=[
            source.BrazilRfbRemoteFile(
                family="empresas",
                url=archive_url,
                archive_name="K3241.K03200Y0.D30612.EMPRECSV.zip",
            )
        ],
        download_dir=tmp_path,
        source_run_id="run-1",
        session=session,
    )

    csv_path = Path(rows[0]["csv_path"])

    assert csv_path.name.endswith(".utf8.csv")
    assert csv_path.read_text(encoding="utf-8") == (
        "12345678;CAFÉ � LTDA;2062;49;1000,00;01;\n"
    )


def test_rfb_archive_object_key_puts_family_before_snapshot() -> None:
    """The key layout IS the reuse contract: a re-run must resolve to the same
    key so `exists` short-circuits the download.

    `family=` comes BEFORE `snapshot=` because S3 lifecycle rules filter by
    prefix, not glob. Only this order makes
    `brazil_rfb/raw_archives/family=socios/` selectable, which is what lets the
    90-day expiry on personal data (design section 6) be expressed as one rule
    instead of a new rule every month.
    """
    assert (
        source.rfb_archive_object_key("2026-07", "socios", "Socios0.zip")
        == "brazil_rfb/raw_archives/family=socios/snapshot=2026-07/Socios0.zip"
    )
    assert source.rfb_archive_object_key(
        "2026-07", "socios", "Socios0.zip"
    ).startswith(source.RFB_SOCIOS_RETENTION_PREFIX)


def test_rfb_metadata_object_key_is_a_sidecar_of_the_archive_key() -> None:
    assert source.rfb_metadata_object_key("2026-07", "socios", "Socios0.zip") == (
        "brazil_rfb/raw_archives/family=socios/snapshot=2026-07/Socios0.zip"
        ".metadata.json"
    )


def test_socios_retention_rule_expires_only_the_personal_data_family() -> None:
    """Design section 6: socios carries person names, masked CPFs and age bands,
    so its raw archives expire after 90 days. The other nine families carry no
    personal data and are kept indefinitely for rebuildability.

    The rule is applied in code rather than by hand because this repo has no
    infrastructure-as-code -- a hand-applied policy would live nowhere, survive
    no bucket recreation, and appear in no review.
    """
    rule = source.rfb_socios_retention_rule()

    assert rule["Filter"]["Prefix"] == "brazil_rfb/raw_archives/family=socios/"
    assert rule["Expiration"]["Days"] == 90
    assert rule["Status"] == "Enabled"


def test_sync_snapshot_archives_downloads_missing_archives_to_object_store() -> None:
    base_url = "https://example.test/arquivos/"
    month_url = "https://example.test/arquivos/2026-07/"
    empresas_url = "https://example.test/arquivos/2026-07/Empresas0.zip"
    cnaes_url = "https://example.test/arquivos/2026-07/Cnaes.zip"
    html = """
    <html><body>
      <a href="Empresas0.zip">empresas</a>
      <a href="Cnaes.zip">cnaes</a>
    </body></html>
    """
    body_by_url = {
        empresas_url: b"empresas-zip-bytes",
        cnaes_url: b"cnaes-zip-bytes",
    }
    session = FakeSession(
        {
            month_url: FakeResponse(html.encode("utf-8")),
            empresas_url: FakeResponse(body_by_url[empresas_url]),
            cnaes_url: FakeResponse(body_by_url[cnaes_url]),
        }
    )
    object_store = FakeObjectStore()

    result = source.sync_snapshot_archives_to_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        base_url=base_url,
        families=("empresas", "cnaes"),
        session=session,
    )

    assert object_store.created_buckets == [source.BRAZIL_RFB_RAW_BUCKET]
    assert len(result.archives) == 2
    for archive in result.archives:
        body = body_by_url[archive.archive_url]
        assert (
            object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, archive.object_key)]
            == body
        )
        assert archive.object_key == source.rfb_archive_object_key(
            "2026-07", archive.family, archive.archive_name
        )
        assert archive.downloaded is True
        assert archive.reused_existing is False
        assert archive.size_bytes == len(body)
        assert archive.sha256 == hashlib.sha256(body).hexdigest()
        metadata = json.loads(
            object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, archive.metadata_key)]
        )
        assert metadata["object_key"] == archive.object_key


def test_sync_snapshot_archives_skips_existing_object_store_keys() -> None:
    """When the requested families are already fully present in object
    storage, the sync resolves entirely from there -- no origin request at
    all, not even the directory listing (see Finding 1 / C1)."""
    object_store = FakeObjectStore()
    object_key = source.rfb_archive_object_key("2026-07", "empresas", "Empresas0.zip")
    metadata_key = source.rfb_metadata_object_key(
        "2026-07", "empresas", "Empresas0.zip"
    )
    existing_body = b"already-synced-zip-bytes"
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, object_key)] = existing_body
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, metadata_key)] = json.dumps(
        {
            "size_bytes": len(existing_body),
            "sha256": hashlib.sha256(existing_body).hexdigest(),
        }
    ).encode("utf-8")

    result = source.sync_snapshot_archives_to_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        families=("empresas",),
        session=NoDownloadSession(),
    )

    assert len(result.archives) == 1
    archive = result.archives[0]
    assert archive.downloaded is False
    assert archive.reused_existing is True
    assert archive.size_bytes == len(existing_body)
    assert archive.sha256 == hashlib.sha256(existing_body).hexdigest()


def test_sync_snapshot_archives_resumes_from_origin_on_partial_object_store_state() -> (
    None
):
    """A prior sync that crashed after uploading only some families leaves a
    *partial* object store state -- that is a resumable condition, not a
    complete one. The sync must fall through to the origin-backed path
    (whose per-archive `object_store.exists` check reuses what is already
    there) rather than raising the LookupError that
    `resolve_snapshot_remote_files_from_object_store` uses to flag partial
    syncs to its other caller, the manifest builder."""
    base_url = "https://example.test/arquivos/"
    month_url = "https://example.test/arquivos/2026-07/"
    cnaes_url = "https://example.test/arquivos/2026-07/Cnaes.zip"
    html = """
    <html><body>
      <a href="Empresas0.zip">empresas</a>
      <a href="Cnaes.zip">cnaes</a>
    </body></html>
    """
    cnaes_body = b"cnaes-zip-bytes"
    session = FakeSession(
        {
            month_url: FakeResponse(html.encode("utf-8")),
            cnaes_url: FakeResponse(cnaes_body),
        }
    )
    object_store = FakeObjectStore()
    empresas_key = source.rfb_archive_object_key("2026-07", "empresas", "Empresas0.zip")
    existing_body = b"already-synced-zip-bytes"
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, empresas_key)] = existing_body

    result = source.sync_snapshot_archives_to_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        base_url=base_url,
        families=("empresas", "cnaes"),
        session=session,
    )

    by_family = {archive.family: archive for archive in result.archives}
    assert by_family["empresas"].reused_existing is True
    assert by_family["empresas"].downloaded is False
    assert by_family["cnaes"].reused_existing is False
    assert by_family["cnaes"].downloaded is True
    assert by_family["cnaes"].sha256 == hashlib.sha256(cnaes_body).hexdigest()


def test_sync_snapshot_archives_to_object_store_resolves_from_object_store_with_no_http_at_all() -> (
    None
):
    """C1 (upstream half): brazil_comp_rfb_raw_archives_s3 runs before the
    manifest asset on every normal rebuild path. When the bucket already
    holds a complete partition, the sync asset itself -- not just the
    downstream manifest asset -- must resolve entirely from the bucket,
    zero HTTP requests, so a month RFB has stopped publishing is still
    rebuildable from storage. A session that raises on any .get() proves
    it."""
    object_store = FakeObjectStore()
    for family, archive_name in (
        ("empresas", "Empresas0.zip"),
        ("cnaes", "Cnaes.zip"),
    ):
        object_key = source.rfb_archive_object_key("2026-07", family, archive_name)
        metadata_key = source.rfb_metadata_object_key("2026-07", family, archive_name)
        body = f"{family}-zip-bytes".encode("utf-8")
        object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, object_key)] = body
        object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, metadata_key)] = (
            json.dumps(
                {
                    "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "archive_url": (
                        f"https://example.test/arquivos/2026-07/{archive_name}"
                    ),
                }
            ).encode("utf-8")
        )

    result = source.sync_snapshot_archives_to_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        families=("empresas", "cnaes"),
        session=NoDownloadSession(),
    )

    assert len(result.archives) == 2
    assert {archive.family for archive in result.archives} == {"empresas", "cnaes"}
    for archive in result.archives:
        assert archive.downloaded is False
        assert archive.reused_existing is True
        assert archive.sha256 is not None
    # No new bucket-creation call either -- the bucket obviously already
    # exists, since we just listed keys from it.
    assert object_store.created_buckets == []


def test_download_extract_reads_archive_from_object_store_and_deletes_local_zip(
    tmp_path: Path,
) -> None:
    archive_url = "https://example.test/Empresas0.zip"
    zip_bytes = _zip_bytes(
        "K3241.K03200Y0.D30612.EMPRECSV",
        b"12345678;ACME LTDA;2062;49;1000,00;01;\n",
    )
    object_store = FakeObjectStore()
    object_key = source.rfb_archive_object_key("2026-07", "empresas", "Empresas0.zip")
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, object_key)] = zip_bytes
    session = NoDownloadSession()

    rows = source.download_extract_snapshot_files(
        remote_files=[
            source.BrazilRfbRemoteFile(
                family="empresas",
                url=archive_url,
                archive_name="Empresas0.zip",
            )
        ],
        download_dir=tmp_path,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        object_store=object_store,
        session=session,
    )

    assert len(rows) == 1
    csv_path = Path(rows[0]["csv_path"])
    assert csv_path.exists()
    assert rows[0]["archive_sha256"] == hashlib.sha256(zip_bytes).hexdigest()
    zip_path = tmp_path / "empresas" / "Empresas0.zip"
    assert not zip_path.exists()
    assert object_store.downloaded_files == [
        (source.BRAZIL_RFB_RAW_BUCKET, object_key, zip_path)
    ]


def test_download_extract_requires_snapshot_year_month_with_object_store(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    try:
        source.download_extract_snapshot_files(
            remote_files=[
                source.BrazilRfbRemoteFile(
                    family="empresas",
                    url="https://example.test/Empresas0.zip",
                    archive_name="Empresas0.zip",
                )
            ],
            download_dir=tmp_path,
            source_run_id="run-1",
            object_store=object_store,
        )
    except ValueError as exc:
        assert "snapshot_year_month is required" in str(exc)
    else:
        raise AssertionError(
            "expected missing snapshot_year_month with object_store to fail"
        )


def test_resolve_snapshot_remote_files_from_object_store_lists_bucket_keys() -> None:
    object_store = FakeObjectStore()
    empresas_key = source.rfb_archive_object_key("2026-07", "empresas", "Empresas0.zip")
    cnaes_key = source.rfb_archive_object_key("2026-07", "cnaes", "Cnaes.zip")
    metadata_key = source.rfb_metadata_object_key("2026-07", "empresas", "Empresas0.zip")
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, empresas_key)] = b"empresas"
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, cnaes_key)] = b"cnaes"
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, metadata_key)] = b"{}"

    files = source.resolve_snapshot_remote_files_from_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        families=("empresas", "cnaes"),
    )

    assert [(item.family, item.archive_name) for item in files] == [
        ("cnaes", "Cnaes.zip"),
        ("empresas", "Empresas0.zip"),
    ]


def test_resolve_snapshot_remote_files_from_object_store_returns_empty_when_unsynced() -> (
    None
):
    object_store = FakeObjectStore()

    files = source.resolve_snapshot_remote_files_from_object_store(
        snapshot_year_month="2026-07",
        object_store=object_store,
        families=("empresas", "cnaes"),
    )

    assert files == []


def test_resolve_snapshot_remote_files_from_object_store_raises_on_partial_sync() -> (
    None
):
    """A partial sync (some families present, some missing) is a bug in the
    sync asset, not something the manifest asset should silently paper over
    by discovering the rest from origin -- fail loudly instead."""
    object_store = FakeObjectStore()
    empresas_key = source.rfb_archive_object_key("2026-07", "empresas", "Empresas0.zip")
    object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, empresas_key)] = b"empresas"

    try:
        source.resolve_snapshot_remote_files_from_object_store(
            snapshot_year_month="2026-07",
            object_store=object_store,
            families=("empresas", "cnaes"),
        )
    except LookupError as exc:
        assert "cnaes" in str(exc)
    else:
        raise AssertionError("expected partial object store sync to fail")


class MissingBucketObjectStore:
    """A fake object store whose bucket has never been created: `list_keys`
    reproduces boto3's real `NoSuchBucket` ClientError instead of an empty
    list."""

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        raise ClientError(
            {"Error": {"Code": "NoSuchBucket"}},
            operation_name="ListObjectsV2",
        )


def test_resolve_snapshot_remote_files_from_object_store_returns_empty_when_bucket_missing() -> (
    None
):
    """The docstring promises an empty list -- and a fallback to origin
    discovery -- when the bucket holds nothing for this snapshot. A bucket
    that does not exist yet (e.g. `source-brazil-rfb` before its first sync)
    must honor that same contract instead of propagating boto3's raw
    NoSuchBucket error."""
    files = source.resolve_snapshot_remote_files_from_object_store(
        snapshot_year_month="2026-07",
        object_store=MissingBucketObjectStore(),
        families=("empresas", "cnaes"),
    )

    assert files == []


def test_brazil_rfb_source_resolves_from_object_store_with_no_http_at_all(
    tmp_path: Path,
) -> None:
    """C1: when the raw-archives-s3 asset has already snapshotted this
    partition, the manifest asset must resolve entirely from the bucket --
    zero HTTP requests -- so a month RFB has stopped publishing is still
    rebuildable from storage. A session that raises on any .get() proves it."""
    object_store = FakeObjectStore()
    for family, archive_name, member_name, body in (
        (
            "empresas",
            "Empresas0.zip",
            "Empresas0",
            b"12345678;ACME LTDA;2062;49;1000,00;01;\n",
        ),
        ("cnaes", "Cnaes.zip", "Cnaes", b"01;COMMERCE\n"),
    ):
        key = source.rfb_archive_object_key("2026-07", family, archive_name)
        object_store.objects[(source.BRAZIL_RFB_RAW_BUCKET, key)] = _zip_bytes(
            member_name, body
        )

    dlt_source = source.brazil_rfb_source(
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        download_dir=tmp_path,
        families=("empresas", "cnaes"),
        object_store=object_store,
        session=NoDownloadSession(),
    )
    rows = list(dlt_source.resources["snapshot_files"])

    assert {row["family"] for row in rows} == {"empresas", "cnaes"}
    for row in rows:
        assert Path(row["csv_path"]).exists()


def test_snapshot_files_resource_declares_explicit_schema(tmp_path: Path) -> None:
    row = source.build_snapshot_file_row(
        family="empresas",
        archive_url="https://example.test/empresas.zip",
        archive_name="empresas.zip",
        archive_sha256="a" * 64,
        csv_member_name="empresas.csv",
        csv_path=tmp_path / "empresas.csv",
        source_run_id="run-1",
    )
    dlt_source = source.brazil_rfb_source(
        manifest_rows=[row],
        source_run_id="run-1",
    )
    schema = dlt_source.resources["snapshot_files"].compute_table_schema()

    assert set(schema["columns"]) == set(row)
    assert schema["columns"]["family"]["data_type"] == "text"
    assert schema["columns"]["csv_path"]["data_type"] == "text"
    assert schema["columns"]["retrieved_at"]["data_type"] == "timestamp"
