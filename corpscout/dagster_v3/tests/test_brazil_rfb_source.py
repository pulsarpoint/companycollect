import io
import zipfile
from pathlib import Path

from dagster_v3.defs.brazil_rfb import source


def _zip_bytes(member_name: str, body: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, body)
    return output.getvalue()


class FakeResponse:
    def __init__(self, body: bytes, *, json_payload: dict | None = None) -> None:
        self.content = body
        self.headers = {"Content-Length": str(len(body))}
        self._json_payload = json_payload

    def raise_for_status(self) -> None:
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


def test_family_from_archive_name_matches_rfb_patterns() -> None:
    assert source.family_from_archive_name("K3241.K03200Y0.D30612.EMPRECSV.zip") == "empresas"
    assert (
        source.family_from_archive_name("K3241.K03200Y0.D30612.ESTABELE.zip")
        == "estabelecimentos"
    )
    assert source.family_from_archive_name("K3241.K03200Y0.D30612.SIMPLES.CSV.zip") == "simples"
    assert source.family_from_archive_name("F.K03200$Z.D30612.CNAECSV.zip") == "cnaes"
    assert source.family_from_archive_name("F.K03200$Z.D30612.NATJUCSV.zip") == "naturezas"
    assert source.family_from_archive_name("F.K03200$Z.D30612.MUNICCSV.zip") == "municipios"
    assert source.family_from_archive_name("F.K03200$Z.D30612.PAISCSV.zip") == "paises"
    assert source.family_from_archive_name("F.K03200$Z.D30612.QUALSCSV.zip") == "qualificacoes"
    assert source.family_from_archive_name("F.K03200$Z.D30612.MOTICSV.zip") == "motivos"
    assert source.family_from_archive_name("SOCIOCSV.zip") == ""


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


def test_snapshot_year_month_builds_month_directory_url() -> None:
    assert source.build_year_month_base_url(
        snapshot_year_month="2026-05",
        base_url="https://example.test/dados_abertos_cnpj/",
    ) == "https://example.test/dados_abertos_cnpj/2026-05/"


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
