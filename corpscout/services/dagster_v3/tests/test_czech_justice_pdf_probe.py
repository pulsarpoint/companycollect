import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "czech_justice_pdf_probe.py"


def _probe_module():
    spec = importlib.util.spec_from_file_location("czech_justice_pdf_probe", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_normalize_ico_keeps_leading_zeroes():
    probe = _probe_module()

    assert probe.normalize_ico("175") == "00000175"
    assert probe.normalize_ico("27074358") == "27074358"


def test_pdf_output_path_uses_partitioned_layout(tmp_path: Path):
    probe = _probe_module()

    assert probe.pdf_output_path(
        output_dir=tmp_path,
        ico="27074358",
        year="2024",
        document_id="123456",
    ) == tmp_path / "ico_prefix=27" / "ico=27074358" / "year=2024" / "document=123456.pdf"


def test_extract_subjekt_id_from_detail_html():
    probe = _probe_module()

    html = '<a href="vypis-sl-firma?subjektId=157589">Sbírka listin</a>'

    assert probe.extract_subjekt_id(html) == "157589"


def test_extract_financial_document_links_from_list_html():
    probe = _probe_module()

    html = """
    <html><body>
      <table>
        <tr>
          <td>účetní závěrka [2024], výroční zpráva [2024]</td>
          <td><a href="vypis-sl-detail?dokument=111&subjektId=157589&spis=abc">detail</a></td>
        </tr>
        <tr>
          <td>změna stanov</td>
          <td><a href="vypis-sl-detail?dokument=222&subjektId=157589&spis=abc">detail</a></td>
        </tr>
      </table>
    </body></html>
    """

    documents = probe.extract_financial_documents(
        html,
        base_url="https://or.justice.cz/ias/ui/vypis-sl-firma?subjektId=157589",
    )

    assert len(documents) == 1
    assert documents[0].document_id == "111"
    assert documents[0].year == "2024"
    assert documents[0].detail_url == (
        "https://or.justice.cz/ias/ui/vypis-sl-detail?"
        "dokument=111&subjektId=157589&spis=abc"
    )


def test_fetch_pdf_documents_skips_non_pdf_and_continues(tmp_path: Path):
    probe = _probe_module()

    class _Response:
        def __init__(self, *, text: str = "", body: bytes = b""):
            self.text = text
            self.content = body
            self.encoding = "utf-8"
            self._body = body

        def raise_for_status(self):
            return None

        def iter_content(self, *, chunk_size):
            yield self._body

    class _Session:
        headers = {}

        def get(self, url, *, timeout, stream=False):
            if "rejstrik-$firma" in url:
                return _Response(text='<a href="vypis-sl-firma?subjektId=157589">x</a>')
            if "vypis-sl-firma" in url:
                return _Response(
                    text="""
                    <tr>
                      <td>účetní závěrka [2024]</td>
                      <td><a href="vypis-sl-detail?dokument=1&subjektId=157589">detail</a></td>
                    </tr>
                    <tr>
                      <td>účetní závěrka [2023]</td>
                      <td><a href="vypis-sl-detail?dokument=2&subjektId=157589">detail</a></td>
                    </tr>
                    """
                )
            if "dokument=1" in url:
                return _Response(text='<a href="/ias/content/download?id=bad">pdf</a>')
            if "dokument=2" in url:
                return _Response(text='<a href="/ias/content/download?id=good">pdf</a>')
            if "id=bad" in url:
                return _Response(body=b"not a pdf")
            if "id=good" in url:
                return _Response(body=b"%PDF-1.7\nbody")
            raise AssertionError(url)

    rows = probe.fetch_pdf_documents_for_ico(
        session=_Session(),
        ico="27074358",
        output_dir=tmp_path,
        max_documents_per_company=0,
        timeout_seconds=5,
        refresh=False,
        request_delay_seconds=0,
    )

    assert len(rows) == 1
    assert rows[0].document_id == "2"
    assert rows[0].size_bytes == len(b"%PDF-1.7\nbody")
