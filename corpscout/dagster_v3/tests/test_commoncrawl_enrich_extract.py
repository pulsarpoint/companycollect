from commoncrawl_enrich import extract
from commoncrawl_enrich.models import FetchedPage

HTML = """
<html lang="sk"><head><title>Firma s.r.o.</title>
<meta name="description" content="Účtovníctvo a dane"></head>
<body>
  Kontakt: info@firma.sk, Ján <jan.novak@firma.sk>. Tel: +421 905 123 456.
  IČO: 31 333 532 DIČ: 2020317068
  <a href="https://www.facebook.com/firmask">FB</a>
  <a href="https://www.linkedin.com/company/firma">LI</a>
  <img src="logo@2x.png">
</body></html>
"""


def _page() -> FetchedPage:
    return FetchedPage(root_domain="firma.sk", final_url="https://firma.sk/", http_status=200,
                       headers={}, html=HTML, capture_date="2025-05-01", crawl_id="CC-MAIN-2025-21")


def test_parse_html_title_meta_lang():
    parsed = extract.parse_html(HTML)
    assert parsed.title == "Firma s.r.o."
    assert parsed.meta_description == "Účtovníctvo a dane"
    assert parsed.content_language == "sk"
    assert "facebook.com/firmask" in " ".join(parsed.links)


def test_extract_emails_filters_noise_and_flags_role():
    emails = extract.extract_emails(HTML)
    addrs = {e.email for e in emails}
    assert "info@firma.sk" in addrs and "jan.novak@firma.sk" in addrs
    assert "logo@2x.png" not in addrs
    assert next(e for e in emails if e.email == "info@firma.sk").is_role is True


def test_extract_phones_and_socials():
    phones = extract.extract_phones(HTML)
    assert any(p.phone_e164 == "+421905123456" for p in phones)
    socials = extract.extract_socials(extract.parse_html(HTML).links)
    platforms = {s.platform for s in socials}
    assert platforms == {"facebook", "linkedin"}


def test_extract_socials_survives_urlparse_breaking_href():
    from urllib.parse import urlparse

    import pytest

    bad = "//x＠evil"  # fullwidth @ in netloc -> urlparse raises under NFKC
    with pytest.raises(ValueError):
        urlparse(bad)
    socials = extract.extract_socials([bad, "https://www.facebook.com/p"])
    assert [s.platform for s in socials] == ["facebook"]  # garbage skipped, valid kept


def test_extract_deterministic_bundles_everything():
    result = extract.extract_deterministic(_page())
    assert result.title == "Firma s.r.o." and result.ico == "31333532"
    assert result.ico_checksum_valid is True and result.dic == "2020317068"
    assert result.emails and result.technologies == []  # tech added by enrich, not here
