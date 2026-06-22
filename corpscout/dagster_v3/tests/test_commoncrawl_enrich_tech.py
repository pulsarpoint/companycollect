from commoncrawl_enrich import tech


def test_detects_html_and_header_fingerprints():
    html = '<link href="/wp-content/themes/x/style.css"><script src="gtag.js"></script>'
    headers = {"server": "nginx/1.25", "x-powered-by": "PHP/8.2"}
    names = {t.technology for t in tech.detect_technologies(html, headers)}
    assert {"WordPress", "Google Analytics", "Nginx", "PHP"} <= names


def test_no_false_positive_on_blank():
    assert tech.detect_technologies("", {}) == []
