from datetime import datetime, timezone

from commoncrawl_enrich.classifier import IndustryResult
from commoncrawl_enrich.models import Technology
from index_enrich import classify, schema

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(nace_code="62.01", nace_label="Programming", nace_division="62",
                               nace_confident=True, nace_score=0.8, method="embedding")
                for _ in texts]


class FakeWappalyzer:
    def analyze_batch(self, items):
        return {k: [Technology(technology="WordPress", category="CMS", version="6.1", confidence=100)]
                for k, _, _ in items}


def test_enrich_domain_builds_domain_and_tech_rows():
    html = "<html><body>ACME software, info@acme.com</body></html>"
    headers = {"Server": "nginx"}
    domain_row, tech_rows = classify.enrich_domain(
        html, headers, root_domain="acme.com", url="http://acme.com/en/",
        crawl_id="CC-MAIN-2026-25", classifier=FakeClassifier(), wappalyzer=FakeWappalyzer(),
        resolved_at=RESOLVED)
    assert len(domain_row) == len(schema.DOMAINS_COLUMNS)
    assert domain_row[2] == "acme.com" and domain_row[8] == "62.01"       # root_domain, nace_code
    assert "info@acme.com" in domain_row[4]                                # emails
    assert domain_row[20] == RESOLVED
    assert tech_rows and tech_rows[0][4] == "WordPress"                    # technology


def test_enrich_domain_without_wappalyzer_yields_no_tech_rows():
    domain_row, tech_rows = classify.enrich_domain(
        "<html>x</html>", {}, root_domain="x.com", url="http://x.com/",
        crawl_id="C", classifier=FakeClassifier(), resolved_at=RESOLVED)
    assert tech_rows == [] and domain_row[2] == "x.com"
