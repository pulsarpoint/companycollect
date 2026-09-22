from dataclasses import asdict
from pathlib import Path

from dagster_v3.defs.esef_filings.domain_context import enrich_domain_context
from dagster_v3.defs.esef_filings.website_candidates import extract_website_candidates


def extract(tmp_path: Path, body: str):
    path = tmp_path / "report.xhtml"
    path.write_text(f'<html xmlns="http://www.w3.org/1999/xhtml"><body>{body}</body></html>')
    reports = {"report.xhtml": path}
    candidates = extract_website_candidates(reports, tagged_values=(), known_email_domains=())
    return enrich_domain_context(candidates, reports)


def test_heading_table_and_distinct_occurrences_survive_json_storage(tmp_path):
    candidates = extract(tmp_path, '''
      <section><h2>Our portfolio at 31 December 2024</h2>
      <p>We hold shares in the following companies.</p>
      <table><tr><th>Investment</th><th>Website</th></tr>
      <tr><td>Ericsson</td><td><a href="https://ericsson.com/">Ericsson</a></td></tr></table>
      </section><section><h2>Suppliers</h2>
      <p>Ericsson supplies our communications equipment: www.ericsson.com.</p></section>''')
    ericsson = next(c for c in candidates if c.registrable_domain == "ericsson.com")
    contexts = [asdict(e)["source_context"] for e in ericsson.evidence]
    portfolio = next(c for c in contexts if "We hold shares" in c["text"])
    assert portfolio["headings"] == ["Our portfolio at 31 December 2024"]
    assert portfolio["table_headers"] == ["Investment", "Website"]
    assert "communications equipment" not in portfolio["text"]
    assert any("communications equipment" in c["text"] for c in contexts)


def test_page_boundary_and_long_context_keep_the_actual_domain(tmp_path):
    candidates = extract(tmp_path, '<div id="pf1"><p>Unrelated previous page.</p></div>'
        '<div id="pf2"><p>' + 'Annual information. ' * 900 + '</p>'
        '<p>Our customer Nova uses our products. www.nova.example.com</p>'
        '</div><div id="pf3"><p>Unrelated next page.</p></div>')
    evidence = candidates[0].evidence[0]
    context = evidence.source_context
    assert context["scope"] == "page"
    assert context["truncated"] is True
    assert "Our customer Nova" in context["text"]
    assert "www.nova.example.com" in context["text"]
    assert "Unrelated" not in context["text"]
    assert context["reading_order"] == "document_order_unverified"


def test_prefixed_xhtml_preserves_context_and_inline_word_fragments(tmp_path):
    path = tmp_path / "report.xhtml"
    path.write_text('''<h:html xmlns:h="http://www.w3.org/1999/xhtml"><h:body>
      <h:section><h:h2>Our suppliers</h:h2>
        <h:p>Nova supplies tele<h:span>com</h:span> equipment to us.
          <h:a href="https://nova.com">Nova</h:a></h:p>
      </h:section></h:body></h:html>''')
    reports = {"report.xhtml": path}
    candidates = extract_website_candidates(reports, tagged_values=(), known_email_domains=())
    enriched = enrich_domain_context(candidates, reports)
    context = enriched[0].evidence[0].source_context
    assert context["scope"] == "section"
    assert context["headings"] == ["Our suppliers"]
    assert "Nova supplies telecom equipment to us." in context["text"]
