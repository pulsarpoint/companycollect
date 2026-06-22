from commoncrawl_enrich import enrich, llm
from commoncrawl_enrich.models import DomainTarget, FetchedPage, IndexRecord

HTML_WITH_CONTACT = "<html lang='sk'><title>A</title><body>IČO: 31 333 532 info@a.sk</body></html>"
HTML_NO_CONTACT = "<html lang='sk'><title>B</title><body>Vitajte</body></html>"


def _targets():
    return [
        DomainTarget("a.sk", 1, 9.0),   # in index, has contact
        DomainTarget("b.sk", 2, 8.0),   # in index, no contact -> LLM recall
        DomainTarget("gone.sk", 3, 7.0)  # not in index
    ]


def _fake_resolve(domains, *, crawl_id):
    out = {}
    for d in domains:
        if d in ("a.sk", "b.sk"):
            out[d] = IndexRecord(d, "f.warc.gz", 0, 1, f"https://{d}/", 200, crawl_id)
    return out


def _fake_fetch(record, **_):
    html = HTML_WITH_CONTACT if record.root_domain == "a.sk" else HTML_NO_CONTACT
    return FetchedPage(record.root_domain, record.url, 200, {"server": "nginx"}, html, "2025-05-01", record.crawl_id)


def _fake_chat(system, user):
    if "industry" in system.lower():
        return '{"label":"Test","nace_hint":"00.00","confidence":50}'
    return '{"emails":["found@b.sk"],"phones":[]}'


def test_enrich_domains_full_flow():
    results = {e.target.root_domain: e for e in enrich.enrich_domains(
        _targets(), resolve=_fake_resolve, fetch=_fake_fetch, llm=llm.LLMArm(_fake_chat),
        crawl_id="CC-MAIN-2025-21", max_workers=2)}

    assert results["gone.sk"].fetch_status == "not_in_index"
    a = results["a.sk"]
    assert a.fetch_status == "ok" and a.ico == "31333532"
    assert any(e.email == "info@a.sk" and e.source_method == "regex" for e in a.emails)
    assert a.industry.label == "Test"
    assert any(t.technology == "Nginx" for t in a.technologies)
    # b had no deterministic contact -> LLM recall added one tagged 'llm'
    b = results["b.sk"]
    assert any(e.email == "found@b.sk" and e.source_method == "llm" for e in b.emails)
