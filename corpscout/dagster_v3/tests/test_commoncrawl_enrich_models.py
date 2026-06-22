from commoncrawl_enrich import models as m


def test_domain_enrichment_defaults_and_construction():
    target = m.DomainTarget(root_domain="example.sk", source_rank=5, open_page_rank=7.5)
    enr = m.DomainEnrichment(target=target, fetch_status="ok")
    assert enr.emails == [] and enr.technologies == []
    assert enr.ico == "" and enr.ico_checksum_valid is False
    assert enr.industry is None

    enr.emails.append(m.Email(email="info@example.sk", is_role=True, source_method="regex"))
    assert enr.emails[0].source_method == "regex"

    ind = m.IndustryGuess(label="Accounting", nace_hint="69.20", confidence=80, method="llm")
    assert ind.nace_hint == "69.20"
