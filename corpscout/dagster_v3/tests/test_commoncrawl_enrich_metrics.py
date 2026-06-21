from dagster_v3.commoncrawl_enrich import metrics
from dagster_v3.commoncrawl_enrich.models import (
    DomainEnrichment, DomainTarget, Email, IndustryGuess)


def _enr(domain, status, emails=(), industry=None, ico="", ico_valid=False):
    return DomainEnrichment(target=DomainTarget(domain, 1, 1.0), fetch_status=status,
                            emails=list(emails), industry=industry, ico=ico,
                            ico_checksum_valid=ico_valid)


def test_report_counts_and_uplift():
    enrichments = [
        _enr("a.sk", "ok", emails=[Email("x@a.sk", False, "regex")], ico="31333532",
             ico_valid=True,
             industry=IndustryGuess("Acc", "69.20", 90, "llm")),
        _enr("b.sk", "ok", emails=[Email("y@b.sk", False, "llm")],
             industry=IndustryGuess("", "", 0, "none")),
        _enr("gone.sk", "not_in_index"),
    ]
    report = metrics.build_report(enrichments, wall_clock_seconds=10.0)
    assert report["total"] == 3
    assert report["found_in_index"] == 2 and report["fetched_ok"] == 2
    assert report["with_valid_ico"] == 1
    assert report["with_email"] == 2
    assert report["email_llm_uplift"] == 1   # b.sk's email came only from the LLM
    assert report["industry_classified"] == 1
    assert report["domains_per_second"] == 0.3
    assert "projected_100k_hours" in report and "projected_10m_hours" in report
