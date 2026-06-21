import duckdb

from dagster_v3.commoncrawl_enrich import parquet_out
from dagster_v3.commoncrawl_enrich.models import (
    DomainEnrichment, DomainTarget, Email, IndustryGuess, Technology)


def test_write_parquet_emits_five_tables(tmp_path):
    enr = DomainEnrichment(
        target=DomainTarget("a.sk", 1, 9.0), fetch_status="ok", title="A",
        content_language="sk", ico="31333532", ico_checksum_valid=True,
        industry=IndustryGuess("Accounting", "69.20", 80, "llm"),
        emails=[Email("info@a.sk", True, "regex")],
        technologies=[Technology("Nginx", "Web server", "", 100)],
    )
    paths = parquet_out.write_parquet([enr], tmp_path)
    con = duckdb.connect()
    spine = con.execute(f"select root_domain, ico, industry_label, email_count, technology_count "
                        f"from read_parquet('{paths['domain_enrichment']}')").fetchone()
    assert spine == ("a.sk", "31333532", "Accounting", 1, 1)
    emails = con.execute(f"select root_domain, email, source_method "
                         f"from read_parquet('{paths['domain_emails']}')").fetchall()
    assert emails == [("a.sk", "info@a.sk", "regex")]
    techs = con.execute(f"select technology from read_parquet('{paths['domain_technologies']}')").fetchone()
    assert techs == ("Nginx",)
