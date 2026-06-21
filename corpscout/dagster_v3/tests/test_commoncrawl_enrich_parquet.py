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


def test_empty_child_tables_are_readable(tmp_path):
    """Regression: empty domain_phones / domain_socials must still carry their columns.

    pa.Table.from_pylist([]) without a schema produces a 0-column table that
    DuckDB cannot read back ('Need at least one non-root column').  The fix is to
    always pass an explicit schema to pa.Table.from_pylist.
    """
    enr = DomainEnrichment(
        target=DomainTarget("b.sk", 2, 7.0), fetch_status="ok", title="B",
        emails=[Email("info@b.sk", False, "regex")],
        technologies=[Technology("Apache", "Web server", "2.4", 90)],
        # phones and socials are intentionally empty
    )
    paths = parquet_out.write_parquet([enr], tmp_path)
    con = duckdb.connect()

    # domain_phones: empty but must be readable and return 0 rows with the right columns
    phones = con.execute(
        f"select root_domain, phone_e164 from read_parquet('{paths['domain_phones']}')"
    ).fetchall()
    assert phones == []

    # domain_socials: empty but must be readable
    socials = con.execute(
        f"select root_domain, platform from read_parquet('{paths['domain_socials']}')"
    ).fetchall()
    assert socials == []
