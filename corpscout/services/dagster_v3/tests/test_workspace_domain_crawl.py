"""Global inventory selection and real draft imports in disposable databases."""

from hashlib import sha256
from pathlib import Path

import pytest
from pydantic import ValidationError

from dagster_v3.defs.website_crawl.input import CrawlInputConfig, selected_domains_sql
from dagster_v3.defs.website_crawl.workspace_domains import WorkspaceDomainFilters
from tests.test_website_crawl_input_assets import (  # noqa: F401
    add,
    database,
    processing_postgres_url,
    server,
    store,
)

SOURCE = {"source_relation": "corpscout.domains_search", "source_final": False,
          "id_column": "root_domain", "website_column": "root_domain"}


@pytest.fixture
def inventory(database):  # noqa: F811
    client, *_ = database
    for table in ("domains_company_filter", "domains", "domains_search", "domains_sources", "website_company_lookup_results"):
        client.execute(f"DROP TABLE IF EXISTS corpscout.{table}")
    client.execute("""CREATE TABLE corpscout.domains_search (
        root_domain String, sources Array(String), has_dns_records UInt8,
        has_website UInt8, observed_website_count UInt32, has_company UInt8,
        domain_id String MATERIALIZED lower(hex(SHA256(root_domain)))
    ) ENGINE=MergeTree ORDER BY root_domain""")
    client.execute("""CREATE TABLE corpscout.domains_sources (
        domain_id String, country_code String, company_id String, is_active UInt8,
        association String DEFAULT 'connected', source_table String DEFAULT 'se_company_domain'
    ) ENGINE=ReplacingMergeTree ORDER BY (domain_id,country_code,company_id)""")
    client.execute("""CREATE TABLE corpscout.website_company_lookup_results (
        domain String, status String
    ) ENGINE=MergeTree ORDER BY domain""")
    domains = [f"untouched-{i:02}.se" for i in range(31)] + [
        "linked.se", "rejected.se", "failed.se", "matched.se", "not-found.se", "cancelled.se",
        "already-mapped.se", "suffix.se.com", "other.no", "no-dns.se", "no-site.se",
    ]
    client.execute("INSERT INTO corpscout.domains_search VALUES", [
        (domain, ["commoncrawl", "commoncrawl_graph"], int(domain != "no-dns.se"),
         int(domain != "no-site.se"), int(domain != "no-site.se"), int(domain == "rejected.se"))
        for domain in domains
    ])
    client.execute("INSERT INTO corpscout.domains_search VALUES", [("one-source.se", ["commoncrawl_graph"], 1, 1, 1, 0)])
    # A stale snapshot says rejected is linked and linked is unassociated.
    client.execute("INSERT INTO corpscout.domains_sources (domain_id,country_code,company_id,is_active) VALUES", [
        (sha256(b"linked.se").hexdigest(), "SE", "1", 1), (sha256(b"linked.se").hexdigest(), "NO", "1", 1), (sha256(b"rejected.se").hexdigest(), "SE", "2", 0),
    ])
    client.execute("CREATE TABLE corpscout.domains (root_domain String,domain_id String MATERIALIZED lower(hex(SHA256(root_domain)))) ENGINE=MergeTree ORDER BY root_domain")
    client.execute("INSERT INTO corpscout.domains SELECT root_domain FROM corpscout.domains_search")
    client.execute("INSERT INTO corpscout.website_company_lookup_results VALUES", [
        ("failed.se", "failed"), ("matched.se", "matched"), ("matched.se", "matched"),
        ("not-found.se", "not_found"), ("cancelled.se", "cancelled"), ("already-mapped.se", "already_mapped"),
    ])
    migration = Path(__file__).resolve().parents[3] / "clickhouse/migrations/000463_corpscout_domain_source_readers.up.sql"
    sql = migration.read_text().split("CREATE MATERIALIZED VIEW", 1)[1]
    for statement in ("CREATE MATERIALIZED VIEW" + sql).split(";"):
        if statement.strip():
            client.execute(statement)
    return database


@pytest.mark.parametrize("crawl_type", ["site_info", "full", "jobs"])
def test_imports_all_pages_with_filters_and_exclusions(inventory, crawl_type):
    client, *_ = inventory
    receipt = add(inventory, crawl_type, **SOURCE, select_all=True,
                  excluded_ids=["untouched-05.se"], workspace_domain_filters={
                      "suffix": "se", "companies": "without", "company_matching": "without",
                      "dns": "with", "websites": "observed",
                      "sources": ["commoncrawl", "commoncrawl_graph"], "source_match": "all",
                  })
    actual = client.execute("SELECT domain FROM corpscout.website_crawl_task_domains WHERE task_id=%(task)s ORDER BY domain", {"task": receipt["task_id"]})
    expected = ["rejected.se"] + [f"untouched-{i:02}.se" for i in range(31) if i != 5]
    assert [row[0] for row in actual] == expected
    assert receipt["input_count"] == 31  # More than the UI's 25-row page.


def test_current_outcomes_change_membership_without_inventory_refresh(inventory):
    client, *_ = inventory
    config = CrawlInputConfig(**SOURCE, select_all=True, workspace_domain_filters={"prefix": "untouched-00", "company_matching": "without"})
    sql, params = selected_domains_sql(config)
    assert client.execute(sql, params) == [("untouched-00.se", "https://untouched-00.se")]
    client.execute("INSERT INTO corpscout.website_company_lookup_results VALUES", [("untouched-00.se", "failed")])
    assert client.execute(sql, params) == []


def test_positive_filters_and_source_matching(inventory):
    client, *_ = inventory
    sql, params = selected_domains_sql(CrawlInputConfig(**SOURCE, workspace_domain_filters={"company_matching": "with"}))
    assert [row[0] for row in client.execute(sql, params)] == ["already-mapped.se", "cancelled.se", "failed.se", "matched.se", "not-found.se"]
    sql, params = selected_domains_sql(CrawlInputConfig(**SOURCE, workspace_domain_filters={"companies": "with"}))
    assert [row[0] for row in client.execute(sql, params)] == ["linked.se"]
    config = {"prefix": "one-source", "sources": ["commoncrawl", "commoncrawl_graph"]}
    sql, params = selected_domains_sql(CrawlInputConfig(**SOURCE, workspace_domain_filters=config | {"source_match": "any"}))
    assert len(client.execute(sql, params)) == 1
    sql, params = selected_domains_sql(CrawlInputConfig(**SOURCE, workspace_domain_filters=config | {"source_match": "all"}))
    assert client.execute(sql, params) == []


def test_parameterized_prefix_and_explicit_inventory_ids(inventory):
    client, *_ = inventory
    config = CrawlInputConfig(**SOURCE, workspace_domain_filters={"prefix": "' OR 1=1 --"})
    sql, params = selected_domains_sql(config)
    assert "' OR 1=1 --" not in sql
    assert client.execute(sql, params) == []
    sql, params = selected_domains_sql(CrawlInputConfig(**SOURCE, ids=["rejected.se", "missing.se"]))
    assert client.execute(sql, params) == [("rejected.se", "https://rejected.se")]


def test_inventory_keeps_idna_and_deduplicates_www_hosts(inventory):
    client, *_ = inventory
    client.execute("INSERT INTO corpscout.domains_search VALUES", [
        (domain, ["commoncrawl"], 1, 1, 1, 0)
        for domain in ["www.rejected.se", "xn--rksmrgs-5wao1o.se"]
    ])
    config = CrawlInputConfig(**SOURCE, ids=["rejected.se", "www.rejected.se", "xn--rksmrgs-5wao1o.se"])
    sql, params = selected_domains_sql(config)
    assert client.execute(sql, params) == [
        ("rejected.se", "https://rejected.se"),
        ("xn--rksmrgs-5wao1o.se", "https://xn--rksmrgs-5wao1o.se"),
    ]
    sql, params = selected_domains_sql(config.model_copy(update={"max_domains": 1}))
    assert client.execute(sql, params) == [("rejected.se", "https://rejected.se")]


@pytest.mark.parametrize("filters", [
    {"company_matching": "bad"}, {"companies": "bad"}, {"sources": ["unknown"]},
    {"source_match": "bad"}, {"suffix": "se'"}, {"suffix": ".se"},
])
def test_invalid_filters_fail_closed(filters):
    with pytest.raises(ValidationError):
        WorkspaceDomainFilters(**filters)


@pytest.mark.parametrize("overrides", [
    {"source_relation": "corpscout.se_company_domain"}, {"source_final": True}, {"website_column": "url"},
])
def test_filters_require_global_inventory(overrides):
    with pytest.raises(ValidationError):
        CrawlInputConfig(**(SOURCE | overrides), select_all=True, workspace_domain_filters={"suffix": "se"})
