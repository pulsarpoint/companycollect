"""Saved matching, parent gates and replayable legacy copy on actual ClickHouse."""

from datetime import timedelta

import pytest
from clickhouse_driver import Client
from corpscout_identity.registration import (
    WebsiteObservation,
    identify_website,
    register_websites,
)

from dagster_v3.defs.se_company.basic_info.extract import ExtractConfig
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.backfill import (
    DomainClaimBackfillConfig,
    copy_legacy_claims,
)
from dagster_v3.defs.se_company.domain.batch import publish_domains
from dagster_v3.defs.se_company.domain.claims import insert_claims
from dagster_v3.defs.se_company.domain.crawler_lookup import crawler_lookup_live_sql
from dagster_v3.defs.se_company.domain.evidence import input_payload
from dagster_v3.defs.se_company.domain.suggestions import run_domain_source
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_domain_result_references import archive_url as archive_url
from tests.test_domain_result_references import migrate
from tests.test_domain_result_references import previous_schema as previous_schema
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)
from tests.test_se_company_domain import COMPANY, STAMP, suggestion


@pytest.fixture
def db(previous_schema, identity_postgres):
    migrate(previous_schema)
    return previous_schema


def run(client):
    return run_domain_source(
        client,
        source="crawler_lookup",
        live_sql=crawler_lookup_live_sql,
        config=ExtractConfig(execute=True),
        run_id="lookup-adapter",
        log=lambda *a: None,
    )


def lookup(
    client,
    status,
    *,
    index=1,
    company=COMPANY["company_id"],
    url="https://shop.example.se/",
    country="SE",
):
    identity = identify_website(url)
    stamp = STAMP + timedelta(days=index)
    register_websites(
        client,
        [WebsiteObservation(identity, stamp, None, None)],
        source="test",
        run_id="test",
    )
    row = dict(
        country=country,
        domain=identity.root_domain,
        website_id=identity.website_id,
        request_id=f"crawl-{index}",
        attempt=1,
        website_url=url,
        status=status,
        found=status == "matched",
        company_id=company if status == "matched" else "",
        confidence=0.96 if status == "matched" else None,
        reasons=["Organisation ID matched"],
        model="local-test",
        result_path=f"crawl-{index}/result.json",
        finished_at=stamp,
    )
    client.execute(
        f"INSERT INTO corpscout.website_company_lookup_results ({','.join(row)}) VALUES",
        [row],
    )
    return identity


def test_matching_latest_attempt_skip_failure_owner_change_and_withdrawal(db):
    client = db
    identity = lookup(client, "already_mapped")
    assert (
        run(client)["inserted"] == 0
    )  # Never manufacture evidence from the existing mapping.
    lookup(client, "matched", index=2)
    assert run(client)["inserted"] == 1
    assert run(client)["inserted"] == 0
    assert client.execute(
        "SELECT domain_id,website_id,source_record_id,association FROM corpscout.se_company_domain_sources FINAL"
    ) == [(identity.domain_id, identity.website_id, "crawl-2:1", "connected")]
    for index, status in enumerate(("failed", "cancelled", "already_mapped"), 3):
        lookup(client, status, index=index)
        assert run(client)["inserted"] == 0
    # A later owner supersedes the old company's source slot, not another source's evidence.
    second = "5560000002"
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info (company_id,legal_name) VALUES",
        [(second, "Other AB")],
    )
    insert_claims(client, [suggestion("esef_filing", domain="example.se")])
    lookup(client, "matched", index=6, company=second)
    assert run(client)["inserted"] == 2
    assert client.execute(
        "SELECT company_id,removed FROM corpscout.se_company_domain_sources FINAL WHERE source='crawler_lookup' ORDER BY company_id"
    ) == [(second, 0), (COMPANY["company_id"], 1)]
    lookup(client, "not_found", index=7)
    assert run(client)["inserted"] == 1
    assert run(client)["inserted"] == 0
    assert client.execute(
        "SELECT source FROM corpscout.se_company_domain_sources FINAL WHERE removed=0"
    ) == [("esef_filing",)]
    lookup(client, "matched", index=8, country="NO")
    assert run(client)["inserted"] == 0


def test_multiple_origins_keep_attempt_provenance_but_count_one_source(db):
    for index, url in enumerate(("https://example.se/", "https://www.example.se/"), 1):
        lookup(db, "matched", index=index, url=url)
    assert run(db)["inserted"] == 2
    counts = publish_domains(
        db,
        page_size=100,
        changed_only=True,
        profile=None,
        run_id="fold",
        log=lambda *a: None,
    )
    assert counts["domains_written"] >= 1
    assert db.execute(
        "SELECT supporting_sources,review_status FROM corpscout.se_company_domain FINAL WHERE root_domain='example.se'"
    ) == [(["crawler_lookup"], "unreviewed")]
    assert db.execute(
        "SELECT uniqExact(website_id),uniqExact(source_record_id) FROM corpscout.se_company_domain_sources FINAL"
    ) == [(2, 2)]


def test_registration_failure_never_publishes_claim_and_saved_page_can_retry(
    db, monkeypatch
):
    rows = [suggestion(domain="fresh.se")]
    monkeypatch.delenv("PROCESSING_PG_URL")
    with pytest.raises(ValueError, match="PROCESSING_PG_URL"):
        insert_claims(db, rows)
    assert db.execute("SELECT count() FROM corpscout.se_company_domain_sources") == [
        (0,)
    ]
    # The caller can retain/replay the same page; original input is not mutated.
    assert "domain_id" not in rows[0]


def test_invalid_reference_rejects_entire_page_before_registration(db):
    with pytest.raises(ValueError, match="domain reference"):
        insert_claims(
            db,
            [
                suggestion(domain="first.se"),
                suggestion(domain="second.se", domain_id="wrong"),
            ],
        )
    assert db.execute(
        "SELECT count() FROM corpscout.domains WHERE root_domain='first.se'"
    ) == [(0,)]
    with pytest.raises(ValueError, match="website reference"):
        insert_claims(db, [suggestion("crawler_lookup", slot="wrong")])


def test_backfill_resume_preserves_withdrawals_reviews_and_paid_input(db):
    rows = [
        suggestion(),
        suggestion("reviewer", domain="manual.se"),
        suggestion("brave", domain="gone.se", removed=1, company_id="5561552761"),
    ]
    db.execute(
        f"INSERT INTO corpscout.se_company_domain_suggestion ({','.join(tables.SUGGESTION_COLUMNS)}) VALUES",
        [tuple(row[column] for column in tables.SUGGESTION_COLUMNS) for row in rows],
    )
    config = DomainClaimBackfillConfig(execute=True, max_companies=1, page_size=1)
    first = copy_legacy_claims(db, config=config, log=lambda *a: None)
    assert first["companies"] == 1 and not first["complete"]
    second = copy_legacy_claims(
        db,
        config=config.model_copy(
            update={"after_company_id": first["after_company_id"]}
        ),
        log=lambda *a: None,
    )
    assert second["complete"]
    # Replay after a lost checkpoint doesn't create another current claim.
    copy_legacy_claims(
        db, config=DomainClaimBackfillConfig(execute=True), log=lambda *a: None
    )
    stored = [
        dict(zip(tables.SUGGESTION_COLUMNS, row, strict=True))
        for row in db.execute(
            f"SELECT {','.join(tables.SUGGESTION_COLUMNS)} FROM corpscout.se_company_domain_sources_resolved FINAL ORDER BY company_id,source,slot"
        )
    ]
    assert len(stored) == len(rows)
    original = {row["slot"]: row for row in rows}
    assert all(row == original[row["slot"]] for row in stored)
    assert input_payload(COMPANY, "example.se", [rows[0]]) == input_payload(
        COMPANY,
        "example.se",
        [next(row for row in stored if row["source"] == "wikidata")],
    )
    assert db.execute(
        "SELECT review_status FROM corpscout.se_company_domain_resolved WHERE root_domain='legacy.se'"
    ) == [("confirmed_related",)]
    counts = publish_domains(
        db,
        page_size=100,
        changed_only=True,
        profile=None,
        run_id="after-copy",
        log=lambda *a: None,
    )
    assert counts["domains_written"] >= 2
    assert db.execute(
        "SELECT review_status,is_active FROM corpscout.se_company_domain_resolved WHERE root_domain='legacy.se'"
    ) == [("confirmed_related", 1)]

    newer = {
        **rows[0],
        "removed": 1,
        "suggested_at": STAMP + timedelta(days=30),
        "suggestion_id": "newer-source-withdrawal",
        "source_run_id": "later-run",
    }
    insert_claims(db, [newer])
    copy_legacy_claims(
        db, config=DomainClaimBackfillConfig(execute=True), log=lambda *a: None
    )
    assert db.execute(
        "SELECT removed,suggestion_id FROM corpscout.se_company_domain_sources FINAL WHERE source='wikidata'"
    ) == [(1, "newer-source-withdrawal")]


def test_saved_claim_replay_after_lost_insert_acknowledgement(db, monkeypatch):
    lookup(db, "matched")
    original = Client.execute
    fail = True

    def lost_ack(self, query, *args, **kwargs):
        nonlocal fail
        result = original(self, query, *args, **kwargs)
        if fail and query.startswith(
            "INSERT INTO corpscout.se_company_domain_sources "
        ):
            fail = False
            raise RuntimeError("lost claim acknowledgement")
        return result

    monkeypatch.setattr(Client, "execute", lost_ack)
    with pytest.raises(RuntimeError, match="lost claim acknowledgement"):
        run(db)
    assert run(db)["inserted"] == 0
    assert db.execute(
        "SELECT count() FROM corpscout.se_company_domain_sources FINAL"
    ) == [(1,)]
    assert db.execute(
        "SELECT count(),uniqExact(domain_id) FROM corpscout.domains WHERE root_domain='example.se'"
    ) == [(1, 1)]


def test_wikidata_legacy_host_and_duplicate_scheme_keep_source_identity(db):
    row = suggestion("wikidata", domain="http:", slot="original-source-slot",
        website_url="https://http://news.example.se/about")
    insert_claims(db, [row])
    identity = identify_website("http://news.example.se/about")
    stored = db.execute("""SELECT slot,domain_id,website_id,website_url,evidence
        FROM corpscout.se_company_domain_sources FINAL""")
    assert stored == [(row["slot"], identity.domain_id, identity.website_id,
        "http://news.example.se/about", row["evidence"])]
    assert row["root_domain"] == "http:"
