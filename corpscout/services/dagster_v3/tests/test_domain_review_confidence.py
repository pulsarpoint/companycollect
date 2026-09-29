"""Reviewer decisions survive source republishing and affect the company filter."""
from datetime import UTC, datetime, timedelta

import pytest
from clickhouse_driver.errors import ServerException

from tests.domain_sources_schema import MIGRATIONS, execute_sql
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_domain_sources import association, insert_company, publish
from tests.test_domain_sources import compact_database as compact_database
from tests.test_domain_sources import database as database
from tests.test_domain_sources import server as server
from tests.test_domains_company_filter import refresh
from tests.test_processing_store import processing_postgres_url as processing_postgres_url

pytestmark = pytest.mark.usefixtures("identity_postgres")
MIGRATION = (MIGRATIONS / "000469_corpscout_domain_review_confidence.up.sql").read_text()
STAMP = datetime(2026, 9, 29, tzinfo=UTC)


@pytest.fixture
def reviewed_database(compact_database):
    execute_sql(compact_database, MIGRATION)
    return compact_database


def review(client, action, offset, score=None, removed=0):
    client.execute("INSERT INTO corpscout.se_company_domain_rule (company_id,root_domain,action,removed,decided_by,note,evidence_hash,decided_at,confidence_override) VALUES",
                   [("5561234567", "new.se", action, removed, "operator", "Checked", "old-evidence", STAMP + timedelta(seconds=offset), score)])


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_rejection_outlives_source_updates_and_only_removes_that_company(reviewed_database, join_use_nulls):
    client = reviewed_database
    client.execute("SET join_use_nulls=%(value)s", {"value": join_use_nulls})
    row = association()
    insert_company(client, row)
    insert_company(client, association(company="5569999999"))
    publish(client)
    assert refresh(client)["new.se"] == 2
    before = client.execute("SELECT * FROM corpscout.domains_sources ORDER BY domain_id,source_table")
    review(client, "rejected", 0)
    assert refresh(client)["new.se"] == 1
    insert_company(client, row | {"confidence": 1., "folded_at": STAMP + timedelta(seconds=1)})
    assert client.execute("SELECT is_active,is_removed FROM corpscout.se_company_domain_resolved WHERE company_id='5561234567'") == [(0, 1)]
    assert refresh(client)["new.se"] == 1
    assert client.execute("SELECT * FROM corpscout.domains_sources ORDER BY domain_id,source_table") == before
    review(client, "unreviewed", 2, removed=1)
    assert refresh(client)["new.se"] == 2


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_override_zero_survives_new_scores_and_clear_uses_latest_source_value(reviewed_database, join_use_nulls):
    client = reviewed_database
    client.execute("SET join_use_nulls=%(value)s", {"value": join_use_nulls})
    row = association() | {"source_confidences": [.95], "sources": ["wikidata"], "source_record_ids": ["Q1"], "source_urls": ["https://example.test"], "confidence_bases": ["official_website"]}
    insert_company(client, row)
    review(client, "unreviewed", 0, score=0)
    def resolved():
        return client.execute("SELECT suggested_confidence,confidence_override,is_active,is_removed FROM corpscout.se_company_domain_resolved WHERE company_id='5561234567'")
    assert resolved() == [(0., 0., 1, 0)]
    insert_company(client, row | {"confidence": .99, "folded_at": STAMP + timedelta(seconds=1)})
    assert resolved() == [(0., 0., 1, 0)]
    assert client.execute("SELECT confidence,source_confidences FROM corpscout.se_company_domain FINAL WHERE company_id='5561234567'") == [(.99, [.95])]
    review(client, "unreviewed", 2, removed=1)
    assert resolved()[0][0] == pytest.approx(.99)
    assert resolved()[0][1:] == (None, 1, 0)


def test_migration_is_repeatable_and_rejects_invalid_scores(reviewed_database):
    client = reviewed_database
    execute_sql(client, MIGRATION)
    for score in (-.1, 1.1, float("nan"), float("inf")):
        with pytest.raises(ServerException, match="valid_confidence_override"):
            review(client, "unreviewed", 0, score=score)


def test_only_rejected_pair_is_absent_from_filter(reviewed_database):
    client = reviewed_database
    insert_company(client, association())
    assert refresh(client)["new.se"] == 1
    review(client, "rejected", 0)
    assert "new.se" not in refresh(client)
    review(client, "confirmed_related", 1, score=.8)
    assert refresh(client)["new.se"] == 1
