from pathlib import Path

import pytest

from dagster_v3.defs.company_serving import tables
from dagster_v3.defs.company_serving.publish import (
    _insert_company_domain_stage,
    _validate_presence_counts,
    publish_company_serving_country,
)


MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse"
    / "migrations"
    / "000267_corpscout_company_serving_tables.up.sql"
)
COMPANY_DOMAINS_MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse"
    / "migrations"
    / "000269_corpscout_company_domains.up.sql"
)


class _EmptyServingClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str, params: object | None = None):
        self.queries.append(query)
        if query.lstrip().startswith("SELECT"):
            return [(0,)]
        return []


def test_empty_required_stage_never_replaces_a_live_partition() -> None:
    client = _EmptyServingClient()

    with pytest.raises(ValueError, match="empty required serving table"):
        publish_company_serving_country(
            client,
            country_code="SE",
            source_run_id="test-run",
        )

    assert not any("REPLACE PARTITION" in query for query in client.queries)
    assert not any("EXCHANGE TABLES" in query for query in client.queries)


def test_migration_owns_all_serving_and_history_tables() -> None:
    sql = MIGRATION.read_text()
    for contract in tables.CURRENT_TABLES:
        owner_sql = (
            COMPANY_DOMAINS_MIGRATION.read_text() if contract is tables.DOMAINS else sql
        )
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{contract.name}" in owner_sql
        if contract.partitioned:
            assert "PARTITION BY country_code" in owner_sql
    for observation_table in tables.HISTORY_TABLES.values():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{observation_table}" in sql
    assert "company_section_item_source_links" in sql
    assert "state_fingerprint FixedString(64)" in sql
    assert "has_observation UInt8" in sql


def test_presence_is_published_after_every_backing_table() -> None:
    assert tables.CURRENT_TABLES[-1] is tables.PRESENCE
    assert tables.CURRENT_TABLES[-2] is tables.DOMAINS
    assert tables.PRESENCE.required
    assert set(tables.VALID_SECTIONS) == {
        "gleif",
        "wikidata",
        "management",
        "descriptions",
        "domains",
        "contracts",
        "financials",
        "industries",
        "addresses",
        "sources",
        "technology",
    }


def test_presence_reconciliation_uses_logical_item_keys() -> None:
    client = _EmptyServingClient()
    stages = {
        contract.name: f"stage_{contract.name}" for contract in tables.CURRENT_TABLES
    }

    _validate_presence_counts(client, stages=stages, country_code="SE")

    sql = "\n".join(client.queries)
    assert "concat('entity:', lei)" in sql
    assert "concat('relationship:', relationship_id)" in sql
    assert "concat('domain:', root_domain)" in sql
    assert "concat('contact:', contact_id)" in sql
    assert "countDistinct(tuple(company_id, classification_code))" in sql
    assert (
        "SELECT DISTINCT company_id FROM stage_company_external_identifier_current"
        in sql
    )


def test_domain_stage_overlays_current_reviews_before_versioned_publish() -> None:
    client = _EmptyServingClient()

    _insert_company_domain_stage(
        client,
        stage="corpscout.stage_company_domains",
        country_code="SE",
    )

    sql = client.queries[0]
    assert "FROM corpscout.company_domains_build AS staged" in sql
    assert "LEFT JOIN corpscout.company_domains AS current FINAL" in sql
    assert "current.review_status" in sql
    assert "current.reviewed_evidence_fingerprint" in sql
    assert "now64(3, 'UTC')" in sql
