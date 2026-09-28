"""Brave extraction boundaries and checkpoint recovery on a real ClickHouse engine."""

from contextlib import nullcontext
from datetime import timedelta

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.domain import brave
from dagster_v3.defs.se_company.domain.claims import insert_claims
from tests.company_domain_source_schema import claim_schema_sql
from tests.domain_sources_schema import execute_sql
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

pytestmark = pytest.mark.usefixtures("identity_postgres")
from tests.test_se_company_domain import STAMP, suggestion
from tests.test_se_company_domain_clickhouse_local import setup_sql
from tests.test_se_company_financial_fold_clickhouse_local import _literal


@pytest.mark.parametrize(
    "answer,expected",
    [
        ("The official website is www.example.se.", ["example.se"]),
        (
            "[www.kbcomponents.com](https://www.kbcomponents.com/) and investors.kbcomponents.com.",
            ["kbcomponents.com"],
        ),
        (
            "https://example.co.uk/path/foo.com?q=bar.se and https://other.se/",
            ["example.co.uk", "other.se"],
        ),
        ("www.räksmörgås.se", ["xn--rksmrgs-5wao1o.se"]),
        (
            "info@example.se ftp://files.example.com https://user:pass@example.com https://127.0.0.1 co.uk foo.zip",
            [],
        ),
        ("No official website found.", []),
        (
            "Official: company.se. Unrelated: namesake.com.",
            ["company.se", "namesake.com"],
        ),
    ],
)
def test_domain_extraction(answer, expected):
    assert brave.extract_domains(answer) == expected


class LocalClient:
    """Replay persisted SQL through clickhouse-local; preserve real driver value types."""

    def __init__(self, join_use_nulls, native):
        self.client = native
        execute_sql(native, "DROP DATABASE corpscout SYNC; CREATE DATABASE corpscout;")
        execute_sql(native, setup_sql() + claim_schema_sql())
        native.execute(f"SET join_use_nulls={join_use_nulls}")
        native.execute("""CREATE TABLE corpscout.se_company_brave_search_results_latest_success (
            company_id String, result_id String, answer_text String, query String, source_url String,
            completed_at DateTime64(6,'UTC'), country_code String DEFAULT 'SE',
            query_type String DEFAULT 'official_website', status String DEFAULT 'success'
        ) ENGINE=ReplacingMergeTree(completed_at) ORDER BY (company_id, query_type)""")
        self.fail_checkpoint = False

    def execute(self, sql, params=None, settings=None):
        if sql.startswith(f"INSERT INTO corpscout.{brave.CHECKPOINT_TABLE} ") and self.fail_checkpoint:
            raise RuntimeError("Checkpoint unavailable")
        return self.client.execute(sql, params, settings=settings)

    def answer(self, company_id, result_id, answer, day=0, **extra):
        values = dict(
            company_id=company_id,
            result_id=result_id,
            answer_text=answer,
            query="Find the official website.",
            source_url="https://search.brave.com/search?q=company",
            completed_at=STAMP + timedelta(days=day),
            **extra,
        )
        self.execute(
            f"INSERT INTO corpscout.se_company_brave_search_results_latest_success ({','.join(values)}) VALUES",
            [tuple(values.values())],
        )


@pytest.mark.integration
@pytest.mark.parametrize("preview", [False, True])
def test_materialization_saves_by_default_and_preserves_explicit_preview(monkeypatch, preview, graph_ch):
    client = LocalClient(join_use_nulls=0, native=graph_ch)
    client.answer("5561552760", "with-domain", "Official website: www.example.se")
    client.answer("5560000002", "without-domain", "No official website found.")
    monkeypatch.setattr(ClickhouseResource, "get_connection", lambda self: nullcontext(client))
    asset = brave.se_company_domain_suggestions_brave
    run_config = (
        {"ops": {"se_company_domain_suggestions_brave": {"config": {"execute": False}}}}
        if preview
        else {}
    )

    result = dg.materialize(
        [asset, *(dg.AssetSpec(key) for key in asset.dependency_keys)],
        run_config=run_config,
        resources={
            "clickhouse": ClickhouseResource(
                host="localhost", user="test", password="", database="corpscout"
            )
        },
    )

    assert result.success
    metadata = result.asset_materializations_for_node("se_company_domain_suggestions_brave")[0].metadata
    assert metadata["domains"].value == 1
    assert metadata["empty_answers"].value == 1
    assert metadata["suggestions_written"].value == (0 if preview else 1)
    assert metadata["checkpoints_written"].value == (0 if preview else 2)
    assert client.execute(
        "SELECT root_domain FROM corpscout.se_company_domain_sources_resolved FINAL WHERE source='brave'"
    ) == ([] if preview else [("example.se",)])
    assert client.execute(
        f"SELECT domains_json FROM corpscout.{brave.CHECKPOINT_TABLE} FINAL ORDER BY company_id"
    ) == ([] if preview else [("[]",), ('["example.se"]',)])


@pytest.mark.integration
@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_incremental_checkpoint_replay_and_empty_withdrawal(join_use_nulls, graph_ch):
    client = LocalClient(join_use_nulls, native=graph_ch)
    company, empty, other = "5561552760", "5560000002", "5560000003"
    client.answer(
        company, "z-first", "Official: www.example.se. Also unrelated: namesake.com."
    )
    client.answer(empty, "empty", "No official website found.")
    client.answer(other, "other-query", "https://ignore.se", query_type="other")
    existing = suggestion()
    insert_claims(client, [existing])

    def run(**config):
        return brave.process_brave_answers(
            client,
            config=brave.BraveExtractConfig(**config),
            run_id="test",
            log=lambda *args: None,
        )

    assert run(execute=False)["companies"] == 2
    assert (
        client.execute(f"SELECT count() FROM corpscout.{brave.CHECKPOINT_TABLE}")[0][0]
        == 0
    )
    client.fail_checkpoint = True
    with pytest.raises(RuntimeError, match="Checkpoint unavailable"):
        run(execute=True)
    assert (
        client.execute(
            "SELECT count() FROM corpscout.se_company_domain_sources_resolved FINAL WHERE source='brave'"
        )[0][0]
        == 2
    )
    client.fail_checkpoint = False
    replay = run(execute=True)
    assert replay["companies"] == 2 and replay["suggestions_written"] == 0
    assert run(execute=True)["companies"] == 0
    assert client.execute(
        f"SELECT domains_json FROM corpscout.{brave.CHECKPOINT_TABLE} FINAL WHERE company_id={_literal(empty)}"
    ) == [("[]",)]
    rows = client.execute(
        "SELECT association,evidence FROM corpscout.se_company_domain_sources_resolved FINAL WHERE source='brave'"
    )
    assert all(
        association == "uncertain" and "unrelated" in evidence
        for association, evidence in rows
    )
    # UUID-like IDs need not increase; a later response withdraws only Brave's rows.
    client.answer(company, "a-later", "No website found.", day=1)
    result = run(execute=True, page_size=1)
    assert result["companies"] == 1 and result["suggestions_written"] == 2
    assert client.execute(
        "SELECT source,removed FROM corpscout.se_company_domain_sources_resolved FINAL ORDER BY source,slot"
    ) == [("brave", 1), ("brave", 1), ("wikidata", 0)]
    assert run(execute=True)["companies"] == 0
    # Detect changed content even if a result ID was reused.
    client.answer(company, "a-later", "www.changed.se", day=2)
    assert run(execute=True)["domains"] == 1
    assert run(execute=True)["companies"] == 0
    # Changing extractor version revisits all successful official-website answers.
    assert (
        len(client.execute(brave.CHANGED_SQL, {"extractor_version": "next-version"}))
        == 2
    )
    assert (
        client.execute(
            "SELECT count() FROM system.tables WHERE name LIKE '_tmp_brave_domain_scope_%'"
        )[0][0]
        == 0
    )
