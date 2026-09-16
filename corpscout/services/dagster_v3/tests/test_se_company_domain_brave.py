"""Brave extraction boundaries and checkpoint recovery on a real ClickHouse engine."""

import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from dagster_v3.defs.se_company.basic_info.extract import ExtractConfig
from dagster_v3.defs.se_company.domain import brave, tables
from tests.clickhouse_local import clickhouse_local_command
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

    def __init__(self, join_use_nulls):
        self.statements = [
            f"SET join_use_nulls={join_use_nulls};",
            setup_sql(),
            """
CREATE TABLE corpscout.se_company_brave_domains (
 company_id String, result_id String, answer_text String, query String, source_url String,
 completed_at DateTime64(6,'UTC'), country_code String DEFAULT 'SE',
 query_type String DEFAULT 'official_website', status String DEFAULT 'success'
) ENGINE=ReplacingMergeTree(completed_at) ORDER BY (company_id, query_type);
""",
        ]
        self.fail_checkpoint = False

    def execute(self, sql, params=None, settings=None):
        if isinstance(params, list):
            if self.fail_checkpoint and brave.CHECKPOINT_TABLE in sql:
                raise RuntimeError("Checkpoint unavailable")
            sql += " " + ",".join(_literal(tuple(row)) for row in params)
        else:
            for key, value in (params or {}).items():
                sql = sql.replace(f"%({key})s", _literal(value))
        assert "%(" not in sql
        if sql.startswith(("INSERT", "CREATE", "DROP", "ALTER")):
            self.statements.append(sql + ";")
            # Validate writes immediately, as the real driver does.
            self.execute("SELECT 1")
            return []
        result = subprocess.run(
            clickhouse_local_command(),
            input="\n".join(self.statements) + "\n" + sql + " FORMAT JSON;",
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        result = json.loads(result.stdout)
        rows = []
        for row in result["data"]:
            values = []
            for column in result["meta"]:
                value = row[column["name"]]
                if "DateTime" in column["type"] and value is not None:
                    value = datetime.fromisoformat(value).replace(tzinfo=UTC)
                values.append(value)
            rows.append(tuple(values))
        return rows

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
            f"INSERT INTO corpscout.se_company_brave_domains ({','.join(values)}) VALUES",
            [tuple(values.values())],
        )


@pytest.mark.integration
@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_incremental_checkpoint_replay_and_empty_withdrawal(join_use_nulls):
    client = LocalClient(join_use_nulls)
    company, empty, other = "5561552760", "5560000002", "5560000003"
    client.answer(
        company, "z-first", "Official: www.example.se. Also unrelated: namesake.com."
    )
    client.answer(empty, "empty", "No official website found.")
    client.answer(other, "other-query", "https://ignore.se", query_type="other")
    existing = suggestion()
    client.execute(
        f"INSERT INTO corpscout.{tables.SUGGESTION_TABLE} ({','.join(tables.SUGGESTION_COLUMNS)}) VALUES",
        [tuple(existing[column] for column in tables.SUGGESTION_COLUMNS)],
    )

    def run(**config):
        return brave.process_brave_answers(
            client,
            config=ExtractConfig(**config),
            run_id="test",
            log=lambda *args: None,
        )

    assert run()["companies"] == 2
    assert (
        client.execute(f"SELECT count() FROM corpscout.{brave.CHECKPOINT_TABLE}")[0][0]
        == 0
    )
    client.fail_checkpoint = True
    with pytest.raises(RuntimeError, match="Checkpoint unavailable"):
        run(execute=True)
    assert (
        client.execute(
            "SELECT count() FROM corpscout.se_company_domain_suggestion FINAL WHERE source='brave'"
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
        "SELECT association,evidence FROM corpscout.se_company_domain_suggestion FINAL WHERE source='brave'"
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
        "SELECT source,removed FROM corpscout.se_company_domain_suggestion FINAL ORDER BY source,slot"
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
