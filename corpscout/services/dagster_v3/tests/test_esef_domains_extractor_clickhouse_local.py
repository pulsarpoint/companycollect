"""The esef_domains extractor's selection SQL against a real ClickHouse (clickhouse-local).

A text assertion on the SQL string cannot see whether the LEFT JOIN's stale predicate
actually finds every never-extracted document once the engine's `join_use_nulls` setting
turns the unmatched side into NULL instead of the type default -- that is exactly the bug
`ifNull(extracted.extractor_version, '')` fixes (see domains_extractor.py). This runs
`stale_documents_sql`/`stale_document_count_sql`/`retry_failed_documents_sql`/
`tagged_website_facts_sql`/`email_domains_sql` on the engine over corpscout.esef_filings
(migration 000149), corpscout.esef_facts and corpscout.esef_document_contact_candidates (the
esef_facts_v2 / esef_document_contact_candidates_v2 tables from migration 000309, renamed the
way migration 000313 renames them in production) and corpscout.esef_domains (migration 000405).

Fixture documents cover every predicate leg (their facts settled long ago unless noted):
- FXO_CURRENT: an esef_domains row at the current version and package -- not stale.
- FXO_STALE_VERSION: an esef_domains row at another version -- stale.
- FXO_CHANGED_PACKAGE: a row at the current version for another package_sha256 -- stale.
- FXO_NO_DOMAINS: parsed, no esef_domains row at all -- stale.
- FXO_RETRY_FAILED: a `failed` marker row at the current version -- not stale; what
  retry_failed re-runs.
- FXO_FRESH_FACTS: parsed, but its newest fact is under an hour old -- never selected (the
  publish multi-asset may not have written its contact candidates yet).
- FXO_NO_FACTS: an esef_filings row but no esef_facts row -- never selected (INNER JOIN).
- FXO_EMPTY_SHA: parsed, but package_sha256 = '' -- never selected.
- FXO_FUTURE: parsed, but period_end is in the future -- never selected.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
)
from dagster_v3.defs.esef_filings.domains_extractor import (
    email_domains_sql,
    retry_failed_documents_sql,
    stale_document_count_sql,
    stale_documents_sql,
    tagged_website_facts_sql,
)
from dagster_v3.defs.esef_filings.website_candidates import WEBSITE_FACT_CONCEPTS
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FILINGS_MIGRATION = "000149_corpscout_esef_filings.up.sql"
PARSING_V2_MIGRATION = "000309_corpscout_esef_parsing_v2.up.sql"
DOMAINS_MIGRATION = "000405_corpscout_esef_domains.up.sql"

FXO_CURRENT = "fxo-current"
FXO_STALE_VERSION = "fxo-stale-version"
FXO_CHANGED_PACKAGE = "fxo-changed-package"
FXO_NO_DOMAINS = "fxo-no-domains"
FXO_RETRY_FAILED = "fxo-retry-failed"
FXO_FRESH_FACTS = "fxo-fresh-facts"
FXO_NO_FACTS = "fxo-no-facts"
FXO_EMPTY_SHA = "fxo-empty-sha"
FXO_FUTURE = "fxo-future"

STALE_VERSION = "esef-domains-v0"
LEI = "LEI1"
# esef_facts.resolved_at: long settled, and just written (inside the one-hour window).
SETTLED = "toDateTime64('2024-01-01 00:00:00', 3)"
FRESH = "now64(3)"


def _statements(migration: str) -> list[str]:
    text = (MIGRATIONS_DIR / migration).read_text(encoding="utf-8")
    return [
        "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        for raw in text.split(";")
    ]


def _schema_statements() -> list[str]:
    """Only the tables the selection SQL touches -- named the way production names them
    after migration 000313 renames esef_facts_v2/esef_document_contact_candidates_v2."""
    statements: list[str] = []
    for statement in _statements(FILINGS_MIGRATION):
        if statement.startswith("CREATE DATABASE") or statement.startswith(
            "CREATE TABLE IF NOT EXISTS corpscout.esef_filings"
        ):
            statements.append(statement)
    for statement in _statements(PARSING_V2_MIGRATION):
        if statement.startswith("CREATE TABLE IF NOT EXISTS corpscout.esef_facts_v2"):
            statements.append(
                statement.replace("corpscout.esef_facts_v2", "corpscout.esef_facts")
            )
        elif statement.startswith(
            "CREATE TABLE IF NOT EXISTS corpscout.esef_document_contact_candidates_v2"
        ):
            statements.append(
                statement.replace(
                    "corpscout.esef_document_contact_candidates_v2",
                    "corpscout.esef_document_contact_candidates",
                )
            )
    for statement in _statements(DOMAINS_MIGRATION):
        if statement.startswith("CREATE TABLE IF NOT EXISTS corpscout.esef_domains"):
            statements.append(statement)
    return statements


def _filings_row(fxo_id: str, period_end: str, package_sha256: str) -> str:
    return f"('{fxo_id}', '{LEI}', toDate32('{period_end}'), '{package_sha256}')"


def _facts_row(
    fxo_id: str,
    period_end: str,
    fact_id: str,
    concept_local_name: str,
    raw_value: str,
    resolved_at: str = SETTLED,
) -> str:
    return (
        f"('{fxo_id}', '{LEI}', toDate32('{period_end}'), '{fact_id}', "
        f"'{concept_local_name}', '{raw_value}', toDate('2024-01-01'), {resolved_at})"
    )


def _domains_row(
    fxo_id: str,
    period_end: str,
    package_sha256: str,
    version: str,
    status: str = "ok",
) -> str:
    return (
        f"('{fxo_id}', '{fxo_id}', '{package_sha256}', '{LEI}', "
        f"toDate32('{period_end}'), '{status}', '{version}', now64(3, 'UTC'))"
    )


INSERTS = (
    "INSERT INTO corpscout.esef_filings (fxo_id, lei, period_end, package_sha256) VALUES "
    + ", ".join(
        [
            _filings_row(FXO_CURRENT, "2024-06-30", "a" * 64),
            _filings_row(FXO_STALE_VERSION, "2024-12-31", "b" * 64),
            _filings_row(FXO_CHANGED_PACKAGE, "2024-09-30", "f" * 64),
            _filings_row(FXO_NO_DOMAINS, "2023-12-31", "c" * 64),
            _filings_row(FXO_RETRY_FAILED, "2022-06-30", "8" * 64),
            _filings_row(FXO_FRESH_FACTS, "2023-06-30", "9" * 64),
            _filings_row(FXO_NO_FACTS, "2022-12-31", "d" * 64),
            _filings_row(FXO_EMPTY_SHA, "2021-12-31", ""),
            _filings_row(FXO_FUTURE, "2099-12-31", "e" * 64),
        ]
    ),
    "INSERT INTO corpscout.esef_facts "
    "(fxo_id, lei, period_end, fact_id, concept_local_name, raw_value, processed_week, "
    "resolved_at) VALUES "
    + ", ".join(
        [
            _facts_row(FXO_CURRENT, "2024-06-30", "f1", "Assets", "100"),
            _facts_row(
                FXO_STALE_VERSION,
                "2024-12-31",
                "f2",
                "WebsitesOfLegalEntity",
                "https://example.com",
            ),
            _facts_row(FXO_CHANGED_PACKAGE, "2024-09-30", "f6", "Assets", "100"),
            _facts_row(FXO_NO_DOMAINS, "2023-12-31", "f3", "Assets", "100"),
            _facts_row(FXO_RETRY_FAILED, "2022-06-30", "f7", "Assets", "100"),
            # FXO_FRESH_FACTS: one settled fact and one just written -- the newest decides.
            _facts_row(FXO_FRESH_FACTS, "2023-06-30", "f8", "Assets", "100"),
            _facts_row(
                FXO_FRESH_FACTS, "2023-06-30", "f9", "Equity", "50", resolved_at=FRESH
            ),
            # FXO_NO_FACTS intentionally has no row here.
            _facts_row(FXO_EMPTY_SHA, "2021-12-31", "f4", "Assets", "100"),
            _facts_row(FXO_FUTURE, "2099-12-31", "f5", "Assets", "100"),
        ]
    ),
    "INSERT INTO corpscout.esef_domains "
    "(domain_id, source_document_id, package_sha256, lei, period_end, extraction_status, "
    "extractor_version, extracted_at) VALUES "
    + ", ".join(
        [
            _domains_row(
                FXO_CURRENT, "2024-06-30", "a" * 64, ESEF_DOMAINS_EXTRACTOR_VERSION
            ),
            _domains_row(FXO_STALE_VERSION, "2024-12-31", "b" * 64, STALE_VERSION),
            # Extracted at the current version, but from a package the index no longer has.
            _domains_row(
                FXO_CHANGED_PACKAGE,
                "2024-09-30",
                "0" * 64,
                ESEF_DOMAINS_EXTRACTOR_VERSION,
            ),
            _domains_row(
                FXO_RETRY_FAILED,
                "2022-06-30",
                "8" * 64,
                ESEF_DOMAINS_EXTRACTOR_VERSION,
                status="failed",
            ),
            # FXO_NO_DOMAINS and FXO_FRESH_FACTS intentionally have no row here.
        ]
    ),
    "INSERT INTO corpscout.esef_document_contact_candidates "
    "(source_document_id, candidate_kind, normalized_value, processed_week) VALUES "
    f"('{FXO_STALE_VERSION}', 'email', 'ir@example.com', toDate('2024-01-01'))",
)


def _script() -> str:
    stale_sql = render(
        stale_documents_sql(all_available=False, only_listed=False),
        {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION},
    )
    all_sql = render(stale_documents_sql(all_available=True, only_listed=False), {})
    count_sql = render(
        stale_document_count_sql(),
        {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION},
    )
    retry_sql = render(
        retry_failed_documents_sql(),
        {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION},
    )
    tagged_sql = render(
        tagged_website_facts_sql(),
        {
            "source_document_ids": (FXO_STALE_VERSION,),
            "concepts": tuple(sorted(WEBSITE_FACT_CONCEPTS)),
        },
    )
    email_sql = render(
        email_domains_sql(), {"source_document_ids": (FXO_STALE_VERSION,)}
    )
    parts = [
        *_schema_statements(),
        *INSERTS,
        "SET join_use_nulls = 0",
        "SELECT '@@stale_0'",
        stale_sql + " FORMAT TSV",
        "SET join_use_nulls = 1",
        "SELECT '@@stale_1'",
        stale_sql + " FORMAT TSV",
        "SELECT '@@all'",
        all_sql + " FORMAT TSV",
        "SELECT '@@count'",
        count_sql + " FORMAT TSV",
        "SELECT '@@retry'",
        retry_sql + " FORMAT TSV",
        "SELECT '@@tagged'",
        tagged_sql + " FORMAT TSV",
        "SELECT '@@emails'",
        email_sql + " FORMAT TSV",
    ]
    return ";\n".join(parts) + ";\n"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    script = _script()
    try:
        completed = subprocess.run(
            clickhouse_local_command(),
            input=script,
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def test_stale_documents_sql_is_correct_under_both_join_use_nulls_settings(
    sections: dict[str, list[list[str]]],
) -> None:
    # Newest period_end first; a changed package is stale at the current version too.
    expected = [FXO_STALE_VERSION, FXO_CHANGED_PACKAGE, FXO_NO_DOMAINS]

    assert [row[0] for row in sections["stale_0"]] == expected
    assert [row[0] for row in sections["stale_1"]] == expected


def test_all_available_variant_keeps_every_settled_document(
    sections: dict[str, list[list[str]]],
) -> None:
    assert [row[0] for row in sections["all"]] == [
        FXO_STALE_VERSION,
        FXO_CHANGED_PACKAGE,
        FXO_CURRENT,
        FXO_NO_DOMAINS,
        FXO_RETRY_FAILED,
    ]


def test_documents_whose_facts_are_under_an_hour_old_are_not_available(
    sections: dict[str, list[list[str]]],
) -> None:
    for name in ("stale_0", "stale_1", "all"):
        assert FXO_FRESH_FACTS not in [row[0] for row in sections[name]]


def test_stale_document_count_sql_counts_the_stale_set(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["count"] == [["3"]]


def test_retry_failed_documents_sql_returns_failed_rows_at_the_current_version(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["retry"] == [[FXO_RETRY_FAILED]]


def test_tagged_website_facts_sql_returns_the_tagged_fact(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["tagged"] == [
        [FXO_STALE_VERSION, "WebsitesOfLegalEntity", "https://example.com"]
    ]


def test_email_domains_sql_returns_the_email_candidate(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["emails"] == [[FXO_STALE_VERSION, "ir@example.com"]]
