"""The esef_domains extractor asset: selection SQL, the loaders, the replace
writer and one end-to-end run over the fake object store + fake ClickHouse
(fakes from tests/test_esef_llm_enrichment.py)."""

import logging
import zipfile
from datetime import date
from hashlib import sha256
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_FAILED,
    STATUS_OK,
    DomainsDocument,
)
from dagster_v3.defs.esef_filings.domains_extractor import (
    ESEF_DOMAINS_POOL,
    EsefDomainsConfig,
    email_domains_sql,
    esef_domains_clickhouse,
    esef_domains_job,
    load_email_domains,
    load_tagged_website_facts,
    run_esef_domains_extraction,
    select_documents,
    stale_document_count_sql,
    stale_documents_sql,
    tagged_website_facts_sql,
)
from dagster_v3.defs.esef_filings.document_rows import replace_document_rows
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    report_package_object_key,
)
from dagster_v3.defs.esef_filings.website_candidates import WEBSITE_FACT_CONCEPTS
from tests.test_esef_llm_enrichment import _FakeClickHouse, _FakeObjectStore

REPORT = """<html xmlns="http://www.w3.org/1999/xhtml"
 xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>
<p>Mer information finns på www.example.com.</p>
<p>Delårsrapporter publiceras på www.example.com/ir.</p>
</body></html>"""

TABLES_EXIST = [("esef_domains",)]


def _package_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "p.zip"
    with zipfile.ZipFile(path, "w") as package:
        package.writestr("pkg/reports/report.xhtml", REPORT)
    return path.read_bytes()


# --- SQL text -----------------------------------------------------------------


def test_stale_documents_sql_selects_available_documents_at_another_version() -> None:
    sql = stale_documents_sql(all_available=False, only_listed=False)
    assert "FROM corpscout.esef_filings AS filings FINAL" in sql
    assert "SELECT DISTINCT fxo_id FROM corpscout.esef_facts" in sql
    assert "FROM corpscout.esef_domains GROUP BY source_document_id" in sql
    assert "filings.package_sha256 != ''" in sql
    assert "filings.period_end <= today()" in sql
    assert "extracted.extractor_version != %(extractor_version)s" in sql
    assert "ORDER BY filings.period_end DESC, filings.fxo_id" in sql
    assert "%(source_document_ids)s" not in sql


def test_stale_documents_sql_variants() -> None:
    everything = stale_documents_sql(all_available=True, only_listed=False)
    assert "%(extractor_version)s" not in everything
    listed = stale_documents_sql(all_available=True, only_listed=True)
    assert "filings.fxo_id IN %(source_document_ids)s" in listed
    assert stale_document_count_sql().startswith("SELECT count() FROM (")
    assert "%(extractor_version)s" in stale_document_count_sql()


def test_input_sql_reads_facts_and_email_candidates() -> None:
    assert "FROM corpscout.esef_facts" in tagged_website_facts_sql()
    assert "concept_local_name IN %(concepts)s" in tagged_website_facts_sql()
    assert "FROM corpscout.esef_document_contact_candidates" in email_domains_sql()
    assert "candidate_kind = 'email'" in email_domains_sql()


# --- selection and loaders ------------------------------------------------------


def test_select_documents_maps_rows_and_applies_max_documents() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                ("doc-2", "b" * 64, "LEI2", date(2024, 12, 31)),
                ("doc-1", "a" * 64, "LEI1", "2023-12-31"),
            ]
        ]
    )

    documents = select_documents(
        clickhouse, EsefDomainsConfig(max_documents=1, workers=1)
    )

    assert documents == [
        DomainsDocument("doc-2", "b" * 64, "LEI2", date(2024, 12, 31), 2024)
    ]
    sql, parameters = clickhouse.client.calls[0]
    assert "extracted.extractor_version != %(extractor_version)s" in sql
    assert parameters == {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION}


def test_select_documents_listed_ids_are_re_extracted_stale_or_not() -> None:
    clickhouse = _FakeClickHouse([[]])

    select_documents(
        clickhouse, EsefDomainsConfig(source_document_ids=["doc-9", "doc-1"], workers=1)
    )

    sql, parameters = clickhouse.client.calls[0]
    assert "%(extractor_version)s" not in sql
    assert "filings.fxo_id IN %(source_document_ids)s" in sql
    assert parameters["source_document_ids"] == ("doc-1", "doc-9")


def test_loaders_group_by_document_and_dedupe() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                ("doc-1", "WebsitesOfLegalEntity", "https://www.a.example"),
                ("doc-1", "WebsitesOfLegalEntity", "https://www.a.example"),
                ("doc-2", "WebsiteOfTheAuditEntity", "https://audit.example"),
            ],
            [
                ("doc-1", "info@mail.example.se"),
                ("doc-1", "ir@example.com"),
                ("doc-3", "x"),
            ],
        ]
    )

    tagged = load_tagged_website_facts(clickhouse, ["doc-1", "doc-2"])
    emails = load_email_domains(clickhouse, ["doc-1", "doc-2", "doc-3"])

    assert tagged == {
        "doc-1": (("WebsitesOfLegalEntity", "https://www.a.example"),),
        "doc-2": (("WebsiteOfTheAuditEntity", "https://audit.example"),),
    }
    assert emails == {"doc-1": ("example.com", "mail.example.se")}
    _, parameters = clickhouse.client.calls[0]
    assert parameters["concepts"] == tuple(sorted(WEBSITE_FACT_CONCEPTS))
    assert load_tagged_website_facts(clickhouse, []) == {}


# --- writer ---------------------------------------------------------------------


def test_replace_document_rows_stages_exchanges_and_drops() -> None:
    clickhouse = _FakeClickHouse([])
    rows = [
        {column: f"{column}-value" for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS},
    ]

    replace_document_rows(
        clickhouse,
        table=tables.ESEF_DOMAINS_TABLE,
        columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
        source_document_ids=["doc-2", "doc-1", "doc-2"],
        rows=rows,
    )

    statements = [sql for sql, _ in clickhouse.client.calls]
    assert statements[0].startswith("CREATE TABLE `corpscout`.`_tmp_esef_domains_")
    assert statements[0].endswith("AS `corpscout`.`esef_domains`")
    assert "WHERE source_document_id NOT IN %(source_document_ids)s" in statements[1]
    assert clickhouse.client.calls[1][1] == {"source_document_ids": ("doc-1", "doc-2")}
    assert statements[2].endswith(
        f"({', '.join(tables.ESEF_DOMAINS_EXPORT_COLUMNS)}) VALUES"
    )
    assert clickhouse.client.calls[2][1] == [
        tuple(f"{column}-value" for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS)
    ]
    assert statements[3].startswith("EXCHANGE TABLES `corpscout`.`_tmp_esef_domains_")
    assert statements[4].startswith(
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_esef_domains_"
    )


def test_replace_document_rows_without_rows_still_clears_the_documents() -> None:
    clickhouse = _FakeClickHouse([])

    replace_document_rows(
        clickhouse,
        table=tables.ESEF_DOMAINS_TABLE,
        columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
        source_document_ids=["doc-1"],
        rows=[],
    )

    statements = [sql for sql, _ in clickhouse.client.calls]
    assert len(statements) == 4
    assert not any(statement.endswith("VALUES") for statement in statements)


# --- the run ----------------------------------------------------------------------


def test_run_extracts_writes_batches_and_marks_a_missing_package(
    tmp_path: Path,
) -> None:
    body = _package_bytes(tmp_path)
    digest = sha256(body).hexdigest()
    object_store = _FakeObjectStore(
        {(ESEF_DOCUMENT_BUCKET, report_package_object_key(digest)): body}
    )
    clickhouse = _FakeClickHouse(
        [
            TABLES_EXIST,
            [
                ("doc-ok", digest, "LEI1", date(2024, 12, 31)),
                ("doc-missing", "c" * 64, "LEI2", date(2023, 12, 31)),
            ],
            [("doc-ok", "WebsitesOfLegalEntity", "https://www.example.com")],
            [],
        ]
    )

    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        config=EsefDomainsConfig(workers=1, batch_size=1, parse_timeout_seconds=120),
        source_run_id="run-1",
        log=logging.getLogger("test"),
    )

    assert summary["candidate_document_count"] == 2
    assert summary["attempted_document_count"] == 2
    assert summary["processed_document_count"] == 1
    assert summary["failed_document_count"] == 1
    assert summary["timed_out_document_count"] == 0
    assert summary["documents_without_domains"] == 0
    assert summary["row_count"] == 2
    assert summary["extractor_version"] == ESEF_DOMAINS_EXTRACTOR_VERSION
    assert summary["table"] == "corpscout.esef_domains"

    inserts = [
        (sql, parameters)
        for sql, parameters in clickhouse.client.calls
        if sql.endswith("VALUES")
    ]
    assert len(inserts) == 2  # batch_size=1 -> one replace per document
    columns = tables.ESEF_DOMAINS_EXPORT_COLUMNS
    ok_row = dict(zip(columns, inserts[0][1][0], strict=True))
    assert ok_row["source_document_id"] == "doc-ok"
    assert ok_row["extraction_status"] == STATUS_OK
    assert ok_row["registrable_domain"] == "example.com"
    assert ok_row["corroborated"] == 1  # tagged fact
    assert '"company_website"' in ok_row["roles_json"]
    assert ok_row["fiscal_year"] == 2024
    assert ok_row["source_run_id"] == "run-1"
    missing_row = dict(zip(columns, inserts[1][1][0], strict=True))
    assert missing_row["source_document_id"] == "doc-missing"
    assert missing_row["extraction_status"] == STATUS_FAILED
    assert missing_row["registrable_domain"] == ""
    assert "package download failed" in missing_row["error_message"]
    # The run's temp dir is gone.
    assert not [
        p for p in Path(tmp_path).iterdir() if p.name.startswith("esef-domains-")
    ]


def test_run_with_nothing_stale_writes_nothing() -> None:
    clickhouse = _FakeClickHouse([TABLES_EXIST, [], [], []])

    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=_FakeObjectStore({}),
        config=EsefDomainsConfig(workers=1),
        source_run_id="run-1",
        log=logging.getLogger("test"),
    )

    assert summary["candidate_document_count"] == 0
    assert summary["row_count"] == 0
    assert not any(sql.startswith("CREATE TABLE") for sql, _ in clickhouse.client.calls)


# --- definitions ------------------------------------------------------------------


def test_asset_and_job_definitions() -> None:
    spec = esef_domains_clickhouse.get_asset_spec()
    assert spec.key.to_user_string() == "esef_domains_clickhouse"
    assert spec.group_name == "esef"
    assert {dep.asset_key.to_user_string() for dep in spec.deps} == {
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_document_contact_candidates_clickhouse",
    }
    assert esef_domains_clickhouse.op.pool == ESEF_DOMAINS_POOL
    assert esef_domains_job.name == "esef_domains_job"
