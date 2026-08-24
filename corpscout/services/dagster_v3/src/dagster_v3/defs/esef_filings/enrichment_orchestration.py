"""Explicit paid enrichment orchestration for ESEF source documents.

Deterministic document evidence is part of ``esef_filings_refresh_job`` and
uses the same processed-week partition as ingestion. Paid model extraction is
kept in this separate, unpartitioned job. It selects the latest unprocessed
final ClickHouse ESEF document per linked company, optionally bounded by
country, company IDs, source-document IDs, and a development limit.
"""

import dagster as dg


ESEF_DOCUMENT_LLM_SELECTION = dg.AssetSelection.assets(
    "esef_document_company_information_clickhouse",
    "esef_document_observations_clickhouse",
)

esef_document_company_information_job = dg.define_asset_job(
    "esef_document_company_information_job",
    selection=ESEF_DOCUMENT_LLM_SELECTION,
)


defs = dg.Definitions(jobs=[esef_document_company_information_job])
