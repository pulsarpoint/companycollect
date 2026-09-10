"""Explicit paid enrichment orchestration for ESEF source documents.

Deterministic document evidence is part of ``esef_filings_refresh_job`` and
uses the same processed-week partition as ingestion. Paid model extraction is
kept in these separate, unpartitioned jobs. The company-information job
selects the latest unprocessed final ClickHouse ESEF document per linked
company, optionally bounded by country, company IDs, source-document IDs, and
a development limit, and publishes the company-information, business-items,
and group-relationships projections. The people pass runs independently, one
extraction per eligible filing per admitted LEI, and its own projection
replaces ``esef_document_people`` from that extraction table.
"""

import dagster as dg


ESEF_DOCUMENT_LLM_SELECTION = dg.AssetSelection.assets(
    "esef_document_company_information_clickhouse",
    "esef_document_business_items_clickhouse",
    "esef_document_group_relationships_clickhouse",
)

ESEF_DOCUMENT_PEOPLE_SELECTION = dg.AssetSelection.assets(
    "esef_document_people_extraction_clickhouse",
    "esef_document_people_clickhouse",
)

esef_document_company_information_job = dg.define_asset_job(
    "esef_document_company_information_job",
    selection=ESEF_DOCUMENT_LLM_SELECTION,
)

esef_document_people_job = dg.define_asset_job(
    "esef_document_people_job",
    selection=ESEF_DOCUMENT_PEOPLE_SELECTION,
)


defs = dg.Definitions(
    jobs=[esef_document_company_information_job, esef_document_people_job]
)
