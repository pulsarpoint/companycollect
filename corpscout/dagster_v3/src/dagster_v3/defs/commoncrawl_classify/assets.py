"""Dagster assets building the CommonCrawl classification reference data.

Two short chains, each: build the embedding matrix into the per-source DuckDB file,
then export it to ClickHouse. The embedding endpoint is env-driven
(`EmbeddingClient.from_env`); the page-type chain reads the committed seed.
"""
from datetime import datetime, timezone

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from commoncrawl_enrich import nace_embed
from dagster_v3.defs.commoncrawl_classify import build, tables
from dagster_v3.defs.commoncrawl_classify.clickhouse import (
    export_nace_category_embeddings,
    export_page_type_exemplars,
)

GROUP_NAME = "commoncrawl_classify"
POOL = tables.POOL


@dg.asset(
    deps=[dg.AssetKey("nace_categories_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=POOL,
    description="Embed NACE categories into the reference matrix (DuckDB).",
)
def nace_category_embeddings_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    embedder = nace_embed.EmbeddingClient.from_env()
    rows = build.build_nace_embeddings_duckdb(
        tables.DUCKDB_PATH, embedder,
        embedding_model=embedder.model, source_run_id=context.run_id,
        resolved_at=datetime.now(timezone.utc),
    )
    context.log.info("Built NACE embeddings: rows=%s model=%s", rows, embedder.model)
    return dg.MaterializeResult(metadata={"rows": rows, "embedding_model": embedder.model})


@dg.asset(
    deps=[dg.AssetKey("nace_category_embeddings_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=POOL,
    metadata={"table": tables.QUALIFIED_NACE_EMBEDDINGS_TABLE},
    description="Export the NACE reference matrix to ClickHouse corpscout.nace_category_embeddings.",
)
def nace_category_embeddings_clickhouse(
    context: AssetExecutionContext, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_nace_category_embeddings(
        database_path=tables.DUCKDB_PATH, clickhouse=clickhouse, log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_NACE_EMBEDDINGS_TABLE},
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=POOL,
    description="Embed the committed page-type seed into prototypes (DuckDB).",
)
def page_type_exemplars_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    embedder = nace_embed.EmbeddingClient.from_env()
    rows = build.build_page_type_exemplars_duckdb(
        tables.DUCKDB_PATH, embedder,
        embedding_model=embedder.model, source_run_id=context.run_id,
        resolved_at=datetime.now(timezone.utc),
    )
    context.log.info("Built page-type exemplars: rows=%s model=%s", rows, embedder.model)
    return dg.MaterializeResult(metadata={"rows": rows, "embedding_model": embedder.model})


@dg.asset(
    deps=[dg.AssetKey("page_type_exemplars_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=POOL,
    metadata={"table": tables.QUALIFIED_PAGE_TYPE_EXEMPLARS_TABLE},
    description="Export page-type prototypes to ClickHouse corpscout.page_type_exemplars.",
)
def page_type_exemplars_clickhouse(
    context: AssetExecutionContext, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_page_type_exemplars(
        database_path=tables.DUCKDB_PATH, clickhouse=clickhouse, log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_PAGE_TYPE_EXEMPLARS_TABLE},
    )
