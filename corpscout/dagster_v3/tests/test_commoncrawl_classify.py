"""Offline tests for the commoncrawl_classify reference-build module."""
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pytest

from dagster_v3.defs.commoncrawl_classify import build, tables

MIGRATIONS = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"


class FakeEmbedder:
    def embed(self, texts, instruction=None):
        out = np.zeros((len(texts), 4), dtype="float32")
        for i in range(len(texts)):
            out[i, i % 4] = 1.0
        return out


def _migration_columns(name: str) -> list[str]:
    sql = (MIGRATIONS / f"{name}.up.sql").read_text()
    cols = []
    for line in sql.splitlines():
        line = line.strip()
        if line and line[0].isidentifier() and not line.upper().startswith(
            ("CREATE", "ENGINE", "ORDER", ")")):
            cols.append(line.split()[0])
    return cols


def test_nace_export_columns_match_migration():
    migration = _migration_columns("000044_corpscout_nace_category_embeddings")
    assert list(tables.NACE_EMBEDDINGS_COLUMNS) == migration


def test_page_type_export_columns_match_migration():
    migration = _migration_columns("000045_corpscout_page_type_exemplars")
    assert list(tables.PAGE_TYPE_EXEMPLARS_COLUMNS) == migration


def test_committed_seed_exists_and_is_well_formed():
    seed = build._read_seed(tables.PAGE_TYPE_SEED_PATH)
    assert len(seed) > 50
    assert all({"page_type", "text"} <= r.keys() for r in seed)


def test_build_page_type_exemplars_writes_duckdb(tmp_path):
    duckdb_path = tmp_path / "ref.duckdb"
    n = build.build_page_type_exemplars_duckdb(
        duckdb_path, FakeEmbedder(), embedding_model="fake",
        source_run_id="run1", resolved_at=datetime(2026, 6, 22, tzinfo=timezone.utc),
    )
    assert n > 50
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        cols = [c[0] for c in con.execute(
            f"select * from {tables.DUCKDB_SCHEMA}.{tables.PAGE_TYPE_EXEMPLARS_TABLE} limit 0"
        ).description]
        assert cols == list(tables.PAGE_TYPE_EXEMPLARS_COLUMNS)
        emb, dim = con.execute(
            f"select embedding, embedding_dim from {tables.DUCKDB_SCHEMA}."
            f"{tables.PAGE_TYPE_EXEMPLARS_TABLE} limit 1"
        ).fetchone()
        assert len(emb) == 4 and dim == 4


def test_build_nace_embeddings_uses_real_taxonomy_if_present(tmp_path):
    if not tables.NACE_SOURCE_DUCKDB.exists():
        pytest.skip("nace_source.duckdb not present (built by the nace module)")
    duckdb_path = tmp_path / "ref.duckdb"
    n = build.build_nace_embeddings_duckdb(
        duckdb_path, FakeEmbedder(), embedding_model="fake",
        source_run_id="run1", resolved_at=datetime(2026, 6, 22, tzinfo=timezone.utc),
    )
    assert n > 100
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        cols = [c[0] for c in con.execute(
            f"select * from {tables.DUCKDB_SCHEMA}.{tables.NACE_EMBEDDINGS_TABLE} limit 0"
        ).description]
        assert cols == list(tables.NACE_EMBEDDINGS_COLUMNS)
