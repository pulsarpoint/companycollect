import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from reference_builder import build
from reference_builder.embed import division, reference_text

# corpscout/clickhouse/migrations (commoncrawl/reference-builder/tests -> up to corpscout)
MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _migration_columns(name: str) -> list[str]:
    sql = (MIGRATIONS / f"{name}.up.sql").read_text()
    cols = []
    for line in sql.splitlines():
        line = line.strip()
        if line and line[0].isidentifier() and not line.upper().startswith(
            ("CREATE", "ENGINE", "ORDER", ")")):
            cols.append(line.split()[0])
    return cols


def test_nace_columns_match_migration():
    assert list(build.NACE_EMBEDDINGS_COLUMNS) == _migration_columns(
        "000044_corpscout_nace_category_embeddings")


def test_page_type_columns_match_migration():
    assert list(build.PAGE_TYPE_EXEMPLARS_COLUMNS) == _migration_columns(
        "000045_corpscout_page_type_exemplars")


def test_committed_seed_exists_and_is_well_formed():
    seed = [json.loads(s) for s in build.PAGE_TYPE_SEED_PATH.read_text().splitlines() if s.strip()]
    assert len(seed) > 50
    assert all({"page_type", "text"} <= r.keys() for r in seed)


def test_division():
    assert division("63.12") == "63"
    assert division("C15.20") == "15"
    assert division("63.12Z") == "63"
    assert division("72") == "72"
    assert division("S") == "S"


def test_reference_text_variants():
    row = ("62.01", "Computer programming", "Information and communication", "Computer programming, consultancy")
    assert reference_text(row, "bare") == "Computer programming"
    # hier prepends the distinct section/parent path
    assert reference_text(row, "hier") == (
        "Information and communication > Computer programming, consultancy > Computer programming"
    )


class FakeEmbedder:
    def __init__(self, dim=4):
        self.dim = dim

    def embed(self, texts, instruction=None):
        a = np.ones((len(texts), self.dim), dtype="float32")
        return a / np.linalg.norm(a, axis=1, keepdims=True)


class FakeCH:
    """Records executes; returns the NACE fixture on the SELECT, captures INSERT rows."""

    def __init__(self, nace_rows):
        self.nace_rows = nace_rows
        self.inserted = {}
        self.verbs = []

    def execute(self, sql, params=None):
        s = sql.strip()
        self.verbs.append(s.split()[0].upper())
        if s.lower().startswith("select"):
            return self.nace_rows
        if s.startswith("INSERT INTO"):
            self.inserted[s.split()[2]] = params  # corpscout.<table>_rebuild_stage
        return None


def test_rebuild_nace_row_shape_and_atomic_swap():
    # (code, level, section_code, parent_code, label, section_desc, parent_desc, classification_version)
    nace = [
        ("62.01", "class", "J", "62.0", "Computer programming", "ICT", "Programming", "NACE2.1"),
        ("10.11", "class", "C", "10.1", "Meat processing", "Manufacturing", "Food", "NACE2.1"),
    ]
    ch = FakeCH(nace)
    n = build.rebuild_nace(ch, FakeEmbedder(), model="m1", run_id="r1", now=datetime.now(timezone.utc))
    assert n == 2
    rows = ch.inserted["corpscout.nace_category_embeddings_rebuild_stage"]
    assert len(rows) == 2
    assert all(len(r) == len(build.NACE_EMBEDDINGS_COLUMNS) for r in rows)  # matches migration 044
    assert rows[0][4] == "62"          # division
    assert rows[0][9] == "m1"          # embedding_model
    assert "EXCHANGE" in ch.verbs      # atomic swap happened


def test_rebuild_page_types_uses_seed_and_matches_columns():
    ch = FakeCH([])
    n = build.rebuild_page_types(ch, FakeEmbedder(), model="m1", run_id="r1", now=datetime.now(timezone.utc))
    assert n > 0  # the committed seed is non-empty
    rows = ch.inserted["corpscout.page_type_exemplars_rebuild_stage"]
    assert len(rows) == n
    assert all(len(r) == len(build.PAGE_TYPE_EXEMPLARS_COLUMNS) for r in rows)  # matches migration 045
    assert "EXCHANGE" in ch.verbs
