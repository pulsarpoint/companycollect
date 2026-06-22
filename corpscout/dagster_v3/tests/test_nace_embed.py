"""Offline tests for embedding-based NACE classification (no live endpoint)."""
import numpy as np
import pytest

from commoncrawl_enrich import nace_embed as ne


def _norm(a: np.ndarray) -> np.ndarray:
    return a / np.linalg.norm(a, axis=1, keepdims=True)


def _ref(codes, matrix) -> ne.NaceReference:
    return ne.NaceReference(
        codes=list(codes),
        labels=[f"label {c}" for c in codes],
        divisions=[ne.division(c) for c in codes],
        matrix=_norm(np.asarray(matrix, dtype="float32")),
    )


class FakeEmbedder:
    """Maps each unique text to a deterministic one-hot row vector (already normalized)."""

    def __init__(self) -> None:
        self._index: dict[str, int] = {}

    def embed(self, texts: list[str], instruction: str | None = None) -> np.ndarray:
        for t in texts:
            self._index.setdefault(t, len(self._index))
        dim = max(len(self._index), 1)
        out = np.zeros((len(texts), dim), dtype="float32")
        for i, t in enumerate(texts):
            out[i, self._index[t]] = 1.0
        return out


# ---- division() -------------------------------------------------------------
@pytest.mark.parametrize("code,expected", [
    ("63.12", "63"), ("C15.20", "15"), ("63.12Z", "63"),
    ("72", "72"), ("S", "S"), ("", ""), ("94.91", "94"),
])
def test_division(code, expected):
    assert ne.division(code) == expected


# ---- gating logic via classify_matrix --------------------------------------
def test_confident_by_margin_on_exact_match():
    ref = _ref(["62.01", "62.09", "47.11"], np.eye(3))
    P = _norm(np.array([[1.0, 0.0, 0.0]]))  # exactly category 0
    [r] = ne.classify_matrix(P, ref)
    assert r.code == "62.01" and r.score == pytest.approx(1.0)
    assert r.margin >= ne.DEFAULT_MARGIN_THRESHOLD and r.confident


def test_division_consensus_makes_confident_despite_tiny_margin():
    # three near-identical division-62 categories + one far division-47
    ref = _ref(["62.01", "62.09", "62.02", "47.11"], np.eye(4))
    P = _norm(np.array([[0.60, 0.59, 0.58, 0.0]]))  # near-tied across all three 62.x, zero on 47
    [r] = ne.classify_matrix(P, ref)
    assert r.division == "62"
    assert {ne.division(c) for c in r.top3_codes} == {"62"}
    assert r.division_consensus and r.confident
    assert r.margin < ne.DEFAULT_MARGIN_THRESHOLD  # consensus, not margin, carried it


def test_cross_division_tie_is_not_confident():
    ref = _ref(["62.01", "62.09", "47.11"], np.eye(3))
    P = _norm(np.array([[0.7, 0.0, 0.7]]))  # tied between div 62 and div 47
    [r] = ne.classify_matrix(P, ref)
    assert not r.division_consensus
    assert r.margin < ne.DEFAULT_MARGIN_THRESHOLD
    assert not r.confident


def test_top3_ordering_and_scores():
    ref = _ref(["10.1", "20.2", "30.3", "40.4"], np.eye(4))
    P = _norm(np.array([[0.9, 0.4, 0.1, 0.0]]))
    [r] = ne.classify_matrix(P, ref)
    assert r.top3_codes == ["10.1", "20.2", "30.3"]
    assert r.top3_scores == sorted(r.top3_scores, reverse=True)


def test_dim_mismatch_raises():
    ref = _ref(["62.01"], np.eye(1))
    with pytest.raises(ValueError, match="dim"):
        ne.classify_matrix(np.ones((1, 5), dtype="float32"), ref)


# ---- build_reference + classify_pages with a fake embedder ------------------
def test_build_reference_and_classify_pages_roundtrip():
    rows = [
        ("62.01", "Computer programming activities", "Information and communication", "Computer programming"),
        ("47.11", "Retail sale in non-specialised stores", "Wholesale and retail trade", "Retail trade"),
    ]
    emb = FakeEmbedder()
    ref = ne.build_reference("unused.duckdb", emb, variant="bare", rows=rows)
    assert ref.codes == ["62.01", "47.11"]
    assert ref.divisions == ["62", "47"]
    # a page whose text exactly equals a category's bare description matches it
    [r] = ne.classify_pages(["Computer programming activities"], ref, emb, instruction=None)
    assert r.code == "62.01" and r.confident


def test_reference_text_variants():
    row = ("62.01", "Computer programming", "Information and communication", "Computer programming services")
    assert ne.reference_text(row, "bare") == "Computer programming"
    hier = ne.reference_text(row, "hier")
    assert "Information and communication" in hier and hier.endswith("Computer programming")


def test_build_reference_empty_rows_raises():
    with pytest.raises(ValueError, match="no NACE rows"):
        ne.build_reference("unused.duckdb", FakeEmbedder(), rows=[])


def test_save_load_roundtrip(tmp_path):
    ref = _ref(["62.01", "47.11"], np.eye(2))
    path = str(tmp_path / "ref.npz")
    ref.save(path)
    loaded = ne.NaceReference.load(path)
    assert loaded.codes == ref.codes and loaded.divisions == ref.divisions
    assert np.allclose(loaded.matrix, ref.matrix)


def test_mismatched_arrays_raise():
    with pytest.raises(ValueError, match="co-ordered"):
        ne.NaceReference(codes=["a", "b"], labels=["x"], divisions=["1"],
                         matrix=np.eye(2, dtype="float32"))
