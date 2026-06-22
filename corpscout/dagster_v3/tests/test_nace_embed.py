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


# ---- page-type (structural) layer -------------------------------------------
def _protos(labels, matrix) -> ne.PrototypeSet:
    return ne.PrototypeSet(labels=list(labels), sources=["d"] * len(labels),
                           matrix=_norm(np.asarray(matrix, dtype="float32")))


def test_page_type_detected_marks_not_confident_and_keeps_nace():
    # dim-4 space: NACE on dims 0-1, page-type prototypes on dims 2-3
    ref = _ref(["62.01", "47.11"], [[1, 0, 0, 0], [0, 1, 0, 0]])
    protos = _protos(["parked", "directory_listing"], [[0, 0, 1, 0], [0, 0, 0, 1]])
    P = _norm(np.array([[0.0, 0.0, 1.0, 0.0]]))  # exactly the 'parked' prototype
    [r] = ne.classify_matrix(P, ref, page_types=protos)
    assert r.page_type == "parked" and r.is_non_content
    assert r.page_type_score == pytest.approx(1.0)
    assert not r.confident          # page-type overrides confidence
    assert r.top3_codes[0] in ("62.01", "47.11")  # NACE top-3 still retained for inspection


def test_real_content_below_threshold_is_not_page_type():
    ref = _ref(["62.01", "47.11"], [[1, 0, 0, 0], [0, 1, 0, 0]])
    protos = _protos(["parked"], [[0, 0, 1, 0]])
    P = _norm(np.array([[1.0, 0.0, 0.0, 0.0]]))  # pure NACE-62, zero page-type similarity
    [r] = ne.classify_matrix(P, ref, page_types=protos)
    assert r.page_type == "" and not r.is_non_content
    assert r.confident and r.code == "62.01"


def test_page_type_returns_nearest_class_label():
    protos = _protos(["parked", "default_server", "directory_listing"], np.eye(3))
    P = _norm(np.array([[0.1, 0.9, 0.1], [0.9, 0.1, 0.1]]))
    scores, labels = protos.best(P)
    assert labels == ["default_server", "parked"]
    assert scores[0] > scores[1] or scores[0] > 0  # both are valid maxima


def test_build_prototype_set_from_rows():
    rows = [
        {"signal": "parked", "text": "this domain is for sale", "root_domain": "x.com"},
        {"signal": "default_server", "text": "Welcome to nginx!", "root_domain": "y.com"},
    ]
    emb = FakeEmbedder()
    ps = ne.build_prototype_set("unused.parquet", emb, rows=rows)
    assert ps.labels == ["parked", "default_server"]
    assert ps.sources == ["x.com", "y.com"]
    assert ps.matrix.shape[0] == 2


def test_prototype_set_roundtrip(tmp_path):
    ps = _protos(["parked", "default_server"], np.eye(2))
    path = str(tmp_path / "pt.npz")
    ps.save(path)
    loaded = ne.PrototypeSet.load(path)
    assert loaded.labels == ps.labels and loaded.sources == ps.sources
    assert np.allclose(loaded.matrix, ps.matrix)


def test_empty_prototype_set_best_is_zero():
    ps = ne.PrototypeSet(labels=[], sources=[], matrix=np.zeros((0, 3), dtype="float32"))
    scores, labels = ps.best(np.ones((2, 3), dtype="float32"))
    assert list(scores) == [0.0, 0.0] and labels == ["", ""]


def test_build_prototype_set_empty_raises():
    with pytest.raises(ValueError, match="no page-type exemplars"):
        ne.build_prototype_set("unused.parquet", FakeEmbedder(), rows=[])


# ---- ClickHouse loaders -----------------------------------------------------
class FakeCHClient:
    def __init__(self, rows):
        self._rows = rows
        self.queries: list[str] = []

    def execute(self, sql):
        self.queries.append(sql)
        return self._rows


def test_load_nace_reference_from_clickhouse():
    rows = [("62.01", "Computer programming", "62", [1.0, 0.0, 0.0]),
            ("47.11", "Retail sale", "47", [0.0, 2.0, 0.0])]  # unnormalized on purpose
    client = FakeCHClient(rows)
    ref = ne.load_nace_reference(client)
    assert ref.codes == ["62.01", "47.11"] and ref.divisions == ["62", "47"]
    assert np.allclose(np.linalg.norm(ref.matrix, axis=1), 1.0)  # normalized on load
    assert ne.NACE_EMBEDDINGS_TABLE in client.queries[0]  # queried the right table


def test_load_page_type_prototypes_from_clickhouse():
    rows = [("parked", "x.com", [1.0, 0.0]), ("default_server", "y.com", [0.0, 1.0])]
    ps = ne.load_page_type_prototypes(FakeCHClient(rows))
    assert ps.labels == ["parked", "default_server"] and ps.sources == ["x.com", "y.com"]
    assert ps.matrix.shape == (2, 2)


def test_nace_reference_from_rows_empty_raises():
    with pytest.raises(ValueError, match="no nace embedding rows"):
        ne.NaceReference.from_rows([])


def test_prototype_set_from_rows_empty_raises():
    with pytest.raises(ValueError, match="no page-type prototype rows"):
        ne.PrototypeSet.from_rows([])


def test_embedding_client_from_env_requires_url(monkeypatch):
    monkeypatch.delenv("COMMONCRAWL_EMBED_BASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="COMMONCRAWL_EMBED_BASE_URL"):
        ne.EmbeddingClient.from_env()
