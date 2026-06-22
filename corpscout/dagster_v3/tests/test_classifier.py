"""Offline tests for the combined PageClassifier (keyword -> embedding -> LLM tail)."""
import numpy as np

from commoncrawl_enrich import nace_embed
from commoncrawl_enrich.classifier import PageClassifier
from commoncrawl_enrich.models import IndustryGuess


def _norm(a):
    return a / np.linalg.norm(a, axis=1, keepdims=True)


def _ref():
    # dim-4: NACE on dims 0-1, page-type prototypes on dims 2-3
    return nace_embed.NaceReference(
        codes=["62.01", "47.11"], labels=["Programming", "Retail"], divisions=["62", "47"],
        matrix=_norm(np.array([[1, 0, 0, 0], [0, 1, 0, 0]], dtype="float32")),
    )


def _protos():
    return nace_embed.PrototypeSet(
        labels=["parked", "default_server"], sources=["a", "b"],
        matrix=_norm(np.array([[0, 0, 1, 0], [0, 0, 0, 1]], dtype="float32")),
    )


class DirEmbedder:
    """Maps each page index to a chosen dim so we can drive NACE vs page-type outcomes."""

    def __init__(self, vectors):
        self._vectors = vectors

    def embed(self, texts, instruction=None):
        return _norm(np.asarray(self._vectors, dtype="float32"))


class FakeLLM:
    def __init__(self):
        self.calls = 0

    def classify_industry(self, text):
        self.calls += 1
        return IndustryGuess(label="LLM Retail", nace_hint="47.11", confidence=90, method="llm")


def test_keyword_layer_short_circuits_embedding():
    # text with a parking keyword never reaches embedding
    emb = DirEmbedder([[1, 0, 0, 0]])  # would be NACE if embedded
    clf = PageClassifier(_ref(), _protos(), emb)
    [r] = clf.classify(["This domain is for sale. Contact the owner."])
    assert r.page_type == "for_sale" and r.method == "keyword_page_type"


def test_embedding_page_type_layer():
    emb = DirEmbedder([[0, 0, 1, 0]])  # nearest the 'parked' prototype
    clf = PageClassifier(_ref(), _protos(), emb)
    [r] = clf.classify(["some generic text without a keyword"])
    assert r.page_type == "parked" and r.method == "embedding_page_type"


def test_embedding_nace_confident_skips_llm():
    emb = DirEmbedder([[1, 0, 0, 0]])  # exactly NACE 62.01
    llm = FakeLLM()
    clf = PageClassifier(_ref(), _protos(), emb, llm=llm)
    [r] = clf.classify(["a software company building web apps"])
    assert r.nace_code == "62.01" and r.nace_confident and r.method == "embedding"
    assert llm.calls == 0  # confident -> no LLM


def test_llm_tail_only_for_not_confident():
    # tie between the two NACE dims -> low margin, no consensus -> not confident
    emb = DirEmbedder([[0.7, 0.7, 0, 0]])
    llm = FakeLLM()
    clf = PageClassifier(_ref(), _protos(), emb, llm=llm)
    [r] = clf.classify(["ambiguous business text"])
    assert llm.calls == 1 and r.method == "llm"
    assert r.nace_code == "47.11" and r.nace_confident  # FakeLLM confidence 90 -> confident


def test_no_llm_leaves_not_confident_as_embedding():
    emb = DirEmbedder([[0.7, 0.7, 0, 0]])
    clf = PageClassifier(_ref(), _protos(), emb)  # no llm
    [r] = clf.classify(["ambiguous business text"])
    assert r.method == "embedding" and not r.nace_confident


def test_mixed_batch_routes_each_page():
    emb = DirEmbedder([[0, 0, 1, 0], [1, 0, 0, 0]])  # parked, then NACE
    clf = PageClassifier(_ref(), _protos(), emb)
    # first has no keyword -> embedded parked; second no keyword -> NACE
    out = clf.classify(["generic no-keyword text one", "generic no-keyword text two"])
    assert out[0].page_type == "parked" and out[0].method == "embedding_page_type"
    assert out[1].nace_code == "62.01" and out[1].method == "embedding"


def test_empty_input():
    clf = PageClassifier(_ref(), _protos(), DirEmbedder([]))
    assert clf.classify([]) == []
