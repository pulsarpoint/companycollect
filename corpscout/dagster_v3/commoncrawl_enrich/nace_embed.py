"""Embedding-based NACE industry classification.

Classify a page by nearest-neighbour in embedding space against the NACE
taxonomy, instead of asking an LLM to generate a code. Validated in
`scripts/spike_nace_embed.py`: on content-rich homepages this agrees with the
35B LLM at division level, and a confidence gate (top1-top2 margin, or all
top-3 sharing a 2-digit division) cleanly separates the confident head — which
embeddings answer at ~100% precision — from the low-signal tail that should
fall back to the LLM or be left Unknown.

Flow:
    ref = build_reference("data/nace_source.duckdb", client)   # once, ~1025 vectors
    ref.save("data/nace_reference.npz")
    ...
    results = classify_pages(page_texts, ref, client)          # per batch of pages
    # results[i].confident -> emit results[i].code; else route to LLM tail

`M` (the reference matrix) is tiny (~1025 x 4096 ≈ 16 MB): a plain matmul against
it beats any vector DB at this scale. The embedder is injected so the matmul and
gating are unit-testable without a live endpoint.
"""

import re
from dataclasses import dataclass
from typing import Protocol

import numpy as np

PAGE_INSTRUCTION = "Classify the business into its industry category"
DEFAULT_MARGIN_THRESHOLD = 0.03  # spike: bare + margin>=0.03 -> 100% LLM agreement
_EMBED_BATCH = 128


def division(code: str) -> str:
    """2-digit NACE division from a possibly-messy code.

    '63.12'->'63', 'C15.20'->'15', '63.12Z'->'63', '72'->'72',
    bare section letter 'S'->'S'.
    """
    s = (code or "").strip()
    m = re.search(r"\d{2}", s)  # first 2-digit run = division
    return m.group(0) if m else s[:1].upper()


class Embedder(Protocol):
    """Anything that turns texts into L2-normalized row vectors."""

    def embed(self, texts: list[str], instruction: str | None = None) -> np.ndarray:
        ...


class EmbeddingClient:
    """OpenAI-compatible embeddings endpoint (e.g. vLLM serving Qwen3-Embedding).

    Documents (NACE categories) are embedded plain; queries (pages) get an
    ``Instruct: ...\\nQuery: ...`` prefix — the asymmetric convention Qwen3-Embedding
    expects (without it, cosine spreads collapse and discrimination drops).
    """

    def __init__(self, *, base_url: str, api_key: str = "x", model: str | None = None,
                 batch: int = _EMBED_BATCH, timeout: int = 120):
        from openai import OpenAI

        self._client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout)
        self._model = model or self._client.models.list().data[0].id
        self._batch = batch

    @property
    def model(self) -> str:
        return self._model

    def embed(self, texts: list[str], instruction: str | None = None) -> np.ndarray:
        if instruction:
            texts = [f"Instruct: {instruction}\nQuery: {t}" for t in texts]
        out: list[list[float]] = []
        for i in range(0, len(texts), self._batch):
            chunk = texts[i:i + self._batch]
            out.extend(d.embedding for d in self._client.embeddings.create(
                model=self._model, input=chunk).data)
        return _normalize(np.asarray(out, dtype="float32"))


def _normalize(a: np.ndarray) -> np.ndarray:
    return a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-9)


@dataclass
class NaceReference:
    """The fixed NACE menu: parallel arrays + the embedding matrix, in lockstep order.

    Row ``j`` of ``matrix`` is the embedding of ``codes[j]`` (``labels[j]`` /
    ``divisions[j]``). The arrays MUST stay co-ordered with ``matrix``; rebuild all
    together. ``matrix`` is L2-normalized so a dot product is cosine similarity.
    """

    codes: list[str]
    labels: list[str]
    divisions: list[str]
    matrix: np.ndarray  # (N, dim), L2-normalized

    def __post_init__(self) -> None:
        n = len(self.codes)
        if not (len(self.labels) == len(self.divisions) == self.matrix.shape[0] == n):
            raise ValueError("NaceReference arrays and matrix must be co-ordered")

    def save(self, path: str) -> None:
        np.savez(path, codes=np.array(self.codes), labels=np.array(self.labels),
                 divisions=np.array(self.divisions), matrix=self.matrix)

    @classmethod
    def load(cls, path: str) -> "NaceReference":
        d = np.load(path, allow_pickle=False)
        return cls(codes=list(d["codes"]), labels=list(d["labels"]),
                   divisions=list(d["divisions"]), matrix=d["matrix"])


@dataclass(frozen=True)
class NaceClassification:
    code: str            # top-1 NACE code
    label: str           # top-1 description
    score: float         # top-1 cosine similarity
    margin: float        # score(top1) - score(top2)
    division: str        # 2-digit division of top-1
    division_consensus: bool  # all top-3 share one division
    confident: bool      # gate decision (margin or consensus)
    top3_codes: list[str]
    top3_scores: list[float]
    method: str = "embedding"


# ---- NACE reference text + matrix -------------------------------------------
_NACE_SQL = """
    select c.code, c.description_en,
           sec.description_en as section_desc,
           par.description_en as parent_desc
    from nace_stage.nace_categories c
    left join nace_stage.nace_categories sec
           on sec.code = c.section_code and sec.is_current
    left join nace_stage.nace_categories par
           on par.code = c.parent_code and par.is_current
    where c.is_current and coalesce(c.description_en,'') <> ''
      and c.level <> 'section'   -- single-letter sections too coarse to be a label
    order by c.code
"""


def load_nace_rows(duckdb_path: str) -> list[tuple[str, str, str, str]]:
    """(code, leaf_desc, section_desc, parent_desc) for current NACE non-section codes."""
    import duckdb

    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        return con.execute(_NACE_SQL).fetchall()
    finally:
        con.close()


def reference_text(row: tuple[str, str, str, str], variant: str = "hier") -> str:
    """Text to embed for a category. 'bare' = leaf description; 'hier' prepends the
    section/parent path (section › parent › leaf) for extra context."""
    _code, leaf, section_desc, parent_desc = row
    if variant == "bare":
        return leaf
    path = [p for p in (section_desc, parent_desc) if p and p != leaf]
    return " > ".join([*path, leaf]) if path else leaf


def build_reference(duckdb_path: str, embedder: Embedder, *, variant: str = "hier",
                    rows: list[tuple[str, str, str, str]] | None = None) -> NaceReference:
    """Build the NACE reference matrix once. Pass `rows` to skip the DuckDB read (tests)."""
    rows = rows if rows is not None else load_nace_rows(duckdb_path)
    if not rows:
        raise ValueError("no NACE rows to embed")
    codes = [r[0] for r in rows]
    labels = [r[1] for r in rows]
    divisions = [division(c) for c in codes]
    texts = [reference_text(r, variant) for r in rows]
    matrix = embedder.embed(texts)  # documents: no instruction prefix
    return NaceReference(codes=codes, labels=labels, divisions=divisions, matrix=matrix)


# ---- classification ----------------------------------------------------------
def classify_matrix(P: np.ndarray, ref: NaceReference, *,
                    margin_threshold: float = DEFAULT_MARGIN_THRESHOLD) -> list[NaceClassification]:
    """Classify already-embedded, normalized page vectors `P` (B x dim) against `ref`."""
    if P.shape[1] != ref.matrix.shape[1]:
        raise ValueError(f"page dim {P.shape[1]} != reference dim {ref.matrix.shape[1]}")
    sims = P @ ref.matrix.T  # (B, N) cosine similarities
    k = min(3, sims.shape[1])
    order = np.argsort(-sims, axis=1)[:, :k]
    results: list[NaceClassification] = []
    for i in range(sims.shape[0]):
        idx = order[i]
        codes = [ref.codes[j] for j in idx]
        scores = [float(sims[i, j]) for j in idx]
        divs = [ref.divisions[j] for j in idx]
        margin = scores[0] - scores[1] if len(scores) > 1 else scores[0]
        consensus = len(set(divs)) == 1
        results.append(NaceClassification(
            code=codes[0], label=ref.labels[idx[0]], score=scores[0], margin=margin,
            division=divs[0], division_consensus=consensus,
            confident=(margin >= margin_threshold or consensus),
            top3_codes=codes, top3_scores=scores,
        ))
    return results


def classify_pages(texts: list[str], ref: NaceReference, embedder: Embedder, *,
                   instruction: str = PAGE_INSTRUCTION,
                   margin_threshold: float = DEFAULT_MARGIN_THRESHOLD) -> list[NaceClassification]:
    """Embed page texts (with the query instruction) and classify against `ref`."""
    if not texts:
        return []
    P = embedder.embed(texts, instruction=instruction)
    return classify_matrix(P, ref, margin_threshold=margin_threshold)
