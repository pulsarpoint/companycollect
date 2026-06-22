"""Combined page classifier: keyword page-type -> embedding (page-type + NACE) -> LLM tail.

Orchestrates the three layers validated in the spike/calibration:
  1. Keyword `match_signal` — high precision, free, no embedding. Catches obvious
     parked/default/directory pages up front.
  2. Embedding — pages with no keyword hit are batch-embedded once, then classified:
     a structural page type (parked/...) if a prototype is near enough, else a NACE
     industry with the margin/division-consensus confidence gate.
  3. LLM tail (optional) — only the not-confident NACE pages go to the LLM, so the
     expensive per-page generation is the rare exception, not the default.

Build it from ClickHouse + env once, reuse across a whole WET file.
"""
from dataclasses import dataclass, field

from commoncrawl_enrich import nace_embed, page_types
from commoncrawl_enrich.llm import LLMArm


@dataclass(frozen=True)
class IndustryResult:
    page_type: str = ""          # structural page type (parked/...) or "" if business content
    page_type_score: float = 0.0
    nace_code: str = ""
    nace_label: str = ""
    nace_division: str = ""
    nace_confident: bool = False
    nace_margin: float = 0.0
    nace_score: float = 0.0
    nace_top3: list[str] = field(default_factory=list)         # audit log: top-3 codes
    nace_top3_labels: list[str] = field(default_factory=list)  # audit log: top-3 labels
    nace_top3_scores: list[float] = field(default_factory=list)  # audit log: top-3 scores
    method: str = ""             # keyword_page_type | embedding_page_type | embedding | llm

    @property
    def is_page_type(self) -> bool:
        return bool(self.page_type)


class PageClassifier:
    def __init__(self, ref: nace_embed.NaceReference, prototypes: nace_embed.PrototypeSet,
                 embedder: nace_embed.Embedder, *, llm: LLMArm | None = None,
                 page_type_threshold: float = nace_embed.DEFAULT_PAGE_TYPE_THRESHOLD,
                 margin_threshold: float = nace_embed.DEFAULT_MARGIN_THRESHOLD):
        self._ref = ref
        self._prototypes = prototypes
        self._embedder = embedder
        self._llm = llm
        self._page_type_threshold = page_type_threshold
        self._margin_threshold = margin_threshold

    @classmethod
    def from_clickhouse(cls, ch_client, embedder: nace_embed.Embedder, *,
                        llm: LLMArm | None = None, database: str = nace_embed.NACE_DATABASE,
                        **kwargs) -> "PageClassifier":
        return cls(
            nace_embed.load_nace_reference(ch_client, database=database),
            nace_embed.load_page_type_prototypes(ch_client, database=database),
            embedder, llm=llm, **kwargs,
        )

    def classify(self, texts: list[str]) -> list[IndustryResult]:
        results: list[IndustryResult | None] = [None] * len(texts)

        # Layer 1: keyword page-type (skip embedding for obvious hits)
        to_embed: list[tuple[int, str]] = []
        for i, text in enumerate(texts):
            label = page_types.match_signal(text)
            if label:
                results[i] = IndustryResult(page_type=label, page_type_score=1.0,
                                            method="keyword_page_type")
            else:
                to_embed.append((i, text))

        # Layer 2: embedding (page-type prototype OR NACE) for the rest
        if to_embed:
            idxs = [i for i, _ in to_embed]
            classed = nace_embed.classify_pages(
                [t for _, t in to_embed], self._ref, self._embedder,
                page_types=self._prototypes, page_type_threshold=self._page_type_threshold,
                margin_threshold=self._margin_threshold,
            )
            for i, c in zip(idxs, classed):
                if c.is_non_content:
                    results[i] = IndustryResult(page_type=c.page_type,
                                                page_type_score=c.page_type_score,
                                                method="embedding_page_type")
                else:
                    results[i] = IndustryResult(
                        nace_code=c.code, nace_label=c.label, nace_division=c.division,
                        nace_confident=c.confident, nace_margin=c.margin, nace_score=c.score,
                        nace_top3=list(c.top3_codes), nace_top3_labels=list(c.top3_labels),
                        nace_top3_scores=list(c.top3_scores), page_type_score=c.page_type_score,
                        method="embedding",
                    )

        # Layer 3: LLM tail — only not-confident NACE pages
        if self._llm is not None:
            for i, r in enumerate(results):
                if r is not None and r.method == "embedding" and not r.nace_confident:
                    guess = self._llm.classify_industry(texts[i])
                    if guess.nace_hint or guess.label:
                        results[i] = IndustryResult(
                            nace_code=guess.nace_hint, nace_label=guess.label,
                            nace_division=nace_embed.division(guess.nace_hint),
                            nace_confident=guess.confidence >= 60, nace_score=r.nace_score,
                            nace_top3=r.nace_top3, nace_top3_labels=r.nace_top3_labels,
                            nace_top3_scores=r.nace_top3_scores, method="llm",
                        )

        return [r if r is not None else IndustryResult() for r in results]
