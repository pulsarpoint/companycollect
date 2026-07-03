"""Tests for the NACE classifier lib (fake embedder / LLM / ClickHouse client)."""

import json
import os
from contextlib import contextmanager

import numpy as np
import pytest

from dagster_v3.defs.classifier.lib import (
    CLASSIFIER_VERSION,
    QUERY_INSTRUCTION,
    _adjudicate,
    _parse_adjudication,
    build_scan_sql,
    candidate_codes,
    classify_source,
    load_corpus,
)


def test_build_scan_sql_shape():
    sql = build_scan_sql("corpscout.lv_companies", "activity_text_original")
    for fragment in (
        "SELECT DISTINCT ifNull(activity_text_original, '') AS source_text",
        "FROM corpscout.lv_companies",
        "cityHash64(src.source_text)",
        "coalesce(nullif(tr.translated_text, ''), src.source_text) AS input_text",
        "FROM corpscout.text_translations",
        "argMax(translated_text, version)",
        "AND target_lang = 'en'",
        "LEFT ANTI JOIN",
        "FROM corpscout.text_classifications",
        "WHERE source_table = 'corpscout.lv_companies' AND source_column = 'activity_text_original'",
    ):
        assert fragment in sql, f"missing {fragment!r} in:\n{sql}"


class _FakeCorpusClient:
    def execute(self, sql, params=None):
        assert "nace_category_embeddings" in sql
        assert "qwen3-embedding-8b" in sql and "NACE_REV_2_1" in sql
        return [
            ("01.11", "01.11 Growing of cereals", [1.0, 0.0, 0.0]),
            ("01.4", "01.4 Animal production", [0.0, 1.0, 0.0]),
            ("47", "47 Retail trade", [0.0, 0.0, 1.0]),
        ]


def test_load_corpus_returns_codes_labels_matrix():
    codes, labels, matrix = load_corpus(_FakeCorpusClient())
    assert codes == ["01.11", "01.4", "47"]
    assert labels[1] == "01.4 Animal production"
    assert matrix.shape == (3, 3)
    assert matrix.dtype == np.float32


def test_candidate_codes_unions_whole_and_segments_ranked():
    corpus = np.eye(4, dtype=np.float32)
    codes = ["A", "B", "C", "D"]
    # whole text closest to A; segment 1 closest to C; segment 2 closest to B
    queries = np.array(
        [[0.9, 0.1, 0.0, 0.0], [0.0, 0.1, 0.9, 0.0], [0.1, 0.9, 0.0, 0.0]],
        dtype=np.float32,
    )
    ranked, sims_by_code = candidate_codes(queries, corpus, codes, k_whole=2, k_seg=1)
    assert ranked[0] == "A"          # whole-text winner leads
    assert set(ranked) >= {"A", "B", "C"}
    assert len(ranked) == len(set(ranked))  # deduped

    # sims_by_code covers exactly the ranked candidates, with each code's
    # BEST similarity across the whole-text row and its segment rows (not
    # just the whole-text row).
    assert set(sims_by_code) == set(ranked)
    assert sims_by_code["A"] == pytest.approx(0.9)
    assert sims_by_code["B"] == pytest.approx(0.9)  # best of whole (0.1) and seg2 (0.9)
    assert sims_by_code["C"] == pytest.approx(0.9)  # best of seg1 (0.9)


def test_parse_adjudication_happy_unknown_and_invalid():
    ok = _parse_adjudication(
        json.dumps({"classifications": [{"id": "1", "nace_code": "01.4"}, {"id": "2", "nace_code": "UNKNOWN"}]}),
        expected_ids={"1", "2"},
        allowed={"1": ["01.4", "47"], "2": ["47"]},
    )
    assert ok == {"1": "01.4", "2": ""}

    # code outside the candidate set is coerced to unknown, not accepted
    coerced = _parse_adjudication(
        json.dumps({"classifications": [{"id": "1", "nace_code": "99.99"}]}),
        expected_ids={"1"},
        allowed={"1": ["01.4"]},
    )
    assert coerced == {"1": ""}

    assert _parse_adjudication("not json", expected_ids={"1"}, allowed={"1": []}) is None
    assert _parse_adjudication(json.dumps({"classifications": [{"id": "1", "nace_code": "01.4"}]}),
                               expected_ids={"1", "2"}, allowed={"1": ["01.4"], "2": []}) is None


def test_parse_adjudication_rejects_unexpected_and_duplicate_ids():
    # id "3" is not among the ids we sent the model — untrustworthy response.
    assert _parse_adjudication(
        json.dumps({"classifications": [{"id": "3", "nace_code": "01.4"}]}),
        expected_ids={"1"},
        allowed={"1": ["01.4"]},
    ) is None

    # id "1" appears twice — strict contract: duplicates make the whole
    # response unusable rather than silently keeping the last value.
    assert _parse_adjudication(
        json.dumps({"classifications": [
            {"id": "1", "nace_code": "01.4"},
            {"id": "1", "nace_code": "47"},
        ]}),
        expected_ids={"1"},
        allowed={"1": ["01.4", "47"]},
    ) is None


def test_parse_adjudication_strips_fence_and_prose_wrapping():
    payload = json.dumps({"classifications": [{"id": "1", "nace_code": "01.4"}]})

    fenced = f"```json\n{payload}\n```"
    assert _parse_adjudication(fenced, expected_ids={"1"}, allowed={"1": ["01.4"]}) == {"1": "01.4"}

    plain_fenced = f"```\n{payload}\n```"
    assert _parse_adjudication(plain_fenced, expected_ids={"1"}, allowed={"1": ["01.4"]}) == {"1": "01.4"}

    prose = f"Here is the answer: {payload} hope this helps"
    assert _parse_adjudication(prose, expected_ids={"1"}, allowed={"1": ["01.4"]}) == {"1": "01.4"}

    # pure garbage (no braces, no fence) still yields None, not a crash
    assert _parse_adjudication(
        "sorry, I cannot help with that request", expected_ids={"1"}, allowed={"1": []}
    ) is None


class _FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)
        self.prompts = []

    def create(self, prompt):
        self.prompts.append(prompt)
        return self._responses.pop(0)


def test_adjudicate_retries_once_then_unknown():
    good = json.dumps({"classifications": [{"id": "1", "nace_code": "01.4"}]})
    llm = _FakeLLM(["garbage", good])
    result = _adjudicate(llm.create, [("1", "lopkopība", "animal husbandry", ["01.4", "47"])],
                         labels_by_code={"01.4": "Animal production", "47": "Retail"})
    assert result == {"1": "01.4"}
    assert len(llm.prompts) == 2

    llm = _FakeLLM(["garbage", "still garbage"])
    result = _adjudicate(llm.create, [("1", "x", "x", ["01.4"])], labels_by_code={"01.4": "A"})
    assert result == {"1": ""}


def test_query_instruction_and_version_pins():
    assert QUERY_INSTRUCTION == "Classify the business into its industry category"
    assert CLASSIFIER_VERSION == "NACE_REV_2_1"


class _FakeLog:
    def info(self, *args, **kwargs):
        pass


class _FakeContext:
    log = _FakeLog()


class _FakeClassifySourceClient:
    """Distinguishes corpus load / anti-join scan / insert by SQL content,
    same pattern as tests/test_brazil_rfb_clickhouse.py's FakeClickHouseClient.
    """

    def __init__(self, pending_rows, corpus_size=900):
        self._pending_rows = pending_rows
        self._corpus_size = corpus_size
        self.inserted_batches: list[list[tuple]] = []

    def execute(self, sql, params=None):
        if "nace_category_embeddings" in sql:
            # A minimal but corpus-floor-passing (>=900 rows) fake corpus;
            # every code carries the same unit vector since the adjudicator
            # under test never inspects candidate identity.
            return [(f"C{i:04d}", f"label {i}", [1.0, 0.0]) for i in range(self._corpus_size)]
        if "INSERT INTO corpscout.text_classifications" in sql:
            self.inserted_batches.append(list(params))
            return None
        return self._pending_rows


class _FakeClassifySourceResource:
    def __init__(self, client):
        self._client = client

    @contextmanager
    def get_connection(self):
        yield self._client


class _FakeEmbedder:
    def embed(self, texts, instruction=None):
        return np.array([[1.0, 0.0]] * len(texts), dtype=np.float32)


def test_classify_source_raises_on_systematic_unknown_after_flushing():
    """>80% UNKNOWN over >=200 scanned rows means the LLM is deterministically
    failing to format its output, not that the corpus is genuinely noisy —
    that must fail the run loudly instead of caching junk. The guard must
    still run AFTER rows already produced are flushed (crash recovery
    resumes from the anti-join, so flushed rows aren't reprocessed for free).
    """
    pending_rows = [
        (f"activity {i}", i, f"activity {i}")  # single-line: no newline segments
        for i in range(205)
    ]
    client = _FakeClassifySourceClient(pending_rows)
    clickhouse = _FakeClassifySourceResource(client)

    def garbage_llm_call(prompt: str) -> str:
        return "I cannot help with that."

    with pytest.raises(ValueError, match=r"205/205 scanned .* UNKNOWN"):
        classify_source(
            _FakeContext(),
            clickhouse,
            table="corpscout.lv_companies",
            column="activity_text_original",
            embedder=_FakeEmbedder(),
            llm_call=garbage_llm_call,
            llm_model="fake-model",
        )

    # Flush must have already happened before the raise: all 205 rows were
    # inserted (nace_code='' for every one, since the LLM never produced
    # anything usable), not silently dropped.
    assert sum(len(batch) for batch in client.inserted_batches) == 205
    assert all(row[4] == "" for batch in client.inserted_batches for row in batch)
    assert all(row[6] == 0.0 for batch in client.inserted_batches for row in batch)


def test_classify_source_embeds_single_line_text_once():
    """A single-line source text has one segment equal to the whole text —
    embedding it twice would be a wasted call for no extra retrieval signal.
    """
    pending_rows = [("single line activity", 1, "single line activity")]
    client = _FakeClassifySourceClient(pending_rows)
    clickhouse = _FakeClassifySourceResource(client)

    embed_calls: list[list[str]] = []

    class _RecordingEmbedder:
        def embed(self, texts, instruction=None):
            embed_calls.append(list(texts))
            return np.array([[1.0, 0.0]] * len(texts), dtype=np.float32)

    good = json.dumps({"classifications": [{"id": "1", "nace_code": "C0000"}]})

    def llm_call(prompt: str) -> str:
        return good

    totals = classify_source(
        _FakeContext(),
        clickhouse,
        table="corpscout.lv_companies",
        column="activity_text_original",
        embedder=_RecordingEmbedder(),
        llm_call=llm_call,
        llm_model="fake-model",
    )

    assert totals == {"scanned": 1, "classified": 1, "unknown": 0}
    assert len(embed_calls) == 1
    assert len(embed_calls[0]) == 1  # whole text embedded once, no duplicate segment row


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_TRANSLATION_INTEGRATION_TESTS") != "1",
    reason="set RUN_TRANSLATION_INTEGRATION_TESTS=1 and CLICKHOUSE_*/COMMONCRAWL_EMBED_* env vars to run",
)
def test_lopkopiba_retrieval_anchor_against_real_corpus():
    from clickhouse_driver import Client

    from dagster_v3.defs.classifier.lib import EmbeddingClient

    client = Client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )
    try:
        codes, labels, corpus = load_corpus(client)
    finally:
        client.disconnect()

    embedder = EmbeddingClient.from_env()
    query = embedder.embed(["lopkopība"], instruction=QUERY_INSTRUCTION)

    ranked, _sims_by_code = candidate_codes(query, corpus, codes, k_whole=5, k_seg=0)
    assert any(code.startswith("01.4") for code in ranked), (
        f"expected an animal-production (01.4x) code in top-5 for 'lopkopība', got {ranked}"
    )
