"""Tests for the NACE classifier lib (fake embedder / LLM / ClickHouse client)."""

import json
import os

import numpy as np
import pytest

from dagster_v3.defs.classifier.lib import (
    CLASSIFIER_VERSION,
    QUERY_INSTRUCTION,
    _adjudicate,
    _parse_adjudication,
    build_scan_sql,
    candidate_codes,
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
    ranked = candidate_codes(queries, corpus, codes, k_whole=2, k_seg=1)
    assert ranked[0] == "A"          # whole-text winner leads
    assert set(ranked) >= {"A", "B", "C"}
    assert len(ranked) == len(set(ranked))  # deduped


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

    ranked = candidate_codes(query, corpus, codes, k_whole=5, k_seg=0)
    assert any(code.startswith("01.4") for code in ranked), (
        f"expected an animal-production (01.4x) code in top-5 for 'lopkopība', got {ranked}"
    )
