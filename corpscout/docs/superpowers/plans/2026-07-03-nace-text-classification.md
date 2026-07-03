# NACE Text Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Classify Latvia UR free-text activity descriptions into NACE Rev 2.1 codes via embedding retrieval + LLM adjudication, cached per distinct text in ClickHouse.

**Architecture:** One ClickHouse migration adds a hash-keyed `text_classifications` cache and an `lv_companies_nace` view. A shared `defs/classifier/lib.py` embeds each unclassified distinct text (query-instruction-prefixed, against the EXISTING `corpscout.nace_category_embeddings` corpus — 1,025 pre-embedded NACE 2.1 entries), takes top-k by cosine (plain numpy), adjudicates ~10 texts per LLM prompt, and inserts results incrementally (resumable via anti-join). A per-source asset (`defs/latvia_ur/classification.py`) wires it into the register job. No new service, no vector store, no corpus ingestion.

**Tech Stack:** Python 3.14 (dagster, clickhouse via `ClickhouseResource`, `openai` SDK against local vLLM endpoints, numpy), ClickHouse (golang-migrate migrations).

**Spec:** `corpscout/docs/superpowers/specs/2026-07-03-nace-text-classification-design.md` (including the reuse-corpus addendum)

## Global Constraints

- Python work dir: `corpscout/dagster_v3` (`uv run` for everything); migrations in `corpscout/clickhouse/migrations/`.
- Corpus: read `corpscout.nace_category_embeddings` filtered `embedding_model = 'qwen3-embedding-8b'` AND `classification_version = 'NACE_REV_2_1'`, dedup per code by `argMax(..., resolved_at)`. Never write to that table.
- Query embedding convention (must match cc-enrich exactly): documents are embedded plain; queries as `"Instruct: {instruction}\nQuery: {text}"` with instruction `"Classify the business into its industry category"`. Embeddings L2-normalized float32.
- Embedder env: `COMMONCRAWL_EMBED_BASE_URL` (OpenAI-compatible `/v1`), `COMMONCRAWL_EMBED_MODEL` (default from endpoint model list), `COMMONCRAWL_EMBED_API_KEY` (default "x"). LLM env: `TRANSLATION_PROVIDER_LOCAL_BASE_URL`, `TRANSLATION_PROVIDER_LOCAL_MODEL`, `TRANSLATION_PROVIDER_LOCAL_API_KEY` (already in `.env.example`).
- Cache semantics: key `(source_table, source_column, source_text_hash)` with `cityHash64` computed in ClickHouse SQL; UNKNOWN stored as `nace_code = ''`; `classifier_version = 'NACE_REV_2_1'`; `method = 'embedding+llm'`; one `version = int(time.time())` per run; incremental flush every 500 texts.
- Classification input per text: `coalesce(non-empty English translation from text_translations, Latvian original)`.
- HTTP session for any plain requests use is `dlt.sources.helpers.requests` (repo convention) — but embedder/LLM go through the `openai` SDK (established pattern: `uk_companies_house/assets.py`, `reference_builder/embed.py`).
- Migration rules (from `dagster_v3/tests/test_clickhouse_migrations.py` + CLAUDE.md): 6-digit sequence naming; `.up.sql` must contain `CREATE DATABASE IF NOT EXISTS corpscout;`; no `;` inside SQL comments; `ORDER BY` must not reference Nullable columns; both `.up.sql`/`.down.sql` files; append the migration name (no suffix) to `EXPECTED_MIGRATIONS` in order.
- Python verification excludes the known-broken files: `--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py` and, ONLY if it still fails for pre-existing missing entries not yours, `--deselect tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit` (your own migration entries must be added regardless).
- Conventional Commits; `uv run ruff check` clean on changed files; `uv run dg check defs` green after asset changes.

---

### Task 1: ClickHouse migration — `text_classifications` + `lv_companies_nace`

**Files:**
- Create: `corpscout/clickhouse/migrations/0000NN_corpscout_text_classifications.up.sql` (NN = highest existing + 1; check `ls corpscout/clickhouse/migrations | tail` at execution time — it was 000084 at planning, the parallel workstream may have advanced it)
- Create: matching `.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (append to `EXPECTED_MIGRATIONS`)

**Interfaces:**
- Produces: table `corpscout.text_classifications` and view `corpscout.lv_companies_nace` (schema below) — Tasks 2–3 write to / are validated against these names and columns.

- [ ] **Step 1: Write the up migration**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.text_classifications
(
    source_table        LowCardinality(String),
    source_column       LowCardinality(String),
    source_text         String,
    source_text_hash    UInt64,
    nace_code           String,
    nace_candidates     Array(String),
    confidence          Float32,
    method              LowCardinality(String),
    model               LowCardinality(String),
    classifier_version  LowCardinality(String),
    version             UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash);

CREATE OR REPLACE VIEW corpscout.lv_companies_nace AS
SELECT
    c.*,
    cls.nace_code AS nace_code,
    cls.confidence AS nace_confidence,
    ifNull(n.label, '') AS nace_label
FROM corpscout.lv_companies AS c
LEFT JOIN (
    SELECT
        source_text_hash,
        argMax(nace_code, version) AS nace_code,
        argMax(confidence, version) AS confidence
    FROM corpscout.text_classifications
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS cls ON cls.source_text_hash = cityHash64(ifNull(c.activity_text_original, ''))
LEFT JOIN (
    SELECT code, argMax(label, resolved_at) AS label
    FROM corpscout.nace_category_embeddings
    WHERE classification_version = 'NACE_REV_2_1'
    GROUP BY code
) AS n ON n.code = cls.nace_code;
```

- [ ] **Step 2: Write the down migration**

```sql
DROP VIEW IF EXISTS corpscout.lv_companies_nace;
DROP TABLE IF EXISTS corpscout.text_classifications;
```

- [ ] **Step 3: Append to EXPECTED_MIGRATIONS and run the contract test**

Append `"0000NN_corpscout_text_classifications"` (your actual number) at the end of the tuple in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`.

Run: `cd corpscout/dagster_v3 && uv run pytest tests/test_clickhouse_migrations.py -v 2>&1 | tail -5`
Expected: your entry no longer among missing. If the test still fails, inspect the failure: if the ONLY missing entries are pre-existing ones from the parallel workstream (numbers you didn't create), leave them and note it in your report; if your entry is implicated, fix it.

- [ ] **Step 4: Apply against the real ClickHouse and smoke the view**

```bash
cd corpscout && make clickhouse-migrate-up 2>&1 | tail -3
```

Then verify (env from `dagster_v3/.env`): `SELECT count() FROM corpscout.text_classifications` → 0; `SELECT count() FROM corpscout.lv_companies_nace` → equals `lv_companies` count; `SELECT nace_code, nace_label FROM corpscout.lv_companies_nace LIMIT 1` → empty strings (no classifications yet). Use a one-liner via `uv run python` with `clickhouse_connect` (pattern exists throughout this session's tests).

- [ ] **Step 5: Commit**

```bash
git add corpscout/clickhouse/migrations/ corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): text_classifications cache and lv_companies_nace view"
```

---

### Task 2: Classifier lib

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/classifier/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/classifier/lib.py`
- Test: `corpscout/dagster_v3/tests/test_classifier_lib.py`

**Interfaces:**
- Produces (consumed by Task 3):

```python
QUERY_INSTRUCTION = "Classify the business into its industry category"
CLASSIFIER_VERSION = "NACE_REV_2_1"

class EmbeddingClient:
    def __init__(self, *, base_url, api_key="x", model=None, batch=128, timeout=120): ...
    @classmethod
    def from_env(cls) -> "EmbeddingClient": ...   # COMMONCRAWL_EMBED_* vars
    def embed(self, texts: list[str], instruction: str | None = None) -> "np.ndarray": ...

def load_corpus(client) -> tuple[list[str], list[str], "np.ndarray"]
    # (codes, labels, matrix) from nace_category_embeddings; client is a
    # clickhouse connection with .execute()

def build_scan_sql(table: str, column: str) -> str
    # distinct unclassified texts with coalesced EN input_text

def candidate_codes(query_vecs, corpus_matrix, codes, k_whole=8, k_seg=3) -> list[str]
    # ranked union: first row of query_vecs is the whole text, rest are segments

def classify_source(context, clickhouse, *, table, column,
                    embedder=None, llm_call=None, llm_model=None,
                    flush_every=500, adjudicate_batch_size=10) -> dict
    # orchestrator; returns {"scanned": n, "classified": n, "unknown": n}
```

- Consumes: Task 1's table (INSERT target). Uses `openai.OpenAI` for both endpoints.

- [ ] **Step 1: Write the failing tests**

Create `corpscout/dagster_v3/tests/test_classifier_lib.py`:

```python
"""Tests for the NACE classifier lib (fake embedder / LLM / ClickHouse client)."""

import json

import numpy as np

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
```

Also add an env-guarded integration test at the bottom (marker + skipif on `RUN_TRANSLATION_INTEGRATION_TESTS != "1"`), which builds a real `EmbeddingClient.from_env()`, loads the real corpus via `clickhouse_connect`, embeds `"lopkopība"` with the query instruction, and asserts the top-5 candidate codes include at least one code starting with `"01.4"` (animal production) — the sanity anchor from the spec. Write it fully, mirroring `test_translator_load.py`'s guarded test.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_classifier_lib.py -v 2>&1 | head -5`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement lib.py**

Create `corpscout/dagster_v3/src/dagster_v3/defs/classifier/lib.py`:

```python
"""Shared NACE-classification machinery (no assets of its own).

Pipeline per source column: anti-join scan for distinct unclassified texts
(classification input prefers the English translation when one exists) →
embed with the production embedder using the SAME query-instruction
convention cc-enrich uses against corpscout.nace_category_embeddings →
brute-force cosine top-k over the pre-embedded corpus (1,025 vectors — no
vector store) → batched LLM adjudication constrained to the candidate set →
incremental inserts into corpscout.text_classifications (a crashed run
resumes from the anti-join). UNKNOWN is stored as nace_code = '' so noise is
adjudicated once, not every run.
"""

import json
import os
import time

import numpy as np

QUERY_INSTRUCTION = "Classify the business into its industry category"
CLASSIFIER_VERSION = "NACE_REV_2_1"
EMBEDDING_MODEL = "qwen3-embedding-8b"
METHOD = "embedding+llm"


class EmbeddingClient:
    """OpenAI-compatible /v1/embeddings client (vLLM Qwen3-Embedding-8B)."""

    def __init__(self, *, base_url, api_key="x", model=None, batch=128, timeout=120):
        from openai import OpenAI

        self._client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout)
        self._model = model or self._client.models.list().data[0].id
        self._batch = batch

    @classmethod
    def from_env(cls) -> "EmbeddingClient":
        base_url = os.environ["COMMONCRAWL_EMBED_BASE_URL"]
        return cls(
            base_url=base_url,
            api_key=os.environ.get("COMMONCRAWL_EMBED_API_KEY", "x"),
            model=os.environ.get("COMMONCRAWL_EMBED_MODEL") or None,
        )

    def embed(self, texts, instruction=None):
        if instruction:
            texts = [f"Instruct: {instruction}\nQuery: {t}" for t in texts]
        out = []
        for start in range(0, len(texts), self._batch):
            chunk = texts[start : start + self._batch]
            out.extend(
                d.embedding
                for d in self._client.embeddings.create(model=self._model, input=chunk).data
            )
        matrix = np.asarray(out, dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return matrix / norms


def load_corpus(client):
    """(codes, labels, unit-norm matrix) from the shared NACE embedding table."""
    rows = client.execute(
        """
        SELECT code,
               argMax(label, resolved_at) AS label,
               argMax(embedding, resolved_at) AS embedding
        FROM corpscout.nace_category_embeddings
        WHERE embedding_model = 'qwen3-embedding-8b'
          AND classification_version = 'NACE_REV_2_1'
        GROUP BY code
        ORDER BY code
        """
    )
    codes = [r[0] for r in rows]
    labels = [r[1] for r in rows]
    matrix = np.asarray([r[2] for r in rows], dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return codes, labels, matrix / norms


def build_scan_sql(table: str, column: str) -> str:
    """Distinct unclassified texts with their preferred classification input.

    Trusted, developer-authored table/column values (same trust boundary as
    the translation loaders).
    """
    return f"""
SELECT
    src.source_text AS source_text,
    cityHash64(src.source_text) AS source_text_hash,
    coalesce(nullif(tr.translated_text, ''), src.source_text) AS input_text
FROM (
    SELECT DISTINCT ifNull({column}, '') AS source_text
    FROM {table}
    WHERE ifNull({column}, '') != ''
) AS src
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS tr ON tr.source_text_hash = cityHash64(src.source_text)
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_classifications
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS done ON done.source_text_hash = cityHash64(src.source_text)"""


def candidate_codes(query_vecs, corpus_matrix, codes, k_whole=8, k_seg=3):
    """Ranked, deduped candidate codes: whole-text top-k ∪ per-segment top-k.

    query_vecs row 0 is the whole text; remaining rows are its segments.
    """
    sims = query_vecs @ corpus_matrix.T
    ranked: list[str] = []
    seen: set[str] = set()

    def take(row, k):
        for idx in np.argsort(-row)[:k]:
            code = codes[int(idx)]
            if code not in seen:
                seen.add(code)
                ranked.append(code)

    take(sims[0], k_whole)
    for row in sims[1:]:
        take(row, k_seg)
    return ranked


def _parse_adjudication(text, *, expected_ids, allowed):
    """Parse the LLM response; None means unusable (caller retries once)."""
    try:
        payload = json.loads(text)
        rows = payload["classifications"]
        result = {}
        for row in rows:
            item_id = str(row["id"])
            if item_id not in expected_ids:
                return None
            code = str(row["nace_code"]).strip()
            if code == "UNKNOWN" or code not in set(allowed.get(item_id, [])):
                result[item_id] = ""
            else:
                result[item_id] = code
    except (KeyError, TypeError, ValueError):
        return None
    if set(result) != set(expected_ids):
        return None
    return result


def _build_prompt(items, labels_by_code):
    lines = [
        "Classify each business activity description into its NACE category.",
        'Pick the best code FROM THE LISTED CANDIDATES ONLY, or "UNKNOWN" if none fit.',
        'Return only JSON: {"classifications":[{"id":"...","nace_code":"..."}]}',
        "",
    ]
    for item_id, original, translated, cands in items:
        lines.append(f"Item {item_id}:")
        lines.append(f"  activity: {original}")
        if translated and translated != original:
            lines.append(f"  english: {translated}")
        for code in cands:
            lines.append(f"  candidate {code}: {labels_by_code.get(code, '')}")
        lines.append("")
    return "\n".join(lines)


def _adjudicate(llm_call, items, *, labels_by_code):
    """items: [(id, original, translated, candidate_codes)] → {id: code-or-''}.

    One retry on unusable output, then everything in the batch is UNKNOWN —
    a bad batch must never crash the run.
    """
    expected = {item[0] for item in items}
    allowed = {item[0]: item[3] for item in items}
    prompt = _build_prompt(items, labels_by_code)
    for _ in range(2):
        result = _parse_adjudication(llm_call(prompt), expected_ids=expected, allowed=allowed)
        if result is not None:
            return result
    return {item_id: "" for item_id in expected}


def _llm_call_from_env():
    from openai import OpenAI

    base_url = os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"]
    model = os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
    client = OpenAI(
        base_url=base_url,
        api_key=os.environ.get("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
        timeout=120,
    )

    def call(prompt: str) -> str:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        return response.choices[0].message.content or ""

    return call, model


def classify_source(
    context,
    clickhouse,
    *,
    table,
    column,
    embedder=None,
    llm_call=None,
    llm_model=None,
    flush_every=500,
    adjudicate_batch_size=10,
):
    """Classify all unclassified distinct texts of one source column."""
    embedder = embedder or EmbeddingClient.from_env()
    if llm_call is None:
        llm_call, llm_model = _llm_call_from_env()
    version = int(time.time())

    with clickhouse.get_connection() as client:
        codes, labels, corpus = load_corpus(client)
        # Sanity floor: an empty/shrunken corpus (renamed model, wrong
        # classification_version, reference-builder mishap) must fail loudly
        # instead of classifying against nothing. Full corpus is 1,025 rows.
        if len(codes) < 900:
            raise ValueError(
                f"nace_category_embeddings corpus too small ({len(codes)} rows) "
                "for embedding_model=qwen3-embedding-8b / NACE_REV_2_1"
            )
        labels_by_code = dict(zip(codes, labels))
        pending = client.execute(build_scan_sql(table, column))
        context.log.info("classifying %d distinct texts for %s.%s", len(pending), table, column)

        buffer = []
        totals = {"scanned": len(pending), "classified": 0, "unknown": 0}

        def flush():
            if not buffer:
                return
            client.execute(
                """
                INSERT INTO corpscout.text_classifications (
                    source_table, source_column, source_text, source_text_hash,
                    nace_code, nace_candidates, confidence, method, model,
                    classifier_version, version
                ) VALUES
                """,
                buffer,
            )
            context.log.info("flushed %d classifications", len(buffer))
            buffer.clear()

        for start in range(0, len(pending), adjudicate_batch_size):
            batch = pending[start : start + adjudicate_batch_size]
            items = []
            per_text = {}
            for offset, (source_text, text_hash, input_text) in enumerate(batch):
                item_id = str(offset + 1)
                segments = [s.strip() for s in input_text.splitlines() if s.strip()]
                queries = embedder.embed([input_text] + segments, instruction=QUERY_INSTRUCTION)
                cands = candidate_codes(queries, corpus, codes)
                similarity = float(np.max(queries[0] @ corpus.T))
                per_text[item_id] = (source_text, text_hash, cands, similarity)
                items.append((item_id, source_text, input_text, cands))

            decisions = _adjudicate(llm_call, items, labels_by_code=labels_by_code)
            for item_id, (source_text, text_hash, cands, similarity) in per_text.items():
                code = decisions[item_id]
                totals["classified" if code else "unknown"] += 1
                buffer.append(
                    (table, column, source_text, text_hash, code, cands,
                     similarity if code else 0.0, METHOD, llm_model or "",
                     CLASSIFIER_VERSION, version)
                )
            if len(buffer) >= flush_every:
                flush()
        flush()
    return totals
```

Create `corpscout/dagster_v3/src/dagster_v3/defs/classifier/__init__.py` with a two-line docstring pointing at `lib.py` (library package, no assets — mirrors `translator_load/__init__.py`).

Adaptation notes for the implementer: (a) `client.execute` row shapes come from `clickhouse_driver` (native protocol) — tuples, `Array(Float32)` arrives as a Python list; the fakes mirror that. (b) Embedding one text + segments per item means small frequent embed calls — acceptable for v1 (46k texts ≈ 46k+segments calls at batch 128 internally); if the integration run shows it's slow, batch the embed across the adjudication batch (collect all texts+segments, one embed call, then split) — behavior identical, tests unaffected. (c) Keep `_parse_adjudication` strict exactly as tested.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_classifier_lib.py -v 2>&1 | tail -6`
Expected: all unit tests PASS; integration test SKIPPED without env.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src/dagster_v3/defs/classifier/ tests/test_classifier_lib.py
git add src/dagster_v3/defs/classifier/ tests/test_classifier_lib.py
git commit -m "feat(dagster): NACE classifier lib — embed-retrieve-adjudicate over shared corpus"
```

---

### Task 3: Latvia classification asset, wiring, env, docs, live verification

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/classification.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py` (register asset; extend `latvia_ur_register_job` selection)
- Modify: `corpscout/dagster_v3/tests/test_latvia_ur_assets.py` (job membership pin)
- Modify: `corpscout/dagster_v3/.env.example` (add `COMMONCRAWL_EMBED_*` block — currently absent)
- Modify: `corpscout/dagster_v3/docs/data-source-guidelines.md` (one bullet in §8: sources without official industry codes add a `defs/<source>/classification.py` mirroring Latvia)
- Test: wiring test added to `corpscout/dagster_v3/tests/test_classifier_lib.py` or `test_latvia_ur_assets.py` (deps + group)

**Interfaces:**
- Consumes: `classify_source`, plus `dg`/`ClickhouseResource` patterns identical to `latvia_ur/translation.py`.
- Produces: asset `latvia_ur_nace_classification` (group `latvia_ur`, deps `latvia_ur_clickhouse_companies`), included in `latvia_ur_register_job`.

- [ ] **Step 1: Write the asset**

Create `corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/classification.py`:

```python
"""Latvia UR NACE classification: classify activity texts after ingest.

Latvia publishes no bulk per-company NACE codes (VID keeps them
lookup-only), so activity_text_original is classified semantically via the
shared classifier lib. Results land in corpscout.text_classifications and
surface through the lv_companies_nace view.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.classifier.lib import classify_source


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Classify corpscout.lv_companies activity texts into NACE Rev 2.1 via "
        "embedding retrieval over nace_category_embeddings plus LLM "
        "adjudication; cached per distinct text in text_classifications."
    ),
)
def latvia_ur_nace_classification(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = classify_source(
        context, clickhouse,
        table="corpscout.lv_companies",
        column="activity_text_original",
    )
    return dg.MaterializeResult(
        metadata={
            "scanned": totals["scanned"],
            "classified": totals["classified"],
            "unknown": totals["unknown"],
        }
    )
```

- [ ] **Step 2: Register and wire the job**

In `latvia_ur/assets.py`: import `latvia_ur_nace_classification` from `.classification` (top of file, with the `.translation` import); add it to `defs.assets`; change the register job selection to include both leaves:

```python
latvia_ur_register_job = dg.define_asset_job(
    "latvia_ur_register_job",
    selection=dg.AssetSelection.assets(
        "latvia_ur_translation_load", "latvia_ur_nace_classification"
    ).upstream(),
)
```

Update `test_latvia_ur_assets.py`'s `register_keys` pin to include `"latvia_ur_nace_classification"` (with a one-line comment mirroring the translation loader's). Add a wiring test asserting the asset's dep on `latvia_ur_clickhouse_companies` (same accessor pattern the translation wiring test uses).

- [ ] **Step 3: Env example + docs**

Append to `corpscout/dagster_v3/.env.example` (values mirror `commoncrawl/.env.example`):

```
# Production embedder (vLLM, Qwen3-Embedding-8B) — used by the NACE classifier.
COMMONCRAWL_EMBED_BASE_URL=http://<embed-host>:8000/v1
COMMONCRAWL_EMBED_MODEL=Qwen/Qwen3-Embedding-8B
COMMONCRAWL_EMBED_API_KEY=x
```

Add to `docs/data-source-guidelines.md` §8, after the loader-pattern bullets: sources WITHOUT official industry codes add `defs/<source>/classification.py` mirroring `defs/latvia_ur/classification.py` (shared machinery in `defs/classifier/lib.py`; cache table `corpscout.text_classifications`; view mirrors `lv_companies_nace`).

- [ ] **Step 4: Verify definitions, tests, lint**

```bash
uv run dg check defs 2>&1 | tail -2
uv run pytest tests/test_classifier_lib.py tests/test_latvia_ur_assets.py -v 2>&1 | tail -4
uv run pytest --ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py 2>&1 | tail -2
uv run ruff check src/dagster_v3/defs/classifier/ src/dagster_v3/defs/latvia_ur/ tests/test_classifier_lib.py tests/test_latvia_ur_assets.py
```

(Apply the migration-ledger deselect only if that test fails on pre-existing entries.)

- [ ] **Step 5: Live smoke (stack permitting)**

With `dagster_v3/.env` loaded (embedder + ClickHouse + LLM reachable):

```bash
RUN_TRANSLATION_INTEGRATION_TESTS=1 uv run pytest tests/test_classifier_lib.py -m integration -v
```

Expected: the "lopkopība" retrieval anchor passes against the real embedder + corpus. Then materialize the real asset once with a small cap if you want an end-to-end sample — OPTIONAL and only if the operator (user) hasn't said otherwise; the scheduled register job will run it in production regardless. Record whichever you did in the report.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/latvia_ur/ tests/ .env.example docs/data-source-guidelines.md
git commit -m "feat(dagster): latvia NACE classification asset wired into register job"
```

---

## Deployment note (not a code task)

`make clickhouse-migrate-up` (from `corpscout/`) must run before the first materialization — Task 1 Step 4 already applies it to the lab ClickHouse; repeat wherever else migrations are applied. First full run classifies ~46k distinct texts: embedding is minutes, adjudication at ~10 texts/prompt is the long pole (hours, LLM-bound) — it shares the qwen3:6b endpoint with the translator's drain, so expect them to contend if run simultaneously. Re-runs only process new texts.
