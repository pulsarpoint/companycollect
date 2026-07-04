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
    def from_env(cls, *, base_url=None, model=None, api_key=None) -> "EmbeddingClient":
        """Build from COMMONCRAWL_EMBED_* env vars; explicit args override.

        Overrides exist because the GPU box hosting the endpoint moves
        between addresses — assets expose them as run config so an operator
        can retarget a materialization without editing .env and restarting.
        """
        return cls(
            base_url=base_url or os.environ["COMMONCRAWL_EMBED_BASE_URL"],
            api_key=api_key or os.environ.get("COMMONCRAWL_EMBED_API_KEY", "x"),
            model=model or os.environ.get("COMMONCRAWL_EMBED_MODEL") or None,
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
    WHERE source_table = '{table}' AND source_column = '{column}' AND target_lang = 'en'
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
    Returns (ranked_codes, sims_by_code) where sims_by_code maps each
    candidate code to its best cosine similarity across the whole-text row
    AND any segment row — the confidence stored for a code must reflect the
    strongest evidence for it, not just the whole-text score.
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

    best_by_col = sims.max(axis=0)
    idx_by_code = {code: idx for idx, code in enumerate(codes)}
    sims_by_code = {code: float(best_by_col[idx_by_code[code]]) for code in ranked}
    return ranked, sims_by_code


def _extract_json(text):
    """Best-effort strip of markdown fencing / prose wrapping around JSON.

    Deterministic LLM output occasionally wraps the JSON payload in a
    ```/```json code fence or a prose preamble ("Here is the answer: {...}
    hope this helps"). json.loads() has zero tolerance for either, so pull
    the payload out before parsing. Returns the original text unchanged if
    no braces are found, so json.loads() still fails with the same
    "unusable" signal for genuine garbage.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        body = stripped[3:]
        if body.startswith("json"):
            body = body[4:]
        body = body.strip()
        if body.endswith("```"):
            body = body[:-3]
        return body.strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text
    return stripped[start : end + 1]


def _parse_adjudication(text, *, expected_ids, allowed):
    """Parse the LLM response; None means unusable (caller retries once)."""
    try:
        payload = json.loads(_extract_json(text))
        rows = payload["classifications"]
        result = {}
        for row in rows:
            item_id = str(row["id"])
            # Unexpected id, or an id repeated in the same response, both
            # mean the response can't be trusted — fail closed rather than
            # silently keeping the last value for a duplicate.
            if item_id not in expected_ids or item_id in result:
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


def llm_call_from_env(*, base_url=None, model=None, api_key=None):
    """Chat-completions call factory from TRANSLATION_PROVIDER_LOCAL_* env
    vars; explicit args override (see EmbeddingClient.from_env for why).
    Returns (call, model)."""
    from openai import OpenAI

    base_url = base_url or os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"]
    model = model or os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
    client = OpenAI(
        base_url=base_url,
        api_key=api_key or os.environ.get("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
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
        llm_call, llm_model = llm_call_from_env()
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
                # Single-line text: the only "segment" is the whole text
                # itself — embedding it twice would waste a call for no
                # extra retrieval signal.
                query_texts = [input_text] if segments == [input_text] else [input_text] + segments
                queries = embedder.embed(query_texts, instruction=QUERY_INSTRUCTION)
                cands, sims_by_code = candidate_codes(queries, corpus, codes)
                per_text[item_id] = (source_text, text_hash, cands, sims_by_code)
                items.append((item_id, source_text, input_text, cands))

            decisions = _adjudicate(llm_call, items, labels_by_code=labels_by_code)
            for item_id, (source_text, text_hash, cands, sims_by_code) in per_text.items():
                code = decisions[item_id]
                totals["classified" if code else "unknown"] += 1
                # Confidence reflects the CHOSEN code's own similarity, not
                # the whole-text top-1 score regardless of what the LLM
                # picked; 0.0 for UNKNOWN (code == "", never a dict key).
                buffer.append(
                    (table, column, source_text, text_hash, code, cands,
                     sims_by_code.get(code, 0.0), METHOD, llm_model or "",
                     CLASSIFIER_VERSION, version)
                )
            if len(buffer) >= flush_every:
                flush()
        flush()
        # A systematic adjudication failure (e.g. the LLM deterministically
        # emitting unparseable output) must fail the unattended schedule
        # loudly instead of quietly caching the whole corpus as UNKNOWN.
        # Guard runs AFTER the final flush(): rows are already flushed
        # incrementally as we go, so failing loudly here does not lose the
        # "resume from anti-join" property, it just also raises once the
        # damage is visible.
        if totals["scanned"] >= 200 and totals["unknown"] / totals["scanned"] > 0.8:
            raise ValueError(
                f"Systematic classification failure for {table}.{column}: "
                f"{totals['unknown']}/{totals['scanned']} scanned texts came back UNKNOWN "
                "(>80% over >=200 scanned) — this points at a deterministic LLM/formatting "
                "failure, not genuine corpus noise. Rows already flushed this run are cached "
                "as nace_code=''; recover with `DELETE FROM corpscout.text_classifications "
                "WHERE nace_code = ''` and rerun this asset."
            )
    return totals
