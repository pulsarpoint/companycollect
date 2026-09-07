"""Run Crawl4AI's extraction and adaptive discovery without custom selection logic."""

import asyncio
import json
import random
import time
from collections import Counter
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from importlib.metadata import version
from pathlib import Path
from unittest.mock import patch

import click
import crawl4ai.utils
import litellm
import numpy as np
from crawl4ai import AdaptiveConfig, AdaptiveCrawler, CacheMode, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from dotenv import dotenv_values

from company_objectives_lab.models import OBJECTIVES, Extraction
from company_objectives_lab.prompts import EXTRACTION_INSTRUCTIONS
from ex3.crawler import _open_crawler
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json

MODEL = "openrouter/deepseek/deepseek-v4-flash-0731"
OPTIONS = {
    "temperature": 0,
    "max_tokens": 65536,
    "timeout": 180,
    "num_retries": 0,
    "extra_body": {
        "provider": {
            "only": ["baidu/fp8"],
            "allow_fallbacks": False,
            "require_parameters": True,
        },
        "reasoning": {"enabled": True, "exclude": True, "effort": "low"},
    },
}
TASK: ContextVar[str] = ContextVar("native_task", default="unknown")
DATA = Path("company_objectives_lab/data/v1")
INSTRUCTION = (
    EXTRACTION_INSTRUCTIONS.replace(
        "Return only the schema JSON.",
        "Follow Crawl4AI's output envelope and the supplied schema.",
    )
    + "\nOBJECTIVES:\n"
    + json.dumps(OBJECTIVES)
)
QUERY = (
    "Find information about this company: its legal identity, identifiers, history "
    "and business; company and department emails, phone numbers and contact forms; "
    "office locations and addresses; products and services; named people, their "
    "roles and business contact details; parent companies, subsidiaries, brands, "
    "partners and other explicitly connected companies; and advertised job openings. "
    "Look for all these objectives on every fetched page, including secondary facts. "
    "Finding one person, office or job does not complete that collection."
)


def freeze(path: Path, value: dict) -> None:
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != value:
            raise ValueError(f"Frozen settings changed: {path}")
    else:
        write_json(path, value)


def api_key() -> str:
    key = dotenv_values("jobs_extraction_lab/.env").get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("Missing OPENROUTER_API_KEY")
    return key


def numpy_json_value(value):
    """Convert NumPy metrics at the JSON output boundary; reject other unknown types."""
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


@contextmanager
def record_completions(root: Path, key: str, options: dict):
    """Observe real LiteLLM calls; supply routing/deadlines missing from AdaptiveConfig.

    Native prompts, chunks, responses, parsers and ranking remain untouched.
    Requests are bounded to three simultaneous calls, including native chunk fans.
    """
    sync_completion, async_completion = litellm.completion, litellm.acompletion
    counts: Counter = Counter()
    semaphore = asyncio.Semaphore(3)

    def start(kwargs: dict) -> tuple[Path, dict]:
        task = TASK.get()
        counts[task] += 1
        path = root / "calls" / task / f"{counts[task]:03d}.json"
        if path.exists():
            raise ValueError(f"Refusing to overwrite a model call: {path}")
        request = {
            k: v
            for k, v in kwargs.items()
            if k
            in {
                "model",
                "messages",
                "temperature",
                "max_tokens",
                "timeout",
                "num_retries",
                "extra_body",
                "response_format",
                "base_url",
            }
        }
        record = {"started_at": utc_now(), "request": request}
        write_json(path, record)
        return path, record

    def finish(path: Path, record: dict, started: float, response=None, error=None):
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        if response is not None:
            record["response"] = response.model_dump(mode="json")
        if error is not None:
            record["error"] = f"{type(error).__name__}: {error}".replace(
                key, "[REDACTED]"
            )
        write_json(path, record)

    def sync_call(**kwargs):
        kwargs.update(options)
        path, record = start(kwargs)
        started = time.monotonic()
        try:
            response = sync_completion(**kwargs)
        except Exception as error:
            finish(path, record, started, error=error)
            raise
        finish(path, record, started, response=response)
        return response

    async def async_call(**kwargs):
        async with semaphore:
            kwargs.update(options)
            path, record = start(kwargs)
            started = time.monotonic()
            try:
                response = await async_completion(**kwargs)
            except Exception as error:
                finish(path, record, started, error=error)
                raise
            finish(path, record, started, response=response)
            return response

    with (
        patch.object(litellm, "completion", sync_call),
        patch.object(litellm, "acompletion", async_call),
    ):
        yield


def extraction_strategy(key: str, chunked: bool) -> LLMExtractionStrategy:
    return LLMExtractionStrategy(
        llm_config=LLMConfig(provider=MODEL, api_token=key, backoff_max_attempts=2),
        schema=Extraction.model_json_schema(),
        instruction=INSTRUCTION,
        input_format="cleaned_html",
        apply_chunking=chunked,
        extra_args=OPTIONS,
        verbose=False,
    )


async def extract_snapshots(
    root: Path, chunked: bool, page_ids: tuple[str, ...]
) -> None:
    manifest_text = (DATA / "extraction-inputs.json").read_text(encoding="utf-8")
    pages = json.loads(manifest_text)["pages"]
    if page_ids:
        pages = [p for p in pages if p["page_id"] in page_ids]
        if {p["page_id"] for p in pages} != set(page_ids):
            raise ValueError("Unknown page ID")
    key = api_key()
    settings = {
        "kind": "native_extraction",
        "crawl4ai_version": version("crawl4ai"),
        "unclecode_litellm_version": version("unclecode-litellm"),
        "model": MODEL,
        "options": OPTIONS,
        "instruction": INSTRUCTION,
        "schema": Extraction.model_json_schema(),
        "force_json_response": False,
        "apply_chunking": chunked,
        "chunk_token_threshold": 2048 if chunked else 1e9,
        "overlap_rate": 0.1,
        "word_token_rate": 1.3,
        "backoff_max_attempts": 2,
        "concurrency": 3,
        "corrections": 0,
        "input_manifest_sha256": content_hash(manifest_text),
        "reference_sha256": content_hash(
            Path("company_objectives_lab/reference-v1.json").read_text(encoding="utf-8")
        ),
        "pages": pages,
    }
    freeze(root / "settings.json", settings)
    semaphore = asyncio.Semaphore(3)

    async def execute(page: dict) -> None:
        async with semaphore:
            page_id = page["page_id"]
            html = (DATA / page["file"]).read_text(encoding="utf-8")
            if content_hash(html) != page["sha256"]:
                raise ValueError(f"Snapshot changed: {page_id}")
            target = root / "pages" / f"{page_id}.json"
            if target.exists():
                return
            if (root / "calls" / page_id).exists():
                raise ValueError(
                    f"Interrupted page has saved calls; use a new run: {page_id}"
                )
            token = TASK.set(page_id)
            started = time.monotonic()
            strategy = extraction_strategy(key, chunked)
            chunks = strategy._merge(
                [html],
                strategy.chunk_token_threshold,
                overlap=int(strategy.chunk_token_threshold * strategy.overlap_rate),
            )
            blocks = await strategy.arun(page["source_url"], [html])
            write_json(
                target,
                {
                    "page_id": page_id,
                    "source_sha256": page["sha256"],
                    "chunk_chars": [len(c) for c in chunks],
                    "chunk_sha256": [content_hash(c) for c in chunks],
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "blocks": blocks,
                },
            )
            TASK.reset(token)
            click.echo(
                f"{root.name} {page_id}: {len(chunks)} chunks; {len(blocks)} native blocks"
            )

    with record_completions(root, key, OPTIONS):
        await asyncio.gather(*(execute(page) for page in pages))
    write_json(
        root / "status.json",
        {"status": "complete", "pages": len(pages), "at": utc_now()},
    )


@contextmanager
def repair_query_envelope(root: Path):
    """Diagnostic-only compatibility adapter; preserve the original recorded response."""
    original = crawl4ai.utils.perform_completion_with_backoff

    def completion(**kwargs):
        response = original(**kwargs)
        raw = response.choices[0].message.content
        parsed = json.loads(raw)
        if not isinstance(parsed, list) or not all(isinstance(v, str) for v in parsed):
            return response
        repaired = response.model_copy(deep=True)
        repaired.choices[0].message.content = json.dumps({"queries": parsed})
        write_json(
            root / "query-envelope-repairs" / f"{TASK.get()}.json",
            {
                "original": raw,
                "adapted": repaired.choices[0].message.content,
                "policy": "Wrap the returned string array in the object expected by native parsing; no query edits.",
            },
        )
        return repaired

    with patch.object(crawl4ai.utils, "perform_completion_with_backoff", completion):
        yield


async def adaptive_sites(
    root: Path, strategy_name: str, port: int, repair_queries: bool = False
) -> None:
    sites = json.loads((DATA / "candidates.json").read_text(encoding="utf-8"))["sites"]
    key = api_key()
    settings = {
        "kind": "native_adaptive",
        "crawl4ai_version": version("crawl4ai"),
        "strategy": strategy_name,
        "max_pages": 5,
        "max_depth": 4,
        "top_k_links": 1,
        "query": QUERY,
        "model": MODEL,
        "options": OPTIONS,
        "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        "seed": 20260906,
        "site_deadline_seconds": 900,
        "browser_overrides": {
            "cache_mode": "bypass",
            "page_timeout": 45000,
            "check_robots_txt": True,
            "delay_before_return_html": 2.0,
        },
        "sites": [{"domain": s["domain"], "base_url": s["base_url"]} for s in sites],
        "policy": "Unmodified native discovery/scoring/stopping. Five rendered-page attempts per site; metadata previews are additional requests. No LLM extraction in AdaptiveCrawler's run config.",
    }
    if repair_queries:
        settings["query_envelope_repair"] = (
            "Array of strings -> object with queries; no ranking changes"
        )
    freeze(root / "settings.json", settings)
    with (
        record_completions(root, key, OPTIONS),
        repair_query_envelope(root) if repair_queries else nullcontext(),
    ):
        async with _open_crawler(
            headless=True, proxy=None, cdp_port=port, accept_language="en-US,en;q=0.9"
        ) as crawler:
            original_arun = crawler.arun
            for site in sites:
                domain = site["domain"]
                target = root / "sites" / f"{domain}.json"
                if target.exists():
                    continue
                if (root / "calls" / domain).exists() or (
                    root / "snapshots" / domain
                ).exists():
                    raise ValueError(
                        f"Interrupted site has saved work; use a new run: {domain}"
                    )
                token = TASK.set(domain)
                random.seed(20260906)
                attempted: list[str] = []

                async def capture_arun(
                    *, url, config, attempted=attempted, domain=domain
                ):
                    if len(attempted) >= 5:
                        raise ValueError("Five rendered-page attempts reached")
                    attempted.append(url)
                    snapshot = (
                        root / "snapshots" / domain / f"{len(attempted):02d}.json"
                    )
                    write_json(
                        snapshot, {"requested_url": url, "started_at": utc_now()}
                    )
                    config = config.clone(
                        cache_mode=CacheMode.BYPASS,
                        page_timeout=45000,
                        check_robots_txt=True,
                        delay_before_return_html=2.0,
                        verbose=False,
                    )
                    results = await original_arun(url=url, config=config)  # ty: ignore[missing-argument] -- Crawl4AI decorator typing.
                    result = next(iter(results))
                    write_json(
                        snapshot,
                        {
                            "requested_url": url,
                            "result": result.model_dump(mode="json"),
                        },
                    )
                    click.echo(
                        f"{strategy_name} {domain}: page {len(attempted)}, success={result.success}, {url}"
                    )
                    return results

                config = AdaptiveConfig(
                    strategy=strategy_name,
                    max_pages=5,
                    max_depth=4,
                    top_k_links=1,
                    query_llm_config=LLMConfig(
                        provider=MODEL, api_token=key, backoff_max_attempts=2
                    ),
                )
                adaptive = AdaptiveCrawler(crawler, config)
                started = time.monotonic()
                outcome = {"domain": domain, "started_at": utc_now()}
                try:
                    with patch.object(crawler, "arun", capture_arun):
                        state = await asyncio.wait_for(
                            adaptive.digest(site["base_url"], QUERY), timeout=900
                        )
                    outcome.update(
                        metrics=state.metrics,
                        crawled_urls=sorted(state.crawled_urls),
                        is_sufficient=adaptive.is_sufficient,
                        pending_links=[
                            link.model_dump(mode="json") for link in state.pending_links
                        ],
                        expanded_queries=state.expanded_queries,
                    )
                except Exception as error:  # noqa: BLE001 -- Keep failed native runs as benchmark outcomes.
                    outcome["error"] = f"{type(error).__name__}: {error}".replace(
                        key, "[REDACTED]"
                    )
                outcome.update(
                    attempted_urls=attempted,
                    elapsed_seconds=round(time.monotonic() - started, 3),
                )
                write_json(
                    target, json.loads(json.dumps(outcome, default=numpy_json_value))
                )
                TASK.reset(token)
                click.echo(
                    f"{strategy_name} {domain}: complete, {len(attempted)} attempts, error={outcome.get('error')}"
                )
    write_json(
        root / "status.json",
        {"status": "complete", "sites": len(sites), "at": utc_now()},
    )


@click.group()
def main() -> None:
    """Separate native Crawl4AI benchmark; completed outcomes are immutable."""


@main.command("extract")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--chunked", is_flag=True)
@click.option("--page", "page_ids", multiple=True)
def extract_command(run_dir: Path, chunked: bool, page_ids: tuple[str, ...]) -> None:
    asyncio.run(extract_snapshots(run_dir, chunked, page_ids))


@main.command("adaptive")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option(
    "--strategy",
    "strategy_name",
    type=click.Choice(["statistical", "embedding"]),
    required=True,
)
@click.option("--port", type=int, default=9442)
@click.option(
    "--repair-query-envelope",
    "repair_queries",
    is_flag=True,
    help="Separate diagnostic arm: adapt native query expansion's array/object mismatch.",
)
def adaptive_command(
    run_dir: Path, strategy_name: str, port: int, repair_queries: bool
) -> None:
    asyncio.run(adaptive_sites(run_dir, strategy_name, port, repair_queries))


if __name__ == "__main__":
    main()
