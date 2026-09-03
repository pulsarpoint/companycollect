"""Execute the prompt × site × repeat matrix with a durable, resumable cache."""

import asyncio
import hashlib
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import Field, ValidationError

from ex1.models import StrictModel
from ex3.crawler import save_model
from ex3.llm_selection import select_pages_with_llm
from ex3.models import LlmCallStatus, ScoredUrl
from ex4.candidates import CandidateSet, candidates_hash, load_candidate_set
from ex4.paths import DataDir
from ex4.prompts import PromptTemplate, load_prompts, prompt_hash, render_prompt
from ex4.sites import load_sites, select_sites

LOGGER = logging.getLogger(__name__)
FIELDS_PREFIX = "fields: "


@dataclass(frozen=True, slots=True)
class RunSettings:
    run_id: str
    data: DataDir
    prompt_names: tuple[str, ...] | None = None
    domains: tuple[str, ...] | None = None
    repeats: int = 1
    limit: int = 20
    concurrency: int = 2
    timeout_seconds: int = 300
    dry_run: bool = False
    retry_failed: bool = False


class Pick(StrictModel):
    url: str
    reason: str
    expected_fields: list[str] = Field(default_factory=list)


class RunResult(StrictModel):
    domain: str
    prompt_name: str
    repeat: int
    prompt_hash: str
    candidates_hash: str
    cache_key: str
    limit: int
    picks: list[Pick]
    llm: LlmCallStatus
    latency_ms: int
    started_at: str


class RunManifest(StrictModel):
    run_id: str
    created_at: str
    prompt_names: list[str]
    domains: list[str]
    repeats: int
    limit: int


@dataclass(frozen=True, slots=True)
class CallPlan:
    domain: str
    prompt_name: str
    repeat: int
    path: Path
    cache_key: str
    cached: bool


def cache_key(prompt_text: str, candidates_hash_value: str, limit: int) -> str:
    return hashlib.sha256(
        f"{prompt_text}\n{candidates_hash_value}\n{limit}".encode()
    ).hexdigest()


def picks_from_scored(scored_urls: Sequence[ScoredUrl]) -> list[Pick]:
    picks: list[Pick] = []
    for scored in scored_urls:
        reason = scored.reasons[1] if len(scored.reasons) > 1 else ""
        fields_entry = next(
            (r for r in scored.reasons[2:] if r.startswith(FIELDS_PREFIX)), None
        )
        expected = (
            [
                f.strip()
                for f in fields_entry[len(FIELDS_PREFIX) :].split(",")
                if f.strip()
            ]
            if fields_entry
            else []
        )
        picks.append(Pick(url=scored.url, reason=reason, expected_fields=expected))
    return picks


def _load_inputs(
    settings: RunSettings,
) -> tuple[list[PromptTemplate], dict[str, CandidateSet]]:
    templates = load_prompts(
        settings.data.prompts,
        list(settings.prompt_names) if settings.prompt_names else None,
    )
    sites = select_sites(
        load_sites(settings.data.sites_file),
        list(settings.domains) if settings.domains else None,
    )
    candidate_sets = {
        site.domain: load_candidate_set(settings.data.candidate_file(site.domain))
        for site in sites
    }
    return templates, candidate_sets


def _is_cached(path: Path, key: str, *, retry_failed: bool) -> bool:
    if not path.is_file():
        return False
    try:
        result = RunResult.model_validate_json(path.read_text(encoding="utf-8"))
    except ValidationError:
        return False
    if result.cache_key != key:
        return False
    return result.llm.error is None or not retry_failed


def plan_calls(settings: RunSettings) -> list[CallPlan]:
    templates, candidate_sets = _load_inputs(settings)
    plans: list[CallPlan] = []
    for domain, candidate_set in candidate_sets.items():
        hash_value = candidates_hash(candidate_set)
        for template in templates:
            key = cache_key(template.text, hash_value, settings.limit)
            for repeat in range(1, settings.repeats + 1):
                path = settings.data.result_file(
                    settings.run_id, domain, template.name, repeat
                )
                plans.append(
                    CallPlan(
                        domain,
                        template.name,
                        repeat,
                        path,
                        key,
                        _is_cached(path, key, retry_failed=settings.retry_failed),
                    )
                )
    return plans


async def execute_run(settings: RunSettings) -> tuple[int, int, int]:
    """Run every non-cached call; return (executed, cached, failed)."""
    templates, candidate_sets = _load_inputs(settings)
    plans = plan_calls(settings)
    cached = sum(p.cached for p in plans)
    todo = [p for p in plans if not p.cached]
    LOGGER.info(
        "Run %s: %d call(s) planned, %d cached, %d to execute",
        settings.run_id,
        len(plans),
        cached,
        len(todo),
    )
    if settings.dry_run:
        return 0, 0, 0

    save_model(
        RunManifest(
            run_id=settings.run_id,
            created_at=datetime.now(UTC).isoformat(timespec="seconds"),
            prompt_names=[t.name for t in templates],
            domains=list(candidate_sets),
            repeats=settings.repeats,
            limit=settings.limit,
        ),
        settings.data.run_dir(settings.run_id) / "manifest.json",
    )
    by_name = {t.name: t for t in templates}
    semaphore = asyncio.Semaphore(settings.concurrency)

    async def one(plan: CallPlan) -> bool:
        template = by_name[plan.prompt_name]
        candidate_set = candidate_sets[plan.domain]
        rendered = render_prompt(
            template,
            base_url=candidate_set.base_url,
            limit=settings.limit,
            candidate_set=candidate_set,
        )
        started = datetime.now(UTC).isoformat(timespec="seconds")
        clock = time.monotonic()
        async with semaphore:
            try:
                scored, status = await select_pages_with_llm(
                    candidate_set.candidates,
                    base_url=candidate_set.base_url,
                    limit=settings.limit,
                    timeout_seconds=settings.timeout_seconds,
                    prompt=rendered,
                )
            except Exception as error:
                LOGGER.exception(
                    "%s/%s#%d failed", plan.domain, plan.prompt_name, plan.repeat
                )
                scored, status = (
                    [],
                    LlmCallStatus(attempted=True, succeeded=False, error=str(error)),
                )
        result = RunResult(
            domain=plan.domain,
            prompt_name=plan.prompt_name,
            repeat=plan.repeat,
            prompt_hash=prompt_hash(template.text),
            candidates_hash=candidates_hash(candidate_set),
            cache_key=plan.cache_key,
            limit=settings.limit,
            picks=picks_from_scored(scored),
            llm=status,
            latency_ms=int((time.monotonic() - clock) * 1000),
            started_at=started,
        )
        save_model(result, plan.path)
        LOGGER.info(
            "%s/%s#%d: %d pick(s)%s",
            plan.domain,
            plan.prompt_name,
            plan.repeat,
            len(result.picks),
            f" ERROR {status.error}" if status.error else "",
        )
        return status.error is not None

    outcomes = await asyncio.gather(*(one(plan) for plan in todo))
    return len(todo), cached, sum(outcomes)
