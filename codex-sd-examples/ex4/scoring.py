"""Score run results against gold sets and rank prompts."""

import logging
from collections import defaultdict
from datetime import UTC, datetime
from statistics import mean

from pydantic import Field

from ex1.models import StrictModel
from ex3.selection import is_preferred_language
from ex3.urls import url_key
from ex4.candidates import CandidateSet, load_candidate_set
from ex4.gold import GoldSet, field_coverage, junk_hits, load_gold
from ex4.paths import DataDir
from ex4.runner import RunResult

LOGGER = logging.getLogger(__name__)
WARNING_KINDS = ("unknown", "duplicate", "limit")


class ResultScore(StrictModel):
    domain: str
    prompt_name: str
    repeat: int
    coverage: float
    covered: list[str]
    missed: list[str]
    applicable: list[str]
    junk_rate: float
    other_language_rate: float
    picks: int
    warnings: dict[str, int]
    input_tokens: int
    output_tokens: int
    total_tokens: int
    latency_ms: int
    error: str | None = None


class PromptSummary(StrictModel):
    prompt_name: str
    sites: int
    mean_coverage: float
    min_coverage: float
    mean_junk_rate: float
    mean_other_language_rate: float
    mean_total_tokens: float
    mean_latency_ms: float
    failures: int
    stability: float | None = None
    missed_by_site: dict[str, list[str]] = Field(default_factory=dict)


class RunScores(StrictModel):
    run_id: str
    scored_at: str
    results: list[ResultScore]
    prompts: list[PromptSummary]


def score_result(
    result: RunResult, gold: GoldSet, candidate_set: CandidateSet
) -> ResultScore:
    pick_urls = [pick.url for pick in result.picks]
    covered, applicable = field_coverage(gold, pick_urls)
    languages = {url_key(c.url): c.language for c in candidate_set.candidates}
    other = [
        u
        for u in pick_urls
        if languages.get(url_key(u))
        and not is_preferred_language(
            languages[url_key(u)] or "",
            preferred_languages=candidate_set.preferred_languages,
        )
    ]
    usage = result.llm.token_usage.last if result.llm.token_usage else None
    failed = result.llm.error is not None
    return ResultScore(
        domain=result.domain,
        prompt_name=result.prompt_name,
        repeat=result.repeat,
        coverage=0.0 if failed or not applicable else len(covered) / len(applicable),
        covered=covered,
        missed=[f for f in applicable if f not in covered],
        applicable=applicable,
        junk_rate=len(junk_hits(gold, pick_urls)) / len(pick_urls)
        if pick_urls
        else 0.0,
        other_language_rate=len(other) / len(pick_urls) if pick_urls else 0.0,
        picks=len(pick_urls),
        warnings={
            kind: sum(kind in w for w in result.llm.warnings) for kind in WARNING_KINDS
        },
        input_tokens=usage.input_tokens if usage else 0,
        output_tokens=usage.output_tokens if usage else 0,
        total_tokens=usage.total_tokens if usage else 0,
        latency_ms=result.latency_ms,
        error=result.llm.error,
    )


def summarize(
    results: list[ResultScore], picks_by_result: dict[tuple[str, str, int], set[str]]
) -> list[PromptSummary]:
    """Aggregate per prompt across sites (repeat 1 for means), ranked."""
    by_prompt: dict[str, list[ResultScore]] = defaultdict(list)
    for score in results:
        by_prompt[score.prompt_name].append(score)
    summaries: list[PromptSummary] = []
    for name, scores in by_prompt.items():
        firsts = [s for s in scores if s.repeat == 1] or scores
        summaries.append(
            PromptSummary(
                prompt_name=name,
                sites=len({s.domain for s in firsts}),
                mean_coverage=mean(s.coverage for s in firsts),
                min_coverage=min(s.coverage for s in firsts),
                mean_junk_rate=mean(s.junk_rate for s in firsts),
                mean_other_language_rate=mean(s.other_language_rate for s in firsts),
                mean_total_tokens=mean(s.total_tokens for s in firsts),
                mean_latency_ms=mean(s.latency_ms for s in firsts),
                failures=sum(s.error is not None for s in scores),
                stability=_stability(name, scores, picks_by_result),
                missed_by_site={s.domain: s.missed for s in firsts if s.missed},
            )
        )
    return sorted(
        summaries,
        key=lambda s: (-s.mean_coverage, s.mean_junk_rate, s.mean_total_tokens),
    )


def _stability(
    name: str,
    scores: list[ResultScore],
    picks_by_result: dict[tuple[str, str, int], set[str]],
) -> float | None:
    overlaps: list[float] = []
    for domain in {s.domain for s in scores}:
        first, second = (
            picks_by_result.get((domain, name, 1)),
            picks_by_result.get((domain, name, 2)),
        )
        if first is None or second is None:
            continue
        union = {url_key(u) for u in first} | {url_key(u) for u in second}
        inter = {url_key(u) for u in first} & {url_key(u) for u in second}
        overlaps.append(len(inter) / len(union) if union else 1.0)
    return mean(overlaps) if overlaps else None


def score_run(run_id: str, data: DataDir) -> RunScores:
    results: list[ResultScore] = []
    picks_by_result: dict[tuple[str, str, int], set[str]] = {}
    gold_cache: dict[str, GoldSet | None] = {}
    candidate_cache: dict[str, CandidateSet] = {}
    for path in sorted(data.run_dir(run_id).glob("*/*/*.json")):
        result = RunResult.model_validate_json(path.read_text(encoding="utf-8"))
        if result.domain not in gold_cache:
            gold_path = data.gold_file(result.domain)
            gold_cache[result.domain] = (
                load_gold(gold_path) if gold_path.is_file() else None
            )
            if gold_cache[result.domain] is None:
                LOGGER.warning(
                    "No gold file for %s; skipping its results", result.domain
                )
        gold = gold_cache[result.domain]
        if gold is None:
            continue
        candidate_set = candidate_cache.setdefault(
            result.domain, load_candidate_set(data.candidate_file(result.domain))
        )
        results.append(score_result(result, gold, candidate_set))
        picks_by_result[(result.domain, result.prompt_name, result.repeat)] = {
            p.url for p in result.picks
        }
    return RunScores(
        run_id=run_id,
        scored_at=datetime.now(UTC).isoformat(timespec="seconds"),
        results=results,
        prompts=summarize(results, picks_by_result),
    )
