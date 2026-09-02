import click
from openai_codex import TurnResult
from openai_codex.generated.v2_all import (
    TokenUsageBreakdown as CodexTokenUsageBreakdown,
)

from ex1.models import AnalysisTokenUsage, TokenUsageBreakdown


def analysis_token_usage(result: TurnResult) -> AnalysisTokenUsage | None:
    """Convert Codex SDK usage into the persisted application model."""
    if result.usage is None:
        return None

    return AnalysisTokenUsage(
        last=_token_breakdown(result.usage.last),
        thread_total=_token_breakdown(result.usage.total),
        model_context_window=result.usage.model_context_window,
        duration_ms=result.duration_ms,
    )


def print_usage(
    result: TurnResult,
    *,
    page_url: str,
) -> AnalysisTokenUsage | None:
    """Print and return all token usage reported for one page analysis."""
    usage = analysis_token_usage(result)
    if usage is None:
        click.echo(f"Analysis usage for {page_url}: not reported")
        return None

    click.echo(f"Analysis usage for {page_url}:")
    _print_breakdown("Last turn", usage.last)
    _print_breakdown("Thread total", usage.thread_total)
    if usage.model_context_window is not None:
        click.echo(f"  Context window:    {usage.model_context_window:,}")
    if usage.duration_ms is not None:
        click.echo(f"  Duration:          {usage.duration_ms:,} ms")
    return usage


def _token_breakdown(value: CodexTokenUsageBreakdown) -> TokenUsageBreakdown:
    return TokenUsageBreakdown(
        input_tokens=value.input_tokens,
        cached_input_tokens=value.cached_input_tokens,
        cache_write_input_tokens=value.cache_write_input_tokens or 0,
        output_tokens=value.output_tokens,
        reasoning_output_tokens=value.reasoning_output_tokens,
        total_tokens=value.total_tokens,
    )


def _print_breakdown(label: str, usage: TokenUsageBreakdown) -> None:
    click.echo(f"  {label}:")
    click.echo(f"    Input:           {usage.input_tokens:,}")
    click.echo(f"    Cached input:    {usage.cached_input_tokens:,}")
    click.echo(f"    Cache-write:     {usage.cache_write_input_tokens:,}")
    click.echo(f"    Output:          {usage.output_tokens:,}")
    click.echo(f"    Reasoning output:{usage.reasoning_output_tokens:>10,}")
    click.echo(f"    Total:           {usage.total_tokens:,}")
