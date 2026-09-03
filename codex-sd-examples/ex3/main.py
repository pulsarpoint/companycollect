import asyncio
import logging
from pathlib import Path
from typing import cast

import click

from ex3.crawler import (
    AnalysisSettings,
    CrawlSettings,
    load_manifest,
    run_analysis,
    run_crawl,
    save_model,
    save_report,
)
from ex3.extend import ExtendSettings, run_extend
from ex3.followup import SuggestSettings, next_pass_number, run_suggest
from ex3.language import is_english_language
from ex3.models import DiscoveryStrategy, SeedingSource, Selector


@click.group()
def cli() -> None:
    """Crawl company sites once, then analyze stored Markdown independently."""


@cli.command("crawl")
@click.argument("start_url")
@click.option(
    "--markdown-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("company-markdown"),
    show_default=True,
    help="Directory for page Markdown and the crawl manifest.",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
    help="Target and hard limit for successfully stored Markdown pages.",
)
@click.option(
    "--max-depth",
    type=click.IntRange(min=0),
    default=2,
    show_default=True,
    help="Link levels beyond the selected English base page.",
)
@click.option(
    "--max-markdown-chars",
    type=click.IntRange(min=1_000),
    default=80_000,
    show_default=True,
    help="Maximum characters persisted from each page.",
)
@click.option(
    "--crawl-concurrency",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Maximum concurrent Crawl4AI page crawls.",
)
@click.option(
    "--cdp-port",
    type=click.IntRange(min=1, max=65_535),
    default=9_245,
    show_default=True,
)
@click.option("--headless/--headed", default=True, show_default=True)
@click.option(
    "--include-external",
    is_flag=True,
    help="Allow BFS to follow external links after selecting the base URL.",
)
@click.option(
    "--respect-robots/--ignore-robots",
    "check_robots_txt",
    default=True,
    show_default=True,
)
@click.option(
    "--proxy",
    envvar="COMPANY_CRAWLER_PROXY",
    help="Optional HTTP or SOCKS proxy used by CloakBrowser.",
)
@click.option(
    "--seed/--no-seed",
    default=True,
    show_default=True,
    help=(
        "Build a URL inventory (sitemap by default) and pick the highest-scoring "
        "pages before crawling; link discovery only fills the remaining budget."
    ),
)
@click.option(
    "--seed-source",
    type=click.Choice(["sitemap", "sitemap+cc", "cc"]),
    default="sitemap",
    show_default=True,
    help="URL inventory source. Common Crawl (cc) adds slow external index queries.",
)
@click.option(
    "--seed-max-urls",
    type=click.IntRange(min=1),
    default=5_000,
    show_default=True,
    help="Maximum inventory URLs read from the seeding source.",
)
@click.option(
    "--seed-share",
    type=click.FloatRange(min=0.0, max=1.0),
    default=0.6,
    show_default=True,
    help=(
        "Share of the page budget rendered from the inventory before links "
        "harvested from those pages are ranked for the second wave."
    ),
)
@click.option(
    "--discovery",
    "discovery_strategy",
    type=click.Choice(["best_first", "breadth_first"]),
    default="breadth_first",
    show_default=True,
    help="Link-following strategy when no sitemap exists or budget remains.",
)
@click.option(
    "--accept-language",
    default="en-US,en;q=0.9",
    show_default=True,
    help="Accept-Language sent by the browser and the inventory fetches.",
)
@click.option(
    "--selector",
    type=click.Choice(["llm", "deterministic"]),
    default="llm",
    show_default=True,
    help="Who picks pages from the sitemap inventory: one Codex call or the scorer.",
)
@click.option(
    "--llm-candidates",
    type=click.IntRange(min=10),
    default=200,
    show_default=True,
    help="Maximum head-checked candidate URLs shown to the LLM selector.",
)
@click.option(
    "--llm-timeout",
    "llm_timeout_seconds",
    type=click.IntRange(min=1),
    default=300,
    show_default=True,
    help="Maximum seconds for the LLM selection call.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Replace an existing crawl manifest and matching Markdown files.",
)
@click.option("--verbose", is_flag=True, help="Show detailed crawl logging.")
def crawl_command(
    start_url: str,
    markdown_dir: Path,
    max_pages: int,
    max_depth: int,
    max_markdown_chars: int,
    crawl_concurrency: int,
    cdp_port: int,
    headless: bool,
    include_external: bool,
    check_robots_txt: bool,
    proxy: str | None,
    seed: bool,
    seed_source: str,
    seed_max_urls: int,
    seed_share: float,
    discovery_strategy: str,
    accept_language: str,
    selector: str,
    llm_candidates: int,
    llm_timeout_seconds: int,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Discover English, select pages, and crawl START_URL into a manifest."""
    _configure_logging(verbose=verbose)
    manifest_path = markdown_dir.resolve() / "crawl-manifest.json"
    if manifest_path.exists() and not overwrite:
        raise click.ClickException(
            f"Refusing to overwrite {manifest_path}. Use --overwrite."
        )

    settings = CrawlSettings(
        start_url=start_url,
        markdown_dir=markdown_dir,
        max_pages=max_pages,
        max_depth=max_depth,
        max_markdown_chars=max_markdown_chars,
        crawl_concurrency=crawl_concurrency,
        cdp_port=cdp_port,
        headless=headless,
        include_external=include_external,
        check_robots_txt=check_robots_txt,
        proxy=proxy,
        seed=seed,
        seed_source=cast(SeedingSource, seed_source),
        seed_max_urls=seed_max_urls,
        seed_share=seed_share,
        discovery_strategy=cast(DiscoveryStrategy, discovery_strategy),
        accept_language=accept_language,
        selector=cast(Selector, selector),
        llm_candidates=llm_candidates,
        llm_timeout_seconds=llm_timeout_seconds,
    )
    try:
        manifest = asyncio.run(run_crawl(settings))
    except KeyboardInterrupt as error:
        click.echo("Crawl interrupted.", err=True)
        raise SystemExit(130) from error
    except Exception as error:
        raise click.ClickException(str(error)) from error

    click.echo(f"English base URL: {manifest.selected_base_url}")
    seeding = manifest.url_seeding
    if seeding is not None and seeding.enabled:
        status = "ok" if seeding.succeeded else f"failed ({seeding.error})"
        click.echo(
            f"Seeding ({seeding.source}, {status}, {seeding.selection_method}): "
            f"{seeding.inventory_urls} inventory URLs, "
            f"{seeding.eligible_urls} eligible, "
            f"{seeding.excluded_urls} excluded, "
            f"{seeding.head_checked_urls} head-checked, "
            f"{seeding.base_page_links} base-page links, "
            f"{seeding.harvested_links} harvested links, "
            f"{len(seeding.selected)} selected in {seeding.selection_waves} wave(s)"
        )
        if seeding.llm is not None and not seeding.llm.succeeded:
            click.echo(f"LLM page selection failed: {seeding.llm.error}", err=True)
    click.echo(
        "Crawl: "
        f"{manifest.crawl_stats.pages_returned} returned, "
        f"{manifest.crawl_stats.stored_markdown_pages} Markdown files stored "
        f"({manifest.crawl_stats.selected_pages} pre-selected, "
        f"{manifest.crawl_stats.discovered_pages} via {manifest.discovery_strategy}), "
        f"max depth {manifest.crawl_stats.max_depth_reached}"
    )
    non_english = [
        page
        for page in manifest.markdown_pages
        if page.language is not None and not is_english_language(page.language)
    ]
    if non_english:
        click.echo(f"Pages declaring a non-English language: {len(non_english)}")
    click.echo(f"Manifest: {manifest_path}")
    click.echo(f'Next: uv run python -m ex3.main analyze "{manifest_path}"')


@cli.command("analyze")
@click.argument(
    "manifest_path",
    type=click.Path(
        path_type=Path,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("company-batched-report.json"),
    show_default=True,
    help="Final consolidated JSON report.",
)
@click.option(
    "--max-page-chars",
    type=click.IntRange(min=1_000),
    default=30_000,
    show_default=True,
    help="Maximum characters read from each stored page for this analysis.",
)
@click.option(
    "--max-batch-pages",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Maximum stored pages sent in one LLM call.",
)
@click.option(
    "--max-batch-chars",
    type=click.IntRange(min=1_000),
    default=60_000,
    show_default=True,
    help="Maximum Markdown characters sent in one LLM call.",
)
@click.option(
    "--analysis-timeout",
    "analysis_timeout_seconds",
    type=click.IntRange(min=1),
    default=300,
    show_default=True,
    help="Maximum seconds allowed for each multi-page extraction.",
)
@click.option(
    "--skip-non-english/--keep-non-english",
    "skip_non_english",
    default=False,
    show_default=True,
    help=(
        "Whether to exclude stored pages whose HTML declares a non-English "
        "language. Kept by default: the LLM extracts and translates them."
    ),
)
@click.option(
    "--previous-report",
    "previous_report_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    default=None,
    help=(
        "Report of the previous pass; only new manifest pages are extracted "
        "and the rounds are merged."
    ),
)
@click.option(
    "--merge/--no-merge",
    "merge_with_llm",
    default=True,
    show_default=True,
    help="Merge rounds with one LLM call (deterministic consolidation otherwise).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Replace an existing analysis report.",
)
@click.option("--verbose", is_flag=True, help="Show detailed analysis logging.")
def analyze_command(
    manifest_path: Path,
    output_path: Path,
    max_page_chars: int,
    max_batch_pages: int,
    max_batch_chars: int,
    analysis_timeout_seconds: int,
    skip_non_english: bool,
    previous_report_path: Path | None,
    merge_with_llm: bool,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Analyze existing Markdown from MANIFEST_PATH without crawling."""
    _configure_logging(verbose=verbose)
    if output_path.exists() and not overwrite:
        raise click.ClickException(
            f"Refusing to overwrite {output_path}. Use --overwrite."
        )

    settings = AnalysisSettings(
        manifest_path=manifest_path,
        max_page_chars=max_page_chars,
        max_batch_pages=max_batch_pages,
        max_batch_chars=max_batch_chars,
        analysis_timeout_seconds=analysis_timeout_seconds,
        skip_non_english=skip_non_english,
        previous_report_path=previous_report_path,
        merge_with_llm=merge_with_llm,
    )
    try:
        report = asyncio.run(run_analysis(settings))
        save_report(report, output_path)
    except KeyboardInterrupt as error:
        click.echo(
            "Analysis interrupted. Stored Markdown remains available for another run.",
            err=True,
        )
        raise SystemExit(130) from error
    except Exception as error:
        raise click.ClickException(str(error)) from error

    click.echo(f"Manifest: {report.manifest_path}")
    click.echo(f"Passes: {len(report.passes)}")
    click.echo(f"Markdown pages submitted: {report.batch_stats.submitted_pages}")
    if report.skipped_pages:
        click.echo(
            f"Non-English pages skipped: {report.batch_stats.skipped_non_english_pages}"
        )
    click.echo(f"Discovered URLs: {len(report.discovered_urls)}")
    if (
        report.related_domain_analysis.attempted
        and not report.related_domain_analysis.succeeded
    ):
        click.echo(
            "Related-domain analysis failed: "
            f"{report.related_domain_analysis.error or 'unknown error'}",
            err=True,
        )
    click.echo(f"Related domains selected by LLM: {len(report.related_domains)}")
    for related_domain in report.related_domains:
        click.echo(f"  {related_domain.url}")
    click.echo(
        "Batches: "
        f"{report.batch_stats.total_batches} total, "
        f"{report.batch_stats.successful_batches} succeeded, "
        f"{report.batch_stats.failed_batches} failed"
    )
    click.echo(
        "Token totals: "
        f"input={report.analysis_stats.token_totals.input_tokens:,}, "
        f"cached={report.analysis_stats.token_totals.cached_input_tokens:,}, "
        f"cache-write={report.analysis_stats.token_totals.cache_write_input_tokens:,}, "
        f"output={report.analysis_stats.token_totals.output_tokens:,}, "
        "reasoning="
        f"{report.analysis_stats.token_totals.reasoning_output_tokens:,}, "
        f"total={report.analysis_stats.token_totals.total_tokens:,}"
    )
    if report.merge_analysis is not None:
        if report.merge_analysis.succeeded:
            click.echo(
                f"Merge: ok, dropped {report.merge_analysis.dropped_items} item(s)"
            )
        else:
            click.echo(
                f"Merge failed: {report.merge_analysis.error or 'unknown error'}",
                err=True,
            )
    click.echo(f"Gaps: {', '.join(gap.field for gap in report.gaps) or 'none'}")
    click.echo(f"Report: {output_path}")


@cli.command("suggest")
@click.argument(
    "manifest_path", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.argument(
    "report_path", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Suggestions JSON. Defaults to suggestions-pass-N.json next to the manifest.",
)
@click.option(
    "--max-suggestions",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Maximum pages the LLM may propose for the next pass.",
)
@click.option(
    "--candidate-limit",
    type=click.IntRange(min=10),
    default=200,
    show_default=True,
    help="Maximum unprocessed candidate URLs shown to the LLM.",
)
@click.option(
    "--analysis-timeout",
    "timeout_seconds",
    type=click.IntRange(min=1),
    default=300,
    show_default=True,
)
@click.option("--overwrite", is_flag=True)
@click.option("--verbose", is_flag=True)
def suggest_command(
    manifest_path: Path,
    report_path: Path,
    output_path: Path | None,
    max_suggestions: int,
    candidate_limit: int,
    timeout_seconds: int,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Find gaps in REPORT_PATH and let the LLM propose unprocessed pages to crawl next."""
    _configure_logging(verbose=verbose)
    try:
        manifest = load_manifest(manifest_path)
    except Exception as error:
        raise click.ClickException(str(error)) from error
    target = output_path or manifest_path.resolve().parent / (
        f"suggestions-pass-{next_pass_number(manifest)}.json"
    )
    if target.exists() and not overwrite:
        raise click.ClickException(f"Refusing to overwrite {target}. Use --overwrite.")
    try:
        suggestions = asyncio.run(
            run_suggest(
                SuggestSettings(
                    manifest_path=manifest_path,
                    report_path=report_path,
                    max_suggestions=max_suggestions,
                    candidate_limit=candidate_limit,
                    timeout_seconds=timeout_seconds,
                )
            )
        )
    except Exception as error:
        raise click.ClickException(str(error)) from error
    save_model(suggestions, target)
    click.echo(f"Gaps: {', '.join(gap.field for gap in suggestions.gaps) or 'none'}")
    click.echo(f"Candidates shown to LLM: {suggestions.candidate_count}")
    click.echo(f"Suggested pages: {len(suggestions.suggestions)}")
    for item in suggestions.suggestions:
        click.echo(
            f"  {item.url}  ({', '.join(item.expected_fields) or 'no fields'}) {item.reason}"
        )
    click.echo(f"Suggestions: {target}")


@cli.command("extend")
@click.argument(
    "manifest_path", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.option(
    "--suggestions",
    "suggestions_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Suggestions JSON written by the suggest command.",
)
@click.option(
    "--max-markdown-chars",
    type=click.IntRange(min=1_000),
    default=80_000,
    show_default=True,
)
@click.option(
    "--crawl-concurrency", type=click.IntRange(min=1), default=5, show_default=True
)
@click.option(
    "--cdp-port",
    type=click.IntRange(min=1, max=65_535),
    default=9_245,
    show_default=True,
)
@click.option("--headless/--headed", default=True, show_default=True)
@click.option(
    "--respect-robots/--ignore-robots",
    "check_robots_txt",
    default=True,
    show_default=True,
)
@click.option("--proxy", envvar="COMPANY_CRAWLER_PROXY")
@click.option("--accept-language", default="en-US,en;q=0.9", show_default=True)
@click.option("--verbose", is_flag=True)
def extend_command(
    manifest_path: Path,
    suggestions_path: Path,
    max_markdown_chars: int,
    crawl_concurrency: int,
    cdp_port: int,
    headless: bool,
    check_robots_txt: bool,
    proxy: str | None,
    accept_language: str,
    verbose: bool,
) -> None:
    """Crawl suggested pages and append them to MANIFEST_PATH."""
    _configure_logging(verbose=verbose)
    settings = ExtendSettings(
        manifest_path=manifest_path,
        suggestions_path=suggestions_path,
        max_markdown_chars=max_markdown_chars,
        crawl_concurrency=crawl_concurrency,
        cdp_port=cdp_port,
        headless=headless,
        check_robots_txt=check_robots_txt,
        proxy=proxy,
        accept_language=accept_language,
    )
    try:
        manifest = asyncio.run(run_extend(settings))
    except KeyboardInterrupt as error:
        raise SystemExit(130) from error
    except Exception as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"Manifest now holds {len(manifest.markdown_pages)} page(s), "
        f"{manifest.crawl_stats.suggested_pages} from suggestions"
    )


def _configure_logging(*, verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )


if __name__ == "__main__":
    cli()
