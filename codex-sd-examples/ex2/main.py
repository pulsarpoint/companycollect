import asyncio
import logging
from pathlib import Path

import click

from ex2.crawler import CrawlSettings, run_crawl, save_report


@click.command()
@click.argument("start_url")
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("company-bfs-report.json"),
    show_default=True,
    help="Final consolidated JSON report.",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
    help="Hard limit enforced by Crawl4AI BFS.",
)
@click.option(
    "--max-depth",
    type=click.IntRange(min=0),
    default=2,
    show_default=True,
    help="Link levels beyond the start page.",
)
@click.option(
    "--max-markdown-chars",
    type=click.IntRange(min=1_000),
    default=120_000,
    show_default=True,
    help="Maximum page Markdown characters sent to the LLM.",
)
@click.option(
    "--crawl-concurrency",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Maximum concurrent Crawl4AI page crawls.",
)
@click.option(
    "--analysis-timeout",
    "analysis_timeout_seconds",
    type=click.IntRange(min=1),
    default=180,
    show_default=True,
    help="Maximum seconds allowed for each LLM extraction.",
)
@click.option(
    "--cdp-port",
    type=click.IntRange(min=1, max=65_535),
    default=9_244,
    show_default=True,
)
@click.option("--headless/--headed", default=True, show_default=True)
@click.option(
    "--include-external",
    is_flag=True,
    help="Allow Crawl4AI BFS to follow links to other domains.",
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
    "--overwrite",
    is_flag=True,
    help="Explicitly replace an existing output file.",
)
@click.option("--verbose", is_flag=True, help="Show detailed crawl logging.")
def cli(
    start_url: str,
    output_path: Path,
    max_pages: int,
    max_depth: int,
    max_markdown_chars: int,
    crawl_concurrency: int,
    analysis_timeout_seconds: int,
    cdp_port: int,
    headless: bool,
    include_external: bool,
    check_robots_txt: bool,
    proxy: str | None,
    overwrite: bool,
    verbose: bool,
) -> None:
    """BFS-crawl START_URL, then extract company data from every page."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if output_path.exists() and not overwrite:
        raise click.ClickException(
            f"Refusing to overwrite {output_path}. Use --overwrite."
        )

    settings = CrawlSettings(
        start_url=start_url,
        output_path=output_path,
        max_pages=max_pages,
        max_depth=max_depth,
        max_markdown_chars=max_markdown_chars,
        analysis_timeout_seconds=analysis_timeout_seconds,
        crawl_concurrency=crawl_concurrency,
        cdp_port=cdp_port,
        headless=headless,
        include_external=include_external,
        check_robots_txt=check_robots_txt,
        proxy=proxy,
    )

    try:
        report = asyncio.run(run_crawl(settings))
        save_report(report, settings.output_path)
    except KeyboardInterrupt as error:
        raise SystemExit(130) from error
    except Exception as error:
        raise click.ClickException(str(error)) from error

    click.echo(
        "Crawl: "
        f"{report.crawl_stats.successful_pages} succeeded, "
        f"{report.crawl_stats.failed_pages} failed, "
        f"max depth {report.crawl_stats.max_depth_reached}"
    )
    click.echo(f"Extracted {len(report.useful_information.contacts)} contact(s).")
    click.echo(f"Extracted {len(report.useful_information.products)} product(s).")
    click.echo(f"Extracted {len(report.useful_information.jobs)} job(s).")
    click.echo(
        "Analysis: "
        f"{report.analysis_stats.successful_analyses} succeeded, "
        f"{report.analysis_stats.failed_analyses} failed"
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
    click.echo(f"Stopped because: {report.stopped_reason}")
    click.echo(f"Report: {settings.output_path}")


if __name__ == "__main__":
    cli()
