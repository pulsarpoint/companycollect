import asyncio
import logging
from pathlib import Path

import click

from ex1.crawler import (
    CrawlSettings,
    create_initial_state,
    load_state,
    run_crawl,
    save_report,
)
from ex1.models import CrawlState


@click.command()
@click.argument("start_url")
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("company-report.json"),
    show_default=True,
    help="Final consolidated JSON report.",
)
@click.option(
    "--state",
    "state_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("company-crawl-state.json"),
    show_default=True,
    help="Checkpoint written after every page.",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
)
@click.option(
    "--max-depth",
    type=click.IntRange(min=0),
    default=2,
    show_default=True,
)
@click.option(
    "--max-prompt-links",
    type=click.IntRange(min=1),
    default=100,
    show_default=True,
    help="Maximum candidate links shown to the LLM per page.",
)
@click.option(
    "--max-markdown-chars",
    type=click.IntRange(min=1_000),
    default=120_000,
    show_default=True,
    help="Maximum page Markdown characters sent to the LLM.",
)
@click.option(
    "--cdp-port",
    type=click.IntRange(min=1, max=65_535),
    default=9_243,
    show_default=True,
)
@click.option("--headless/--headed", default=True, show_default=True)
@click.option(
    "--allow-external",
    is_flag=True,
    help="Allow only LLM-recommended external links into the frontier.",
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
    "--resume",
    is_flag=True,
    help="Resume from --state. The start URL must match.",
)
@click.option(
    "--restart",
    is_flag=True,
    help="Explicitly replace existing state and output files.",
)
@click.option("--verbose", is_flag=True, help="Show detailed crawl logging.")
def cli(
    start_url: str,
    output_path: Path,
    state_path: Path,
    max_pages: int,
    max_depth: int,
    max_prompt_links: int,
    max_markdown_chars: int,
    cdp_port: int,
    headless: bool,
    allow_external: bool,
    check_robots_txt: bool,
    proxy: str | None,
    resume: bool,
    restart: bool,
    verbose: bool,
) -> None:
    """Extract company information from START_URL into a JSON report."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    settings = CrawlSettings(
        start_url=start_url,
        output_path=output_path,
        state_path=state_path,
        max_pages=max_pages,
        max_depth=max_depth,
        max_prompt_links=max_prompt_links,
        max_markdown_chars=max_markdown_chars,
        cdp_port=cdp_port,
        headless=headless,
        allow_external=allow_external,
        check_robots_txt=check_robots_txt,
        proxy=proxy,
    )

    try:
        state = _prepare_state(settings, resume=resume, restart=restart)
        report = asyncio.run(run_crawl(settings, state))
        save_report(report, settings.output_path)
    except KeyboardInterrupt as error:
        click.echo(
            f"Crawl interrupted. Resume it with --resume and state {state_path}",
            err=True,
        )
        raise SystemExit(130) from error
    except Exception as error:
        click.echo(f"Error: {error}", err=True)
        raise SystemExit(1) from error

    click.echo(f"Visited {len(report.visited_urls)} page(s).")
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
    click.echo(f"Checkpoint: {settings.state_path}")


def _prepare_state(
    settings: CrawlSettings,
    *,
    resume: bool,
    restart: bool,
) -> CrawlState:
    if resume and restart:
        raise ValueError("Use either --resume or --restart, not both")

    if resume:
        if not settings.state_path.exists():
            raise ValueError(f"State file does not exist: {settings.state_path}")
        return load_state(
            settings.state_path,
            expected_start_url=settings.start_url,
        )

    existing_paths = [
        path for path in (settings.state_path, settings.output_path) if path.exists()
    ]
    if existing_paths and not restart:
        formatted_paths = ", ".join(str(path) for path in existing_paths)
        raise ValueError(
            f"Refusing to overwrite existing file(s): {formatted_paths}. "
            "Use --resume or --restart."
        )

    return create_initial_state(settings.start_url)


if __name__ == "__main__":
    cli()
