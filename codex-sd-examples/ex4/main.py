"""Command line for the page-selection prompt lab."""

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

import click

from ex3.crawler import save_model
from ex4.candidates import build_site_candidates, load_candidate_set
from ex4.gold import GOLD_FIELDS, draft_gold
from ex4.paths import DataDir
from ex4.runner import RunSettings, execute_run, plan_calls
from ex4.scoring import score_run
from ex4.sites import load_sites, save_sites, select_sites, verify_site

DEFAULT_DATA_DIR = Path("experiments/page-selection")


@click.group()
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=DEFAULT_DATA_DIR,
    show_default=True,
)
@click.option("--verbose", is_flag=True)
@click.pass_context
def cli(context: click.Context, data_dir: Path, verbose: bool) -> None:
    """Compare page-selection prompts against real sitemaps."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    context.obj = DataDir(data_dir.resolve())


@cli.group("sites")
def sites_group() -> None:
    """Manage the test-site list."""


@sites_group.command("verify")
@click.option(
    "--seed-max-urls", type=click.IntRange(min=1), default=5_000, show_default=True
)
@click.option("--accept-language", default="en-US,en;q=0.9", show_default=True)
@click.pass_obj
def sites_verify(data: DataDir, seed_max_urls: int, accept_language: str) -> None:
    """Check every site for a sitemap and record its size and base language."""
    site_list = load_sites(data.sites_file)

    async def verify_all():
        return [
            await verify_site(
                site, seed_max_urls=seed_max_urls, accept_language=accept_language
            )
            for site in site_list.sites
        ]

    verified = asyncio.run(verify_all())
    save_sites(site_list.model_copy(update={"sites": verified}), data.sites_file)
    for site in verified:
        status = (
            f"{site.inventory_urls} URLs, lang={site.base_language}"
            if site.sitemap_found
            else f"FAILED: {site.error}"
        )
        click.echo(f"{site.domain:24} {status}")


@cli.group("candidates")
def candidates_group() -> None:
    """Build the candidate lists the prompts will see."""


@candidates_group.command("build")
@click.option("--limit", type=click.IntRange(min=10), default=200, show_default=True)
@click.option(
    "--sites",
    "domains",
    default=None,
    help="Comma-separated domains (default: all verified sites).",
)
@click.option(
    "--seed-max-urls", type=click.IntRange(min=1), default=5_000, show_default=True
)
@click.option("--accept-language", default="en-US,en;q=0.9", show_default=True)
@click.option("--overwrite", is_flag=True)
@click.pass_obj
def candidates_build(
    data: DataDir,
    limit: int,
    domains: str | None,
    seed_max_urls: int,
    accept_language: str,
    overwrite: bool,
) -> None:
    """Write candidates/<domain>.json for each site (skips existing files unless --overwrite)."""
    sites = select_sites(
        load_sites(data.sites_file), domains.split(",") if domains else None
    )

    async def build_all() -> list[str]:
        written: list[str] = []
        for site in sites:
            target = data.candidate_file(site.domain)
            if target.exists() and not overwrite:
                click.echo(f"{site.domain:24} exists, skipped")
                continue
            candidate_set = await build_site_candidates(
                site,
                limit=limit,
                accept_language=accept_language,
                seed_max_urls=seed_max_urls,
            )
            save_model(candidate_set, target)
            written.append(site.domain)
            click.echo(
                f"{site.domain:24} {len(candidate_set.candidates)} candidates -> {target}"
            )
        return written

    asyncio.run(build_all())


@cli.group("gold")
def gold_group() -> None:
    """Gold sets of must-have pages per site."""


@gold_group.command("draft")
@click.option("--sites", "domains", default=None)
@click.option("--per-field", type=click.IntRange(min=1), default=5, show_default=True)
@click.option("--overwrite", is_flag=True)
@click.pass_obj
def gold_draft(
    data: DataDir, domains: str | None, per_field: int, overwrite: bool
) -> None:
    """Write gold/<domain>.json drafts from the candidate files for you to correct."""
    for site in select_sites(
        load_sites(data.sites_file), domains.split(",") if domains else None
    ):
        target = data.gold_file(site.domain)
        if target.exists() and not overwrite:
            click.echo(f"{site.domain:24} exists, skipped")
            continue
        gold = draft_gold(
            load_candidate_set(data.candidate_file(site.domain)), per_field=per_field
        )
        save_model(gold, target)
        counts = ", ".join(f"{f}={len(gold.must_have[f])}" for f in GOLD_FIELDS)
        click.echo(f"{site.domain:24} {counts}; junk={len(gold.junk)} -> {target}")


@cli.command("run")
@click.option(
    "--run-id", default=None, help="Defaults to YYYYMMDD-HHMMSS; reuse an id to resume."
)
@click.option(
    "--prompts",
    "prompt_names",
    default=None,
    help="Comma-separated prompt names (default: all).",
)
@click.option(
    "--sites",
    "domains",
    default=None,
    help="Comma-separated domains (default: all verified).",
)
@click.option("--repeats", type=click.IntRange(min=1), default=1, show_default=True)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
    help="Maximum picks per call.",
)
@click.option("--concurrency", type=click.IntRange(min=1), default=2, show_default=True)
@click.option(
    "--timeout",
    "timeout_seconds",
    type=click.IntRange(min=1),
    default=300,
    show_default=True,
)
@click.option("--retry-failed", is_flag=True)
@click.option(
    "--dry-run", is_flag=True, help="Only report how many calls would be made."
)
@click.pass_obj
def run_command(
    data: DataDir,
    run_id: str | None,
    prompt_names: str | None,
    domains: str | None,
    repeats: int,
    limit: int,
    concurrency: int,
    timeout_seconds: int,
    retry_failed: bool,
    dry_run: bool,
) -> None:
    """Call Codex for every prompt × site × repeat that is not cached yet."""
    settings = RunSettings(
        run_id=run_id or datetime.now(UTC).strftime("%Y%m%d-%H%M%S"),
        data=data,
        prompt_names=tuple(prompt_names.split(",")) if prompt_names else None,
        domains=tuple(domains.split(",")) if domains else None,
        repeats=repeats,
        limit=limit,
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        dry_run=dry_run,
        retry_failed=retry_failed,
    )
    try:
        plans = plan_calls(settings)
        executed, _, failed = asyncio.run(execute_run(settings))
    except Exception as error:
        raise click.ClickException(str(error)) from error
    cached = sum(p.cached for p in plans)
    if dry_run:
        to_execute = len(plans) - cached
        click.echo(
            f"Run {settings.run_id}: {len(plans)} planned, {cached} cached, "
            f"{to_execute} would be executed (dry run, nothing called)"
        )
    else:
        click.echo(
            f"Run {settings.run_id}: {len(plans)} planned, {cached} cached, "
            f"{executed} executed, {failed} failed"
        )
    click.echo(f"Results: {data.run_dir(settings.run_id)}")


@cli.command("score")
@click.option("--run-id", required=True)
@click.pass_obj
def score_command(data: DataDir, run_id: str) -> None:
    """Score a run against the gold files; never calls Codex."""
    scores = score_run(run_id, data)
    save_model(scores, data.scores_file(run_id))
    for summary in scores.prompts:
        click.echo(
            f"{summary.prompt_name:26} coverage={summary.mean_coverage:.2f} (min {summary.min_coverage:.2f}) junk={summary.mean_junk_rate:.2f} tokens={summary.mean_total_tokens:,.0f} failures={summary.failures}"
        )
    click.echo(f"Scores: {data.scores_file(run_id)}")


if __name__ == "__main__":
    cli()
