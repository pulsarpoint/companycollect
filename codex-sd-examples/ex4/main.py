"""Command line for the page-selection prompt lab."""

import asyncio
import logging
from pathlib import Path

import click

from ex3.crawler import save_model
from ex4.candidates import build_site_candidates
from ex4.paths import DataDir
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


if __name__ == "__main__":
    cli()
