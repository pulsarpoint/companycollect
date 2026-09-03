"""Command line for the page-selection prompt lab."""

import asyncio
import logging
from pathlib import Path

import click

from ex4.paths import DataDir
from ex4.sites import load_sites, save_sites, verify_site

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


if __name__ == "__main__":
    cli()
