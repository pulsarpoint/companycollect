"""Commands for collecting the corpus, extracting jobs, and comparing results."""

import asyncio
import os
import re
import tomllib
from pathlib import Path
from typing import Literal

import click
from dotenv import dotenv_values

from jobs_extraction_lab.compare import create_comparison
from jobs_extraction_lab.corpus import collect_pages
from jobs_extraction_lab.extract import run_extractions


@click.group()
def cli() -> None:
    """Compare Codex and Liquid LFM on identical downloaded job-board Markdown."""


@cli.command("collect")
@click.option(
    "--sources",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "sources.json",
)
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    default=Path(__file__).parent / "data",
)
@click.option("--target", type=click.IntRange(min=1), default=40)
@click.option("--max-jobs", type=click.IntRange(min=2), default=40)
@click.option("--concurrency", type=click.IntRange(1, 8), default=3)
@click.option("--cdp-port", type=click.IntRange(1024, 65535), default=9325)
@click.option("--retry-rejected", is_flag=True)
def collect_command(
    sources: Path,
    data_dir: Path,
    target: int,
    max_jobs: int,
    concurrency: int,
    cdp_port: int,
    retry_rejected: bool,
) -> None:
    pages = asyncio.run(
        collect_pages(
            sources,
            data_dir,
            target=target,
            max_jobs=max_jobs,
            concurrency=concurrency,
            cdp_port=cdp_port,
            retry_rejected=retry_rejected,
        )
    )
    click.echo(f"Saved {len(pages)}/{target} pages in {data_dir.resolve()}")
    if len(pages) < target:
        raise click.ClickException(
            "Candidate sources exhausted; add sources and run collect again"
        )


@cli.command("run")
@click.option("--backend", type=click.Choice(["codex", "openrouter"]), required=True)
@click.option("--run-id", default="initial")
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "data",
)
@click.option("--env-file", type=click.Path(path_type=Path, exists=True))
@click.option(
    "--codex-bin", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.option("--concurrency", type=click.IntRange(1, 8), default=2)
@click.option("--timeout", type=click.IntRange(min=1), default=300)
@click.option("--attempts", type=click.IntRange(1, 5), default=3)
@click.option("--max-tokens", type=click.IntRange(1, 8192), default=8192)
@click.option("--limit", type=click.IntRange(min=1))
@click.option("--retry-failed", is_flag=True)
def run_command(
    backend: Literal["codex", "openrouter"],
    run_id: str,
    data_dir: Path,
    env_file: Path | None,
    codex_bin: Path | None,
    concurrency: int,
    timeout: int,
    attempts: int,
    max_tokens: int,
    limit: int | None,
    retry_failed: bool,
) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id):
        raise click.ClickException(
            "run-id must contain only letters, numbers, dots, dashes, and underscores"
        )
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if env_file is not None:
        api_key = dotenv_values(env_file).get("OPENROUTER_API_KEY") or api_key
    codex_config_path = (
        Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "config.toml"
    )
    codex_model_label = "configured_default_unresolved"
    if backend == "codex" and codex_config_path.is_file():
        config = tomllib.loads(codex_config_path.read_text(encoding="utf-8"))
        codex_model_label = f"configured_default:{config.get('model', 'unspecified')};effort:{config.get('model_reasoning_effort', 'unspecified')}"
    try:
        results = asyncio.run(
            run_extractions(
                data_dir,
                backend=backend,
                run_id=run_id,
                api_key=api_key,
                concurrency=concurrency,
                timeout=timeout,
                attempts=attempts,
                max_tokens=max_tokens,
                limit=limit,
                retry_failed=retry_failed,
                codex_model_label=codex_model_label,
                codex_bin=codex_bin,
            )
        )
        report = create_comparison(data_dir, run_id)
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"{backend}: {sum(result.succeeded for result in results)}/{len(results)} successful; {report['totals']['paired_pages']} paired pages"
    )
    if any(not result.succeeded for result in results):
        raise click.ClickException(
            "Some extractions failed; saved results can be resumed with --retry-failed"
        )


@cli.command("compare")
@click.option("--run-id", default="initial")
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "data",
)
def compare_command(run_id: str, data_dir: Path) -> None:
    report = create_comparison(data_dir, run_id)
    click.echo(f"Compared {report['totals']['paired_pages']}/{report['pages']} pages")


if __name__ == "__main__":
    cli()
