"""URL-only CLI with JSON on stdout and progress on stderr."""

import asyncio
import logging
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

import click
import httpx
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from dotenv import dotenv_values

from company_research.models import ResearchConfig, ResearchResult
from company_research.research import research_company
from company_research.technology_catalog import TechnologyCatalog, sync_catalog


@click.command()
@click.argument("url")
@click.option("--output-dir", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--env-file", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.option("--max-pages", type=click.IntRange(1, 500), default=None)
@click.option("--max-model-calls", type=click.IntRange(1), default=None)
@click.option(
    "--technology-catalog",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Local catalog cache. Refreshed from ClickHouse when credentials are configured.",
)
@click.option(
    "--offline-catalog",
    is_flag=True,
    help="Use the specified snapshot without refreshing it.",
)
def main(
    url: str,
    output_dir: Path | None,
    env_file: Path | None,
    max_pages: int | None,
    max_model_calls: int | None,
    technology_catalog: Path | None,
    offline_catalog: bool,
) -> None:
    """Research a company URL with built-in prompts; emit attributed JSON findings."""
    values = {"max_pages": max_pages, "max_model_calls": max_model_calls}
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    environment = dict(os.environ)
    if env_file is not None:
        environment.update(
            {
                key: value
                for key, value in dotenv_values(env_file).items()
                if value is not None
            }
        )
    key = environment.get("OPENROUTER_API_KEY")
    try:
        catalog = None
        if offline_catalog:
            if technology_catalog is None:
                raise ValueError("--offline-catalog requires --technology-catalog")
            catalog = TechnologyCatalog.read(technology_catalog)
        elif environment.get("CLICKHOUSE_HOST"):
            catalog_path = technology_catalog or Path(
                ".cache/company-research/technology-catalog.json"
            )
            client = Client(
                host=environment["CLICKHOUSE_HOST"],
                port=int(environment.get("CLICKHOUSE_NATIVE_PORT", "9000")),
                user=environment.get("CLICKHOUSE_USER", "default"),
                password=environment.get("CLICKHOUSE_PASSWORD", ""),
                secure=environment.get("CLICKHOUSE_SECURE", "false").lower()
                in {"1", "true", "yes"},
                connect_timeout=10,
                send_receive_timeout=30,
                settings={"readonly": 1, "max_execution_time": 20},
            )
            try:
                catalog = sync_catalog(client, catalog_path)
            except (ClickHouseError, OSError, EOFError) as error:
                raise ValueError(
                    "ClickHouse catalog sync failed; no crawl was started"
                ) from error
            finally:
                client.disconnect()
        elif technology_catalog is not None:
            raise ValueError(
                "Configure CLICKHOUSE_HOST to refresh the catalog, or use --offline-catalog explicitly"
            )
        else:
            raise ValueError(
                "Configure CLICKHOUSE_HOST for catalog sync, or provide --technology-catalog with --offline-catalog"
            )
        config = ResearchConfig.model_validate(
            {k: v for k, v in values.items() if v is not None}
        )
        # Crawl4AI/browser libraries also print progress. Keep stdout machine-readable.
        with redirect_stdout(sys.stderr):
            result = asyncio.run(
                research_company(
                    url,
                    api_key=key,
                    output_dir=output_dir,
                    config=config,
                    technology_catalog=catalog,
                )
            )
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(result.model_dump_json(indent=2))
    if result.status == "failed":
        raise SystemExit(2)


@click.command()
@click.argument(
    "result_file", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
@click.option("--backend-url", required=True, help="Corpscout backoffice base URL.")
@click.option(
    "--env-file", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
def submit(result_file: Path, backend_url: str, env_file: Path | None) -> None:
    """Submit a saved run's technology proposals; retries reuse run/record IDs."""
    token = (
        dotenv_values(env_file).get("TECHNOLOGY_SUBMISSION_TOKEN")
        if env_file is not None
        else os.environ.get("TECHNOLOGY_SUBMISSION_TOKEN")
    )
    if not token:
        raise click.ClickException("Set TECHNOLOGY_SUBMISSION_TOKEN")
    try:
        result = ResearchResult.model_validate_json(result_file.read_bytes())
        failed = sum(
            finding.data.get("catalog_match") is None
            for finding in result.records.technology_signals
        )
        if failed:
            click.echo(
                f"Skipping {failed} observations with technology processing errors; retained in result.json",
                err=True,
            )
        response = httpx.post(
            backend_url.rstrip("/") + "/admin/api/technology-submissions",
            json={
                "schema_version": "1.0",
                "run_id": result.run_id,
                "site_url": result.site_url,
                "model": result.config.model,
                "records": [
                    finding.model_dump()
                    for finding in result.records.technology_signals
                    if finding.data.get("catalog_match") is not None
                ],
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if response.is_error:
            raise click.ClickException(
                f"Technology submission failed: HTTP {response.status_code}"
            )
        click.echo(response.text)
    except (ValueError, OSError, httpx.HTTPError) as error:
        raise click.ClickException(
            "Could not read or submit the research result"
        ) from error


if __name__ == "__main__":
    main()
