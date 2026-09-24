"""MCP tools for a database-backed catalog and validated, reviewable proposal drafts."""

import logging
import os
import sys
from pathlib import Path
from typing import Annotated, Any

import click
from dotenv import dotenv_values
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from crawler_service.catalog_config import load_catalog
from crawler_service.models import ModelTechnologyMatch, ProposedTechnology
from crawler_service.technology_catalog import (
    TechnologyCatalog,
    validate_technology_match,
)


def create_catalog_server(catalog: TechnologyCatalog) -> FastMCP:
    server = FastMCP(
        "Company technology catalog",
        instructions="Search the published technology catalog before matching or proposing a technology. Compare descriptions and source context, not just names. Use a canonical identity when it means the same technology. Otherwise prepare a proposal with a category and your own concise description. Proposed metadata is an unverified draft. Keep company-usage evidence separate from technology definitions. This server pins a snapshot for its lifetime; it does not approve catalog entries or submit observations to the backend.",
    )
    read_only = ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )

    @server.tool(annotations=read_only)
    def get_catalog_info() -> dict[str, Any]:
        """Identify the completed database publication and its snapshot timestamp."""
        return {
            "catalog_version": catalog.snapshot.version,
            "synced_at": catalog.snapshot.synced_at,
            "technology_count": len(catalog.entries),
            "accepted_alias_count": len(catalog.aliases),
            "scope": "published_catalog_snapshot",
            "refresh": "Restart the server without --offline-catalog to sync a new completed publication.",
        }

    @server.tool(annotations=read_only)
    def search_technologies(
        queries: Annotated[
            list[Annotated[str, Field(min_length=1, max_length=200)]],
            Field(min_length=1, max_length=20),
        ],
        context: Annotated[str, Field(max_length=4000)] = "",
    ) -> dict[str, Any]:
        """Search existing identities/aliases with descriptions, websites and categories.

        Include context for ambiguous names, e.g. ADS with RF circuit simulation.
        Candidates require a meaning check; empty results do not prove global absence.
        """
        return {"results": [catalog.search(query, context) for query in queries]}

    @server.tool(annotations=read_only)
    def get_technology(
        name: Annotated[str, Field(min_length=1, max_length=200)],
    ) -> dict[str, Any]:
        """Fetch a canonical technology by exact, case-insensitive or accepted-alias name."""
        canonical, method = catalog.resolve(name)
        return {
            "query": name,
            "catalog_version": catalog.snapshot.version,
            "match_method": method,
            "technology": catalog.entries[canonical].model_dump()
            if canonical is not None
            else None,
        }

    @server.tool(annotations=read_only)
    def list_technology_categories() -> dict[str, Any]:
        """List existing category IDs/names; use a category suggestion when none fits."""
        return catalog.category_options()

    @server.tool(annotations=read_only)
    def prepare_technology_proposal(
        observed_name: Annotated[str, Field(min_length=1, max_length=200)],
        proposal: ProposedTechnology,
        reason: Annotated[str, Field(min_length=1, max_length=2000)],
        alternative_names: Annotated[
            list[Annotated[str, Field(min_length=1, max_length=200)]],
            Field(max_length=18),
        ],
    ) -> dict[str, Any]:
        """Validate a new technology draft with an LLM-written description and category.

        Search first and compare candidates. This tool repeats the observed-name,
        proposed-name and alternative-name searches against the actual catalog.
        Known exact/alias identities resolve as matched. Category IDs must exist;
        otherwise category_suggestion is mandatory. Website/licensing may be unknown.
        The returned catalog_match is ready to attach to a source-backed crawler
        observation for the existing submission/review flow. Nothing is inserted or
        approved by this preparation tool; the database client operates read-only.
        """
        queries = list(
            dict.fromkeys([observed_name, proposal.name, *alternative_names])
        )
        searches = [catalog.search(query, proposal.description) for query in queries]
        claimed = ModelTechnologyMatch(
            status="proposed",
            canonical_technology=None,
            proposed_technology=proposal,
            reason=reason,
        )
        match = validate_technology_match(
            observed_name, claimed.model_dump(), catalog, searches
        )
        return {
            "catalog_match": match,
            "submission_status": "prepared_not_submitted",
            "metadata_origin": "agent_generated_draft",
            "next_step": "Attach catalog_match to the original source-backed technology observation and submit through the existing backend. Administrator review is required for proposed identities.",
        }

    return server


@click.command()
@click.option(
    "--technology-catalog",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Snapshot cache; refreshed from ClickHouse unless explicitly offline.",
)
@click.option(
    "--offline-catalog",
    is_flag=True,
    help="Use an explicit saved snapshot without database access.",
)
@click.option(
    "--env-file", type=click.Path(path_type=Path, exists=True, dir_okay=False)
)
def main(
    technology_catalog: Path | None, offline_catalog: bool, env_file: Path | None
) -> None:
    """Serve technology lookup and proposal preparation over MCP stdio."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    environment = dict(os.environ)
    if env_file is not None:
        environment.update(
            {
                key: value
                for key, value in dotenv_values(env_file).items()
                if value is not None
            }
        )
    try:
        catalog = load_catalog(environment, technology_catalog, offline_catalog)
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    create_catalog_server(catalog).run(transport="stdio")


if __name__ == "__main__":
    main()
