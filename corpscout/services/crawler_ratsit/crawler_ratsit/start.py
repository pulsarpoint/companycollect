import asyncio
import json
import os
from dataclasses import asdict
from uuid import uuid4

import click
from temporalio.client import Client

from crawler_ratsit.config import TemporalSettings
from crawler_ratsit.models import CrawlCompanyInput
from crawler_ratsit.submission import (
    CompanyWorkflowSubmission,
    submit_company_workflow,
)


async def _submit_company_workflow(
    settings: TemporalSettings,
    workflow_input: CrawlCompanyInput,
) -> CompanyWorkflowSubmission:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )
    return await submit_company_workflow(
        client,
        workflow_input,
        task_queue=settings.temporal_task_queue,
    )


@click.command()
@click.argument("company_id")
@click.option(
    "--batch-id",
    type=str,
    help="Batch UUID. A new UUID is generated when omitted.",
)
def main(company_id: str, batch_id: str | None) -> None:
    """Submit one company to the Ratsit Temporal worker."""
    try:
        settings = TemporalSettings.from_environment(os.environ)
        workflow_input = CrawlCompanyInput(
            company_id=company_id,
            batch_id=batch_id or str(uuid4()),
        )
        result = asyncio.run(_submit_company_workflow(settings, workflow_input))
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(asdict(result), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
