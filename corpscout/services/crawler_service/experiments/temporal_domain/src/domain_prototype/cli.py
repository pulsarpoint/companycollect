"""Run a private Temporal server and drive simulated verification manually."""

import asyncio
import json
import re
import signal
from dataclasses import asdict
from pathlib import Path

import click
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from domain_prototype.activities import LocalResults, simulate_action
from domain_prototype.models import DomainInput, HumanRequest, Verification
from domain_prototype.workflow import DomainWorkflow

TASK_QUEUE = "domain-prototype"


def workflow_id(domain: str) -> str:
    canonical = domain.strip().removesuffix(".").lower()
    if len(canonical) > 253 or any(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None
        for label in canonical.split(".")
    ):
        raise click.BadParameter("Use an ASCII domain name without scheme or path")
    return f"domain-prototype:{canonical}"


@click.group()
@click.option("--address", default="127.0.0.1:17233", show_default=True)
@click.pass_context
def main(ctx: click.Context, address: str) -> None:
    """Experiment with Temporal; all crawl and browser actions are simulated."""
    ctx.obj = address


@main.command()
@click.option("--port", default=17233, show_default=True, type=int)
@click.option("--ui-port", default=18233, show_default=True, type=int)
@click.option("--state-dir", default=".state", type=click.Path(path_type=Path))
@click.option("--output", default="output", type=click.Path(path_type=Path))
def serve(port: int, ui_port: int, state_dir: Path, output: Path) -> None:
    """Start the local Temporal server, UI, and worker. Ctrl-C stops all three."""

    async def run() -> None:
        state_dir.mkdir(parents=True, exist_ok=True)
        stop = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        results = LocalResults(output.resolve())
        async with await WorkflowEnvironment.start_local(
            ip="127.0.0.1",
            port=port,
            ui=True,
            ui_port=ui_port,
            dev_server_database_filename=str((state_dir / "temporal.db").resolve()),
        ) as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[DomainWorkflow],
                activities=[simulate_action, results.publish_result],
            ):
                click.echo(f"Temporal UI: http://127.0.0.1:{ui_port}")
                click.echo(f"Client address: 127.0.0.1:{port}")
                click.echo("SIMULATION: no real browsing, verification, or S3 upload.")
                click.echo(
                    "Stop and restart this command to exercise durable recovery."
                )
                await stop.wait()

    asyncio.run(run())


@main.command()
@click.argument("domain")
@click.option("--challenges", default=2, show_default=True, type=click.IntRange(0, 20))
@click.option(
    "--wait-seconds",
    default=600.0,
    show_default=True,
    type=click.FloatRange(min=0, min_open=True),
)
@click.option(
    "--verification-seconds",
    default=60.0,
    show_default=True,
    type=click.FloatRange(min=0, min_open=True),
)
@click.pass_obj
def start(
    address: str,
    domain: str,
    challenges: int,
    wait_seconds: float,
    verification_seconds: float,
) -> None:
    """Start a domain; duplicate active submissions return the existing run."""
    identifier = workflow_id(domain)

    async def run() -> None:
        client = await Client.connect(address)
        handle = await client.start_workflow(
            DomainWorkflow.run,
            DomainInput(
                identifier.split(":", 1)[1],
                challenges,
                wait_seconds,
                verification_seconds,
            ),
            id=identifier,
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        click.echo(
            json.dumps(
                {"workflow_id": handle.id, "run_id": handle.result_run_id}, indent=2
            )
        )

    asyncio.run(run())


@main.command()
@click.argument("domain")
@click.pass_obj
def status(address: str, domain: str) -> None:
    """Inspect checkpoints, pending actions, waiting request, and deadline."""

    async def run() -> None:
        client = await Client.connect(address)
        state = await client.get_workflow_handle_for(
            DomainWorkflow.run, workflow_id(domain)
        ).query(DomainWorkflow.status)
        click.echo(json.dumps(asdict(state), indent=2))

    asyncio.run(run())


@main.command()
@click.argument("domain")
@click.argument("request_id")
@click.pass_obj
def activate(address: str, domain: str, request_id: str) -> None:
    """Activate a simulated verification session for the exact waiting request."""

    async def run() -> None:
        client = await Client.connect(address)
        request = await client.get_workflow_handle_for(
            DomainWorkflow.run, workflow_id(domain)
        ).execute_update("activate_verification", request_id, result_type=HumanRequest)
        click.echo(json.dumps(asdict(request), indent=2))

    asyncio.run(run())


@main.command()
@click.argument("domain")
@click.argument("request_id")
@click.argument("session_id")
@click.option("--failed", is_flag=True, help="Simulate an unsuccessful verification.")
@click.pass_obj
def verify(
    address: str, domain: str, request_id: str, session_id: str, failed: bool
) -> None:
    """Simulate the browser service's callback; this does not solve a real challenge."""

    async def run() -> None:
        client = await Client.connect(address)
        await client.get_workflow_handle_for(
            DomainWorkflow.run, workflow_id(domain)
        ).signal(
            DomainWorkflow.verification_completed,
            Verification(request_id, session_id, not failed),
        )
        click.echo(
            "Signal recorded. Use status to see whether it matched an active session."
        )

    asyncio.run(run())


@main.command()
@click.argument("domain")
@click.pass_obj
def result(address: str, domain: str) -> None:
    """Wait for final publication and print the completed result."""

    async def run() -> None:
        client = await Client.connect(address)
        state = await client.get_workflow_handle_for(
            DomainWorkflow.run, workflow_id(domain)
        ).result()
        click.echo(json.dumps(asdict(state), indent=2))

    asyncio.run(run())


if __name__ == "__main__":
    main()
