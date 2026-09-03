"""One structured Codex turn with timeout handling and no exceptions."""

import asyncio
import logging
from dataclasses import dataclass

from openai_codex import AsyncCodex, AsyncTurnHandle, Sandbox, TurnResult
from pydantic import BaseModel, ValidationError

from ex1.aux import print_usage
from ex1.models import AnalysisTokenUsage, structured_output_schema

LOGGER = logging.getLogger(__name__)
TURN_INTERRUPT_TIMEOUT_SECONDS = 5
TURN_COMPLETION_TIMEOUT_SECONDS = 10


@dataclass(frozen=True, slots=True)
class StructuredTurnOutcome[T: BaseModel]:
    value: T | None
    token_usage: AnalysisTokenUsage | None
    error: str | None


async def run_structured_turn[T: BaseModel](
    *,
    prompt: str,
    base_instructions: str,
    output_model: type[T],
    timeout_seconds: int,
    operation_name: str,
) -> StructuredTurnOutcome[T]:
    """Run one ephemeral, read-only Codex turn and parse its structured output.

    A separate client per call keeps a failed or force-closed Codex process
    from affecting later calls. Every failure mode is returned as ``error``.
    """
    try:
        async with AsyncCodex() as codex:
            thread = await codex.thread_start(
                base_instructions=base_instructions,
                ephemeral=True,
                sandbox=Sandbox.read_only,
            )
            turn = await thread.turn(
                prompt,
                output_schema=structured_output_schema(output_model),
                sandbox=Sandbox.read_only,
            )
            result, timed_out = await _run_turn_with_timeout(
                codex,
                turn,
                timeout_seconds=timeout_seconds,
                operation_name=operation_name,
            )
    except Exception as error:
        LOGGER.exception("%s failed", operation_name)
        return StructuredTurnOutcome(value=None, token_usage=None, error=str(error))

    if result is None:
        return StructuredTurnOutcome(
            value=None,
            token_usage=None,
            error=f"analysis timed out after {timeout_seconds} seconds",
        )
    token_usage = print_usage(result, page_url=operation_name)
    if timed_out:
        return StructuredTurnOutcome(
            value=None,
            token_usage=token_usage,
            error=f"analysis timed out after {timeout_seconds} seconds",
        )
    if result.final_response is None:
        error_message = (
            result.error.message if result.error is not None else result.status
        )
        return StructuredTurnOutcome(
            value=None,
            token_usage=token_usage,
            error=f"Codex returned no final response: {error_message}",
        )
    try:
        value = output_model.model_validate_json(result.final_response)
    except ValidationError as error:
        return StructuredTurnOutcome(
            value=None,
            token_usage=token_usage,
            error=f"Codex returned invalid structured data: {error}",
        )
    return StructuredTurnOutcome(value=value, token_usage=token_usage, error=None)


async def _run_turn_with_timeout(
    codex: AsyncCodex,
    turn: AsyncTurnHandle,
    *,
    timeout_seconds: int,
    operation_name: str,
) -> tuple[TurnResult | None, bool]:
    """Run a turn without cancelling its blocking notification worker."""
    turn_task = asyncio.create_task(turn.run())
    completed, _ = await asyncio.wait({turn_task}, timeout=timeout_seconds)
    if turn_task in completed:
        return turn_task.result(), False

    LOGGER.warning(
        "%s timed out after %d seconds; interrupting it",
        operation_name,
        timeout_seconds,
    )
    interrupt_task = asyncio.create_task(turn.interrupt())
    interrupted, _ = await asyncio.wait(
        {interrupt_task},
        timeout=TURN_INTERRUPT_TIMEOUT_SECONDS,
    )
    interrupt_succeeded = False
    if interrupt_task in interrupted:
        try:
            interrupt_task.result()
            interrupt_succeeded = True
        except Exception:
            LOGGER.exception(
                "Could not interrupt timed-out operation %s",
                operation_name,
            )

    turn_completed: set[asyncio.Task[TurnResult]] = set()
    if interrupt_succeeded:
        turn_completed, _ = await asyncio.wait(
            {turn_task},
            timeout=TURN_COMPLETION_TIMEOUT_SECONDS,
        )

    if turn_task not in turn_completed or interrupt_task not in interrupted:
        LOGGER.warning(
            "Closing the Codex client for timed-out operation %s to release its waiters",
            operation_name,
        )
        await codex.close()

    # Closing the client sends a transport error to every still-registered waiter.
    # Drain both tasks before returning so asyncio has no executor work left at exit.
    turn_outcome, _ = await asyncio.gather(
        turn_task,
        interrupt_task,
        return_exceptions=True,
    )
    result = turn_outcome if isinstance(turn_outcome, TurnResult) else None
    return result, True
