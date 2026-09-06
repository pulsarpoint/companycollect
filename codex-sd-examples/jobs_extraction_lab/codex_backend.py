"""Use the existing SDK and timeout handling with an optional newer CLI runtime."""

import logging
from pathlib import Path

from openai_codex import AsyncCodex, CodexConfig, Sandbox
from pydantic import ValidationError

from ex1.aux import analysis_token_usage
from ex1.models import structured_output_schema
from ex3.llm import StructuredTurnOutcome, _run_turn_with_timeout, run_structured_turn
from jobs_extraction_lab.models import JobExtraction

LOGGER = logging.getLogger(__name__)


async def extract_codex(
    prompt: str,
    *,
    instructions: str,
    timeout: int,
    operation: str,
    codex_bin: Path | None,
) -> StructuredTurnOutcome[JobExtraction]:
    if codex_bin is None:
        return await run_structured_turn(
            prompt=prompt,
            base_instructions=instructions,
            output_model=JobExtraction,
            timeout_seconds=timeout,
            operation_name=operation,
        )
    try:
        async with AsyncCodex(CodexConfig(codex_bin=str(codex_bin))) as codex:
            thread = await codex.thread_start(
                base_instructions=instructions,
                ephemeral=True,
                sandbox=Sandbox.read_only,
            )
            turn = await thread.turn(
                prompt,
                output_schema=structured_output_schema(JobExtraction),
                sandbox=Sandbox.read_only,
            )
            result, timed_out = await _run_turn_with_timeout(
                codex, turn, timeout_seconds=timeout, operation_name=operation
            )
    except Exception as error:
        LOGGER.exception("%s failed", operation)
        return StructuredTurnOutcome(value=None, token_usage=None, error=str(error))
    usage = analysis_token_usage(result) if result is not None else None
    if timed_out or result is None:
        return StructuredTurnOutcome(
            value=None, token_usage=usage, error=f"Timed out after {timeout} seconds"
        )
    if result.final_response is None:
        return StructuredTurnOutcome(
            value=None, token_usage=usage, error="Codex returned no final response"
        )
    try:
        extraction = JobExtraction.model_validate_json(result.final_response)
    except ValidationError as error:
        return StructuredTurnOutcome(
            value=None,
            token_usage=usage,
            error=f"Invalid structured extraction: {error}",
        )
    return StructuredTurnOutcome(value=extraction, token_usage=usage, error=None)
