import unittest
from typing import Any, ClassVar, Self
from unittest.mock import patch

from pydantic import Field

from ex1.models import StrictModel
from ex3.llm import run_structured_turn


class _Answer(StrictModel):
    names: list[str] = Field(default_factory=list)


class _FakeResult:
    def __init__(
        self, final_response: str | None, *, status: str = "completed"
    ) -> None:
        self.final_response = final_response
        self.usage = None
        self.duration_ms = 12
        self.error = None
        self.status = status


class _FakeTurn:
    def __init__(self, result: _FakeResult) -> None:
        self._result = result

    async def run(self) -> _FakeResult:
        return self._result

    async def interrupt(self) -> None:
        return None


class _FakeThread:
    def __init__(self, result: _FakeResult, calls: list[dict[str, Any]]) -> None:
        self._result = result
        self._calls = calls

    async def turn(self, prompt: str, **kwargs: Any) -> _FakeTurn:
        self._calls.append({"prompt": prompt, **kwargs})
        return _FakeTurn(self._result)


class _FakeCodex:
    result: ClassVar[_FakeResult] = _FakeResult('{"names": []}')
    calls: ClassVar[list[dict[str, Any]]] = []
    raise_on_start: ClassVar[Exception | None] = None

    def __init__(self) -> None:
        self.closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def thread_start(self, **kwargs: Any) -> _FakeThread:
        if _FakeCodex.raise_on_start is not None:
            raise _FakeCodex.raise_on_start
        _FakeCodex.calls.append({"thread_start": kwargs})
        return _FakeThread(_FakeCodex.result, _FakeCodex.calls)

    async def close(self) -> None:
        self.closed = True


class StructuredTurnTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _FakeCodex.result = _FakeResult('{"names": ["Ada"]}')
        _FakeCodex.calls = []
        _FakeCodex.raise_on_start = None

    async def test_parses_the_final_response_into_the_output_model(self) -> None:
        with patch("ex3.llm.AsyncCodex", _FakeCodex):
            outcome = await run_structured_turn(
                prompt="Return names.",
                base_instructions="Only data.",
                output_model=_Answer,
                timeout_seconds=30,
                operation_name="test call",
            )

        self.assertIsNone(outcome.error)
        self.assertEqual(outcome.value, _Answer(names=["Ada"]))
        turn_call = _FakeCodex.calls[1]
        self.assertEqual(turn_call["prompt"], "Return names.")
        self.assertIn("output_schema", turn_call)
        self.assertEqual(
            _FakeCodex.calls[0]["thread_start"]["base_instructions"], "Only data."
        )

    async def test_reports_invalid_structured_output_as_an_error(self) -> None:
        _FakeCodex.result = _FakeResult('{"unexpected": 1}')

        with patch("ex3.llm.AsyncCodex", _FakeCodex):
            outcome = await run_structured_turn(
                prompt="x",
                base_instructions="y",
                output_model=_Answer,
                timeout_seconds=30,
                operation_name="test call",
            )

        self.assertIsNone(outcome.value)
        self.assertIn("invalid structured data", outcome.error or "")

    async def test_reports_missing_final_response_and_exceptions(self) -> None:
        _FakeCodex.result = _FakeResult(None, status="failed")
        with patch("ex3.llm.AsyncCodex", _FakeCodex):
            missing = await run_structured_turn(
                prompt="x",
                base_instructions="y",
                output_model=_Answer,
                timeout_seconds=30,
                operation_name="test call",
            )

        _FakeCodex.raise_on_start = RuntimeError("codex unavailable")
        with patch("ex3.llm.AsyncCodex", _FakeCodex):
            failed = await run_structured_turn(
                prompt="x",
                base_instructions="y",
                output_model=_Answer,
                timeout_seconds=30,
                operation_name="test call",
            )

        self.assertIn("no final response", missing.error or "")
        self.assertIn("codex unavailable", failed.error or "")
        self.assertIsNone(failed.token_usage)


if __name__ == "__main__":
    unittest.main()
