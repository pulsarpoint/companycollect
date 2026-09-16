"""Budgeted OpenRouter or DeepSeek calls with JSON output and per-call artifacts."""

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import httpx

from company_research.models import ResearchConfig
from company_research.storage import utc_now, write_json
from company_research.technology_catalog import (
    LIST_TECHNOLOGY_CATEGORIES_TOOL,
    SEARCH_TECHNOLOGIES_TOOL,
    TechnologyCatalog,
)


class ModelBudgetExceeded(RuntimeError):
    pass


class ModelUnavailable(RuntimeError):
    pass


def unique_json_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_model_json(raw: str) -> tuple[object, bool]:
    """Unwrap formatting or identical repeated JSON; never choose conflicting data."""
    try:
        return json.loads(raw, object_pairs_hook=unique_json_object), False
    except ValueError:
        remaining, documents = raw.lstrip(), []
        try:
            while remaining:
                document, end = json.JSONDecoder(
                    object_pairs_hook=unique_json_object
                ).raw_decode(remaining)
                documents.append(document)
                remaining = remaining[end:].lstrip()
        except ValueError:
            documents = []
        if len(documents) > 1 and all(
            document == documents[0] for document in documents
        ):
            return documents[0], True
        blocks = re.findall(
            r"^```(?:json)?[ \t]*\n(.*?)\n```[ \t]*$",
            raw,
            re.MULTILINE | re.DOTALL | re.IGNORECASE,
        )
        if raw.count("```") != 2 or len(blocks) != 1:
            raise
        return json.loads(blocks[0], object_pairs_hook=unique_json_object), True


@dataclass
class ModelReply:
    document: object | None
    raw: str | None
    error: str | None
    message: dict | None = None
    searches: list[dict] = field(default_factory=list)


class ModelClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        config: ResearchConfig,
        output_dir: Path,
        *,
        api: Literal["openrouter", "deepseek"] = "openrouter",
        json_mode: Literal["json_schema", "json_object"] | None = None,
    ):
        self.client, self.api_key, self.config, self.output_dir = (
            client,
            api_key,
            config,
            output_dir,
        )
        self.api = api
        self.api_name = "DeepSeek" if api == "deepseek" else "OpenRouter"
        self.json_mode = json_mode or (
            "json_object" if api == "deepseek" else "json_schema"
        )
        if api == "deepseek" and self.json_mode != "json_object":
            raise ValueError("DeepSeek supports json_object, not json_schema")
        self.calls: list[dict] = []
        self.semaphore = asyncio.Semaphore(config.extraction_concurrency)
        self.consecutive_errors = 0
        self.unavailable = False

    @property
    def remaining(self) -> int:
        return self.config.max_model_calls - len(self.calls)

    def usage(self) -> dict:
        return {
            "calls": len(self.calls),
            "successful_responses": sum("usage" in c for c in self.calls),
            "known_cost_usd": sum(
                c.get("usage", {}).get("cost") or 0 for c in self.calls
            ),
            "unknown_cost_calls": sum(
                c.get("usage", {}).get("cost") is None for c in self.calls
            ),
            "prompt_tokens": sum(
                c.get("usage", {}).get("prompt_tokens", 0) for c in self.calls
            ),
            "completion_tokens": sum(
                c.get("usage", {}).get("completion_tokens", 0) for c in self.calls
            ),
            "by_call": self.calls,
        }

    async def ask(
        self,
        prompt: str,
        schema: dict,
        *,
        task: str,
        catalog: TechnologyCatalog | None = None,
    ) -> ModelReply:
        messages: list[dict] = [
            {
                "role": "system",
                "content": "Use only the supplied source and local catalog search. After any tool calls, return exactly one JSON object matching the supplied schema. Do not include an introduction, explanation or Markdown fences around the final JSON.",
            },
            {"role": "user", "content": prompt},
        ]
        if self.json_mode == "json_object":
            messages[0]["content"] += (
                "\n\nRequired JSON Schema (validated by the application):\n"
                + json.dumps(schema, ensure_ascii=False, sort_keys=True)
            )
        searches = []
        rounds = self.config.max_technology_tool_rounds if catalog is not None else 0
        for round_index in range(rounds + 1):
            reply = await self._ask(
                schema, task=task, messages=messages, catalog=catalog
            )
            calls = (
                reply.message.get("tool_calls") if reply.message is not None else None
            )
            if not calls or reply.message is None:
                reply.searches = searches
                return reply
            if catalog is None or round_index == rounds:
                return ModelReply(
                    None, None, "Technology tool round limit reached", searches=searches
                )
            if not isinstance(calls, list) or len(calls) > 20:
                return ModelReply(
                    None, None, "Invalid technology tool calls", searches=searches
                )
            messages.append(reply.message)
            for call in calls:
                if not isinstance(call, dict) or not isinstance(call.get("id"), str):
                    return ModelReply(
                        None, None, "Invalid technology tool call ID", searches=searches
                    )
                function = call.get("function", {})
                try:
                    if not isinstance(function, dict) or function.get("name") not in {
                        "search_technologies",
                        "list_technology_categories",
                    }:
                        raise ValueError(
                            "Only the supplied catalog tools are available"
                        )
                    arguments = json.loads(function.get("arguments", ""))
                    if function["name"] == "list_technology_categories":
                        if arguments != {}:
                            raise ValueError("Category listing takes no arguments")
                        output = catalog.category_options()
                    else:
                        if (
                            not isinstance(arguments, dict)
                            or "queries" not in arguments
                            or set(arguments) - {"queries", "context"}
                        ):
                            raise ValueError(
                                "Supply a queries array and optional context"
                            )
                        queries = arguments["queries"]
                        if not isinstance(queries, list) or not 1 <= len(queries) <= 20:
                            raise ValueError("Supply 1–20 queries")
                        results = [
                            catalog.search(query, arguments.get("context", ""))
                            for query in queries
                        ]
                        searches.extend(results)
                        output = {"results": results}
                except (ValueError, TypeError) as error:
                    output = {"error": str(error)}
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call["id"],
                        "content": json.dumps(output),
                    }
                )
        raise AssertionError("Tool loop must return within its configured rounds")

    async def _ask(
        self,
        schema: dict,
        *,
        task: str,
        messages: list[dict],
        catalog: TechnologyCatalog | None,
    ) -> ModelReply:
        async with self.semaphore:
            request: dict = {
                "model": self.config.model,
                "stream": False,
                "max_tokens": self.config.max_output_tokens,
                "messages": messages,
                "response_format": {"type": self.json_mode},
            }
            if self.json_mode == "json_schema":
                request["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "company_research",
                        "strict": True,
                        "schema": schema,
                    },
                }
            if self.api == "deepseek":
                request["thinking"] = {
                    "type": "disabled"
                    if self.config.reasoning_effort == "none"
                    else "enabled"
                }
                request["reasoning_effort"] = self.config.reasoning_effort
                if self.config.reasoning_effort == "none":
                    request["temperature"] = 0
            else:
                request["temperature"] = 0
                request["reasoning"] = (
                    {"enabled": False}
                    if self.config.reasoning_effort == "none"
                    else {
                        "enabled": True,
                        "exclude": True,
                        "effort": self.config.reasoning_effort,
                    }
                )
                request["provider"] = {
                    **(
                        {"only": [self.config.provider]}
                        if self.config.provider is not None
                        else {"sort": "latency"}
                    ),
                    "allow_fallbacks": self.config.provider is None,
                    "require_parameters": True,
                }
            if catalog is not None:
                request["tools"] = [
                    SEARCH_TECHNOLOGIES_TOOL,
                    LIST_TECHNOLOGY_CATEGORIES_TOOL,
                ]
            last_error = "Request did not complete"
            record: dict | None = None
            started = time.monotonic()
            try:
                async with asyncio.timeout(self.config.model_timeout_seconds):
                    for attempt in range(1, self.config.max_http_attempts + 1):
                        if self.unavailable:
                            raise ModelUnavailable(
                                f"{self.api_name} unavailable after repeated or permanent errors"
                            )
                        if self.remaining <= 0:
                            raise ModelBudgetExceeded("Model request budget reached")
                        record = {
                            "call_id": len(self.calls) + 1,
                            "task": task,
                            "attempt": attempt,
                            "started_at": utc_now(),
                            "api": self.api,
                        }
                        self.calls.append(record)
                        target = (
                            self.output_dir / "calls" / f"{record['call_id']:05}.json"
                        )
                        write_json(target, {**record, "request": request})
                        retry_delay = min(2**attempt, 10)
                        try:
                            response = await self.client.post(
                                "chat/completions",
                                json=request,
                                headers={"Authorization": f"Bearer {self.api_key}"},
                                timeout=self.config.model_timeout_seconds,
                            )
                        except httpx.HTTPError as error:
                            last_error = f"{type(error).__name__}: {self.api_name} transport failed"
                            record["error"] = last_error
                            write_json(target, {**record, "request": request})
                        else:
                            record["http_status"] = response.status_code
                            record["elapsed_seconds"] = round(
                                time.monotonic() - started, 3
                            )
                            if response.is_error:
                                last_error = (
                                    f"{self.api_name} HTTP {response.status_code}"
                                )
                                record["error"] = last_error
                                try:
                                    error_payload = response.json()
                                except ValueError:
                                    error_payload = None
                                if isinstance(error_payload, dict) and isinstance(
                                    error_payload.get("error"), dict
                                ):
                                    provider_error = error_payload["error"]
                                    record["provider_error"] = {
                                        "code": provider_error.get("code"),
                                        "message": str(
                                            provider_error.get("message", "")
                                        ).replace(self.api_key, "[REDACTED]")[:2000],
                                    }
                                if response.status_code == 429:
                                    retry_after = response.headers.get(
                                        "retry-after", ""
                                    )
                                    retry_delay = (
                                        min(int(retry_after), 60)
                                        if retry_after.isdecimal()
                                        else 30
                                    )
                                    record["retry_delay_seconds"] = retry_delay
                                write_json(target, {**record, "request": request})
                                if response.status_code not in {
                                    408,
                                    429,
                                    500,
                                    502,
                                    503,
                                    504,
                                }:
                                    self.unavailable = True
                                    raise ModelUnavailable(last_error)
                            else:
                                try:
                                    payload = response.json()
                                except ValueError:
                                    last_error = f"{self.api_name} returned a non-JSON HTTP response"
                                    record["error"] = last_error
                                    write_json(target, {**record, "request": request})
                                else:
                                    payload = json.loads(
                                        json.dumps(payload).replace(
                                            self.api_key, "[REDACTED]"
                                        )
                                    )
                                    if not isinstance(payload, dict):
                                        payload = {
                                            "error": "Unexpected HTTP response envelope"
                                        }
                                    record["response_id"] = payload.get("id")
                                    record["provider"] = payload.get("provider")
                                    record["response_model"] = payload.get("model")
                                    if isinstance(payload.get("usage"), dict):
                                        record["usage"] = payload["usage"]
                                    choices = payload.get("choices")
                                    choice = (
                                        choices[0]
                                        if isinstance(choices, list)
                                        and choices
                                        and isinstance(choices[0], dict)
                                        else {}
                                    )
                                    message = choice.get("message")
                                    if not isinstance(message, dict):
                                        message = {}
                                    raw = message.get("content")
                                    finish = choice.get("finish_reason")
                                    record["finish_reason"] = finish
                                    document = None
                                    if finish == "tool_calls" and catalog is not None:
                                        write_json(
                                            target,
                                            {
                                                **record,
                                                "request": request,
                                                "response": payload,
                                            },
                                        )
                                        self.consecutive_errors = 0
                                        return ModelReply(
                                            None, None, None, message=message
                                        )
                                    if finish != "stop" or not isinstance(raw, str):
                                        record["error"] = (
                                            f"Incomplete model response ({finish})"
                                        )
                                    else:
                                        try:
                                            document, unwrapped = parse_model_json(raw)
                                            if unwrapped:
                                                record["json_wrapper_removed"] = True
                                        except ValueError:
                                            record["error"] = (
                                                "Invalid JSON model output"
                                            )
                                    write_json(
                                        target,
                                        {
                                            **record,
                                            "request": request,
                                            "response": payload,
                                        },
                                    )
                                    if "error" in record:
                                        self.consecutive_errors += 1
                                        if self.consecutive_errors >= 3:
                                            self.unavailable = True
                                        return ModelReply(
                                            None,
                                            raw if isinstance(raw, str) else None,
                                            record["error"],
                                        )
                                    self.consecutive_errors = 0
                                    return ModelReply(document, raw, None)
                        if attempt < self.config.max_http_attempts:
                            await asyncio.sleep(retry_delay)
            except TimeoutError:
                last_error = f"{self.api_name} exceeded {self.config.model_timeout_seconds:g}s total deadline"
                if record is not None:
                    record.update(
                        error=last_error,
                        elapsed_seconds=round(time.monotonic() - started, 3),
                    )
                    write_json(
                        self.output_dir / "calls" / f"{record['call_id']:05}.json",
                        {**record, "request": request},
                    )
            self.consecutive_errors += 1
            if self.consecutive_errors >= 3:
                self.unavailable = True
            return ModelReply(None, None, last_error)
