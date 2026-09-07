"""Forward Responses requests and retain public actions and reported usage."""

import json
import time
from pathlib import Path

import httpx
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionResetError


def public_value(value: object) -> object:
    """Exclude private reasoning from nested SDK and provider payloads."""
    if isinstance(value, dict):
        if value.get("type") in {"reasoning", "reasoning_text", "reasoning_summary"}:
            return None
        return {
            key: public_value(item)
            for key, item in value.items()
            if key
            not in {
                "reasoning",
                "reasoning_content",
                "reasoning_details",
                "encrypted_content",
            }
        }
    if isinstance(value, list):
        return [
            filtered for item in value if (filtered := public_value(item)) is not None
        ]
    return value


def append_json(path: Path, value: object) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, ensure_ascii=False) + "\n")


class ResponsesRecorder:
    """Keep the API key in the host process, outside the agent shell environment."""

    def __init__(self, api_key: str, output: Path) -> None:
        self.client = httpx.AsyncClient(
            base_url="https://openrouter.ai/api/v1/",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(900, connect=30),
        )
        self.output = output
        self.requests = 0
        self.runner: web.AppRunner | None = None

    async def start(self) -> str:
        app = web.Application(client_max_size=50 * 1024**2)
        app.router.add_post("/responses", self.forward)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = self.runner.addresses[0][1]
        return f"http://127.0.0.1:{port}"

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
        await self.client.aclose()

    def record_event(self, request_id: int, data: str) -> None:
        if not data or data == "[DONE]":
            return
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            return
        event_type = event.get("type", "")
        record: dict[str, object] = {
            "request": request_id,
            "time": time.time(),
            "type": event_type,
        }
        if event_type == "response.created":
            response = event.get("response", {})
            record.update({key: response.get(key) for key in ("id", "model", "status")})
        elif event_type in {
            "response.completed",
            "response.incomplete",
            "response.failed",
        }:
            response = event.get("response", {})
            record.update(
                {
                    key: public_value(response.get(key))
                    for key in (
                        "id",
                        "status",
                        "model",
                        "usage",
                        "error",
                        "incomplete_details",
                        "output",
                    )
                }
            )
        elif event_type == "response.output_item.done":
            item = public_value(event.get("item"))
            if item is None:
                return
            record["item"] = item
        elif event_type == "error":
            record["error"] = public_value(event)
        else:
            return
        append_json(self.output / "provider-events.jsonl", record)

    async def forward(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        self.requests += 1
        request_id = self.requests
        append_json(
            self.output / "requests.jsonl",
            {
                "request": request_id,
                "time": time.time(),
                "model": body.get("model"),
                "tools": [
                    {key: tool.get(key) for key in ("type", "name")}
                    for tool in body.get("tools", [])
                ],
                "input_characters": len(json.dumps(body.get("input", []))),
                "max_output_tokens": body.get("max_output_tokens"),
                "reasoning_configuration": body.get("reasoning"),
            },
        )
        async with self.client.stream("POST", "responses", json=body) as upstream:
            if upstream.status_code != 200:
                error = await upstream.aread()
                append_json(
                    self.output / "provider-events.jsonl",
                    {
                        "request": request_id,
                        "type": "http_error",
                        "status": upstream.status_code,
                        "error": error.decode(errors="replace"),
                    },
                )
                return web.Response(
                    status=upstream.status_code,
                    body=error,
                    content_type="application/json",
                )
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            try:
                async for line in upstream.aiter_lines():
                    if line.startswith("data:"):
                        self.record_event(request_id, line[5:].strip())
                    await response.write((line + "\n").encode())
                await response.write_eof()
            except ClientConnectionResetError:
                # Codex may close immediately after response.completed, before [DONE].
                append_json(
                    self.output / "provider-events.jsonl",
                    {
                        "request": request_id,
                        "type": "client_disconnected",
                        "time": time.time(),
                    },
                )
            return response
