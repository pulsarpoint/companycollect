"""HTTP protocol fixture for queue/result tests that replace page acquisition separately."""

import json
from unittest.mock import patch
from uuid import uuid4

import httpx

from company_research.service import CrawlService


def install_browser_api(test):
    original = httpx.AsyncHTTPTransport.handle_async_request
    original_init = CrawlService.__init__
    sessions = {}

    async def handle(transport, request):
        if request.url.host != "browser-fixture":
            return await original(transport, request)
        path = request.url.path
        if request.method == "POST" and path == "/v1/browser/sessions":
            payload = json.loads(request.content)
            sessions[payload["id"]] = {
                **payload,
                "state": "ready",
                "profileId": payload["id"],
                "executionId": uuid4().hex,
                "generation": "fixture-generation",
            }
            return httpx.Response(201, json=sessions[payload["id"]])
        identifier = path.split("/")[4]
        if path.endswith("browser-ticket"):
            return httpx.Response(
                200,
                json={
                    "websocket_path": f"/v1/browser/sessions/{identifier}/browser?ticket=fixture",
                    "expires_in": 30,
                },
            )
        if request.method == "DELETE":
            sessions.pop(identifier, None)
            return httpx.Response(200, json={"id": identifier, "state": "released"})
        return httpx.Response(200, json=sessions[identifier])

    def initialize(service, output_dir, environment, **kwargs):
        return original_init(
            service,
            output_dir,
            {"BROWSER_API_URL": "http://browser-fixture", **environment},
            **kwargs,
        )

    for mocked in (
        patch.object(httpx.AsyncHTTPTransport, "handle_async_request", handle),
        patch.object(CrawlService, "__init__", initialize),
        patch.dict("os.environ", {"BROWSER_API_URL": "http://browser-fixture"}),
    ):
        mocked.start()
        test.addCleanup(mocked.stop)


original_send = httpx.AsyncClient.send


async def no_model_requests(client, request, **kwargs):
    if request.url.host == "browser-fixture":
        return await original_send(client, request, **kwargs)
    raise AssertionError("No model/catalog request expected")
