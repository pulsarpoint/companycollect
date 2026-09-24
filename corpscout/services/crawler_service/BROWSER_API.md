# External browser API

Since 0.37.0 the API is owned by the independent [browser service](../browser_service/README.md),
not the crawler. That document defines session allocation, extraction, management,
SQLite persistence, expiry and deployment.

Set `BROWSER_API_URL` and `BROWSER_API_TOKEN` in the crawler. The crawler stores the
persistent `browser_lease_id` and active `browser_execution_id` in its job history.
`CrawlRequest.session_id` can explicitly reuse a saved profile. Manual retries reuse
the failed attempt's profile and reopen it headed; automatic crawls launch headless.

The HTTP client reserves capacity, sends execution IDs on operations, heartbeats,
and closes the browser in cancellation/finalization. Site and search share the same
session. A busy claim returns 503 and the client retries the same ID once per second.
Closed profiles remain on disk for the browser service's retention period. There is
no browser allocation queue, fallback launcher, or navigation replay. An expired
saved profile returns 410; submit a fresh request without its session ID to start over.

For programmatic collection, reserve with `BrowserLeaseClient.lease(identifier=uuid4().hex,
request_id=..., domain=...)`, then pass that client as `browser_client` to `crawl_company`.
The collection CLI handles this automatically using its environment configuration.

Browser integration tests require the separate package as a test dependency:

```sh
uv sync --extra service --extra mcp --group test
uv run --no-sync python -m unittest discover -s tests -v
```

The production deployment exports only the `service` extra; it has no Playwright or
CloakBrowser dependency.
