# Ratsit crawler

Start the persistent local Chromium instance:

```shell
uv run python main.py --profile-dir ./profile
```

In another terminal, start the FastAPI service:

```shell
uv run python api.py
```

Trigger a crawl of the fixed Ratsit company page:

```shell
curl --fail-with-body -X POST http://127.0.0.1:8000/crawl
```

The endpoint takes no parameters yet. It opens the configured Ratsit URL in the
Chromium context exposed at `http://127.0.0.1:9222` and returns the main page
content as Markdown.
