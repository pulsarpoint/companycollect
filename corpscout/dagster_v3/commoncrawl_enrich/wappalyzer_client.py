"""Client for the wappalyzer-service Go sidecar (projectdiscovery/wappalyzergo).

Batches WARC page (headers, body) into POST /analyze and maps the response into the
shared `Technology` model. The Go service owns the fingerprint set; this is a thin
transport so the three `detect_technologies` callers don't change.
"""
import os

from dlt.sources.helpers import requests  # retry/backoff session

from commoncrawl_enrich.models import Technology

DEFAULT_BATCH_SIZE = 200


class WappalyzerClient:
    def __init__(self, base_url: str, *, session=None, timeout: int = 120,
                 batch_size: int = DEFAULT_BATCH_SIZE):
        self._base = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._timeout = timeout
        self._batch_size = batch_size

    @classmethod
    def from_env(cls, **kwargs) -> "WappalyzerClient | None":
        """Build from COMMONCRAWL_WAPPALYZER_URL, or None if the service isn't configured."""
        url = os.environ.get("COMMONCRAWL_WAPPALYZER_URL")
        return cls(url, **kwargs) if url else None

    def analyze_batch(
        self, items: list[tuple[str, dict[str, list[str]], str]]
    ) -> dict[str, list[Technology]]:
        """items: (id, headers{name:[values]}, body) -> {id: [Technology]}."""
        out: dict[str, list[Technology]] = {}
        for start in range(0, len(items), self._batch_size):
            chunk = items[start:start + self._batch_size]
            payload = {"items": [{"id": i, "headers": h, "body": b} for i, h, b in chunk]}
            resp = self._session.post(f"{self._base}/analyze", json=payload, timeout=self._timeout)
            resp.raise_for_status()
            for result in resp.json().get("results", []):
                out[result["id"]] = [
                    Technology(
                        technology=t["name"],
                        category=(t.get("categories") or [""])[0],  # primary category
                        version=t.get("version", ""),
                        confidence=int(t.get("confidence", 100)),
                    )
                    for t in result.get("technologies", [])
                ]
        return out

    def analyze(self, headers: dict[str, list[str]], body: str) -> list[Technology]:
        """Single-page convenience over analyze_batch."""
        return self.analyze_batch([("0", headers, body)]).get("0", [])
