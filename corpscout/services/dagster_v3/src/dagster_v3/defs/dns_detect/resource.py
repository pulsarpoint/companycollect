"""Dagster resource for the dns-detect HTTP service (on the Dagster host)."""

import json

import dagster as dg
import requests

# Loopback: the service runs on the Dagster host for the initial scan. Moving
# it is a change of DNS_DETECT_API_URL (and of the Ansible inventory).
DEFAULT_API_URL = "http://127.0.0.1:8096"


class DnsDetectError(Exception):
    """The service refused a batch or is unavailable."""


class DnsDetectResource(dg.ConfigurableResource):
    """Sends batches of DNS records to dns-detect and reads its knowledge versions."""

    api_url: str = DEFAULT_API_URL
    request_timeout_s: float = 300.0

    def knowledge(self) -> dict:
        response = requests.get(f"{self.api_url}/v1/knowledge", timeout=30)
        if response.status_code != 200:
            raise DnsDetectError(f"knowledge: HTTP {response.status_code} {response.text}")
        return response.json()

    def resolve(self, records: list[dict]) -> tuple[list[dict], str, str]:
        """Resolve a batch; returns the output lines (input order) and the
        rules and IP versions that produced them."""
        body = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records)
        response = requests.post(
            f"{self.api_url}/v1/resolve",
            data=body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=self.request_timeout_s,
        )
        if response.status_code != 200:
            raise DnsDetectError(f"resolve: HTTP {response.status_code} {response.text[:500]}")
        lines = [json.loads(x) for x in response.text.splitlines() if x]
        return lines, response.headers["X-Rules-Version"], response.headers["X-IP-Version"]
