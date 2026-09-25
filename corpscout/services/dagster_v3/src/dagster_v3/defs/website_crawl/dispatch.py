"""Validated request payloads and idempotent submission to the crawler HTTP API."""

import hashlib
import json
from time import monotonic, sleep

from dlt.sources.helpers.requests import Session
from requests.exceptions import RequestException

from dagster_v3.defs.website_crawl.input import INPUT_TABLES

INPUTS_BY_TYPE = dict(zip(("full", "jobs", "site_info"), INPUT_TABLES, strict=True))
JOBS_INSTRUCTIONS = (
    "Collect the target company's current job vacancies and full job descriptions. "
    "Discover careers and jobs pages, including the company's confirmed recruiting "
    "platform. Preserve job links and simplified HTML; do not infer technologies or analyze jobs."
)


def crawl_payload(row: dict, crawl_type: str, batch_id: str) -> dict:
    """Refuse proposed input features the current crawler cannot yet represent."""
    if row["preset_version"] != 1:
        raise ValueError("unsupported crawl preset version")
    if row["proxy_route"] != "direct":
        raise ValueError("the crawler HTTP API does not yet accept a proxy route")
    overrides = json.loads(row["config_json"])
    if not isinstance(overrides, dict):
        raise ValueError("config_json must be an object")
    forbidden = {
        "refresh_interval_days",
        "headless",
        "proxy_route",
        "request_id",
        "url",
        "crawl",
        "pages",
        "instructions",
        "site_info",
        "save_artifacts",
        "interactive",
        "priority",
        "enabled",
    }
    if forbidden.intersection(overrides):
        raise ValueError(
            "config_json cannot override input identity, routing or refresh policy"
        )
    # One identity per domain/type/batch. Changed payloads conflict at the service,
    # rather than silently creating a second execution while recovering a batch.
    identity = hashlib.sha256(
        f"{batch_id}:{crawl_type}:{row['domain']}".encode()
    ).hexdigest()
    payload = {
        "request_id": f"dagster-crawl-{identity}",
        "url": row["website_url"],
        "save_artifacts": bool(row["save_artifacts"]),
        "interactive": not bool(row["headless"]),
    }
    for key in ("api", "challenge_agent_max_runs", "challenge_agent_model"):
        if key in overrides:
            payload[key] = overrides.pop(key)
    if overrides:
        payload["config"] = overrides
    if crawl_type == "site_info" and (row["pages"] or row["instructions"]):
        raise ValueError("basic info requests cannot include custom pages/instructions")
    if row["page_mode"] == "explicit":
        payload["pages"] = row["pages"]
    elif row["pages"]:
        raise ValueError("pages require explicit page mode")
    if row["instructions"]:
        payload["instructions"] = row["instructions"]
    if crawl_type == "full":
        if "pages" not in payload and "instructions" not in payload:
            payload["crawl"] = "full"
    elif crawl_type == "jobs":
        payload.setdefault("instructions", JOBS_INSTRUCTIONS)
    elif crawl_type == "site_info":
        payload.update(site_info=True, crawl=False)
    else:
        raise ValueError("unknown crawl type")
    return payload


def send_crawl(http: Session, url: str, payload: dict, *, validate: bool) -> dict:
    """Retry ambiguous POSTs with the same durable crawler request ID."""
    deadline = monotonic() + 120
    path = "/v1/crawls/validate" if validate else "/v1/crawls"
    while True:
        try:
            response = http.post(
                url.rstrip("/") + path,
                json=payload,
                timeout=(10, 30),
                allow_redirects=False,
            )
        except RequestException:
            if monotonic() >= deadline:
                raise RuntimeError(
                    "Crawler did not acknowledge the request; retry this batch ID to recover safely"
                ) from None
        else:
            if response.status_code in (200, 202):
                result = response.json()
                if result.get("request_id") != payload["request_id"]:
                    raise ValueError("Crawler returned a different request identity")
                return result
            if response.status_code not in (429, 502, 503, 504):
                raise ValueError(
                    f"Crawler rejected {'validation' if validate else 'submission'} with HTTP {response.status_code}"
                )
            if monotonic() >= deadline:
                raise RuntimeError("Crawler capacity unavailable; retry this batch ID")
        sleep(2)


def fetch_crawl(http: Session, url: str, request_id: str) -> dict | None:
    """The crawler's job for a request ID, or None when it has none."""
    response = http.get(
        f"{url.rstrip('/')}/v1/crawls/{request_id}",
        timeout=(10, 30),
        allow_redirects=False,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    job = response.json()
    if job.get("request_id") != request_id:
        raise ValueError("Crawler returned a different request identity")
    return job


def fetch_result(http: Session, url: str, request_id: str) -> dict:
    response = http.get(
        f"{url.rstrip('/')}/v1/crawls/{request_id}/result",
        timeout=(10, 60),
        allow_redirects=False,
    )
    response.raise_for_status()
    return response.json()
