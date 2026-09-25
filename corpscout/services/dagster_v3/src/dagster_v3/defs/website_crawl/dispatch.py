"""Validated request payloads and idempotent submission to the crawler HTTP API."""

from datetime import UTC, datetime
from dagster_v3.defs.common.llm_control import check_admission, current_request_id, invalidate_revision

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


def reject_crawl_credentials(value: object) -> None:
    """Credentials must use the encrypted LLM envelope, never research overrides."""
    if isinstance(value, dict):
        for name, child in value.items():
            normalized = str(name).lower().replace("_", "").replace("-", "")
            if normalized in {
                "apikey",
                "apikeyencrypted",
                "authorization",
                "password",
                "secret",
                "token",
                "accesstoken",
                "bearertoken",
                "credentials",
                "llm",
            } or normalized.endswith("apikey"):
                raise ValueError(
                    "crawl overrides cannot contain credentials; use llm.api_key_encrypted"
                )
            reject_crawl_credentials(child)
    elif isinstance(value, list):
        for child in value:
            reject_crawl_credentials(child)


def crawl_payload(row: dict, crawl_type: str, batch_id: str) -> dict:
    """Refuse proposed input features the current crawler cannot yet represent."""
    if row["preset_version"] != 1:
        raise ValueError("unsupported crawl preset version")
    if row["proxy_route"] != "direct":
        raise ValueError("the crawler HTTP API does not yet accept a proxy route")
    overrides = json.loads(row["config_json"])
    if not isinstance(overrides, dict):
        raise ValueError("config_json must be an object")
    reject_crawl_credentials(overrides)
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


class CrawlRequestConflict(ValueError):
    """The crawler already holds this request ID with a different payload (HTTP 409)."""


# Statuses worth retrying with the same request: capacity and gateway failures.
TRANSIENT_STATUSES = (429, 500, 502, 503, 504)
RETRY_SECONDS = 120


def verify_crawl_llm(http: Session, url: str, llm: dict) -> None:
    """Check the frozen profile immediately before admitting any domain work."""
    check_admission(llm)
    started_at = datetime.now(UTC)
    try:
        response = http.post(
            url.rstrip("/") + "/v1/llm/verify",
            json={"llm": llm},
            timeout=(10, 40),
            allow_redirects=False,
        )
    except RequestException:
        raise ValueError(
            "The crawler could not verify the selected LLM; no new domains were submitted. Retry after checking the crawler connection."
        ) from None
    if response.status_code != 200:
        raise ValueError(
            f"The crawler rejected LLM verification with HTTP {response.status_code}; no new domains were submitted. Check the shared key and selected LLM configuration."
        )
    try:
        result = response.json()
    except ValueError:
        raise ValueError(
            "The crawler returned an invalid LLM verification response; no new domains were submitted."
        ) from None
    if not isinstance(result, dict):
        raise ValueError(
            "The crawler returned an invalid LLM verification response; no new domains were submitted."
        )
    invalidate_revision(llm, "crawler", started_at, result)
    if result.get("ok") is not True:
        reason = result.get("error")
        safe_reason = (
            reason[:1000]
            if isinstance(reason, str)
            else "The model did not pass verification."
        )
        raise ValueError(
            f"Selected LLM verification failed: {safe_reason} No new domains were submitted."
        )


def send_crawl(http: Session, url: str, payload: dict, *, validate: bool) -> dict:
    """Retry ambiguous POSTs with the same durable crawler request ID."""
    llm = payload.get("llm") or {}
    owner = current_request_id() if llm.get("profile_id") and not validate else None
    deadline = monotonic() + RETRY_SECONDS
    path = "/v1/crawls/validate" if validate else "/v1/crawls"
    while True:
        check_admission(llm, owner, service="crawler" if not validate else None,
                        external_request_id=payload["request_id"])
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
            if response.status_code == 409 and not validate:
                raise CrawlRequestConflict("Crawler rejected submission with HTTP 409")
            if response.status_code not in (429, 502, 503, 504):
                raise ValueError(
                    f"Crawler rejected {'validation' if validate else 'submission'} with HTTP {response.status_code}"
                )
            if monotonic() >= deadline:
                raise RuntimeError("Crawler capacity unavailable; retry this batch ID")
        sleep(2)


def _get(http: Session, url: str, *, read_timeout: int):
    """GET with the same bounded retry as send_crawl for transport and 5xx failures."""
    deadline = monotonic() + RETRY_SECONDS
    while True:
        try:
            response = http.get(url, timeout=(10, read_timeout), allow_redirects=False)
        except RequestException:
            if monotonic() >= deadline:
                raise RuntimeError(
                    "Crawler did not answer; resume this task to recover safely"
                ) from None
        else:
            if response.status_code not in TRANSIENT_STATUSES:
                return response
            if monotonic() >= deadline:
                response.raise_for_status()
        sleep(2)


def fetch_crawl(http: Session, url: str, request_id: str) -> dict | None:
    """The crawler's job for a request ID, or None when it has none."""
    response = _get(http, f"{url.rstrip('/')}/v1/crawls/{request_id}", read_timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    job = response.json()
    if job.get("request_id") != request_id:
        raise ValueError("Crawler returned a different request identity")
    return job


def fetch_result(http: Session, url: str, request_id: str) -> dict | None:
    """The stored crawl result, or None while the crawler says it is not ready (409)."""
    response = _get(
        http, f"{url.rstrip('/')}/v1/crawls/{request_id}/result", read_timeout=60
    )
    if response.status_code == 409:
        return None
    response.raise_for_status()
    return response.json()
