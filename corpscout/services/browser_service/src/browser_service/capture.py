"""Generic browser capture and safe response metadata."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(kw_only=True)
class PageCapture:
    url: str
    html: str
    status_code: int | None
    headers: dict[str, str]
    error: str | None


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def access_problem(
    status: int | None, html: str, error: str = ""
) -> Literal["blocked", "captcha"] | None:
    content = html.casefold()
    if (
        any(
            marker in content
            for marker in (
                "<title>just a moment",
                "cf-chl-widget",
                "cf-chl-opt",
                "_cf_chl_opt",
                "<title>attention required",
                "verify you are human",
            )
        )
        or "cloudflare js challenge" in error.casefold()
    ):
        return "captcha"
    if status in {401, 403, 429} or "blocked by anti-bot" in error.casefold():
        return "blocked"
    return None


RESPONSE_HEADERS = set(
    "content-type content-language server x-powered-by x-generator via "
    "strict-transport-security content-security-policy content-security-policy-report-only "
    "x-frame-options x-content-type-options referrer-policy permissions-policy "
    "cross-origin-opener-policy cross-origin-embedder-policy cross-origin-resource-policy "
    "cache-control expires last-modified etag x-robots-tag link".split()
)


def public_response_headers(headers: dict | None) -> dict[str, str]:
    """Keep content, technology and security signals without cookies/session headers."""
    return {
        name.lower(): ", ".join(map(str, value))
        if isinstance(value, list)
        else str(value)
        for name, value in (headers or {}).items()
        if name.lower() in RESPONSE_HEADERS
    }
