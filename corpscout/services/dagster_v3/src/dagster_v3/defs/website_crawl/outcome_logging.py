"""Expose saved crawl failures without logging model payloads or credentials."""

import json
import re
from urllib.parse import urlencode

import dagster as dg


def diagnostic_text(value: str) -> str:
    # Errors can contain request URLs or provider headers. Keep paths, never
    # URL credentials/query strings, bearer tokens, or labelled secrets.
    value = re.sub(r"(https?://)[^/\s@]+@", r"\1[redacted]@", value)
    value = re.sub(r"(https?://[^\s?#]+)[?#][^\s]*", r"\1?[redacted]", value)
    value = re.sub(r"(?i)\bBearer\s+\S+", "Bearer [redacted]", value)
    value = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[redacted]", value)
    value = re.sub(
        r"""(?i)((?:api[_-]?key|password|token|secret)["']?\s*[=:]\s*["']?)[^"'\s,;}]+""",
        r"\1[redacted]",
        value,
    )
    value = " ".join(value.split())
    return value[:2000] + ("… (see debug trace)" if len(value) > 2000 else "")


def crawl_failure_reason(record: dict) -> str:
    reasons = [record["error"]] if record.get("error") else []
    try:
        pages = json.loads(record.get("pages") or "[]")
        info = json.loads(record.get("site_info") or "{}")
    except json.JSONDecodeError:
        return diagnostic_text(
            "; ".join(
                reasons
                + ["Saved page diagnostics are invalid JSON; inspect the debug trace"]
            )
        )
    for page in pages or []:
        errors = list(page.get("errors") or [])
        errors.extend(
            item["error"]
            for item in page.get("navigation_attempts", [])
            if item.get("error")
        )
        if errors:
            reasons.append(
                f"page={page.get('source_url') or page.get('requested_url')} "
                + "; ".join(dict.fromkeys(errors))
            )
    if record.get("crawl_status") == "needs_review":
        if info and info.get("site_description"):
            reasons.append(info["site_description"])
    return diagnostic_text(
        "; ".join(reasons) or "No reason recorded; inspect the debug trace"
    )


def log_crawl_outcomes(
    context: dg.AssetExecutionContext, records: list[dict], crawl_type: str
) -> list[dict]:
    """Called after durable storage. Return failures for batch table metadata."""
    failures = []
    for record in records:
        stages = []
        if not record["successful"]:
            stages.append(
                (
                    crawl_type,
                    record["crawl_status"] or record["state"],
                    crawl_failure_reason(record),
                )
            )
        if record.get("matching_status") in {"failed", "cancelled"}:
            reason = "; ".join(
                [record.get("matching_stop_reason") or record["matching_status"]]
                + list(record.get("matching_reasons") or [])
            )
            stages.append(
                ("company_matching", record["matching_status"], diagnostic_text(reason))
            )
        for stage, status, reason in stages:
            failure = {
                "domain": record["domain"],
                "stage": stage,
                "status": status,
                "reason": reason,
                "request_id": record["request_id"],
                "attempt": record["attempt"],
                "backoffice_trace": "/admin/crawls?"
                + urlencode(
                    {
                        "tab": "attempts",
                        "domain": record["domain"],
                        "debug_request": record["request_id"],
                        "debug_attempt": record["attempt"],
                    }
                ),
            }
            failures.append(failure)
            context.log.warning(
                "Crawl outcome domain=%s stage=%s status=%s reason=%s request=%s attempt=%s debug=%s",
                failure["domain"],
                stage,
                status,
                reason,
                failure["request_id"],
                failure["attempt"],
                failure["backoffice_trace"],
            )
    return failures
