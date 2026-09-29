"""Safe database failure details for publication receipts and service logs."""

import errno
import re
import sqlite3
from http import HTTPStatus
from urllib.parse import urlsplit

import httpx
import psycopg2
from clickhouse_driver.errors import Error as ClickHouseError
from clickhouse_driver.errors import ErrorCodes, ServerException
from psycopg2 import errorcodes

SAFE_REASONS = {
    "ClickHouse parent verification failed",
    "Result destination is missing registered website/page parents",
    "Expected a native ClickHouse connection URL",
    "Expected a native ClickHouse result connection URL",
    "Set CLICKHOUSE_NATIVE_URL or CLICKHOUSE_MIGRATE_URL",
    "Configure CLICKHOUSE_NATIVE_URL and PROCESSING_PG_URL for central identity registration",
    "PROCESSING_PG_URL is required for inventory publication",
    "INVENTORY_LOCK_TIMEOUT_MS must be between 1 and 300000",
    "Identity registration batch exceeds 10000 records",
    "Identity registration requires a source and run ID",
    "Website identity or domain reference does not match its URL",
    "Crawl result requires a registered website reference",
    "Crawl result requires a completion timestamp",
}


def safe_endpoint(value: str) -> str:
    """Keep only the connection address; paths may carry credentials too."""
    try:
        parsed = urlsplit(value)
        if not parsed.scheme or parsed.hostname is None:
            return "<invalid endpoint>"
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        port = f":{parsed.port}" if parsed.port is not None else ""
        return f"{parsed.scheme}://{host}{port}"
    except ValueError:
        return "<invalid endpoint>"


def error_details(error: Exception, *, endpoint: str, operation: str) -> str:
    """Describe known errors without copying SQL, response bodies, or credentials."""
    causes = []
    seen = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen and len(causes) < 5:
        seen.add(id(current))
        detail = type(current).__name__
        if isinstance(current, ClickHouseError):
            name = next(
                (name for name, code in vars(ErrorCodes).items() if name.isupper() and code == current.code),
                # These current server codes are absent from clickhouse-driver 0.2.x.
                {497: "ACCESS_DENIED", 516: "AUTHENTICATION_FAILED"}.get(
                    current.code, "UNKNOWN_CLICKHOUSE_ERROR"
                ),
            )
            detail += f" code={current.code} reason={name}"
        elif isinstance(current, httpx.HTTPStatusError):
            status = current.response.status_code
            reason = HTTPStatus(status).phrase if status in HTTPStatus else "HTTP error"
            detail += f" status={status} reason={reason!r}"
            endpoint = str(current.request.url)
        elif isinstance(current, psycopg2.Error):
            reason = (
                errorcodes.lookup(current.pgcode)
                if current.pgcode is not None and current.pgcode in vars(errorcodes).values()
                else "PostgreSQL connection or execution failed"
            )
            detail += f" code={current.pgcode} reason={reason!r}"
        elif isinstance(current, sqlite3.Error):
            code = getattr(current, "sqlite_errorcode", None)
            reason = getattr(current, "sqlite_errorname", None) or {
                "database is locked": "SQLITE_BUSY",
                "database table is locked": "SQLITE_LOCKED",
                "database or disk is full": "SQLITE_FULL",
            }.get(str(current), "SQLITE_ERROR")
            detail += f" code={code} reason={reason}"
        elif isinstance(current, OSError):
            detail += f" errno={current.errno} reason={errno.errorcode.get(current.errno, 'OS_ERROR')}"
        elif isinstance(current, httpx.TimeoutException):
            detail += " reason='HTTP request timed out'"
        else:
            reason = str(current)
            if isinstance(current, httpx.InvalidURL):
                reason = reason.partition(":")[0]
                if not re.fullmatch(
                    r"URL too long|URL component '(?:query|host|port|path|userinfo|fragment|scheme|authority)' too long"
                    r"|Invalid (?:port|IPv4 address|IPv6 address|IDNA hostname)",
                    reason,
                ):
                    reason = "Invalid URL"
            elif reason not in SAFE_REASONS and not re.fullmatch(
                r"ClickHouse insert failed for corpscout\.website_(?:site_info_results|full_crawl_results|company_lookup_(?:evidence|candidates|searches|results))"
                r"|(?:Duplicate|Mismatched) central (?:domains|websites|pages) identity; repair inventory before publishing"
                r"|Central (?:domains|websites|pages) registration is incomplete; retain results and retry",
                reason,
            ):
                reason = "unclassified error"
            detail += f" reason={reason!r}"
        causes.append(detail)
        current = (
            current.__cause__
            or (current.nested if isinstance(current, ServerException) else None)
            or (None if current.__suppress_context__ else current.__context__)
        )
    return f"operation={operation!r}; endpoint={safe_endpoint(endpoint)!r}; " + " <- ".join(causes)
