"""Bounded, read-only Swedish registry searches for website lookup tests."""

import json
import re
import time
from uuid import uuid4

import httpx

from crawler_service.debug_trace import trace_event

COMPANY_TABLE = "corpscout.se_companies_serving"
COMPANY_COLUMNS = """company_id, legal_name, status, primary_street_address,
    primary_postal_code, primary_city, substringUTF8(activity_description, 1, 2000) AS activity_description"""


def normalized_name(value: str) -> str:
    # Registry legal forms can be joined to Investment (e.g. Latour).
    value = re.sub(r"\binvestmentaktiebolag(?:et)?\b", "investment ab", value.lower())
    words = re.sub(r"[^\w]+", " ", value, flags=re.UNICODE).split()
    return "".join(
        word
        for word in words
        if word not in {"ab", "aktiebolag", "aktiebolaget", "publ"}
    )


def swedish_company_id(value: str) -> str | None:
    """Resolve an explicit Swedish VAT number without shortening sole-trader IDs."""
    compact = re.sub(r"[\s\u2010-\u2015\u2212-]", "", value).upper()
    vat = re.fullmatch(r"SE([0-9]{10})01", compact)
    if vat is not None:
        return vat.group(1)
    if re.fullmatch(r"(?:[0-9]{10}|[0-9]{12})", compact):
        return compact
    return None


async def search_companies(
    http: httpx.AsyncClient, *, kind: str, value: str, searches: list[dict]
) -> list[dict]:
    query_id = f"company-lookup-{uuid4().hex}"
    table = COMPANY_TABLE
    if kind == "existing_mapping":
        table = "corpscout.se_company_domain_resolved"
        needle = value.lower().removeprefix("www.").rstrip(".")
        sql = f"SELECT DISTINCT company_id FROM {table} WHERE country_code = 'SE' AND root_domain = {{value:String}} AND is_active = 1 ORDER BY company_id LIMIT 10"
    elif kind == "registration_number":
        sql = f"SELECT {COMPANY_COLUMNS}, 1.0 AS name_similarity FROM {COMPANY_TABLE} WHERE company_id = {{value:String}} ORDER BY company_id LIMIT 10"
        needle = swedish_company_id(value)
        if needle is None:
            raise ValueError("Unsupported Swedish company identifier")
    elif kind == "name":
        needle = normalized_name(value)
        if len(needle) < 3:
            return []
        # Name similarity retrieves candidates; it is never a match confidence.
        sql = rf"""WITH replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(lowerUTF8(legal_name), '\\binvestmentaktiebolag(?:et)?\\b', 'investment ab'),
                '\\b(ab|aktiebolag|aktiebolaget|publ)\\b', ''),
            '[^\\p{{L}}\\p{{N}}]', '') AS normalized,
            1 - editDistanceUTF8(substringUTF8(normalized, 1, 250), {{value:String}})
                / greatest(lengthUTF8(normalized), lengthUTF8({{value:String}}), 1) AS name_similarity
        SELECT {COMPANY_COLUMNS}, name_similarity
        FROM {COMPANY_TABLE}
        WHERE lengthUTF8(normalized) BETWEEN greatest(3, lengthUTF8({{value:String}}) / 2)
            AND lengthUTF8({{value:String}}) * 2
          AND (positionUTF8(normalized, {{value:String}}) > 0
            OR ngramDistanceCaseInsensitiveUTF8(substringUTF8(normalized, 1, 250), {{value:String}}) < 0.6)
        ORDER BY normalized = {{value:String}} DESC, name_similarity DESC, status = 'active' DESC, company_id LIMIT 10"""
    else:
        raise ValueError("Unsupported company search kind")
    parameters = {"value": needle}
    search = {
        "query_id": query_id,
        "country": "SE",
        "table": table,
        "kind": kind,
        "value": value,
        "sql": sql,
        "parameters": parameters,
        "rows": [],
        "status": "started",
    }
    searches.append(search)
    trace_event(
        "company_search",
        f"Search Swedish companies by {kind}: {value}",
        details=search,
        operation=query_id,
    )
    started = time.monotonic()
    try:
        response = await http.post(
            "",
            content=sql + " FORMAT JSONEachRow",
            params={
                "param_value": needle,
                "query_id": query_id,
                "readonly": 2,
                "max_execution_time": 15,
                "max_threads": 2,
                "max_result_rows": 10,
                "result_overflow_mode": "throw",
                "use_top_k_dynamic_filtering": 0,
            },
        )
        search["http_status"] = response.status_code
        response.raise_for_status()
        rows = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        if any(
            not isinstance(row, dict)
            or not isinstance(row.get("company_id"), str)
            or not re.fullmatch(r"(?:\d{10}|\d{12})", row["company_id"])
            for row in rows
        ):
            raise ValueError("Invalid company registry response")
        search.update(status="completed", rows=rows, row_count=len(rows))
        return rows
    except (httpx.HTTPError, ValueError) as error:
        search.update(status="failed", error=type(error).__name__)
        raise RuntimeError(
            f"Company database search failed ({type(error).__name__})"
        ) from error
    finally:
        search["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        trace_event(
            "company_search",
            f"Company search {search['status']} · {len(search['rows'])} candidates",
            level="error" if search["status"] == "failed" else "info",
            duration_ms=search["duration_ms"],
            details=search,
            operation=query_id,
        )
