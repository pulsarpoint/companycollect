"""Industry corroboration: versioned registry evidence, never company identity."""

import json
import re
import time
from uuid import uuid4

import httpx

from crawler_service.debug_trace import trace_event

INDUSTRY_CHOICES = {
    "consistent": "At least one reliable registered industry is compatible with the quoted website activities. This supports identity but never proves it.",
    "conflicting": "The quoted website activities clearly contradict all reliable registered operating industries. No plausible holding-company, head-office or group/subsidiary explanation.",
    "insufficient_evidence": "Missing, vague or uncertain activity/industry evidence, uncertain classification version, or only a broad group/holding-company description. Stay neutral.",
}
INDUSTRY_INSTRUCTIONS = (
    "Assess ONLY compatibility between quoted business_activity facts and registered industries, independently of company identity. "
    "Do not use differences in company names, addresses, IDs or ownership as industry conflicts. Different companies can have consistent industries. "
    "Compare business_activity facts with the candidates' registered industries. "
    "Use only reference_consistent codes and their supplied versioned labels; never invent or translate codes across revisions. "
    "One compatible primary or secondary industry is sufficient. Missing codes/activities stay neutral. "
    "Holding companies and head offices may describe their subsidiaries' activities; that alone is not a conflict. "
    "Industry similarity alone never proves ownership, and an industry mismatch never overrides a verified operator organisation number."
)


async def load_company_industries(
    http: httpx.AsyncClient, candidates: list[dict], searches: list[dict]
) -> None:
    ids = [row["company_id"] for row in candidates]
    if not ids:
        return
    if any(re.fullmatch(r"(?:\d{10}|\d{12})", value) is None for value in ids):
        raise ValueError("Invalid company identifier for industry lookup")
    sql = """WITH labels AS (
        SELECT classification_version, normalized_code,
            argMax(description_en, pulled_at) AS reference_label
        FROM corpscout.nace_categories
        GROUP BY classification_version, normalized_code
    )
    SELECT industry.company_id, industry.classification_system,
        industry.classification_code, industry.label_en AS reported_label,
        industry.is_primary, industry.source, industry.source_record_uid,
        multiIf(industry.classification_system IN ('NACE_REV2', 'NACE_REV_2'), 'NACE_REV_2',
            industry.classification_system IN ('NACE_REV2_1', 'NACE_REV_2_1'), 'NACE_REV_2_1', '') AS classification_version,
        ifNull(labels.reference_label, '') AS reference_label
    FROM corpscout.se_company_industry_display_current AS industry
    LEFT JOIN labels ON labels.classification_version = multiIf(
        industry.classification_system IN ('NACE_REV2', 'NACE_REV_2'), 'NACE_REV_2',
        industry.classification_system IN ('NACE_REV2_1', 'NACE_REV_2_1'), 'NACE_REV_2_1', '')
        AND labels.normalized_code = industry.classification_code
    WHERE industry.company_id IN {ids:Array(String)}
    ORDER BY industry.company_id, industry.is_primary DESC,
        industry.classification_system, industry.classification_code, industry.source
    LIMIT 10 BY industry.company_id"""
    query_id = f"company-industry-{uuid4().hex}"
    search = {
        "query_id": query_id,
        "country": "SE",
        "table": "corpscout.se_company_industry_display_current",
        "reference_table": "corpscout.nace_categories",
        "kind": "industry",
        "sql": sql,
        "parameters": {"ids": ids},
        "rows": [],
        "status": "started",
    }
    searches.append(search)
    trace_event(
        "company_search",
        "Load versioned candidate industries",
        details=search,
        operation=query_id,
    )
    started = time.monotonic()
    try:
        response = await http.post(
            "",
            content=sql + " FORMAT JSONEachRow",
            params={
                "param_ids": repr(ids),
                "query_id": query_id,
                "readonly": 2,
                "max_execution_time": 15,
                "max_threads": 2,
                "max_result_rows": 500,
                "result_overflow_mode": "throw",
                "use_top_k_dynamic_filtering": 0,
            },
        )
        search["http_status"] = response.status_code
        response.raise_for_status()
        rows = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        if any(
            not isinstance(row, dict) or row.get("company_id") not in ids
            for row in rows
        ):
            raise ValueError("Invalid company industry response")
        search.update(status="completed", rows=rows, row_count=len(rows))
        for candidate in candidates:
            industries = {}
            for row in rows:
                if row["company_id"] != candidate["company_id"]:
                    continue
                code = dict(row)
                code.pop("company_id")
                code["reference_status"] = (
                    "unknown_version"
                    if not code["classification_version"]
                    else "invalid_code_for_version"
                    if not code["reference_label"]
                    else "label_version_conflict"
                    if code["reported_label"].strip().casefold()
                    != code["reference_label"].strip().casefold()
                    else "reference_consistent"
                )
                industries.setdefault(
                    (code["classification_system"], code["classification_code"]), code
                )
            candidate["industries"] = list(industries.values())
        uncertain = [
            row
            for candidate in candidates
            for row in candidate["industries"]
            if row["reference_status"] != "reference_consistent"
        ]
        if uncertain:
            trace_event(
                "company_industry",
                "Uncertain industry mappings excluded from match validation",
                level="warning",
                details=uncertain,
            )
    except (httpx.HTTPError, ValueError, KeyError) as error:
        search.update(status="failed", error=type(error).__name__)
        raise RuntimeError("Company industry lookup failed") from error
    finally:
        search["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        trace_event(
            "company_search",
            f"Industry lookup {search['status']}",
            details=search,
            operation=query_id,
            duration_ms=search["duration_ms"],
            level="error" if search["status"] == "failed" else "info",
        )


def checked_industry_assessments(
    checks: list[dict], candidates: list[dict], facts: list[dict]
) -> list[dict]:
    """Missing or ambiguous reference evidence cannot manufacture a conflict."""
    by_id = {}
    for check in checks:
        by_id.setdefault(check["company_id"], []).append(check)
    result = []
    for candidate in candidates:
        cid = candidate["company_id"]
        supplied = by_id.get(cid, [])
        check = (
            supplied[0]
            if len(supplied) == 1
            else {
                "status": "insufficient_evidence",
                "reasons": ["No unambiguous model industry assessment was returned."],
            }
        )
        industries = candidate.get("industries", [])
        reliable = [
            row
            for row in industries
            if row["reference_status"] == "reference_consistent"
        ]
        neutral_reason = None
        if not any(fact["kind"] == "business_activity" for fact in facts):
            neutral_reason = "No quoted website business activity was verified."
        elif not reliable:
            neutral_reason = "No registry industry with a consistent classification version and label is available."
        elif check["status"] == "conflicting" and any(
            row["classification_code"] in {"6420", "6421", "6422", "7010"}
            for row in reliable
        ):
            neutral_reason = "Holding-company/head-office classifications can describe a group; this difference is not enough to reject its identity."
        elif check["status"] == "conflicting" and len(reliable) != len(industries):
            neutral_reason = "Some registered industries have uncertain versions; an overall conflict cannot be established."
        result.append(
            {
                "company_id": cid,
                "legal_name": candidate["legal_name"],
                "status": "insufficient_evidence"
                if neutral_reason
                else check["status"],
                "reasons": [neutral_reason] if neutral_reason else check["reasons"],
                "model_status": check["status"],
                "model_reasons": check["reasons"],
                "industries": industries,
            }
        )
    return result
