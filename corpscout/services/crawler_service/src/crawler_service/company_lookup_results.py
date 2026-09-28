"""Append lookup findings and canonical basic results; never accept domain links."""

import hashlib
import json
from datetime import datetime

import httpx
from corpscout_identity.observations import crawl_observations
from corpscout_identity.observations import timestamp as observation_time
from corpscout_identity.urls import website_reference


def timestamp(value: str) -> str:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )


def result_rows(result: dict) -> dict[str, list[dict]]:
    website_reference(result["website_url"], result["website_id"])
    common = {
        key: result[key]
        for key in ("country", "domain", "website_id", "request_id", "attempt")
    }
    finished = timestamp(result["finished_at"])
    common["finished_at"] = finished
    common["ingested_at"] = finished
    usage = result.get("usage") or {}
    assessment = result.get("assessment") or {}
    rows = {
        table: []
        for table in (
            "website_site_info_results",
            "website_full_crawl_results",
            "website_company_lookup_evidence",
            "website_company_lookup_candidates",
            "website_company_lookup_searches",
            "website_company_lookup_results",
        )
    }
    basic = result["site_info_result"]
    crawl = basic["crawl"]
    success = crawl["status"] == "finished"
    rows["website_site_info_results"].append(
        {
            "domain": result["domain"],
            "website_url": result["website_url"],
            "website_id": result["website_id"],
            "request_id": result["request_id"],
            "attempt": result["attempt"],
            "input_revision": result.get("input_revision", 1),
            "work_key": result["work_key"],
            "company_matching_status": result["status"],
            "run_id": result.get("run_id", ""),
            "state": "completed" if success else "failed",
            "crawl_status": crawl["status"],
            "successful": success,
            "started_at": timestamp(crawl["started_at"]),
            "finished_at": timestamp(crawl["finished_at"]),
            "ingested_at": finished,
            "site_info": json.dumps(crawl["site_info"], ensure_ascii=False),
            "pages": json.dumps(crawl["pages"], ensure_ascii=False),
            "page_observations": json.dumps(
                [
                    d["input"]["observations"]
                    for d in basic.get("documents", [])
                    if "observations" in d.get("input", {})
                ]
            ),
            "model_usage": json.dumps(crawl["usage"]),
            "error": "" if success else crawl["stop_reason"],
            "s3_state": "not_configured",
            "s3_path": "",
        }
    )
    if result.get("crawl_type") == "full":
        full = result.get("crawl_result", {"crawl": crawl, "documents": []})
        collected = full["crawl"]
        ok = collected["status"] in {"finished", "skip_crawling"}
        rows["website_full_crawl_results"].append(
            dict(
                rows["website_site_info_results"][0],
                state="completed" if ok else "failed",
                crawl_status=collected["status"],
                successful=ok,
                started_at=timestamp(collected["started_at"]),
                finished_at=timestamp(collected["finished_at"]),
                pages=json.dumps(collected.get("pages", [])),
                page_observations=json.dumps(
                    [
                        d["input"]["observations"]
                        for d in full.get("documents", [])
                        if "observations" in d.get("input", {})
                    ]
                ),
                model_usage=json.dumps(collected.get("usage", {})),
                error="" if ok else collected.get("stop_reason", "crawl_failed"),
            )
        )
    for origin, facts in (
        ("extracted", result.get("identity", [])),
        ("observed_identifier", result.get("registration_evidence", [])),
    ):
        for index, fact in enumerate(facts):
            rows["website_company_lookup_evidence"].append(
                dict(
                    common,
                    origin=origin,
                    row_index=index,
                    kind=fact.get("kind", "registration_number"),
                    value=fact.get("value", ""),
                    normalized_company_id=fact.get("normalized_company_id", ""),
                    source_url=fact["source_url"],
                    quote=fact["quote"],
                )
            )
    rankings = {r["company_id"]: r for r in result.get("candidate_assessments", [])}
    industries = {r["company_id"]: r for r in result.get("industry_assessments", [])}
    for candidate in result.get("candidates", []):
        identifier = candidate["company_id"]
        ranking = rankings.get(
            identifier, assessment if assessment.get("company_id") == identifier else {}
        )
        industry = industries.get(identifier, {})
        row = dict(
            common,
            company_id=identifier,
            selected=bool(
                result.get("found") and result.get("company_id") == identifier
            ),
            name_similarity=candidate.get("name_similarity"),
            confidence=ranking.get("confidence"),
            basis=ranking.get("basis") or "",
            reasons=ranking.get("reasons", []),
            industry_status=industry.get("status", "not_assessed"),
            industry_reasons=industry.get("reasons", []),
            industry_codes=[
                r.get("classification_code", "")
                for r in candidate.get("industries", [])
            ],
            industry_versions=[
                r.get("classification_version", "")
                for r in candidate.get("industries", [])
            ],
            industry_labels=[
                r.get("reported_label", "") for r in candidate.get("industries", [])
            ],
        )
        row.update(
            {
                "industry_reference_statuses": [
                    item.get("reference_status") or ""
                    for item in candidate.get("industries", [])
                ],
                "industry_reference_labels": [
                    item.get("reference_label") or ""
                    for item in candidate.get("industries", [])
                ],
                "industry_sources": [
                    item.get("source") or "" for item in candidate.get("industries", [])
                ],
                "industry_primary": [
                    int(bool(item.get("is_primary")))
                    for item in candidate.get("industries", [])
                ],
            }
        )
        row.update(
            {
                key: candidate.get(key) or ""
                for key in (
                    "legal_name",
                    "status",
                    "primary_street_address",
                    "primary_postal_code",
                    "primary_city",
                    "activity_description",
                )
            }
        )
        rows["website_company_lookup_candidates"].append(row)
    for index, search in enumerate(result.get("searches", [])):
        rows["website_company_lookup_searches"].append(
            dict(
                common,
                row_index=index,
                query_id=search["query_id"],
                kind=search["kind"],
                table_name=search["table"],
                sql=search["sql"],
                parameters={
                    k: json.dumps(v, ensure_ascii=False)
                    for k, v in search["parameters"].items()
                },
                status=search["status"],
                duration_ms=search.get("duration_ms", 0),
                http_status=search.get("http_status"),
                row_count=search.get("row_count", 0),
                returned_company_ids=list(
                    dict.fromkeys(r["company_id"] for r in search.get("rows", []))
                ),
                error=search.get("error", ""),
            )
        )
    rows["website_company_lookup_results"].append(
        dict(
            common,
            batch_id=result.get("batch_id", ""),
            input_id=result.get("input_id", ""),
            run_id=result.get("run_id", ""),
            website_url=result["website_url"],
            status=result["status"],
            existing_company_ids=result.get("existing_company_ids", []),
            found=result.get("found", False),
            company_id=result.get("company_id") or "",
            confidence=result.get("confidence"),
            no_match_probability=result.get("no_match_probability"),
            site_type=result.get("site_type", "unknown"),
            reasons=result.get("reasons", []),
            basis=assessment.get("basis", ""),
            stop_reason=result.get("stop_reason") or "",
            started_at=timestamp(result["started_at"]),
            basic_status=crawl["status"],
            model=result.get("models", {}).get("requested", ""),
            served_models=result.get("models", {}).get("served", []),
            model_calls=usage.get("calls", 0),
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            known_cost_usd=usage.get("known_cost_usd", 0),
            unknown_cost_calls=usage.get("unknown_cost_calls", 0),
            result_path=result["result_path"],
            candidate_count=len(result.get("candidates", [])),
            search_count=len(result.get("searches", [])),
        )
    )
    return rows


async def publish(http: httpx.AsyncClient, results: list[dict]) -> None:
    grouped: dict[str, list[dict]] = {}
    for result in results:
        for table, rows in result_rows(result).items():
            grouped.setdefault(table, []).extend(rows)
    # Verify against the actual publication target as well as the native registry.
    # Misconfigured connections must never publish child rows in another database.
    expected = {}
    for result in results:
        for observation in crawl_observations(
            result,
            requested_url=result["website_url"],
            website_id=result["website_id"],
            discovered_at=observation_time(result["finished_at"]),
        ):
            identity = observation.identity
            expected[identity.page_id] = (
                identity.website_id,
                identity.domain_id,
                identity.page_url,
            )
    ids = list(expected)
    for start in range(0, len(ids), 10000):
        group = ids[start : start + 10000]
        response = await http.post(
            "",
            params={
                "query": """SELECT p.page_id AS page_id, w.website_id AS website_id, d.domain_id AS domain_id, p.page_url AS page_url
                FROM corpscout.pages AS p
                INNER JOIN (SELECT website_id,domain_id FROM corpscout.websites
                    WHERE website_id IN {websites:Array(String)}) AS w ON p.website_id=w.website_id
                INNER JOIN (SELECT domain_id FROM corpscout.domains
                    WHERE domain_id IN {domains:Array(String)}) AS d ON w.domain_id=d.domain_id
                WHERE p.page_id IN {ids:Array(String)} FORMAT JSONEachRow""",
                "param_ids": repr(
                    group
                ),  # ClickHouse Array(String) query-parameter syntax; IDs are hex.
                "param_websites": repr(sorted({expected[key][0] for key in group})),
                "param_domains": repr(sorted({expected[key][1] for key in group})),
            },
        )
        response.raise_for_status()
        rows = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        found = {
            row["page_id"]: (row["website_id"], row["domain_id"], row["page_url"])
            for row in rows
        }
        if len(rows) != len(group) or found != {key: expected[key] for key in group}:
            raise ValueError(
                "Result destination is missing registered website/page parents"
            )
    # Summary is the completion marker: basic data and child findings arrive first.
    for table, rows in grouped.items():
        if not rows:
            continue
        body = "\n".join(
            json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows
        )
        response = await http.post(
            "",
            params={
                "query": f"INSERT INTO corpscout.{table} FORMAT JSONEachRow",
                "async_insert": 0,
                "insert_deduplication_token": hashlib.sha256(body.encode()).hexdigest(),
            },
            content=body.encode(),
        )
        response.raise_for_status()
