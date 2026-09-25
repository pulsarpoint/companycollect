"""Normalize stored crawl responses, never revisiting websites or calling models."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import UUID

from dagster_v3.defs.website_crawl.normalization.structured import (
    array_value,
    job_row,
    locator,
    object_value,
    project_structured,
    strings,
    text,
    timestamp,
)
from dagster_v3.defs.website_crawl.normalization.tables import (
    COLUMNS,
    PARSER_VERSION,
    new_row,
)

RESULT_SCHEMAS = {
    "company-crawl-result/1.0",
    "company-crawl-result/1.1",
    "company-crawl-result/1.2",
    "company-crawl/1.0",
    "company-crawl-error/1.0",
}
OBSERVATION_SCHEMAS = {"company-page-observations/1.0", "company-page-observations/1.1"}


def decode_archive(value: str) -> dict:
    def invalid_constant(value):
        raise ValueError("Non-finite JSON number")

    return object_value(
        json.loads(value, parse_float=Decimal, parse_constant=invalid_constant),
        "archive",
    )


def host(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Invalid crawl website URL")
    return (
        parsed.hostname.rstrip(".").lower().encode("idna").decode().removeprefix("www.")
    )


def error_messages(values) -> list[str]:
    messages = []
    for item in array_value(values, "errors"):
        if isinstance(item, str):
            messages.append(item)
        else:
            item = object_value(item, "error")
            value = item.get("error") or item.get("message") or item.get("reason")
            if value:
                messages.append(text(value))
    return messages


def evidence_text(sources) -> list[str]:
    result = []
    for source in array_value(sources, "evidence sources"):
        for fragment in array_value(
            object_value(source, "evidence source").get("evidence"), "evidence"
        ):
            fragment = object_value(fragment, "evidence fragment")
            if fragment.get("matched_in") != "not_found" and fragment.get("text"):
                result.append(text(fragment["text"]))
    return list(dict.fromkeys(result))


def profile_rows(crawl: dict, common: dict, rows: dict, pages: list[dict]) -> None:
    gate = object_value(crawl.get("site_gate"), "site gate")
    finding = object_value(gate.get("profile"), "site classification finding")
    data = object_value(finding.get("data"), "classification data")
    info = object_value(crawl.get("site_info"), "site information")
    if not info and not data:
        return
    profile = {**data, **info}
    sources = array_value(finding.get("sources"), "profile sources")
    first_source = object_value(sources[0], "profile source") if sources else {}
    first_page = pages[0] if pages else {}
    origin = dict(
        common,
        page_id=first_source.get("page_id") or first_page.get("page_id") or "p0001",
        source_url=info.get("source_url")
        or first_source.get("url")
        or first_page.get("source_url")
        or crawl.get("site_url")
        or "",
        source_locator="/crawl/site_info" if info else "/crawl/site_gate/profile/data",
        row_index=0,
    )
    evidence = (
        strings(info.get("evidence")) if "evidence" in info else evidence_text(sources)
    )
    scope = info.get("scope") or gate.get("scope") or "first_page_only"
    full_crawl_all = crawl.get("full_crawl_all")
    overridden = gate.get("overridden")
    if any(
        value is not None and not isinstance(value, bool)
        for value in (full_crawl_all, overridden)
    ):
        raise ValueError("Crawl overrides must be boolean")
    rows["site_profiles"].append(
        new_row(
            "site_profiles",
            **origin,
            evidence_status=info.get("evidence_status")
            or finding.get("evidence_status")
            or "unknown",
            crawl_decision=profile.get("crawl_decision")
            or gate.get("decision")
            or "needs_review",
            scope=scope,
            site_types=strings(profile.get("site_types")),
            research_profiles=strings(profile.get("research_profiles")),
            purpose_original=text(profile.get("purpose")),
            operator_name=text(profile.get("operator_name")),
            description_original=text(profile.get("site_description")),
            evidence=evidence,
            full_crawl_all=full_crawl_all,
            classification_overridden=overridden,
        )
    )
    for index, activity in enumerate(strings(profile.get("business_activities"))):
        rows["business_activities"].append(
            new_row(
                "business_activities",
                **dict(
                    origin,
                    row_index=index,
                    source_locator=origin["source_locator"]
                    + f"/business_activities/{index}",
                ),
                activity_original=activity,
                scope=scope,
                classification_evidence=evidence,
            )
        )


def page_rows(
    page: dict, observation: dict, common: dict, rows: dict, page_index: int
) -> str:
    page_id = text(page.get("page_id"))
    if not page_id:
        raise ValueError("Crawl page lacks page_id")
    if observation and observation.get("page_id") != page_id:
        raise ValueError("Observation page identity mismatch")
    if observation and observation.get("schema_version") not in OBSERVATION_SCHEMAS:
        raise ValueError("Unsupported page observation schema")
    metadata = object_value(observation.get("metadata"), "page metadata")
    meta = object_value(metadata.get("meta"), "meta tags")
    origin = dict(
        common,
        page_id=page_id,
        source_url=page.get("source_url")
        or observation.get("source_url")
        or page.get("requested_url")
        or "",
        source_locator=f"/crawl/pages/{page_index}",
        row_index=0,
    )
    if (
        observation.get("source_url")
        and page.get("source_url")
        and observation["source_url"] != page["source_url"]
    ):
        raise ValueError("Observation source URL mismatch")
    rows["pages"].append(
        new_row(
            "pages",
            **origin,
            requested_url=page.get("requested_url") or "",
            final_url=text(page.get("source_url")),
            canonical_url=text(metadata.get("canonical_url")),
            http_status=page.get("status_code"),
            fetch_status=page.get("fetch_status") or "unknown",
            observation_status=text(observation.get("status")),
            observation_schema=text(observation.get("schema_version")),
            fetched_at=timestamp(page.get("fetched_at")),
            title_original=text(metadata.get("title")),
            description_original=text(meta.get("description")),
            language=text(metadata.get("language")),
            errors=error_messages(page.get("errors"))
            + error_messages(observation.get("errors")),
        )
    )
    for section, kind_column in (
        ("contacts", "contact_type"),
        ("identifiers", "identifier_type"),
    ):
        for index, item in enumerate(array_value(observation.get(section), section)):
            item = object_value(item, section + " item")
            if not item.get("value") or not item.get("type"):
                raise ValueError(f"Incomplete {section} observation")
            source_locator = object_value(item.get("locator"), "source locator")
            rows[section].append(
                new_row(
                    section,
                    **dict(
                        origin, row_index=index, source_locator=locator(source_locator)
                    ),
                    **{kind_column: text(item["type"])},
                    value=text(item["value"]),
                    raw_value=text(item.get("raw_value")),
                    extraction_source=text(item.get("source")) or "unknown",
                    entity_reference=locator(
                        {
                            k: source_locator[k]
                            for k in ("script_index", "entity_path", "entity_index")
                            if k in source_locator
                        }
                    )
                    if any(k in source_locator for k in ("entity_path", "entity_index"))
                    else None,
                )
            )
    links = [
        (section, item, index)
        for section in ("document_links", "financial_links")
        for index, item in enumerate(array_value(observation.get(section), section))
    ]
    links += [
        (section, item, index)
        for section in ("links", "alternate_languages")
        for index, item in enumerate(array_value(metadata.get(section), section))
    ]
    if metadata.get("canonical_url"):
        links.append(
            ("canonical", {"url": metadata["canonical_url"], "rel": ["canonical"]}, 0)
        )
    for index, (section, item, source_index) in enumerate(links):
        item = object_value(item, "link")
        if not item.get("url"):
            raise ValueError("Observed link has no target URL")
        link_locator = (
            "/metadata/canonical_url"
            if section == "canonical"
            else (
                f"/metadata/{section}/{source_index}"
                if section in {"links", "alternate_languages"}
                else f"/{section}/{source_index}"
            )
        )
        rows["links"].append(
            new_row(
                "links",
                **dict(origin, row_index=index, source_locator=link_locator),
                target_url=text(item["url"]),
                label_original=text(item.get("text") or item.get("label")),
                category={
                    "document_links": "document",
                    "financial_links": "financial",
                    "links": "metadata",
                    "alternate_languages": "alternate_language",
                    "canonical": "canonical",
                }[section],
                document_type=text(item.get("document_type") or item.get("type")),
                language=text(item.get("hreflang")),
                relationships=strings(item.get("rel")),
            )
        )
    return project_structured(observation, origin, rows)


def parse_attempt(
    source: dict,
    payload: dict | None,
    normalization_id: UUID,
    revision: int,
    run_id: str,
) -> dict[str, list[dict]]:
    common = {
        key: source[key] for key in ("domain", "crawl_type", "request_id", "attempt")
    }
    common["normalization_id"] = normalization_id
    rows = {table: [] for table in COLUMNS}
    if host(source["website_url"]) != source["domain"]:
        raise ValueError("Catalog website/domain mismatch")
    if payload is None:
        crawl = {
            key: json.loads(source.get(key) or "null") for key in ("site_info", "pages")
        }
        observations = json.loads(source.get("page_observations") or "[]")
        documents = [
            {"page_id": item.get("page_id"), "input": {"observations": item}}
            for item in array_value(observations, "catalog observations")
        ]
        schema = "catalog"
        payload = {}
    else:
        schema = payload.get("schema_version")
        if schema not in RESULT_SCHEMAS:
            raise ValueError("Unsupported crawl archive schema")
        crawl = object_value(payload.get("crawl", payload), "crawl")
        if (
            "schema_version" in crawl
            and crawl is not payload
            and crawl["schema_version"] != "company-crawl/1.0"
        ):
            raise ValueError("Unsupported crawl schema")
        for key in ("request_id", "attempt"):
            if key in payload and payload[key] != source[key]:
                raise ValueError("Archive request/attempt mismatch")
        input_url = crawl.get("input_url") or payload.get("target_url")
        if input_url and host(input_url) != host(source["website_url"]):
            raise ValueError("Archive website identity mismatch")
        documents = array_value(payload.get("documents"), "documents")
    pages = [
        object_value(page, "page") for page in array_value(crawl.get("pages"), "pages")
    ]
    by_page = {}
    for document in documents:
        document = object_value(document, "document")
        item = object_value(
            object_value(document.get("input"), "document input").get("observations"),
            "observations",
        )
        page_id = document.get("page_id") or item.get("page_id")
        if page_id in by_page:
            raise ValueError("Duplicate document page identity")
        if item:
            by_page[page_id] = item
    page_ids = [page.get("page_id") for page in pages]
    if len(page_ids) != len(set(page_ids)):
        raise ValueError("Duplicate page identity")
    if set(by_page) - set(page_ids):
        raise ValueError("Observation has no matching crawl page")
    profile_rows(crawl, common, rows, pages)
    coverage = [
        page_rows(page, by_page.get(page.get("page_id"), {}), common, rows, index)
        for index, page in enumerate(pages)
    ]
    for index, finding in enumerate(
        array_value(
            object_value(payload.get("records"), "records").get("jobs"), "legacy jobs"
        )
    ):
        finding = object_value(finding, "job finding")
        if finding.get("evidence_status") != "source_matched":
            continue
        for origin_index, origin in enumerate(
            array_value(finding.get("sources"), "job sources")
        ):
            origin = object_value(origin, "job source")
            if origin.get("evidence_status") == "needs_review":
                continue
            if origin.get("page_id") not in page_ids:
                raise ValueError("Legacy job source has no crawl page")
            rows["jobs"].append(
                job_row(
                    object_value(finding.get("data"), "job data"),
                    dict(
                        common,
                        page_id=origin["page_id"],
                        row_index=len(rows["jobs"]),
                        source_url=text(origin.get("url")) or "",
                        source_locator=f"/records/jobs/{index}/sources/{origin_index}",
                    ),
                    legacy=True,
                    evidence=evidence_text([origin]),
                )
            )
    errors = error_messages(crawl.get("errors"))
    if isinstance(crawl.get("error"), str) and crawl["error"]:
        errors.append(crawl["error"])
    usage = object_value(crawl.get("usage"), "model usage")
    for call in array_value(usage.get("by_call"), "model calls"):
        call = object_value(call, "model call")
        provider = object_value(call.get("provider_error"), "provider error")
        message = provider.get("message") or call.get("error")
        if isinstance(message, str) and message:
            errors.append(message)
    reason = (
        source.get("error")
        or ("; ".join(dict.fromkeys(errors)) if not source["successful"] else None)
        or None
    )
    jobs_status = (
        "not_available"
        if not coverage or all(c == "not_available" for c in coverage)
        else "completed"
        if all(c == "completed" for c in coverage)
        else "partial"
    )
    rows["scans"].append(
        new_row(
            "scans",
            **common,
            normalization_revision=revision,
            parser_version=PARSER_VERSION,
            source_schema=schema,
            source_ingested_at=timestamp(source["ingested_at"]),
            normalized_at=datetime.now(UTC),
            source_run_id=source["run_id"],
            normalization_run_id=run_id,
            input_revision=source["input_revision"],
            work_key=source["work_key"],
            website_url=source["website_url"],
            final_url=text(crawl.get("site_url")),
            state=source["state"],
            crawl_status=source["crawl_status"],
            successful=bool(source["successful"]),
            started_at=timestamp(source.get("started_at")),
            finished_at=timestamp(source["finished_at"]),
            stop_reason=text(
                crawl.get("stop_reason")
                or object_value(crawl.get("site_gate"), "site gate").get("reason")
            ),
            error=reason,
            archive_path=source["s3_path"],
            page_count=len(rows["pages"]),
            row_counts={k: len(v) for k, v in rows.items() if k != "scans"},
            structured_jobs_status=jobs_status,
        )
    )
    return rows
