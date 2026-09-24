"""Company technology observations grouped without turning requirements into usage."""

import json

from crawler_service.content import normalize, proposal_metadata_hash
from crawler_service.discovery import normalize_url
from crawler_service.models import (
    EntitySummaries,
    EntitySummary,
    Finding,
    Findings,
    Page,
    TechnologySummary,
    validate_specific_technology_name,
)
from crawler_service.storage import content_hash


def source_supported_finding(finding: Finding) -> bool:
    """Check evidence and meaning independently of catalog metadata."""
    required = finding.data.get("required_reviews", [])
    if "technology" in finding.data and "source_meaning" in required:
        observed = (finding.data.get("interpretation_review") or {}).get(
            "source_interpretation"
        ) or {}
        company = finding.data.get("company")
        if (
            normalize(observed.get("source_subject") or "") != normalize(company or "")
            or observed.get("source_subject_kind")
            != ("company" if company else "unknown")
            or observed.get("source_signal") != finding.data.get("signal")
            or observed.get("source_scope") != finding.data.get("scope")
            or finding.data.get("job_employer") is not None
            and normalize(observed.get("source_subject") or "")
            != normalize(finding.data["job_employer"])
        ):
            return False
    return (
        finding.evidence_status == "source_matched"
        and (
            "source_meaning" not in required
            or (
                finding.data.get("interpretation_review", {}).get("supported") is True
                and finding.data.get("interpretation_review", {}).get("status")
                == "accepted"
            )
        )
        and finding.data.get("interpretation_review", {}).get("supported") is not False
        and finding.data.get("interpretation_review", {}).get("status", "accepted")
        == "accepted"
        and any(s.evidence_status == "source_matched" for s in finding.sources)
    )


def accepted_finding(finding: Finding) -> bool:
    if "technology" in finding.data:
        name = finding.data["technology"]
        if not isinstance(name, str):
            return False
        try:
            validate_specific_technology_name(name)
        except ValueError:
            return False
    if (
        "standard_name" in finding.data
        and finding.data.get("document_type") is not None
        and not finding.data.get("document_url")
    ):
        return False
    review = finding.data.get("proposal_review", {})
    match = finding.data.get("catalog_match") or {}
    proposal = match.get("proposed_technology")
    required = finding.data.get("required_reviews", [])
    return (
        source_supported_finding(finding)
        and finding.data.get("catalog_error") is None
        and ("proposal_metadata" not in required or review.get("status") == "accepted")
        and review.get("status", "accepted") == "accepted"
        and (
            review.get("metadata_sha256") is None
            or proposal is not None
            and review["metadata_sha256"] == proposal_metadata_hash(proposal)
        )
    )


def technology_submission_records(findings: list[Finding]) -> list[dict]:
    """Export accepted claims and verified sources; keep rejected material in the run."""
    return [
        finding.model_dump()
        | {
            "sources": [
                s.model_dump()
                for s in finding.sources
                if s.evidence_status == "source_matched"
            ]
        }
        for finding in findings
        if accepted_finding(finding)
        and finding.data.get("company") is not None
        and (finding.data.get("catalog_match") or {}).get("status")
        in {"matched", "proposed"}
    ]


def summarize_entities(records: Findings, pages: list[Page]) -> EntitySummaries:
    """Conservative identities with all field variants and original record references.

    People require the same normalized name and company. Conflicting profile URLs
    stay separate. Jobs join through their published URL, actual redirects, or a
    linked detail page that yields a single opening. No fuzzy title matching.
    Technology identities group signals without asserting those signals are usage.
    """
    people = [r for r in records.people if accepted_finding(r)]
    jobs = [r for r in records.jobs if accepted_finding(r)]
    profiles: dict[tuple, set[str]] = {}
    for person in people:
        key = (
            normalize(person.data["name"]),
            normalize(person.data.get("company") or ""),
        )
        if person.data.get("profile_url"):
            profiles.setdefault(key, set()).add(
                normalize_url(person.data["profile_url"])
            )

    job_urls = {normalize_url(r.data["job_url"]) for r in jobs if r.data.get("job_url")}
    aliases = {url: url for url in job_urls}

    def resolve(url: str) -> str:
        url = normalize_url(url)
        while url in aliases and aliases[url] != url:
            url = aliases[url]
        return url

    def join(left: str, right: str) -> None:
        left, right = resolve(left), resolve(right)
        if left != right:
            aliases[max(left, right)] = min(left, right)

    for page in pages:
        if page.fetch_status in {"fetched", "duplicate"}:
            join(page.requested_url, page.source_url)
    source_jobs: dict[str, list[Finding]] = {}
    for job in jobs:
        for source in job.sources:
            if source.evidence_status == "source_matched":
                source_jobs.setdefault(normalize_url(source.url), []).append(job)
    for url, findings in source_jobs.items():
        targets = {
            resolve(r.data["job_url"]) for r in findings if r.data.get("job_url")
        }
        employers = {normalize(r.data.get("employer") or "") for r in findings}
        if (
            url in job_urls
            and len(targets) == 1
            and len(employers) == 1
            and "" not in employers
        ):
            join(url, next(iter(targets)))

    output = EntitySummaries()
    for objective, findings, fields in (
        ("people", people, ("name", "company", "role", "profile_url", "as_of")),
        (
            "jobs",
            jobs,
            (
                "employer",
                "title",
                "location",
                "department",
                "employment_type",
                "workplace_type",
                "job_url",
            ),
        ),
        (
            "technologies",
            records.technology_signals,
            (
                "company",
                "technology",
                "category",
                "signal",
                "scope",
                "as_of",
                "alternative_group",
                "job_title",
                "job_url",
            ),
        ),
    ):
        groups: dict[str, EntitySummary] = {}
        for finding in findings:
            if not accepted_finding(finding):
                continue
            data = finding.data
            if objective == "people":
                identity = {
                    "name": normalize(data["name"]),
                    "company": normalize(data.get("company") or ""),
                }
                key = (identity["name"], identity["company"])
                if len(profiles.get(key, set())) > 1:
                    identity["profile_url"] = (
                        normalize_url(data["profile_url"])
                        if data.get("profile_url")
                        else finding.record_id
                    )
                if not identity["company"]:
                    identity["unresolved_record_id"] = finding.record_id
            elif objective == "jobs":
                identity = {"employer": normalize(data.get("employer") or "")}
                if data.get("job_url") and identity["employer"]:
                    identity["job_url"] = resolve(data["job_url"])
                else:
                    identity["unresolved_record_id"] = finding.record_id
            else:
                match = data.get("catalog_match") or {}
                technology_id = match.get("canonical_technology") or match.get(
                    "proposal_id"
                )
                if not data.get("company") or not technology_id:
                    continue
                identity = {
                    "company": normalize(data["company"]),
                    "technology": technology_id,
                    "catalog_status": match["status"],
                }
            entity_id = content_hash(json.dumps([objective, identity], sort_keys=True))
            if entity_id not in groups:
                groups[entity_id] = EntitySummary(
                    entity_id=entity_id,
                    identity=identity,
                    field_values={},
                    record_ids=[],
                    source_urls=[],
                )
            group = groups[entity_id]
            if finding.record_id not in group.record_ids:
                group.record_ids.append(finding.record_id)
            for field in fields:
                value = data.get(field)
                if value is not None and value not in group.field_values.setdefault(
                    field, []
                ):
                    group.field_values[field].append(value)
            group.source_urls = sorted(
                {
                    *group.source_urls,
                    *(
                        s.url
                        for s in finding.sources
                        if s.evidence_status == "source_matched"
                    ),
                }
            )
        setattr(output, objective, list(groups.values()))
    return output


def summarize_technologies(findings: list[Finding]) -> list[TechnologySummary]:
    """Only attributed, source-matched observations enter the analytical summary.

    Counts refer to distinct observed job URLs, not independent jobs or deployments.
    Preserve scope, dates and alternatives; normalize case/spacing, never fuzzy aliases.
    """
    groups: dict[tuple, TechnologySummary] = {}
    for finding in findings:
        data = finding.data
        if not accepted_finding(finding):
            continue
        sources = [s for s in finding.sources if s.evidence_status == "source_matched"]
        if not sources or data["company"] is None:
            continue
        match = data.get("catalog_match") or {}
        dimensions = {
            key: data[key]
            for key in (
                "company",
                "technology",
                "category",
                "signal",
                "scope",
                "alternative_group",
                "as_of",
            )
        }
        identity = match.get("canonical_technology") or match.get("proposal_id")
        key = tuple(
            identity
            if field == "technology" and identity is not None
            else normalize(value)
            if isinstance(value, str)
            else value
            for field, value in dimensions.items()
        ) + (match.get("status"),)
        if key not in groups:
            groups[key] = TechnologySummary(
                **dimensions,
                canonical_technology=match.get("canonical_technology"),
                catalog_status=match.get("status"),
                proposal_id=match.get("proposal_id"),
                distinct_job_url_count=0,
                job_urls=[],
                source_urls=[],
                record_ids=[],
            )
        summary = groups[key]
        if data["job_url"] is not None:
            job_url = normalize_url(data["job_url"])
            if job_url not in summary.job_urls:
                summary.job_urls.append(job_url)
        summary.source_urls = sorted({*summary.source_urls, *(s.url for s in sources)})
        if finding.record_id not in summary.record_ids:
            summary.record_ids.append(finding.record_id)
        summary.job_urls.sort()
        summary.distinct_job_url_count = len(summary.job_urls)
    return list(groups.values())
