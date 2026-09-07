"""Company technology observations grouped without turning requirements into usage."""

from company_research.content import normalize
from company_research.discovery import normalize_url
from company_research.models import Finding, TechnologySummary


def summarize_technologies(findings: list[Finding]) -> list[TechnologySummary]:
    """Only attributed, source-matched observations enter the analytical summary.

    Counts refer to distinct observed job URLs, not independent jobs or deployments.
    Preserve scope, dates and alternatives; normalize case/spacing, never fuzzy aliases.
    """
    groups: dict[tuple, TechnologySummary] = {}
    for finding in findings:
        data = finding.data
        if (
            finding.evidence_status != "source_matched"
            or data.get("catalog_error") is not None
        ):
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
