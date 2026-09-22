"""Typed Webtech observations and exact/reviewed catalog identity resolution."""

from dataclasses import dataclass
from datetime import datetime

from clickhouse_driver import Client
from pydantic import BaseModel, Field

from dagster_v3.defs.technology_catalog.aliases import normalize_technology_name
from dagster_v3.defs.webtech.models import (
    StoredDomainResultDocument,
    StoredResultReference,
)

WEBTECH_TECHNOLOGY_TABLE = "webtech_domain_technologies"
WEBTECH_TECHNOLOGY_COLUMNS = (
    "root_domain",
    "crawl_id",
    "detector_version",
    "scan_id",
    "detected_name",
    "technology_id",
    "technology",
    "catalog_match",
    "detected_slug",
    "version",
    "confidence",
    "category_ids",
    "categories",
    "category_slugs",
    "requested_url",
    "final_url",
    "final_hostname",
    "outcome",
    "analysis_status",
    "analysis_complete",
    "scanned_at",
    "result_bucket",
    "result_object_key",
    "report_sha256",
    "run_id",
    "recorded_at",
)


class DetectedCategory(BaseModel):
    id: int = Field(ge=0, le=65535)
    name: str = Field(min_length=1)
    slug: str = Field(min_length=1)


class DetectedTechnology(BaseModel):
    name: str = Field(min_length=1)
    slug: str = Field(min_length=1)
    version: str
    confidence: int = Field(ge=0, le=100)
    categories: list[DetectedCategory]


class TechnologyReport(BaseModel):
    technologies: list[DetectedTechnology]
    analysis_status: str
    analysis_complete: bool


@dataclass(frozen=True)
class TechnologyCatalog:
    ids: dict[str, int]
    normalized: dict[str, list[str]]
    aliases: dict[str, str]

    def resolve(self, name: str) -> tuple[int | None, str, str]:
        if name in self.ids:
            return self.ids[name], name, "exact"
        key = normalize_technology_name(name)
        matches = self.normalized.get(key, [])
        if len(matches) == 1:
            return self.ids[matches[0]], matches[0], "normalized"
        if len(matches) > 1:
            return None, "", "ambiguous"
        canonical = self.aliases.get(key)
        if canonical is not None:
            return self.ids[canonical], canonical, "alias"
        return None, "", "unmapped"


def load_technology_catalog(client: Client) -> TechnologyCatalog:
    rows = client.execute(
        "SELECT technology, technology_id FROM corpscout.technology_catalog FINAL"
    )
    if len(rows) == 0:
        raise ValueError(
            "Technology catalog is empty; publish it before indexing Webtech"
        )
    ids = {str(name): int(identity) for name, identity in rows}
    if len(set(ids.values())) != len(ids):
        raise ValueError("Technology catalog contains an ID collision")
    normalized: dict[str, list[str]] = {}
    for name in ids:
        normalized.setdefault(normalize_technology_name(name), []).append(name)
    aliases: dict[str, str] = {}
    for key, canonical in client.execute(
        "SELECT alias_key, technology FROM corpscout.technology_aliases "
        "WHERE review_status = 'accepted'"
    ):
        if canonical not in ids:
            raise ValueError(f"Technology alias has an unknown target: {canonical}")
        if key in aliases and aliases[key] != canonical:
            raise ValueError(f"Technology alias has conflicting targets: {key}")
        aliases[key] = canonical
    return TechnologyCatalog(ids, normalized, aliases)


def technology_rows(
    document: StoredDomainResultDocument,
    reference: StoredResultReference,
    catalog: TechnologyCatalog,
    *,
    bucket: str,
    run_id: str,
    recorded_at: datetime,
) -> list[tuple[object, ...]]:
    if document.report is None:
        return []
    report = TechnologyReport.model_validate(document.report)
    names = [technology.name for technology in report.technologies]
    if len(set(names)) != len(names):
        raise ValueError(
            f"Duplicate technologies in report for {document.candidate.root_domain}"
        )
    rows = []
    for detected in report.technologies:
        technology_id, technology, match = catalog.resolve(detected.name)
        rows.append(
            (
                document.candidate.root_domain,
                document.crawl_id,
                document.detector_version,
                document.scan_id,
                detected.name,
                technology_id,
                technology,
                match,
                detected.slug,
                detected.version,
                detected.confidence,
                [category.id for category in detected.categories],
                [category.name for category in detected.categories],
                [category.slug for category in detected.categories],
                document.requested_url,
                document.final_url,
                document.final_hostname,
                document.outcome,
                report.analysis_status,
                int(report.analysis_complete),
                document.scanned_at,
                bucket,
                reference.object_key,
                reference.sha256,
                run_id,
                recorded_at,
            )
        )
    return rows


def insert_technology_rows(client: Client, rows: list[tuple[object, ...]]) -> None:
    for offset in range(0, len(rows), 50_000):
        client.execute(
            f"INSERT INTO corpscout.{WEBTECH_TECHNOLOGY_TABLE} "
            f"({', '.join(WEBTECH_TECHNOLOGY_COLUMNS)}) VALUES",
            rows[offset : offset + 50_000],
        )
