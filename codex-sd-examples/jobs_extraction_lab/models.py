"""Contracts shared by both extractors and their saved experiment results."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class Job(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    location: str | None
    department: str | None
    employment_type: str | None
    workplace_type: str | None
    job_url: str | None
    evidence: str = Field(
        description="A short verbatim quotation from this job's listing."
    )


class JobExtraction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    jobs: list[Job]


class Source(BaseModel):
    id: str
    company: str
    url: str
    platform: str


class Page(BaseModel):
    id: str
    company: str
    source_url: str
    final_url: str
    platform: str
    fetched_at: str
    markdown_file: str
    html_file: str
    markdown_sha256: str
    markdown_chars: int
    job_links: list[str]
    title: str | None
    language: str | None


class ExtractionRun(BaseModel):
    page_id: str
    backend: Literal["codex", "openrouter"]
    requested_model: str
    actual_model: str | None = None
    input_hash: str
    markdown_sha256: str
    started_at: str
    elapsed_seconds: float
    succeeded: bool
    extraction: JobExtraction | None = None
    validation_issues: list[str] = Field(default_factory=list)
    error: str | None = None
    raw_response: str | None = None
    usage: dict[str, Any] | None = None
    attempts: int = 1
