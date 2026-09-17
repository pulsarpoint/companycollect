"""Compatibility exports for the frozen page-agent benchmark."""

from company_research.captures import page_inventory
from company_research.page_agent import (
    LinkAssessment,
    PageAgent,
    PageInput,
    RouteDecision,
    dispatch_decisions,
    response_schema,
    validate_links,
    validate_records,
)

__all__ = [
    "LinkAssessment",
    "PageAgent",
    "PageInput",
    "RouteDecision",
    "dispatch_decisions",
    "page_inventory",
    "response_schema",
    "validate_links",
    "validate_records",
]
