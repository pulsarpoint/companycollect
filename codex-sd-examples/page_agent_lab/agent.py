"""Compatibility exports for the frozen page-agent benchmark."""

from crawler_service.captures import page_inventory
from crawler_service.page_agent import (
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
