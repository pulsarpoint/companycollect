"""Collect company source pages for later offline interpretation."""

from company_research.crawl import crawl_company
from company_research.models import ResearchConfig

__all__ = ["ResearchConfig", "crawl_company"]
