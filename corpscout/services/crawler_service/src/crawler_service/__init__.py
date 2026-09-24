"""Collect company source pages for later offline interpretation."""

from crawler_service.crawl import crawl_company
from crawler_service.models import ResearchConfig

__all__ = ["ResearchConfig", "crawl_company"]
