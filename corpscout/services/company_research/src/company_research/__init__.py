"""One company website URL in, attributed JSON findings out."""

from company_research.models import ResearchConfig, ResearchResult
from company_research.research import research_company

__all__ = ["ResearchConfig", "ResearchResult", "research_company"]
