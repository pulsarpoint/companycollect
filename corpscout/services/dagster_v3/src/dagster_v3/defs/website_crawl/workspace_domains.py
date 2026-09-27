"""Inventory predicates shared in behavior with Backoffice's global domain list."""

from typing import Literal

import dagster as dg
from pydantic import Field, field_validator

DOMAIN_INVENTORY = "corpscout.domains_search"


class WorkspaceDomainFilters(dg.Config):
    prefix: str = Field(default="", max_length=253)
    suffix: str = Field(
        default="", max_length=253,
        pattern=r"^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*)?$",
    )
    sources: list[str] = Field(default_factory=list)
    source_match: Literal["any", "all"] = "any"
    dns: Literal["any", "with", "without"] = "any"
    websites: Literal["any", "with", "without", "observed"] = "any"
    companies: Literal["any", "with", "without"] = "any"
    company_matching: Literal["any", "with", "without"] = "any"

    @field_validator("sources")
    @classmethod
    def known_sources(cls, value: list[str]) -> list[str]:
        if any(source not in {"commoncrawl", "commoncrawl_graph", "se_company_domain"} for source in value):
            raise ValueError("Unknown domain inventory source")
        return sorted(set(value))

    def predicates(self) -> tuple[list[str], dict]:
        where = []
        params = {}
        if self.prefix:
            where.append("startsWith(root_domain, %(inventory_prefix)s)")
            params["inventory_prefix"] = self.prefix
        if self.suffix:
            where.append("endsWith(root_domain, %(inventory_suffix)s)")
            params["inventory_suffix"] = f".{self.suffix}"
        if self.sources:
            function = "hasAll" if self.source_match == "all" else "hasAny"
            where.append(f"{function}(sources, %(inventory_sources)s)")
            params["inventory_sources"] = sorted(set(self.sources))
        if self.dns != "any":
            where.append(f"has_dns_records = {int(self.dns == 'with')}")
        if self.websites == "observed":
            where.append("has_website = 1 AND observed_website_count > 0")
        elif self.websites != "any":
            where.append(f"has_website = {int(self.websites == 'with')}")
        if self.companies != "any":
            negation = "NOT " if self.companies == "without" else ""
            where.append(f"root_domain {negation}IN (SELECT root_domain FROM corpscout.company_domains_resolved WHERE is_active = 1)")
        if self.company_matching != "any":
            negation = "NOT " if self.company_matching == "without" else ""
            where.append(f"root_domain {negation}IN (SELECT domain FROM corpscout.website_company_lookup_results)")
        return where, params
