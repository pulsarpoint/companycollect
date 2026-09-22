"""Predicates matching the Backoffice SE domains list, without page limits."""

from typing import Literal, Self

import dagster as dg
from pydantic import Field, model_validator

SE_DOMAIN_TABLE = "corpscout.se_company_domain"


class SeDomainFilters(dg.Config):
    domain: str = Field(default="", max_length=253)
    company: str = Field(default="", pattern=r"^[0-9]*$")
    source: Literal["", "brave", "wikidata", "esef_filing", "common_crawl_identity"] = (
        ""
    )
    association: Literal["", "connected", "uncertain", "not_connected"] = ""
    status: Literal["", "active", "inactive"] = ""
    min_confidence: float | None = Field(default=None, ge=0, le=1)
    max_confidence: float | None = Field(default=None, ge=0, le=1)
    shared: bool = False

    @model_validator(mode="after")
    def confidence_range(self) -> Self:
        if (
            self.min_confidence is not None
            and self.max_confidence is not None
            and self.min_confidence > self.max_confidence
        ):
            raise ValueError("minimum confidence cannot exceed maximum confidence")
        return self

    def predicates(self) -> tuple[list[str], dict]:
        where = []
        params = {}
        if self.domain:
            where.append("root_domain LIKE %(se_domain)s")
            params["se_domain"] = f"%{self.domain}%"
        if self.company:
            where.append("company_id = %(se_company)s")
            params["se_company"] = self.company
        if self.source:
            where.append("has(sources, %(se_source)s)")
            params["se_source"] = self.source
        if self.association:
            where.append("association = %(se_association)s")
            params["se_association"] = self.association
        if self.status:
            where.append("active = %(se_active)s")
            params["se_active"] = int(self.status == "active")
        if self.min_confidence is not None:
            where.append("confidence >= %(se_min_confidence)s")
            params["se_min_confidence"] = self.min_confidence
        if self.max_confidence is not None:
            where.append("confidence <= %(se_max_confidence)s")
            params["se_max_confidence"] = self.max_confidence
        if self.shared:
            # The list counts owners over the entire current entity, before filters.
            where.append(
                f"root_domain IN (SELECT root_domain FROM {SE_DOMAIN_TABLE} FINAL GROUP BY root_domain HAVING uniqExact(company_id) > 1)"
            )
        return where, params
