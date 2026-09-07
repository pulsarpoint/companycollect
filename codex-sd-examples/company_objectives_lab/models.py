"""Explicit schemas shared by objective selection and evidence extraction."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

type Objective = Literal[
    "company_profile",
    "company_contacts",
    "locations",
    "products_services",
    "people",
    "company_relationships",
    "jobs",
]

OBJECTIVES = {
    "company_profile": "Company trading/legal identity, typed identifiers, activities, industries, founding date, dated employee count, official domains and social profiles.",
    "company_contacts": "Published company, department and person business contacts: exact email/phone/form/social values, owner and purpose.",
    "locations": "Headquarters, registered offices and branches: exact postal address, company association, location type and country when stated.",
    "products_services": "Products or services the company actually offers: names, kind, descriptions, URLs and prices when stated.",
    "people": "Named people with stated professional roles and affiliations, profile URLs, and explicitly attributable business contacts.",
    "company_relationships": "Explicit corporate or commercial relationships, including parent/subsidiary, owned brand, partner, customer, supplier and distributor; preserve direction, date, and entity identity.",
    "jobs": "Identifiable current advertised openings with title, location, employer, department, work types and application URL. Careers landing pages can provide navigation to a board.",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Potential(StrictModel):
    potential: Literal["high", "medium", "low", "unknown"]
    role: Literal["direct", "navigation", "none"]


class ObjectivePotentials(StrictModel):
    company_profile: Potential
    company_contacts: Potential
    locations: Potential
    products_services: Potential
    people: Potential
    company_relationships: Potential
    jobs: Potential


class CandidateAssessment(StrictModel):
    candidate_id: str
    objectives: ObjectivePotentials
    reason: str = Field(min_length=1)


class Selection(StrictModel):
    assessments: list[CandidateAssessment]


class CompanyFact(StrictModel):
    company: str
    field: Literal[
        "trading_name",
        "legal_name",
        "registration_number",
        "vat_number",
        "lei",
        "other_identifier",
        "description",
        "industry",
        "founded",
        "employee_count",
        "official_domain",
        "social_profile",
    ]
    value: str
    as_of: str | None
    evidence: str = Field(min_length=1)


class Contact(StrictModel):
    owner: str | None
    owner_kind: Literal["company", "department", "person", "location", "unknown"]
    type: Literal["email", "phone", "form", "social"]
    value: str
    purpose: str | None
    evidence: str = Field(min_length=1)


class Location(StrictModel):
    company: str | None
    label: str | None
    kind: Literal["headquarters", "registered_office", "branch", "other", "unknown"]
    address: str
    country: str | None
    evidence: str = Field(min_length=1)


class Offering(StrictModel):
    company: str | None
    kind: Literal["product", "service"]
    name: str
    description: str | None
    url: str | None
    price: str | None
    evidence: str = Field(min_length=1)


class Person(StrictModel):
    name: str
    role: str | None
    company: str | None
    profile_url: str | None
    as_of: str | None
    evidence: str = Field(min_length=1)


class Relationship(StrictModel):
    subject: str
    relationship: Literal[
        "parent_of",
        "subsidiary_of",
        "owns_brand",
        "partner_of",
        "customer_of",
        "supplier_of",
        "distributor_of",
        "acquired",
        "other_explicit",
    ]
    object: str
    as_of: str | None
    evidence: str = Field(min_length=1)


class Job(StrictModel):
    employer: str | None
    title: str
    location: str | None
    department: str | None
    employment_type: str | None
    workplace_type: str | None
    job_url: str | None
    evidence: str = Field(min_length=1)


class NegativeClaim(StrictModel):
    objective: Objective
    evidence: str = Field(min_length=1)


class Extraction(StrictModel):
    company_profile: list[CompanyFact]
    company_contacts: list[Contact]
    locations: list[Location]
    products_services: list[Offering]
    people: list[Person]
    company_relationships: list[Relationship]
    jobs: list[Job]
    explicit_negatives: list[NegativeClaim]


RECORD_TYPES = {
    "company_profile": CompanyFact,
    "company_contacts": Contact,
    "locations": Location,
    "products_services": Offering,
    "people": Person,
    "company_relationships": Relationship,
    "jobs": Job,
    "explicit_negatives": NegativeClaim,
}
