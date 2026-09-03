"""The information we want from a company website, and what is still missing."""

import re
from dataclasses import dataclass
from typing import Literal

from ex1.models import StrictModel, UsefulInformation

MIN_DESCRIPTION_CHARS = 80
YEAR_PATTERN = re.compile(r"\b(1[6-9]\d{2}|20\d{2})\b")
NUMBER_PATTERN = re.compile(r"\d[\d\s.,]*")
FOUNDED_PATTERN = re.compile(
    r"found|establish|since|grundad|gegründet|perustettu|stiftet", re.IGNORECASE
)
EMPLOYEE_PATTERN = re.compile(
    r"employee|staff|headcount|anställda|medarbetare|mitarbeiter|ansatte|työntekij",
    re.IGNORECASE,
)
MANAGEMENT_PATTERN = re.compile(
    r"\bceo\b|\bcfo\b|\bcto\b|chief|managing director|management|board|director|founder|\bvd\b|styrelse|ledning|geschäftsführ|vorstand",
    re.IGNORECASE,
)
GROUP_PATTERN = re.compile(
    r"subsidiar|parent company|group|owner|brand|dotterbolag|koncern|moderbolag|tochter",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TargetField:
    key: str
    description: str


TARGET_FIELDS: tuple[TargetField, ...] = (
    TargetField("company_name", "trading name of the company"),
    TargetField("legal_name", "registered legal name including the legal form"),
    TargetField(
        "identifiers", "registration or organisation number, VAT number, LEI, DUNS"
    ),
    TargetField("headquarters_address", "head office postal address"),
    TargetField("phone", "main phone numbers"),
    TargetField("email", "official email addresses"),
    TargetField("description", "what the company does, in a few sentences"),
    TargetField("industries", "industries or sectors the company operates in"),
    TargetField("founded_year", "year the company was founded"),
    TargetField("employee_count", "number of employees"),
    TargetField("management", "names and roles of executives and board members"),
    TargetField("jobs", "open positions and where to apply"),
    TargetField(
        "products_services", "products and services offered, with prices when shown"
    ),
    TargetField("social_profiles", "official social media profiles"),
    TargetField("group_structure", "parent company, subsidiaries and brands"),
)
TARGET_FIELD_KEYS = frozenset(field.key for field in TARGET_FIELDS)


class Gap(StrictModel):
    field: str
    description: str
    status: Literal["missing", "weak"]
    detail: str


def requirements_text() -> str:
    """Return the target fields as prompt-ready bullet lines."""
    return "\n".join(f"- {field.key}: {field.description}" for field in TARGET_FIELDS)


def compute_gaps(information: UsefulInformation) -> list[Gap]:
    """Report target fields the consolidated result does not cover yet."""
    company = information.company
    contact_types = {contact.type for contact in information.contacts}
    fact_text = " ".join(
        f"{fact.name} {fact.value}" for fact in information.other_facts
    )
    gaps: list[Gap] = []

    def missing(key: str, detail: str) -> None:
        gaps.append(
            Gap(field=key, description=_describe(key), status="missing", detail=detail)
        )

    if company.name is None:
        missing("company_name", "no company name extracted")
    if company.legal_name is None:
        missing("legal_name", "no legal name extracted")
    if not company.identifiers:
        missing("identifiers", "no registration, VAT or LEI identifiers")
    if "address" not in contact_types and not company.locations:
        missing("headquarters_address", "no postal address")
    if "phone" not in contact_types:
        missing("phone", "no phone number")
    if "email" not in contact_types:
        missing("email", "no email address")
    if company.description is None:
        missing("description", "no description")
    elif len(company.description) < MIN_DESCRIPTION_CHARS:
        gaps.append(
            Gap(
                field="description",
                description=_describe("description"),
                status="weak",
                detail=f"description has only {len(company.description)} characters",
            )
        )
    if not company.industries:
        missing("industries", "no industries")
    if not (FOUNDED_PATTERN.search(fact_text) and YEAR_PATTERN.search(fact_text)):
        missing("founded_year", "no founding year among the extracted facts")
    if not (EMPLOYEE_PATTERN.search(fact_text) and NUMBER_PATTERN.search(fact_text)):
        missing("employee_count", "no employee count among the extracted facts")
    if not MANAGEMENT_PATTERN.search(fact_text):
        missing(
            "management", "no executives or board members among the extracted facts"
        )
    if not information.jobs:
        missing("jobs", "no open positions")
    if not information.products:
        missing("products_services", "no products or services")
    if not company.social_profiles and "social" not in contact_types:
        missing("social_profiles", "no social media profiles")
    if not GROUP_PATTERN.search(fact_text):
        missing(
            "group_structure",
            "no parent, subsidiaries or brands among the extracted facts",
        )
    return gaps


def _describe(key: str) -> str:
    return next(field.description for field in TARGET_FIELDS if field.key == key)
