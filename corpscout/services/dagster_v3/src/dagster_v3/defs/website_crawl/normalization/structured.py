"""Typed structured properties and vacancies, preserving source occurrence identity."""

import json
import math
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urljoin, urlsplit

from dagster_v3.defs.website_crawl.normalization.tables import new_row


def object_value(value, label: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def array_value(value, label: str) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def text(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected source text")
    return value


def strings(value) -> list[str]:
    items = array_value(value, "text values")
    if any(not isinstance(item, str) for item in items):
        raise ValueError("Text values must contain only strings")
    return items


def timestamp(value) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(text(value).replace("Z", "+00:00"))
    return (
        result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)
    )


def posting_date(value) -> datetime | None:
    # A malformed source date is retained separately; it must not drop the posting.
    if value is None:
        return None
    try:
        return timestamp(value)
    except ValueError:
        return None


def web_url(value, base: str) -> str | None:
    if not value:
        return None
    candidate = urljoin(base, text(value))
    parsed = urlsplit(candidate)
    return (
        candidate
        if parsed.scheme in {"http", "https"}
        and parsed.hostname
        and not parsed.username
        and not parsed.password
        else None
    )


def locator(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def scalar_rows(value, path="", array_index=None):
    if isinstance(value, dict):
        for key, child in value.items():
            escaped = key.replace("~", "~0").replace("/", "~1")
            yield from scalar_rows(child, path + "/" + escaped)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from scalar_rows(child, path + "/" + str(index), index)
    else:
        fields = dict(property_path=path, array_index=array_index)
        if value is None:
            fields.update(value_type="null")
        elif isinstance(value, bool):
            fields.update(value_type="boolean", value_boolean=value)
        elif isinstance(value, (int, float, Decimal)):
            number = float(value)
            if not math.isfinite(number):
                raise ValueError("Non-finite structured number")
            fields.update(
                value_type="number",
                value_number=number,
                value_number_original=str(value),
            )
        elif isinstance(value, str):
            fields.update(value_type="string", value_string=value)
        else:
            raise ValueError("Unsupported structured scalar")
        yield fields


def named_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        values = [named_text(item) for item in value]
        return "; ".join(item for item in values if item is not None) or None
    if isinstance(value, dict):
        if "name" in value:
            return named_text(value["name"])
        if "value" in value:
            return named_text(value["value"])
        if "address" in value:
            return named_text(value["address"])
        return named_text(
            [
                value[key]
                for key in (
                    "streetAddress",
                    "postalCode",
                    "addressLocality",
                    "addressRegion",
                    "addressCountry",
                )
                if key in value
            ]
        )
    if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        return str(value)
    return text(value)


def job_row(data: dict, common: dict, *, legacy=False, evidence=None) -> dict:
    posted = text(data.get("datePosted"))
    expires = text(data.get("validThrough"))
    title = text(data.get("title"))
    if not title or not title.strip():
        raise ValueError("Job posting has no title")
    return new_row(
        "jobs",
        **common,
        source_job_id=named_text(data.get("identifier")) or text(data.get("@id")),
        job_url=web_url(data.get("job_url" if legacy else "url"), common["source_url"]),
        title_original=title,
        employer=named_text(data.get("employer" if legacy else "hiringOrganization")),
        location_original=named_text(data.get("location" if legacy else "jobLocation")),
        department_original=named_text(data.get("department")),
        employment_type=named_text(
            data.get("employment_type" if legacy else "employmentType")
        ),
        workplace_type=named_text(
            data.get("workplace_type" if legacy else "jobLocationType")
        ),
        description_original=text(data.get("description")),
        posted_at=posting_date(posted),
        posted_at_original=posted,
        expires_at=posting_date(expires),
        expires_at_original=expires,
        extraction_source="legacy_record" if legacy else "jsonld",
        evidence=evidence or [],
    )


def project_structured(observations: dict, common: dict, rows: dict) -> str:
    structured = observations.get("structured_data")
    if structured is None:
        return "not_available"
    structured = object_value(structured, "structured_data")
    blocks = array_value(structured.get("jsonld_blocks"), "jsonld_blocks")
    coverage = (
        "partial"
        if any(
            object_value(block, "jsonld block").get("status") != "parsed"
            for block in blocks
        )
        else "completed"
    )
    entities = array_value(structured.get("jsonld_entities"), "jsonld_entities")
    for entity_index, entity in enumerate(entities):
        entity = object_value(entity, "jsonld entity")
        data = object_value(entity.get("data"), "entity data")
        types = strings(entity.get("types"))
        origin = dict(
            common, source_locator=f"/structured_data/jsonld_entities/{entity_index}"
        )
        for fields in scalar_rows(data):
            rows["structured_data"].append(
                new_row(
                    "structured_data",
                    **dict(origin, row_index=len(rows["structured_data"])),
                    script_index=entity.get("script_index"),
                    entity_path=text(entity.get("entity_path")) or "",
                    entity_id=text(entity.get("id")),
                    entity_types=types,
                    parse_status="parsed",
                    **fields,
                )
            )
        if any(kind.rsplit("/", 1)[-1] == "JobPosting" for kind in types):
            if not data.get("title"):
                coverage = "partial"
                continue
            rows["jobs"].append(
                job_row(data, dict(origin, row_index=len(rows["jobs"])))
            )
    # Microdata is kept in the same typed property contract, without pretending it is JSON-LD.
    for index, item in enumerate(array_value(structured.get("microdata"), "microdata")):
        item = object_value(item, "microdata item")
        for fields in scalar_rows(
            object_value(item.get("properties"), "microdata properties")
        ):
            rows["structured_data"].append(
                new_row(
                    "structured_data",
                    **dict(
                        common,
                        row_index=len(rows["structured_data"]),
                        source_locator=f"/structured_data/microdata/{index}",
                    ),
                    entity_path=f"microdata/{index}",
                    entity_id=text(item.get("id")),
                    entity_types=strings(item.get("types")),
                    parse_status="parsed",
                    **fields,
                )
            )
    return coverage
