"""Resolve extracted technology identities without mixing tools into HTML extraction."""

import json
from pathlib import Path

from pydantic import ValidationError

from company_research.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from company_research.models import Finding, ModelTechnologyMatch, TechnologyResolution
from company_research.prompts import CATALOG_INSTRUCTIONS
from company_research.storage import write_json
from company_research.technology_catalog import (
    TechnologyCatalog,
    validate_technology_match,
)


async def resolve_technologies(
    findings: list[Finding],
    catalog: TechnologyCatalog,
    llm: ModelClient,
    root: Path,
    task: str,
) -> list[str]:
    """Exact/alias lookup is deterministic; ambiguous proposals use a bounded model step."""
    by_name: dict[str, list[Finding]] = {}
    for finding in findings:
        by_name.setdefault(finding.data["technology"], []).append(finding)
    unresolved = []
    for name, records in by_name.items():
        if catalog.resolve(name)[0] is None:
            unresolved.append(name)
        else:
            for record in records:
                record.data["catalog_match"] = validate_technology_match(
                    name, None, catalog, []
                )
                record.data.pop("catalog_error", None)
                record.data.pop("proposal_review", None)
                record.data["required_reviews"] = [
                    review
                    for review in record.data.get("required_reviews", [])
                    if review != "proposal_metadata"
                ]
    errors = []
    for start in range(0, len(unresolved), llm.config.catalog_resolution_batch_size):
        names = unresolved[start : start + llm.config.catalog_resolution_batch_size]
        searches = [
            catalog.search(
                name,
                " ".join(record.data["context"] for record in by_name[name])[:4000],
            )
            for name in names
        ]
        aliases = {f"t{i}": name for i, name in enumerate(names, 1)}
        resolved, declined, issues = {}, set(), []
        for correction in range(llm.config.max_corrections + 1):
            active = {
                key: name
                for key, name in aliases.items()
                if name not in resolved and name not in declined
            }
            if not active:
                break
            match_schema = ModelTechnologyMatch.model_json_schema()
            definitions = match_schema.pop("$defs")
            definitions["CatalogMatch"] = match_schema
            schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["resolutions"],
                "properties": {
                    "resolutions": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": list(active),
                        "properties": {
                            key: {
                                "anyOf": [
                                    {"$ref": "#/$defs/CatalogMatch"},
                                    {"type": "null"},
                                ]
                            }
                            for key in active
                        },
                    }
                },
                "$defs": definitions,
            }
            prompt = (
                CATALOG_INSTRUCTIONS
                + """
Resolve only the supplied names. Initial searches have already run against the pinned
catalog. Return resolutions as an OBJECT with every request_id as a REQUIRED KEY.
Each value is the catalog_match object: status, canonical_technology,
proposed_technology and reason. Code binds that key back to the unchanged source name.
For status=proposed, canonical_technology MUST be null and proposed_technology MUST
contain the complete draft. A missing catalog entry needs a new proposal, not a
fabricated canonical name. Return null only for a generic/non-specific item that
should not be a technology. Never replace an application with a similarly named vendor.
Process ALL required keys, not just the first. Retry inputs contain only unfinished
names; do not return names or keys absent from this request. Product descriptions
must not include internal search logs, matching status or claims of company usage.
INPUT DATA:
"""
                + json.dumps(
                    {
                        "task": "technology_resolution",
                        "technologies": [
                            {
                                "request_id": key,
                                "technology": name,
                                "contexts": [
                                    record.data["context"] for record in by_name[name]
                                ],
                            }
                            for key, name in active.items()
                        ],
                        "initial_search_results": [
                            search
                            for search in searches
                            if search.get("query") in active.values()
                        ],
                        "category_options": catalog.category_options(),
                        "previous_errors": issues,
                    }
                )
            )
            issues = []
            try:
                reply = await llm.ask(
                    prompt,
                    schema,
                    task=f"resolve:{task}:{start}:{correction}",
                    catalog=catalog,
                )
            except (ModelBudgetExceeded, ModelUnavailable) as error:
                issues.append(str(error))
                break
            searches.extend(reply.searches)
            values = (
                reply.document.get("resolutions")
                if isinstance(reply.document, dict)
                else None
            )
            if isinstance(values, dict):
                for key, value in values.items():
                    if key in active and value is None:
                        declined.add(active[key])
                values = [
                    {"technology": active[key], "catalog_match": value}
                    for key, value in values.items()
                    if key in active and value is not None
                ]
            # Some providers return the earlier array shape despite the requested schema.
            # Every such entry still gets the same exact-name and catalog validation.
            if not isinstance(values, list):
                issues.append(reply.error or "Missing technology resolutions")
            else:
                for value in values:
                    try:
                        parsed = TechnologyResolution.model_validate(value)
                        if parsed.technology not in active.values():
                            raise ValueError("Unexpected technology name")
                        resolved[parsed.technology] = validate_technology_match(
                            parsed.technology,
                            parsed.catalog_match.model_dump(),
                            catalog,
                            searches,
                        )
                    except (ValidationError, ValueError) as error:
                        issues.append(str(error))
            missing = [
                name
                for name in active.values()
                if name not in resolved and name not in declined
            ]
            if missing:
                issues.append("Missing resolutions: " + ", ".join(missing))
            if not missing or reply.error is not None and reply.raw is None:
                break
        issues.extend(
            f"{name}: catalog review declined a specific technology identity"
            for name in sorted(declined)
        )
        write_json(
            root / "resolutions" / f"{task}-{start}.json",
            {
                "technologies": names,
                "searches": searches,
                "resolved": resolved,
                "issues": issues,
            },
        )
        for name in names:
            for record in by_name[name]:
                record.data["catalog_match"] = resolved.get(name)
                if name in resolved:
                    record.data.pop("catalog_error", None)
                    record.data.pop("proposal_review", None)
                    if resolved[name]["status"] == "matched":
                        record.data["required_reviews"] = [
                            review
                            for review in record.data.get("required_reviews", [])
                            if review != "proposal_metadata"
                        ]
                else:
                    record.data["catalog_error"] = (
                        "; ".join(issues) or "Technology identity was not resolved"
                    )
        errors.extend(issues)
    return errors
