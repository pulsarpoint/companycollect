"""Resolve extracted technology identities without mixing tools into HTML extraction."""

import json
from pathlib import Path

from pydantic import ValidationError

from company_research.llm import ModelBudgetExceeded, ModelUnavailable, OpenRouter
from company_research.models import Finding, TechnologyResolution, TechnologyResolutions
from company_research.prompts import CATALOG_INSTRUCTIONS
from company_research.storage import write_json
from company_research.technology_catalog import (
    TechnologyCatalog,
    validate_technology_match,
)


async def resolve_technologies(
    findings: list[Finding],
    catalog: TechnologyCatalog,
    llm: OpenRouter,
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
    errors = []
    for start in range(0, len(unresolved), 20):
        names = unresolved[start : start + 20]
        searches = [catalog.search(name) for name in names]
        original = (
            CATALOG_INSTRUCTIONS
            + "\nResolve ONLY the supplied extracted technology names. HTML extraction has already finished. Initial searches below were executed by the application against the pinned catalog; use the tool for alternative names when necessary. Return one resolution per supplied technology, preserving source spelling. Do not return extraction arrays or drop a technology because its identity is new.\nINPUT DATA:\n"
            + json.dumps(
                {
                    "technologies": [
                        {
                            "technology": name,
                            "contexts": [
                                record.data["context"] for record in by_name[name]
                            ],
                        }
                        for name in names
                    ],
                    "initial_search_results": searches,
                }
            )
        )
        prompt, resolved, issues = original, {}, []
        for correction in range(llm.config.max_corrections + 1):
            issues = []
            try:
                reply = await llm.ask(
                    prompt,
                    TechnologyResolutions.model_json_schema(),
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
            if not isinstance(values, list):
                issues.append(reply.error or "Missing technology resolutions")
            else:
                for value in values:
                    try:
                        parsed = TechnologyResolution.model_validate(value)
                        if parsed.technology not in names:
                            raise ValueError("Unexpected technology name")
                        resolved[parsed.technology] = validate_technology_match(
                            parsed.technology,
                            parsed.catalog_match.model_dump(),
                            catalog,
                            searches,
                        )
                    except (ValidationError, ValueError) as error:
                        issues.append(str(error))
            missing = [name for name in names if name not in resolved]
            if missing:
                issues.append("Missing resolutions: " + ", ".join(missing))
            if not issues or reply.error is not None and reply.raw is None:
                break
            prompt = (
                original
                + "\nCORRECTION: Return all requested resolutions. Fix:\n"
                + json.dumps(issues)
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
                if name not in resolved:
                    record.data["catalog_error"] = (
                        "; ".join(issues) or "Technology identity was not resolved"
                    )
        errors.extend(issues)
    return errors
