"""Page-owned results and independent extractors; no crawl or catalog mutations."""

import asyncio
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator
from pydantic import Field, ValidationError

from crawler_service.content import HtmlWindow, source_finding
from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import OBJECTIVES, RECORD_TYPES, Objective, StrictModel
from crawler_service.page_observations import observation_hash
from crawler_service.page_prompts import COMMON, LINKS, ROUTER, RULES
from crawler_service.storage import content_hash, utc_now, write_json


class RouteDecision(StrictModel):
    decision: Literal["run", "uncertain", "skip"]
    reason: str
    section_ids: list[str]


class LinkAssessment(StrictModel):
    link_id: str
    priority: int = Field(ge=0, le=100)
    objectives: list[Objective]
    target_relevance: Literal[
        "target", "target_evidence", "related_company", "unrelated", "unknown"
    ]
    reason: str


@dataclass(frozen=True)
class PageInput:
    page: dict
    html: str
    links: list[dict]
    headings: list[dict]
    target_url: str
    observations: dict | None = None

    @classmethod
    def load(cls, root: Path) -> "PageInput":
        metadata = json.loads((root / "input.json").read_text(encoding="utf-8"))
        html = (root / "page.html").read_text(encoding="utf-8")
        if content_hash(html) != metadata["page"]["html_sha256"]:
            raise ValueError("Frozen page hash mismatch")
        expected = metadata["page"].get("observations_sha256")
        if expected is not None and (
            not isinstance(metadata.get("observations"), dict)
            or observation_hash(metadata["observations"]) != expected
        ):
            raise ValueError("Frozen observations hash mismatch")
        return cls(html=html, **metadata)

    def payload(self) -> str:
        return json.dumps(
            {
                "target_url": self.target_url,
                "page": self.page,
                "headings": self.headings,
                "observed_links": self.links,
                "native_cleaned_html": self.html,
            },
            ensure_ascii=False,
        )


def response_schema(objectives: Sequence[str], *, routing: bool, links: bool) -> dict:
    definitions, properties = {}, {}
    if objectives:
        fields = {}
        for objective in objectives:
            model = RECORD_TYPES[objective]
            value = model.model_json_schema()
            definitions.update(value.pop("$defs", {}))
            definitions[model.__name__] = value
            fields[objective] = {
                "type": "array",
                "items": {"$ref": f"#/$defs/{model.__name__}"},
            }
        properties["data"] = {
            "type": "object",
            "properties": fields,
            "required": list(objectives),
            "additionalProperties": False,
        }
    if routing:
        definitions["RouteDecision"] = RouteDecision.model_json_schema()
        properties["decisions"] = {
            "type": "object",
            "properties": {o: {"$ref": "#/$defs/RouteDecision"} for o in OBJECTIVES},
            "required": list(OBJECTIVES),
            "additionalProperties": False,
        }
    if links:
        value = LinkAssessment.model_json_schema()
        definitions.update(value.pop("$defs", {}))
        definitions["LinkAssessment"] = value
        properties["links"] = {
            "type": "array",
            "items": {"$ref": "#/$defs/LinkAssessment"},
        }
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
        "$defs": definitions,
    }


def validate_links(raw: object, observed: list[dict]) -> tuple[list[dict], list[str]]:
    expected = {item["link_id"]: item for item in observed}
    valid, invalid_ids, errors = {}, set(), []
    for value in raw if isinstance(raw, list) else []:
        try:
            item = LinkAssessment.model_validate(value)
        except ValidationError as error:
            errors.append(
                f"invalid_link:{error.errors(include_input=False, include_url=False)}"
            )
            continue
        if item.link_id not in expected:
            errors.append(f"invented_link:{item.link_id}")
        elif item.link_id in valid or item.link_id in invalid_ids:
            invalid_ids.add(item.link_id)
            valid.pop(item.link_id, None)
            errors.append(f"duplicate_link:{item.link_id}")
        else:
            valid[item.link_id] = item.model_dump()
    results = []
    for key, observation in expected.items():
        assessment = valid.get(key)
        if assessment is None:
            errors.append(f"unassessed_link:{key}")
        results.append(
            observation
            | {
                "assessment": assessment,
                "assessment_status": "assessed" if assessment else "not_assessed",
            }
        )
    return results, errors


def validate_records(raw: object, objectives: Sequence[str], page: PageInput) -> dict:
    """Retain per-record failures; source presence is not semantic acceptance."""
    result = {"records": {}, "rejections": [], "coverage": {}}
    payload = raw if isinstance(raw, dict) else {}
    window = HtmlWindow(content=page.html, start=0, end=len(page.html))
    for objective in objectives:
        values = payload.get(objective)
        records, issues = [], []
        if not isinstance(values, list):
            issues.append("missing_or_invalid_array")
        else:
            for index, value in enumerate(values):
                try:
                    parsed = RECORD_TYPES[objective].model_validate(value).model_dump()
                except ValidationError as error:
                    issues.append(f"schema_record:{index}")
                    result["rejections"].append(
                        {
                            "objective": objective,
                            "index": index,
                            "record": value,
                            "errors": error.errors(
                                include_input=False,
                                include_url=False,
                                include_context=False,
                            ),
                        }
                    )
                    continue
                finding = source_finding(
                    objective, parsed, page=page.page, window=window
                )
                records.append(finding.model_dump())
                if finding.evidence_status != "source_matched":
                    issues.append(f"evidence_record:{index}")
        result["records"][objective] = records
        result["coverage"][objective] = {
            "status": "processed"
            if not issues
            else "partial"
            if isinstance(values, list)
            else "failed",
            "issues": issues,
            "html_range": [0, len(page.html)],
        }
    return result


def dispatch_decisions(raw: object, page: PageInput) -> tuple[dict, dict]:
    decisions, overrides = {}, {}
    supplied = raw if isinstance(raw, dict) else {}
    heading_ids = {heading["section_id"] for heading in page.headings}
    for objective in OBJECTIVES:
        try:
            item = RouteDecision.model_validate(supplied.get(objective))
            if set(item.section_ids) - heading_ids:
                raise ValueError("Unknown section IDs")
            decisions[objective] = item.model_dump()
        except (ValidationError, ValueError):
            decisions[objective] = {
                "decision": "uncertain",
                "reason": "Missing or invalid router decision",
                "section_ids": [],
                "invalid": True,
            }
    soup = BeautifulSoup(page.html, "html.parser")
    safeguards = {}
    if soup.select('a[href^="mailto:"],a[href^="tel:"]'):
        safeguards["company_contacts"] = "observed_mailto_or_tel"
    if any(
        "JobPosting" in script.get_text()
        for script in soup.find_all("script", type="application/ld+json")
    ):
        safeguards["jobs"] = "observed_jobposting"
    for objective, reason in safeguards.items():
        if decisions[objective]["decision"] == "skip":
            overrides[objective] = reason
    return decisions, overrides


class PageAgent:
    def __init__(self, llm: ModelClient):
        self.llm = llm

    async def request(
        self,
        page: PageInput,
        objectives: Sequence[str],
        *,
        stage: str,
        routing: bool = False,
        links: bool = False,
    ) -> dict:
        instruction = COMMON
        if routing:
            instruction += (
                "\n" + ROUTER + "\nObjectives to inspect:\n" + json.dumps(OBJECTIVES)
            )
        for objective in objectives:
            instruction += f"\n{objective}:\n{RULES[objective]}\n"
        if links:
            instruction += "\n" + LINKS
        prompt = instruction + "\nSOURCE SNAPSHOT:\n" + page.payload()
        schema = response_schema(objectives, routing=routing, links=links)
        try:
            reply = await self.llm.ask(
                prompt, schema, task=f"{page.page['page_id']}:{stage}"
            )
        except (ModelBudgetExceeded, ModelUnavailable) as error:
            return {"error": str(error), "document": None}
        schema_errors = [
            f"{list(error.absolute_path)}: {error.message}"
            for error in Draft202012Validator(schema).iter_errors(reply.document)
        ]
        return {
            "error": reply.error,
            "document": reply.document,
            "schema_errors": schema_errors,
        }

    async def analyze(
        self, page: PageInput, mode: Literal["one_pass", "routed"], output: Path
    ) -> dict:
        started, elapsed = utc_now(), time.monotonic()
        objectives = list(OBJECTIVES)
        decisions, overrides = {}, {}
        data: dict
        if mode == "one_pass":
            response = await self.request(page, objectives, stage=mode, links=True)
            document = (
                response["document"] if isinstance(response["document"], dict) else {}
            )
            data = validate_records(document.get("data"), objectives, page)
            links, link_errors = validate_links(document.get("links"), page.links)
            responses = {mode: response}
        else:
            response = await self.request(
                page, [], stage="route", routing=True, links=True
            )
            document = (
                response["document"] if isinstance(response["document"], dict) else {}
            )
            decisions, overrides = dispatch_decisions(document.get("decisions"), page)
            selected = [
                o
                for o in objectives
                if decisions[o]["decision"] != "skip" or o in overrides
            ]
            links, link_errors = validate_links(document.get("links"), page.links)
            responses = {"route": response}
            # The ModelClient semaphore is shared across pages AND specialists.
            replies = await asyncio.gather(
                *(self.request(page, [o], stage=f"specialist:{o}") for o in selected)
            )
            data = {
                "records": {o: [] for o in objectives},
                "rejections": [],
                "coverage": {
                    o: {"status": "not_selected", "issues": [], "html_range": None}
                    for o in objectives
                },
            }
            for objective, reply in zip(selected, replies, strict=True):
                responses[objective] = reply
                doc = reply["document"] if isinstance(reply["document"], dict) else {}
                checked = validate_records(doc.get("data"), [objective], page)
                data["records"].update(checked["records"])
                data["coverage"].update(checked["coverage"])
                data["rejections"].extend(checked["rejections"])
        data.update(
            {
                "source": page.page,
                "mode": mode,
                "processing": {
                    "started_at": started,
                    "finished_at": utc_now(),
                    "wall_seconds": round(time.monotonic() - elapsed, 3),
                    "responses": responses,
                    "decisions": decisions,
                    "routing_overrides": overrides,
                    "link_errors": link_errors,
                    "semantic_validation": "not_performed; source presence checks only",
                    "correction_calls": 0,
                },
            }
        )
        result = {"schema_version": "page-agent-lab/0.1", "data": data, "links": links}
        write_json(output, result)
        return result
