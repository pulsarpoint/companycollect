"""Collect source-owned mentions, then classify them in bounded company batches."""

import json
import re
from collections import Counter

from bs4 import BeautifulSoup, Comment, Tag
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from company_research import content, mention_models, page_agent, page_prompts
from company_research.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from company_research.models import OBJECTIVES
from company_research.storage import content_hash, utc_now
from company_research.technology_catalog import TechnologyCatalog, technology_key


def source_sections(html: str, namespace: str, representation: str) -> list[dict]:
    """Index all visible text, preserving paragraphs, list items and table context."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "template"]):
        tag.decompose()
    blocks = {
        "p",
        "li",
        "tr",
        "pre",
        "blockquote",
        "dt",
        "dd",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
    }
    groups: list[tuple[Tag, list[str]]] = []
    for node in soup.find_all(string=True):
        text = str(node).strip()
        if not text or isinstance(node, Comment):
            continue
        owner = next(
            (parent for parent in node.parents if parent.name in blocks), node.parent
        )
        if owner is None:
            continue
        if groups and groups[-1][0] is owner:
            groups[-1][1].append(text)
        else:
            groups.append((owner, [text]))
    sections, headings = [], []
    for index, (owner, parts) in enumerate(groups):
        text = " ".join(parts)
        identifier = f"{namespace}:{representation}:s{index:04}"
        if re.fullmatch(r"h[1-6]", owner.name):
            level = int(owner.name[1])
            headings = [(rank, sid) for rank, sid in headings if rank < level]
            headings.append((level, identifier))
        table = owner.find_parent("table")
        sections.append(
            {
                "section_id": identifier,
                "representation": representation,
                "text": text,
                "text_sha256": content_hash(text),
                "element": owner.name,
                "heading_ids": [sid for _, sid in headings],
                "table_headers": [
                    tag.get_text(" ", strip=True) for tag in table.find_all("th")
                ]
                if table
                else [],
                "locator": {
                    "text_block_index": index,
                    "method": "visible_text_dom_order",
                },
            }
        )
    return sections


def validate_mentions(raw: object, sections: list[dict], namespace: str) -> dict:
    indexed = {section["section_id"]: section for section in sections}
    mentions, issues = [], []
    if not isinstance(raw, list):
        return {
            "mentions": [],
            "status": "failed",
            "issues": ["missing_mentions_array"],
        }
    for index, value in enumerate(raw):
        identifier = f"{namespace}:m{index:04}"
        try:
            parsed = mention_models.RawMention.model_validate(value)
        except ValidationError as error:
            mentions.append(
                {
                    "mention_id": identifier,
                    "raw": value,
                    "status": "needs_review",
                    "issues": error.errors(
                        include_input=False, include_context=False, include_url=False
                    ),
                }
            )
            issues.append(f"invalid_mention:{identifier}")
            continue
        errors = []
        references = list(
            dict.fromkeys([*parsed.section_ids, *parsed.context_section_ids])
        )
        if set(references) - indexed.keys():
            errors.append("unknown_section_id")
        # Keep original headings and adjacent introductions even if the model
        # forgets to cite them. These are context, not independent claim support.
        positions = {
            section["section_id"]: index for index, section in enumerate(sections)
        }
        for sid in list(references):
            if sid not in indexed:
                continue
            references.extend(indexed[sid]["heading_ids"])
            position = positions[sid]
            for neighbor in (position - 1, position + 1):
                if (
                    0 <= neighbor < len(sections)
                    and sections[neighbor]["representation"]
                    == indexed[sid]["representation"]
                ):
                    references.append(sections[neighbor]["section_id"])
        references = list(dict.fromkeys(references))
        name = technology_key(parsed.source_name)
        pattern = re.compile(r"(?<![\w+#.])" + re.escape(name) + r"(?![\w+#])")
        if not any(
            pattern.search(technology_key(indexed[sid]["text"]))
            for sid in parsed.section_ids
            if sid in indexed
        ):
            errors.append("name_not_in_source_section")
        mentions.append(
            parsed.model_dump()
            | {
                "mention_id": identifier,
                "status": "source_linked" if not errors else "needs_review",
                "issues": errors,
                "source_section_ids": [sid for sid in references if sid in indexed],
            }
        )
        issues.extend(f"{identifier}:{error}" for error in errors)
    return {
        "mentions": mentions,
        "status": "partial" if issues else "processed",
        "issues": issues,
    }


async def request_json(
    llm: ModelClient,
    prompt: str,
    schema: dict,
    task: str,
    *,
    sections: list[dict] | None = None,
) -> dict:
    """One bounded retry for invalid responses; retain both attempts for audit."""
    attempts = []
    for attempt in range(2):
        try:
            reply = await llm.ask(prompt, schema, task=f"{task}:attempt{attempt + 1}")
        except (ModelBudgetExceeded, ModelUnavailable) as error:
            attempts.append(
                {"document": None, "error": str(error), "schema_errors": []}
            )
            break
        errors = [
            f"{list(error.absolute_path)}: {error.message}"
            for error in Draft202012Validator(schema).iter_errors(reply.document)
        ]
        if sections is not None and isinstance(reply.document, dict):
            checked = validate_mentions(
                reply.document.get("technology_mentions"), sections, "validation"
            )
            errors.extend(checked["issues"])
        attempts.append(
            {
                "document": reply.document,
                "raw": reply.raw,
                "error": reply.error,
                "schema_errors": errors,
            }
        )
        if not reply.error and not errors:
            break
        if sections is not None and errors:
            prompt = (
                "Validation failed on the previous attempt: "
                + json.dumps(errors)
                + "\nReturn the complete corrected page result. Each source_name must be the technical name itself present in its cited section, never the job title. Use only supplied section IDs.\n"
                + prompt
            )
    # Prefer the last object over a failed retry, while preserving its partial status.
    selected = next(
        (item for item in reversed(attempts) if isinstance(item["document"], dict)),
        attempts[-1],
    )
    return {
        "document": selected["document"],
        "status": "processed"
        if not selected["error"] and not selected["schema_errors"]
        else "partial",
        "attempts": attempts,
    }


def validate_page_records(
    raw: object, page: page_agent.PageInput, rendered_html: str | None
) -> dict:
    objectives = [
        objective for objective in OBJECTIVES if objective != "technology_signals"
    ]
    data = page_agent.validate_records(raw, objectives, page)
    # Check each capture independently. A rendered-only quotation must never
    # be credited to the native cleaned HTML snapshot.
    for objective, findings in data["records"].items():
        for finding in findings:
            finding["sources"][0]["representation"] = "native_cleaned_html"
            if rendered_html is None or finding["evidence_status"] == "source_matched":
                continue
            record = finding["data"] | {
                "evidence": [
                    fragment["text"] for fragment in finding["sources"][0]["evidence"]
                ]
            }
            alternate = content.source_finding(
                objective,
                record,
                page=page.page | {"html_sha256": content_hash(rendered_html)},
                window=content.HtmlWindow(0, len(rendered_html), rendered_html),
            ).model_dump()
            alternate["sources"][0]["representation"] = "rendered_html"
            finding["sources"].extend(alternate["sources"])
            if alternate["evidence_status"] == "source_matched":
                finding["evidence_status"] = "source_matched"
        coverage = data["coverage"][objective]
        coverage["issues"] = [
            issue
            for issue in coverage["issues"]
            if not issue.startswith("evidence_record:")
        ]
        coverage["issues"].extend(
            f"evidence_record:{index}"
            for index, finding in enumerate(findings)
            if finding["evidence_status"] != "source_matched"
        )
        if coverage["status"] != "failed":
            coverage["status"] = "partial" if coverage["issues"] else "processed"
    return data


class MentionPageAgent:
    def __init__(self, llm: ModelClient):
        self.llm = llm

    async def analyze(
        self, page: page_agent.PageInput, rendered_html: str | None = None
    ) -> dict:
        namespace = content_hash(page.page["source_url"] + "\n" + page.html)[:20]
        sections = source_sections(page.html, namespace, "native_cleaned_html")
        if rendered_html is not None:
            sections.extend(source_sections(rendered_html, namespace, "rendered_html"))
        objectives = [
            objective for objective in OBJECTIVES if objective != "technology_signals"
        ]
        schema = page_agent.response_schema(objectives, routing=False, links=True)
        mention_schema = mention_models.RawMention.model_json_schema()
        schema["$defs"]["RawMention"] = mention_schema
        schema["properties"]["technology_mentions"] = {
            "type": "array",
            "items": {"$ref": "#/$defs/RawMention"},
        }
        schema["required"].append("technology_mentions")
        # Preserve the established nontechnology rules; replace technology collection.
        common = page_prompts.COMMON.replace(
            "Ignore generic cookie-vendor inventories for operational-company\ntechnology extraction; do not transfer a recruitment platform's map vendors to its\nemployer.",
            "Do not transfer a recruitment platform's vendors to its employer.",
        )
        instruction = (
            common
            + "\n"
            + "\n".join(
                f"{objective}: {page_prompts.RULES[objective]}"
                for objective in objectives
            )
        )
        instruction += "\n" + page_prompts.LINKS + "\n" + mention_models.COLLECT
        source = json.loads(page.payload())
        source["source_sections"] = sections
        # Native HTML remains exactly as captured. Rendered evidence has its own IDs/hash.
        source["rendered_evidence_policy"] = (
            "Rendered sections may support facts absent from native HTML; cite their rendered IDs. Never pretend rendered text was present in native HTML."
        )
        response = await request_json(
            self.llm,
            instruction
            + "\nSOURCE SNAPSHOT:\n"
            + json.dumps(source, ensure_ascii=False),
            schema,
            f"{namespace}:collect",
            sections=sections,
        )
        doc = response["document"] if isinstance(response["document"], dict) else {}
        data = validate_page_records(doc.get("data"), page, rendered_html)
        links, errors = page_agent.validate_links(doc.get("links"), page.links)
        mentions = validate_mentions(
            doc.get("technology_mentions"), sections, namespace
        )
        return {
            "schema_version": "company-research-page/1.0",
            "page_id": namespace,
            "data": data,
            "links": links,
            "source": page.page,
            "observations": page.observations,
            "source_sections": sections,
            "technology_mentions": mentions,
            "captures": {
                "native_cleaned_html": {
                    "sha256": content_hash(page.html),
                    "content": page.html,
                },
                "rendered_html": {
                    "sha256": content_hash(rendered_html),
                    "content": rendered_html,
                }
                if rendered_html is not None
                else None,
            },
            "processing": {
                "finished_at": utc_now(),
                "response": response,
                "link_errors": errors,
                "prompt_version": "mention-page/1.1",
                "semantic_validation": "not_independently_verified",
            },
        }


def accept_decisions(
    raw: object, mentions: list[dict]
) -> tuple[list[dict], list[dict]]:
    supplied = raw if isinstance(raw, list) else []
    counts = Counter(
        value.get("mention_id")
        for value in supplied
        if isinstance(value, dict) and isinstance(value.get("mention_id"), str)
    )
    expected = {mention["mention_id"]: mention for mention in mentions}
    decisions, rejections = {}, []
    for value in supplied:
        try:
            decision = mention_models.MentionDecision.model_validate(value)
            if decision.mention_id not in expected or counts[decision.mention_id] != 1:
                raise ValueError("Unknown or duplicate mention ID")
            allowed = set(expected[decision.mention_id].get("source_section_ids", []))
            if any(
                set(relation.section_ids) - allowed
                for relation in decision.relationships
            ):
                raise ValueError(
                    "Relationship cites a section outside this mention's source context"
                )
            primary = set(expected[decision.mention_id].get("section_ids", []))
            if any(
                not primary.intersection(relation.section_ids)
                for relation in decision.relationships
            ):
                raise ValueError(
                    "Relationship must cite a primary mention section as well as any supporting context"
                )
            decisions[decision.mention_id] = decision.model_dump()
        except (ValueError, ValidationError) as error:
            rejections.append({"raw": value, "reason": str(error)})
    result = []
    for identifier, mention in expected.items():
        decision = decisions.get(identifier)
        result.append(
            {
                "mention_id": identifier,
                "classification": decision,
                "status": "classified"
                if decision is not None
                and decision["disposition"] != "needs_review"
                and mention["status"] == "source_linked"
                else "needs_review",
                "reason": None
                if decision is not None
                else "missing_or_invalid_decision",
            }
        )
    return result, rejections


def lookup_mention(mention: dict, catalog: TechnologyCatalog | None) -> dict:
    if catalog is None:
        return {
            "status": "failed",
            "reason": "catalog_unavailable",
            "canonical_technology": None,
        }
    if not mention.get("source_name"):
        return {
            "status": "not_attempted",
            "reason": "invalid_mention",
            "canonical_technology": None,
        }
    search = catalog.search(mention["source_name"], mention.get("context", "")[:4000])
    canonical = search["canonical_technology"]
    status = (
        "matched"
        if canonical is not None
        else "ambiguous"
        if search["candidates"] or search["match_method"] == "ambiguous"
        else "not_found"
    )
    return search | {
        "status": status,
        "scope": "pinned_catalog_snapshot",
        "identity_review": "candidate_only"
        if status == "ambiguous"
        else "name_or_accepted_alias"
        if status == "matched"
        else "no_search_match",
    }


def classification_context(page: dict) -> list[str]:
    """Link company/client/product context back to original text, never summaries."""
    fragments = []
    for objective in ("company_profile", "company_relationships", "products_services"):
        for record in page["data"]["records"][objective]:
            if (
                objective == "products_services"
                and record["data"].get("description") is None
            ):
                continue
            for source in record["sources"]:
                fragments.extend(
                    content.normalize_evidence(fragment["text"])
                    for fragment in source["evidence"]
                )
    return [
        section["section_id"]
        for section in page["source_sections"]
        if any(
            fragment in content.normalize_evidence(section["text"])
            for fragment in fragments
            if fragment
        )
    ]


def classification_payload(batch: list[dict], sections: dict[str, dict]) -> str:
    identifiers = dict.fromkeys(
        sid for mention in batch for sid in mention.get("source_section_ids", [])
    )
    return json.dumps(
        {"mentions": batch, "source_sections": [sections[sid] for sid in identifiers]},
        ensure_ascii=False,
    )


def batch_validation_mentions(batch: list[dict]) -> list[dict]:
    """Allow supplied same-page context while requiring the mention's own evidence."""
    supplied = set(
        sid for mention in batch for sid in mention.get("source_section_ids", [])
    )
    return [
        mention
        | {
            "source_section_ids": sorted(
                sid
                for sid in supplied
                if sid.split(":", 1)[0] == mention["mention_id"].split(":", 1)[0]
            )
        }
        for mention in batch
    ]


async def classify_mentions(
    llm: ModelClient,
    pages: list[dict],
    catalog: TechnologyCatalog | None,
    *,
    max_mentions: int = 20,
    max_batch_chars: int = 64000,
) -> dict:
    if max_mentions < 1 or max_batch_chars < 1:
        raise ValueError("Classification batch limits must be positive")
    mentions = [
        mention for page in pages for mention in page["technology_mentions"]["mentions"]
    ]
    sections = {
        section["section_id"]: section
        for page in pages
        for section in page["source_sections"]
    }
    context_by_mention = {
        mention["mention_id"]: classification_context(page)
        for page in pages
        for mention in page["technology_mentions"]["mentions"]
    }
    batches, current = [], []
    for mention in mentions:
        item = mention | {
            "page_context_section_ids": context_by_mention[mention["mention_id"]],
            "source_section_ids": list(
                dict.fromkeys(
                    [
                        *mention.get("source_section_ids", []),
                        *context_by_mention[mention["mention_id"]],
                    ]
                )
            ),
        }
        if current and (
            len(current) >= max_mentions
            or len(classification_payload([*current, item], sections)) > max_batch_chars
        ):
            batches.append(current)
            current = []
        current.append(item)
    if current:
        batches.append(current)
    results, diagnostics = [], []
    for index, batch in enumerate(batches):
        payload = classification_payload(batch, sections)
        if len(payload) > max_batch_chars:
            # Never silently truncate evidence; keep an oversized mention for review.
            response = {
                "document": None,
                "status": "partial",
                "attempts": [],
                "error": "source_context_exceeds_batch_limit",
            }
        else:
            response = await request_json(
                llm,
                mention_models.CLASSIFY
                + "\nMENTIONS AND ORIGINAL SECTIONS:\n"
                + payload,
                mention_models.MentionDecisions.model_json_schema(),
                f"classify:{pages[0]['page_id']}:{index}",
            )
        doc = response["document"] if isinstance(response["document"], dict) else {}
        accepted, rejected = accept_decisions(
            doc.get("decisions"), batch_validation_mentions(batch)
        )
        results.extend(accepted)
        diagnostics.append(
            {
                "batch": index,
                "mention_ids": [item["mention_id"] for item in batch],
                "response": response,
                "rejections": rejected,
            }
        )
    for result, mention in zip(results, mentions, strict=True):
        result["catalog_lookup"] = lookup_mention(mention, catalog)
    return {
        "prompt_version": "mention-classifier/1.1",
        "semantic_validation": "model_interpretation; no_independent_verification",
        "page_context_section_ids": context_by_mention,
        "decisions": results,
        "batches": diagnostics,
        "status": "processed"
        if all(result["status"] == "classified" for result in results)
        and all(
            not batch["rejections"] and batch["response"]["status"] == "processed"
            for batch in diagnostics
        )
        else "partial",
    }
