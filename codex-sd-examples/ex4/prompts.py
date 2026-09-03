"""Prompt variants as Markdown templates with three placeholders."""

import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path

from ex1.models import StrictModel
from ex3.requirements import requirements_text
from ex4.candidates import CandidateSet, candidates_payload

PLACEHOLDERS = ("requirements", "limit", "candidates")
PLACEHOLDER_PATTERN = re.compile(r"\{([a-z_]+)\}")


class PromptTemplate(StrictModel):
    name: str
    text: str
    path: str


def load_prompts(
    directory: Path, names: Sequence[str] | None = None
) -> list[PromptTemplate]:
    """Load prompts/*.md; validate placeholders; optionally restrict to names."""
    templates: list[PromptTemplate] = []
    for path in sorted(directory.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        unknown = sorted(
            {m.group(1) for m in PLACEHOLDER_PATTERN.finditer(text)} - set(PLACEHOLDERS)
        )
        if unknown:
            raise ValueError(
                f"{path.name}: unknown placeholder(s) {', '.join(unknown)}"
            )
        templates.append(PromptTemplate(name=path.stem, text=text, path=str(path)))
    if names is None:
        return templates
    by_name = {t.name: t for t in templates}
    missing = [n for n in names if n not in by_name]
    if missing:
        raise ValueError(f"Unknown prompt(s): {', '.join(missing)}")
    return [by_name[n] for n in names]


def render_prompt(
    template: PromptTemplate, *, base_url: str, limit: int, candidate_set: CandidateSet
) -> str:
    input_data = {
        "base_url": base_url,
        "max_pages": limit,
        "candidates": candidates_payload(candidate_set),
    }
    values = {
        "requirements": requirements_text(),
        "limit": str(limit),
        "candidates": json.dumps(input_data, ensure_ascii=False, indent=2),
    }
    return PLACEHOLDER_PATTERN.sub(lambda m: values[m.group(1)], template.text).strip()


def prompt_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
