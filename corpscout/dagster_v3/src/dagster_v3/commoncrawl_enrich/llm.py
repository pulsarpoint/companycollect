import json
import re
from collections.abc import Callable

from dagster_v3.commoncrawl_enrich.extract import _to_e164
from dagster_v3.commoncrawl_enrich.models import Email, IndustryGuess, Phone

ChatFn = Callable[[str, str], str]

_INDUSTRY_SYSTEM = (
    "You classify a company by its website text into one industry. "
    "Return ONLY JSON: {\"label\": str, \"nace_hint\": str (EU NACE rev2 code like '69.20' "
    "or section letter, '' if unknown), \"confidence\": int 0-100}."
)
_CONTACT_SYSTEM = (
    "Extract contact details from the website text, including obfuscated ones "
    "(e.g. 'info [at] x [dot] sk'). Return ONLY JSON: "
    "{\"emails\": [str], \"phones\": [str]}. Empty arrays if none."
)
_MAX_CHARS = 8000  # ~2-4k tokens; bound LLM cost


def _parse_json_object(content: str) -> dict | None:
    text = re.sub(r"<think>.*?</think>", "", content, flags=re.S).strip()
    text = re.sub(r"```(?:json)?", "", text).strip()
    match = re.search(r"\{.*\}", text, re.S)
    if match is None:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


class LLMArm:
    def __init__(self, chat: ChatFn):
        self._chat = chat

    def classify_industry(self, text: str) -> IndustryGuess:
        raw = self._chat(_INDUSTRY_SYSTEM, text[:_MAX_CHARS])
        data = _parse_json_object(raw)
        if not data or not data.get("label"):
            return IndustryGuess(label="", nace_hint="", confidence=0, method="none")
        try:
            confidence = int(data.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = 0
        return IndustryGuess(
            label=str(data.get("label", "")),
            nace_hint=str(data.get("nace_hint", "")),
            confidence=max(0, min(100, confidence)),
            method="llm",
        )

    def recover_contacts(self, text: str) -> tuple[list[Email], list[Phone]]:
        data = _parse_json_object(self._chat(_CONTACT_SYSTEM, text[:_MAX_CHARS])) or {}
        emails = [
            Email(email=str(e), is_role=False, source_method="llm")
            for e in (data.get("emails") or [])
            if "@" in str(e)
        ]
        phones = [
            Phone(phone_raw=str(p), phone_e164=_to_e164(str(p)), source_method="llm")
            for p in (data.get("phones") or [])
            if any(ch.isdigit() for ch in str(p))
        ]
        return emails, phones


def from_openai(*, base_url: str, model: str, api_key: str,
                timeout_seconds: int = 180, enable_thinking: bool = True) -> LLMArm:
    """Build an LLMArm backed by an OpenAI-compatible endpoint (thinking mode by default)."""
    from openai import OpenAI

    client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout_seconds)
    # Suppress the thinking trace ONLY if explicitly disabled (qwen extra body).
    extra_body = {} if enable_thinking else {"chat_template_kwargs": {"enable_thinking": False}}

    def chat(system: str, user: str) -> str:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0,
            extra_body=extra_body,
        )
        return response.choices[0].message.content or ""

    return LLMArm(chat=chat)
