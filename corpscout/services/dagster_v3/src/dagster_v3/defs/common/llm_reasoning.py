"""Provider-specific request fields for an explicitly selected reasoning effort."""

from typing import Any
from urllib.parse import urlsplit

def reasoning_options(base_url: str, effort: str | None) -> dict[str, Any]:
    if effort is None:
        return {}
    host = urlsplit(base_url).hostname
    if host == "api.deepseek.com":
        return {"extra_body": {
            "thinking": {"type": "disabled" if effort == "none" else "enabled"},
            **({"reasoning_effort": effort} if effort != "none" else {}),
        }}
    if host == "openrouter.ai":
        return {"extra_body": {
            "reasoning": {"enabled": False} if effort == "none" else {"enabled": True, "effort": effort},
            "provider": {"require_parameters": True},
        }}
    return {"reasoning_effort": effort}
