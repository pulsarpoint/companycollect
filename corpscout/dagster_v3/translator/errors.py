# translator/errors.py
"""Error categorisation helpers for the translation provider."""
from __future__ import annotations


def _categorize_exception(exc: Exception) -> str:
    message = str(exc).lower()
    chain_messages = _exception_chain_messages(exc)
    combined = " ".join([message, *chain_messages])
    class_names = _exception_chain_class_names(exc)

    if "connectionrefusederror" in class_names or "connection refused" in combined:
        return "connection_refused"
    if "timeout" in combined or "timeout" in class_names:
        return "timeout"
    if "json" in combined or "jsondecodeerror" in class_names:
        return "invalid_json"
    if "missing item_id" in combined:
        return "missing_item_ids"
    if "item_id" in combined or "translation response" in combined:
        return "invalid_response"
    return "provider_error"


def _exception_chain_messages(exc: BaseException) -> list[str]:
    messages: list[str] = []
    current = exc.__cause__ or exc.__context__
    while current is not None:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    return messages


def _exception_chain_class_names(exc: BaseException) -> str:
    names: list[str] = []
    current: BaseException | None = exc
    while current is not None:
        names.append(type(current).__name__.lower())
        current = current.__cause__ or current.__context__
    return " ".join(names)
