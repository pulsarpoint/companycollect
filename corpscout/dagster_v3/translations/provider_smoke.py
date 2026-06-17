from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import time
from typing import Protocol

from translations.smoke import (
    DEFAULT_EXTRA_BODY,
    DEFAULT_MAX_TOKENS,
    LocalOpenAICompatibleTranslationProvider,
)
from translations.types import SmokeTranslationInput, SmokeTranslationResult


DEFAULT_BATCH_SIZES = (1, 5, 10, 50)


class TranslationProvider(Protocol):
    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]: ...


@dataclass(frozen=True)
class TranslationProviderSmokeResult:
    item_count: int
    status: str
    duration_seconds: float
    translated_count: int
    error_category: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def run_translation_provider_smoke(
    *,
    provider: TranslationProvider,
    batch_sizes: list[int],
    timeout_seconds: int,
) -> list[TranslationProviderSmokeResult]:
    results: list[TranslationProviderSmokeResult] = []
    for batch_size in batch_sizes:
        started_at = time.perf_counter()
        try:
            translations = provider.translate(
                _build_smoke_items(batch_size),
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            results.append(
                TranslationProviderSmokeResult(
                    item_count=batch_size,
                    status="failed",
                    duration_seconds=_elapsed_seconds(started_at),
                    translated_count=0,
                    error_category=_categorize_exception(exc),
                    error_message=str(exc),
                )
            )
            continue

        results.append(
            TranslationProviderSmokeResult(
                item_count=batch_size,
                status="success",
                duration_seconds=_elapsed_seconds(started_at),
                translated_count=len(translations),
            )
        )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke test the local translation provider.")
    parser.add_argument(
        "--batch-sizes",
        default=",".join(str(size) for size in DEFAULT_BATCH_SIZES),
        help="Comma-separated batch sizes to test. Default: 1,5,10,50.",
    )
    parser.add_argument(
        "--timeout-seconds",
        default=120,
        type=int,
        help="Per-request provider timeout in seconds.",
    )
    parser.add_argument(
        "--max-tokens",
        default=int(os.getenv("TRANSLATION_PROVIDER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))),
        type=int,
        help=f"Maximum generated tokens per provider request. Default: {DEFAULT_MAX_TOKENS}.",
    )
    parser.add_argument(
        "--extra-body-json",
        default=os.getenv(
            "TRANSLATION_PROVIDER_EXTRA_BODY_JSON",
            json.dumps(DEFAULT_EXTRA_BODY, separators=(",", ":")),
        ),
        help="JSON object passed to OpenAI chat completions as extra_body.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Optional dotenv-style file to load before reading provider settings.",
    )
    args = parser.parse_args(argv)

    _load_env_file(Path(args.env_file))
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"],
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
        max_tokens=args.max_tokens,
        extra_body=_parse_extra_body(args.extra_body_json),
    )
    results = run_translation_provider_smoke(
        provider=provider,
        batch_sizes=_parse_batch_sizes(args.batch_sizes),
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True))
    return 0 if all(result.status == "success" for result in results) else 1


def _build_smoke_items(count: int) -> list[SmokeTranslationInput]:
    samples = [
        "Allmennaksjeselskap",
        "Utvinning av raolje",
        "Utvinning av naturgass",
        "Produksjon av raffinerte petroleumsprodukter",
        "Selv, eller gjennom andre selskaper aa utvikle energi",
    ]
    return [
        SmokeTranslationInput(
            item_id=f"provider-smoke-{index:02d}",
            source_text=samples[index % len(samples)],
        )
        for index in range(count)
    ]


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


def _parse_batch_sizes(value: str) -> list[int]:
    batch_sizes = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not batch_sizes:
        raise ValueError("at least one batch size is required")
    if any(size <= 0 for size in batch_sizes):
        raise ValueError("batch sizes must be positive integers")
    return batch_sizes


def _parse_extra_body(value: str) -> dict[str, object] | None:
    if value.strip() == "":
        return None
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("extra body JSON must be an object")
    return payload


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped == "" or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _elapsed_seconds(started_at: float) -> float:
    return round(time.perf_counter() - started_at, 3)


if __name__ == "__main__":
    raise SystemExit(main())
