"""Authenticated LLM credentials carried through the queue without plaintext secrets."""

import asyncio
import base64
import json
import time
import re
from typing import Literal
from urllib.parse import urlsplit

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import Field, field_validator

from crawler_service.llm import ModelClient, ModelUnavailable
from crawler_service.models import ResearchConfig, StrictModel


class LLMProfileError(ValueError):
    """A safe, public explanation of a rejected encrypted credential."""


class EncryptedLLMProfile(StrictModel):
    profile_id: str | None = Field(default=None, exclude_if=lambda value: value is None)
    profile_revision: int | None = Field(default=None, ge=1, exclude_if=lambda value: value is None)
    provider: str = Field(max_length=100)
    base_url: str = Field(max_length=2048)
    model: str = Field(max_length=500)
    reasoning_effort: Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    api_key_encrypted: str = Field(max_length=16384, repr=False)

    @field_validator("provider", "model")
    @classmethod
    def identifier(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError(
                "LLM provider and model must not contain whitespace padding or control characters"
            )
        return value

    @field_validator("base_url")
    @classmethod
    def endpoint(cls, value: str) -> str:
        if (
            any(char.isspace() or ord(char) < 32 for char in value)
            or "?" in value
            or "#" in value
        ):
            raise ValueError(
                "LLM base URL must not contain whitespace, queries or fragments"
            )
        try:
            parsed = urlsplit(value)
            valid = parsed.scheme in {"http", "https"} and parsed.hostname is not None
            valid = valid and parsed.username is None and parsed.password is None
            _ = parsed.port  # Access validates malformed or out-of-range ports.
        except ValueError as error:
            raise ValueError(
                "LLM base URL must be a valid HTTP or HTTPS URL"
            ) from error
        if not valid:
            raise ValueError(
                "LLM base URL must be HTTP or HTTPS without embedded credentials"
            )
        return value

    def decrypt_api_key(self, environment: dict[str, str]) -> str:
        key = environment.get("CRAWLER_LLM_ENCRYPTION_KEY", "")
        if re.fullmatch(r"[A-Fa-f0-9]{64}", key) is None:
            raise LLMProfileError("Crawler LLM encryption is not configured correctly")
        parts = self.api_key_encrypted.split(".")
        if len(parts) != 3 or parts[0] != "v1":
            raise LLMProfileError("Invalid encrypted LLM credential format")
        if any(re.fullmatch(r"[A-Za-z0-9_-]+", part) is None for part in parts[1:]):
            raise LLMProfileError("Invalid encrypted LLM credential format")
        try:
            nonce, ciphertext = [
                base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))
                for part in parts[1:]
            ]
            if len(nonce) != 12 or len(ciphertext) < 16:
                raise LLMProfileError("Invalid encrypted LLM credential format")
            aad = (
                "corpscout-crawler-llm:v1\0"
                + self.provider
                + "\0"
                + self.base_url
                + "\0"
                + self.model
            ).encode("utf-8")
            plaintext = AESGCM(bytes.fromhex(key)).decrypt(nonce, ciphertext, aad)
            api_key = plaintext.decode("utf-8")
        except (ValueError, InvalidTag) as error:
            raise LLMProfileError(
                "Encrypted LLM credential could not be authenticated; check the shared encryption key and model configuration"
            ) from error
        if (api_key != "" and not api_key.strip()) or len(api_key.encode()) > 8192 or any(ord(char) < 32 or ord(char) == 127 for char in api_key):
            raise LLMProfileError("Decrypted LLM credential is invalid")
        return api_key

    def crawl_config(self, config: ResearchConfig | None) -> ResearchConfig:
        if self.is_decision_model:
            raise LLMProfileError("Jev is a typed decision model; choose a text-generation model for crawl processing")
        # The selected profile owns model routing; never inherit the old provider.only.
        return ResearchConfig.model_validate(
            (config.model_dump(exclude_unset=True) if config is not None else {})
            | {"model": self.model, "provider": None, "reasoning_effort": self.reasoning_effort}
        )

    @property
    def is_decision_model(self) -> bool:
        return re.match(r"^typesafe/jev-\d", self.model) is not None or self.model == "~typesafe/jev-latest"

    @property
    def api(self) -> str:
        # Provider is an operator label in backoffice, not necessarily an API family.
        host = urlsplit(self.base_url).hostname
        if host == "api.deepseek.com":
            return "deepseek"
        return "openrouter" if host == "openrouter.ai" else "openai"


class VerifyLLMRequest(StrictModel):
    llm: EncryptedLLMProfile
    preview_only: bool = False
    include_exchange: bool = False


async def verify_llm(
    profile: EncryptedLLMProfile, environment: dict[str, str], *,
    preview_only: bool = False, include_exchange: bool = False,
) -> dict:
    try:
        api_key = profile.decrypt_api_key(environment)
    except LLMProfileError as error:
        return {"ok": False, "error": str(error), "failure_kind": "service"}
    exchange = {"request": None, "response": None, "elapsed_ms": None}

    async def capture_request(request: httpx.Request) -> None:
        exchange["request"] = {
            "method": request.method, "url": str(request.url),
            "body": json.loads(await request.aread()),
        }

    async def capture_response(response: httpx.Response) -> None:
        await response.aread()
        exchange["response"] = {
            "status": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "body": response.text,
        }

    started = time.monotonic()
    async with httpx.AsyncClient(
        base_url=profile.base_url.rstrip("/") + "/", timeout=30,
        event_hooks={"request": [capture_request], "response": [capture_response]}
            if include_exchange and not preview_only else None,
    ) as client:
        if profile.is_decision_model:
            result = await verify_jev(profile, api_key, client=client, preview_only=preview_only)
        else:
            result = await verify_completion(profile, api_key, client, preview_only=preview_only)
    if include_exchange and not preview_only:
        exchange["elapsed_ms"] = round((time.monotonic() - started) * 1000)
        result["exchange"] = exchange
    # Redact known credentials even when a provider echoes them inside nested JSON
    # strings, error pages, or response metadata. No request headers are captured.
    def redact(value):
        if isinstance(value, str):
            for secret in (api_key, profile.api_key_encrypted):
                if secret:
                    value = value.replace(secret, "[REDACTED]")
                    value = value.replace(json.dumps(secret)[1:-1], "[REDACTED]")
            return value
        if isinstance(value, dict):
            return {redact(key): redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value
    return redact(result)


async def verify_completion(
    profile: EncryptedLLMProfile, api_key: str, client: httpx.AsyncClient, *, preview_only: bool,
) -> dict:
    config = profile.crawl_config(None).model_copy(
        update={
            "model_timeout_seconds": 30.0,
            "max_http_attempts": 1,
            "max_model_calls": 1,
            "max_output_tokens": 8192 if profile.reasoning_effort not in {None, "none"} else 2048,
        }
    )
    llm = ModelClient(client, api_key, config, None, api=profile.api)
    prompt = 'Return JSON with exactly one field: {"ok": true}.'
    schema = {
        "type": "object",
        "properties": {"ok": {"type": "boolean", "const": True}},
        "required": ["ok"],
        "additionalProperties": False,
    }
    if preview_only:
        return {"ok": True, "exchange": {"request": {
            "method": "POST", "url": str(client.base_url.join("chat/completions")),
            "body": llm.request_payload(schema, messages=llm.initial_messages(prompt, schema), catalog=None),
        }, "response": None, "elapsed_ms": None}}
    try:
        async with asyncio.timeout(30):
            reply = await llm.ask(prompt, schema, task="llm_verification")
        if reply.error is None and reply.document == {"ok": True}:
            return {"ok": True}
        error_message = (
            reply.error or "The model did not return the required JSON response"
        )
    except ModelUnavailable as error:
        error_message = str(error)
    except TimeoutError:
        error_message = "LLM verification exceeded the 30 second deadline"
    except Exception:
        # Provider/library errors may contain request headers; do not expose them.
        error_message = (
            "LLM verification failed while contacting the configured endpoint"
        )
    status = None
    if llm is not None and llm.calls:
        status = llm.calls[-1].get("http_status")
        provider_error = llm.calls[-1].get("provider_error", {}).get("message")
        if isinstance(provider_error, str) and provider_error:
            error_message += ": " + provider_error
    return {"ok": False, "error": (error_message.replace(api_key, "[REDACTED]") if api_key else error_message)[:2000],
            "failure_kind": "configuration" if status in {401,403,404} else
                "transient" if status is None or status == 429 or status >= 500 else "capability"}


async def verify_jev(
    profile: EncryptedLLMProfile, api_key: str, *,
    client: httpx.AsyncClient, preview_only: bool,
) -> dict:
    if profile.base_url.rstrip("/") not in {"https://openrouter.ai/api", "https://openrouter.ai/api/v1"}:
        return {"ok": False, "failure_kind": "capability", "error": "Jev requires the OpenRouter Decisions API"}
    if profile.reasoning_effort is not None:
        return {"ok": False, "failure_kind": "capability", "error": "Reasoning effort does not apply to Jev decisions"}
    url = "https://openrouter.ai/api/alpha/decisions"
    body = {"model": profile.model, "state": {"status": "ready"}, "questions": {
        "status": {"type": "choice", "instructions": "Read the status field in the state.",
                   "criteria": {"ready": "The status is ready.", "not_ready": "The status is not ready."}}
    }}
    if preview_only:
        return {"ok": True, "exchange": {"request": {"method": "POST", "url": url, "body": body},
                                       "response": None, "elapsed_ms": None}}
    try:
        async with asyncio.timeout(30):
            response = await client.post(
                url, headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, json=body,
            )
            if response.is_error:
                return {"ok": False, "error": f"Jev provider rejected verification (HTTP {response.status_code})",
                        "failure_kind": "configuration" if response.status_code in {401, 403, 404} else
                        "transient" if response.status_code == 429 or response.status_code >= 500 else "capability"}
            answer = response.json().get("answers", {}).get("status", {})
            if answer.get("type") == "choice" and answer.get("choice") == "ready":
                return {"ok": True}
            return {"ok": False, "failure_kind": "capability", "error": "Jev did not return the expected typed decision"}
    except (TimeoutError, httpx.HTTPError):
        return {"ok": False, "failure_kind": "transient", "error": "Jev verification could not complete within 30 seconds"}
    except (ValueError, AttributeError, TypeError):
        return {"ok": False, "failure_kind": "capability", "error": "Jev returned an invalid Decisions API response"}
