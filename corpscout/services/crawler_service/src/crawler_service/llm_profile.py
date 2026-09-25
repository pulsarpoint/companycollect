"""Authenticated LLM credentials carried through the queue without plaintext secrets."""

import asyncio
import base64
import re
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
    provider: str = Field(max_length=100)
    base_url: str = Field(max_length=2048)
    model: str = Field(max_length=500)
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
            if len(nonce) != 12 or len(ciphertext) <= 16:
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
        if not api_key.strip() or any(ord(char) < 32 for char in api_key):
            raise LLMProfileError("Decrypted LLM credential is empty or invalid")
        return api_key

    def crawl_config(self, config: ResearchConfig | None) -> ResearchConfig:
        # The selected profile owns model routing; never inherit the old provider.only.
        return ResearchConfig.model_validate(
            (config.model_dump(exclude_unset=True) if config is not None else {})
            | {"model": self.model, "provider": None, "reasoning_effort": None}
        )

    @property
    def api(self) -> str:
        # Provider is an operator label in backoffice, not necessarily an API family.
        host = urlsplit(self.base_url).hostname
        if host == "api.deepseek.com":
            return "deepseek"
        return "openrouter" if host == "openrouter.ai" else "openai"


class VerifyLLMRequest(StrictModel):
    llm: EncryptedLLMProfile


async def verify_llm(profile: EncryptedLLMProfile, environment: dict[str, str]) -> dict:
    try:
        api_key = profile.decrypt_api_key(environment)
    except LLMProfileError as error:
        return {"ok": False, "error": str(error)}
    config = profile.crawl_config(None).model_copy(
        update={
            "model_timeout_seconds": 30.0,
            "max_http_attempts": 1,
            "max_model_calls": 1,
            "max_output_tokens": 2048,
        }
    )
    llm = None
    try:
        async with asyncio.timeout(30):
            async with httpx.AsyncClient(
                base_url=profile.base_url.rstrip("/") + "/", timeout=30
            ) as client:
                llm = ModelClient(client, api_key, config, None, api=profile.api)
                reply = await llm.ask(
                    'Return JSON with exactly one field: {"ok": true}.',
                    {
                        "type": "object",
                        "properties": {"ok": {"type": "boolean", "const": True}},
                        "required": ["ok"],
                        "additionalProperties": False,
                    },
                    task="llm_verification",
                )
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
    if llm is not None and llm.calls:
        provider_error = llm.calls[-1].get("provider_error", {}).get("message")
        if isinstance(provider_error, str) and provider_error:
            error_message += ": " + provider_error
    return {"ok": False, "error": error_message.replace(api_key, "[REDACTED]")[:2000]}
