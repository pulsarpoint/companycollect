"""Authenticated LLM credentials carried through the queue without plaintext secrets."""

import asyncio
import base64
import json
import re
from urllib.parse import urlsplit

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import Field, field_validator

from browser_service.capture import StrictModel
from browser_service.challenge_agent import ACTION, FinishAction, completion_payload


class LLMProfileError(ValueError):
    """A safe, public explanation of a rejected encrypted credential."""


class EncryptedLLMProfile(StrictModel):
    provider: str = Field(min_length=1, max_length=100)
    base_url: str = Field(max_length=2048)
    model: str = Field(min_length=1, max_length=500)
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

    def decrypt_api_key(self, key: str | None) -> str:
        key = key or ""
        if re.fullmatch(r"[A-Fa-f0-9]{64}", key) is None:
            raise LLMProfileError("Browser LLM encryption is not configured correctly")
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


class VerifyLLMRequest(StrictModel):
    llm: EncryptedLLMProfile


async def verify_llm(profile: EncryptedLLMProfile, encryption_key: str | None) -> dict:
    try:
        api_key = profile.decrypt_api_key(encryption_key)
    except LLMProfileError as error:
        return {"ok": False, "error": str(error)}
    messages = [
        {
            "role": "system",
            "content": (
                "Inspect the image and return exactly one JSON action matching this schema. "
                "Set action to finish, outcome to appears_clear, and reason to the single "
                "English color name of the large rectangle at the image center. "
                "This is a harmless image capability check; do not take any browser actions. "
                + json.dumps(ACTION.json_schema())
            ),
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Inspect the center rectangle and return the requested JSON action.",
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64," + VERIFICATION_IMAGE,
                        "detail": "high",
                    },
                },
            ],
        },
    ]
    try:
        async with asyncio.timeout(30):
            async with httpx.AsyncClient(
                base_url=profile.base_url.rstrip("/") + "/",
                timeout=30,
                headers={"Authorization": f"Bearer {api_key}"},
            ) as http:
                response = await http.post(
                    "chat/completions",
                    json=completion_payload(
                        profile.model, messages, explicit_profile=True
                    ),
                )
                if response.is_error:
                    reason = f"Model provider rejected the browser verification request (HTTP {response.status_code})"
                    try:
                        detail = response.json().get("error", {}).get("message")
                    except (ValueError, AttributeError, TypeError):
                        detail = None
                    if isinstance(detail, str) and detail:
                        reason += ": " + detail
                    return {
                        "ok": False,
                        "error": reason.replace(api_key, "[REDACTED]")[:2000],
                    }
                document = response.json()
                choice = document["choices"][0]
                action = ACTION.validate_json(choice["message"]["content"])
                if (
                    choice["finish_reason"] == "stop"
                    and isinstance(action, FinishAction)
                    and action.outcome == "appears_clear"
                    and action.reason.strip().casefold() == "blue"
                ):
                    return {"ok": True}
                return {
                    "ok": False,
                    "error": "The selected model did not correctly interpret the image and return the required browser action JSON",
                }
    except TimeoutError:
        return {
            "ok": False,
            "error": "Browser LLM verification exceeded the 30 second deadline",
        }
    except Exception:
        # Provider errors and validation input can contain credentials or request bodies.
        return {
            "ok": False,
            "error": "Browser LLM verification failed: the selected model must accept images and return valid browser action JSON",
        }


# A blue rectangle on white, used only for a harmless vision-capability probe.
VERIFICATION_IMAGE = "iVBORw0KGgoAAAANSUhEUgAAAIAAAABgCAIAAABaGO0eAAAA3ElEQVR4nO3RgQmAQAADsd9/ad1AEISCl6MDFHIuTTvrA/UAjAMwDsA4AOMAjAMwDsA4AOMAjAMwDsA4AOMAjAMw7jXAOfY0AADaAwCgPQAA2gMAoD0AANoDAKA9AADaAwCgPQAA2gMAoD0AANoDAKA9AADaAwCgPQAA2gMAoD0AANoDAKA9AADaAwCgPQAA2gMAoD0AANoDAKA9AADaAwCgPQAA2gMAoD0AANoDAKA9AADaA/B3AH0bgHEAxgEYB2AcgHEAxgEYB2AcgHEAxgEYB2AcgHEAxgEYdwM2paWS6XhuegAAAABJRU5ErkJggg=="
