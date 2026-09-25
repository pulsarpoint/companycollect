"""Encrypted credentials for LLM workers and service handoffs."""

import base64
import os
import re
from urllib.parse import urlsplit

import dagster as dg
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import ConfigDict, Field, model_validator


class EncryptedLLMConfig(dg.Config):
    """Credentials supplied by Backoffice, decrypted only by the worker making LLM calls."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    provider: str = Field(min_length=1, max_length=100)
    base_url: str = Field(min_length=1, max_length=2048)
    model: str = Field(min_length=1, max_length=200)
    api_key_encrypted: str = Field(
        pattern=r"^v1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{23,}$",
        max_length=16384,
        repr=False,
    )

    @model_validator(mode="after")
    def validate_profile(self):
        for value in (self.provider, self.model, self.base_url):
            if value != value.strip() or any(
                ord(char) < 32 or ord(char) == 127 for char in value
            ):
                raise ValueError(
                    "LLM profile fields cannot contain control characters or whitespace padding"
                )
        try:
            endpoint = urlsplit(self.base_url)
            endpoint.port  # urlsplit defers invalid-port validation until access.
        except ValueError:
            raise ValueError("LLM base_url must have a valid URL and port") from None
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or any(char.isspace() for char in self.base_url)
            or endpoint.username is not None
            or endpoint.password is not None
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError(
                "LLM base_url must be an HTTP(S) endpoint without credentials, query or fragment"
            )
        return self

    def decrypt_api_key(self) -> str:
        """Decrypt only at a direct model client boundary, never in forwarding assets."""
        key = os.getenv("CRAWLER_LLM_ENCRYPTION_KEY", "")
        if re.fullmatch(r"[A-Fa-f0-9]{64}", key) is None:
            raise ValueError("Configure the shared CRAWLER_LLM_ENCRYPTION_KEY on the Dagster worker before using saved LLM profiles")
        _, nonce_part, ciphertext_part = self.api_key_encrypted.split(".")
        try:
            nonce, ciphertext = [
                base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))
                for part in (nonce_part, ciphertext_part)
            ]
            aad = f"corpscout-crawler-llm:v1\0{self.provider}\0{self.base_url}\0{self.model}".encode()
            api_key = AESGCM(bytes.fromhex(key)).decrypt(nonce, ciphertext, aad).decode("utf-8")
        except (ValueError, InvalidTag):
            raise ValueError("Encrypted LLM credential could not be authenticated; check the shared key and selected model configuration") from None
        if not api_key.strip() or len(api_key.encode()) > 8192 or any(ord(char) < 32 or ord(char) == 127 for char in api_key):
            raise ValueError("Decrypted LLM credential is empty or invalid")
        return api_key


def redact_llm_error(error: Exception, client: object) -> str:
    """Provider errors may echo credentials; keep them out of saved errors and logs."""
    message = str(error)
    api_key = getattr(client, "api_key", None)
    return message.replace(api_key, "[redacted]") if isinstance(api_key, str) and api_key else message
