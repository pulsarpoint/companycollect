"""Validated opaque LLM credentials for crawler and browser service requests."""

from urllib.parse import urlsplit

import dagster as dg
from pydantic import ConfigDict, Field, model_validator


class EncryptedLLMConfig(dg.Config):
    """Opaque credentials supplied by Backoffice; only the destination service can decrypt them."""

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
