from typing import Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from s3_store import S3Location, parse_s3_uri
from scanner import WebtechScannerSettings


class WebtechServiceSettings(BaseSettings):
    """Environment-owned scanner capacity, API, and RustFS settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_token: SecretStr = Field(
        min_length=20,
        validation_alias="WEBTECH_API_TOKEN",
    )
    s3_path: str = Field(validation_alias="WEBTECH_S3_PATH")
    s3_endpoint: str = Field(validation_alias="CORPSCOUT_S3_ENDPOINT")
    s3_access_key: SecretStr = Field(validation_alias="CORPSCOUT_S3_ACCESS_KEY")
    s3_secret_key: SecretStr = Field(validation_alias="CORPSCOUT_S3_SECRET_KEY")
    s3_region: str = Field(
        default="us-east-1",
        validation_alias="CORPSCOUT_S3_REGION",
    )
    headless: bool = Field(default=True, validation_alias="WEBTECH_HEADLESS")
    browser_count: int = Field(
        default=20,
        ge=1,
        le=20,
        validation_alias="WEBTECH_BROWSER_COUNT",
    )
    pages_per_browser: int = Field(
        default=1,
        ge=1,
        le=30,
        validation_alias="WEBTECH_PAGES_PER_BROWSER",
    )
    domain_timeout_seconds: float = Field(
        default=60.0,
        gt=0,
        validation_alias="WEBTECH_DOMAIN_TIMEOUT_SECONDS",
    )
    domains_per_context: int | None = Field(
        default=1,
        gt=0,
        validation_alias="WEBTECH_DOMAINS_PER_CONTEXT",
    )
    context_launch_interval_seconds: float = Field(
        default=0.25,
        ge=0,
        validation_alias="WEBTECH_CONTEXT_LAUNCH_INTERVAL_SECONDS",
    )
    progress_batch_size: int = Field(
        default=20,
        ge=1,
        le=1_000,
        validation_alias="WEBTECH_PROGRESS_BATCH_SIZE",
    )
    max_candidates: int = Field(
        default=1_000_000,
        ge=1,
        le=1_000_000,
        validation_alias="WEBTECH_MAX_CANDIDATES",
    )

    @field_validator("s3_endpoint")
    @classmethod
    def validate_s3_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
            raise ValueError("CORPSCOUT_S3_ENDPOINT must be an HTTP(S) URL")
        return value.rstrip("/")

    @model_validator(mode="after")
    def validate_scanner_capacity(self) -> Self:
        scanner_settings = self.scanner_settings()
        if scanner_settings.browser_count * scanner_settings.pages_per_browser > 30:
            raise ValueError(
                "WEBTECH_BROWSER_COUNT * WEBTECH_PAGES_PER_BROWSER must not exceed 30"
            )
        if (
            scanner_settings.domains_per_context is not None
            and scanner_settings.pages_per_browser
            > scanner_settings.domains_per_context
        ):
            raise ValueError(
                "WEBTECH_PAGES_PER_BROWSER must not exceed "
                "WEBTECH_DOMAINS_PER_CONTEXT"
            )
        return self

    @property
    def base_location(self) -> S3Location:
        return parse_s3_uri(self.s3_path)

    def scanner_settings(self) -> WebtechScannerSettings:
        return WebtechScannerSettings(
            headless=self.headless,
            browser_count=self.browser_count,
            pages_per_browser=self.pages_per_browser,
            domain_timeout_seconds=self.domain_timeout_seconds,
            domains_per_context=self.domains_per_context,
            context_launch_interval_seconds=self.context_launch_interval_seconds,
        )

    def public_scanner_settings(self) -> dict[str, object]:
        return {
            "headless": self.headless,
            "browser_count": self.browser_count,
            "pages_per_browser": self.pages_per_browser,
            "domain_timeout_seconds": self.domain_timeout_seconds,
            "domains_per_context": self.domains_per_context,
            "context_launch_interval_seconds": self.context_launch_interval_seconds,
        }
