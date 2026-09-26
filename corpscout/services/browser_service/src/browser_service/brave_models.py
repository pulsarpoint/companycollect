"""Public contracts for individual Brave requests and durable company batches."""

from typing import Literal
from uuid import UUID, uuid5

from pydantic import Field, field_validator, model_serializer, model_validator

from browser_service.capture import StrictModel
from browser_service.llm_profile import EncryptedLLMProfile


class BraveOptions(StrictModel):
    max_requests_per_browser: int = Field(default=10, ge=1, le=1000, strict=True)
    headless: bool | None = Field(default=None, strict=True)
    page_timeout_seconds: float = Field(default=60, gt=0, le=300)
    answer_timeout_seconds: float = Field(default=180, gt=0, le=600)
    timeout_seconds: float = Field(default=900, gt=0, le=1800)
    challenge_agent_max_runs: int = Field(default=3, ge=0, le=1000, strict=True)
    challenge_agent_model: Literal["deepseek-flash", "z-ai/glm-5.3-flash"] = (
        "deepseek-flash"
    )
    llm: EncryptedLLMProfile | None = None

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        value = handler(self)
        if self.llm is None:
            value.pop("llm", None)
        return value


class BraveAskRequest(BraveOptions):
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    session_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")
    query: str = Field(min_length=1, max_length=10000)
    route: Literal["direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3"] = "direct"

    @field_validator("query")
    @classmethod
    def nonempty_query(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Query must contain text")
        return value


class BraveBatchItem(StrictModel):
    input_id: str = Field(min_length=1, max_length=1024)
    country_code: str = Field(pattern=r"^[A-Z]{2}$")
    company_id: str = Field(min_length=1, max_length=512)
    company_name: str = Field(min_length=1, max_length=10000)
    result_id: UUID
    query: str = Field(min_length=1, max_length=10000)

    @field_validator("company_name", "query")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Value must contain text")
        return value


class BraveBatchRequest(StrictModel):
    batch_id: UUID
    task_id: UUID
    execution_id: UUID
    source_run_id: UUID
    owner_request_id: UUID
    query_type: str = Field(min_length=1, max_length=200)
    processor_version: str = Field(min_length=1, max_length=100)
    search_id: str = Field(default="", max_length=100)
    search_name: str = Field(default="", max_length=120)
    search_revision: int = Field(default=0, ge=0)
    requests_per_route: int = Field(default=1, ge=1, le=8, strict=True)
    options: BraveOptions
    items: list[BraveBatchItem] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def identities(self):
        if (
            self.options.llm is None
            or not self.options.llm.profile_id
            or not self.options.llm.profile_revision
        ):
            raise ValueError("Batches require a registered LLM profile and revision")
        if len({item.input_id for item in self.items}) != len(self.items):
            raise ValueError("Batch input IDs must be unique")
        if any(
            item.result_id != uuid5(self.execution_id, item.input_id)
            for item in self.items
        ):
            raise ValueError("Result IDs must match the execution and input ID")
        return self


class BraveBatchController(StrictModel):
    controller_id: UUID
    owner_request_id: UUID
