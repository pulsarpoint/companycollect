from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Literal, Self
from urllib.parse import quote

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


OBJECT_CATALOG_SCHEMA_VERSION = 2
OBJECT_CATALOG_REQUIRED_COLUMNS = (
    "schema_version",
    "source",
    "dataset",
    "partition_json",
    "source_run_id",
    "created_at",
    "object_key",
    "object_format",
    "size_bytes",
    "sha256",
    "row_count",
)

_STORAGE_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_OBJECT_FORMAT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ObjectCatalogLocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str
    dataset: str
    partition: dict[str, str]

    @field_validator("source", "dataset")
    @classmethod
    def validate_storage_name(cls, value: str) -> str:
        return _validated_storage_name(value)

    @field_validator("partition")
    @classmethod
    def validate_partition(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError(
                "object catalog partition must have at least one dimension"
            )

        validated: dict[str, str] = {}
        for dimension, dimension_value in value.items():
            validated_dimension = _validated_storage_name(dimension)
            if dimension_value == "":
                raise ValueError(
                    f"object catalog partition value must not be blank: {dimension}"
                )
            if dimension_value.strip() != dimension_value:
                raise ValueError(
                    "object catalog partition value must not have surrounding whitespace: "
                    f"{dimension}"
                )
            validated[validated_dimension] = dimension_value
        return dict(sorted(validated.items()))

    def partition_prefix(self) -> str:
        dimensions = "/".join(
            f"{dimension}={_encoded_key_value(value)}"
            for dimension, value in self.partition.items()
        )
        return f"v2/source={self.source}/dataset={self.dataset}/partition/{dimensions}/"

    def data_object_key(self, sha256: str, *, object_format: str) -> str:
        _validate_sha256(sha256)
        if _OBJECT_FORMAT_PATTERN.fullmatch(object_format) is None:
            raise ValueError(
                "object format must use lowercase letters, numbers, and dots only"
            )
        return f"{self.partition_prefix()}objects/sha256={sha256}.{object_format}"

    def catalog_object_key(self, source_run_id: str) -> str:
        return (
            f"{self.partition_prefix()}catalogs/"
            f"run_id={_encoded_nonblank_value(source_run_id, 'source run ID')}/"
            "catalog.parquet"
        )

    def commit_object_key(self) -> str:
        return f"{self.partition_prefix()}commit.json"


class ObjectCatalogFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1)
    sha256: str
    size_bytes: int = Field(ge=1)
    row_count: int = Field(ge=0)

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return _validate_sha256(value)


class ObjectCatalogCommit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = OBJECT_CATALOG_SCHEMA_VERSION
    location: ObjectCatalogLocation
    source_run_id: str
    created_at: AwareDatetime
    catalog: ObjectCatalogFile
    data_object_count: int = Field(ge=0)
    data_size_bytes: int = Field(ge=0)
    data_row_count: int | None = Field(default=None, ge=0)

    @field_validator("source_run_id")
    @classmethod
    def validate_source_run_id(cls, value: str) -> str:
        _encoded_nonblank_value(value, "source run ID")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_catalog_identity_and_totals(self) -> Self:
        expected_catalog_key = self.location.catalog_object_key(self.source_run_id)
        if self.catalog.key != expected_catalog_key:
            raise ValueError(
                "object catalog key must match its location and source run ID: "
                f"expected={expected_catalog_key} actual={self.catalog.key}"
            )
        if self.data_object_count != self.catalog.row_count:
            raise ValueError(
                "object catalog data object count must equal its catalog row count: "
                f"objects={self.data_object_count} rows={self.catalog.row_count}"
            )
        return self

    def to_json_bytes(self) -> bytes:
        payload = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return f"{payload}\n".encode()

    @classmethod
    def from_json_bytes(cls, body: bytes) -> ObjectCatalogCommit:
        return cls.model_validate_json(body)


def _validated_storage_name(value: str) -> str:
    if _STORAGE_NAME_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "object catalog value must be a lowercase storage name using only "
            "letters, numbers, underscores, and hyphens"
        )
    return value


def _validate_sha256(value: str) -> str:
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError("object catalog value must be a lowercase SHA-256 digest")
    return value


def _encoded_key_value(value: str) -> str:
    return quote(value, safe="-._~")


def _encoded_nonblank_value(value: str, label: str) -> str:
    if value == "":
        raise ValueError(f"object catalog {label} must not be blank")
    if value.strip() != value:
        raise ValueError(f"object catalog {label} must not have surrounding whitespace")
    return _encoded_key_value(value)
