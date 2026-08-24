"""Lightweight producer/consumer contract for persisted ESEF artifacts."""

ARTIFACT_SCHEMA_VERSION = 5
# Schema v5 adds visible sections; the persisted fact shape is unchanged from v4.
SUPPORTED_FACT_ARTIFACT_SCHEMA_VERSIONS = (4, ARTIFACT_SCHEMA_VERSION)
