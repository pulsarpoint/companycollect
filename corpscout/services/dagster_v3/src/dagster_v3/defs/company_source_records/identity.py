from hashlib import sha256
import re


SOURCE_RECORD_IDENTITY_VERSION = "company-source-record-v1"
_SQL_TOKEN = re.compile(r"^[a-z0-9_]+$")


def file_source_record_uid(*, record_kind: str, content_sha256: str) -> str:
    normalized_hash = _validated_sha256(content_sha256)
    return _uid("file", record_kind, normalized_hash)


def structured_source_record_uid(
    *,
    source_slug: str,
    record_kind: str,
    source_record_key: str,
    payload_sha256: str,
) -> str:
    normalized_hash = _validated_sha256(payload_sha256)
    if source_slug.strip() == "":
        raise ValueError("Company source-record source_slug must not be empty")
    if source_record_key.strip() == "":
        raise ValueError("Company source-record source_record_key must not be empty")
    return _uid(
        "structured",
        source_slug.strip(),
        record_kind,
        source_record_key.strip(),
        normalized_hash,
    )


def observation_uid(
    *,
    source_record_uid: str,
    observation_kind: str,
    natural_key: str,
) -> str:
    if len(source_record_uid) != 64:
        raise ValueError("Company source_record_uid must be a 64-character SHA-256")
    return _uid("observation", source_record_uid, observation_kind, natural_key)


def clickhouse_file_source_record_uid_sql(
    *, record_kind: str, content_sha256_expression: str
) -> str:
    _validate_sql_token(record_kind)
    return (
        "lower(hex(SHA256(concat("
        f"'{SOURCE_RECORD_IDENTITY_VERSION}\\nfile\\n{record_kind}\\n', "
        f"lowerUTF8(toString({content_sha256_expression}))"
        "))))"
    )


def clickhouse_structured_source_record_uid_sql(
    *,
    source_slug: str,
    record_kind: str,
    source_record_key_expression: str,
    payload_sha256_expression: str,
) -> str:
    _validate_sql_token(source_slug)
    _validate_sql_token(record_kind)
    return (
        "lower(hex(SHA256(concat("
        f"'{SOURCE_RECORD_IDENTITY_VERSION}\\nstructured\\n{source_slug}\\n"
        f"{record_kind}\\n', toString({source_record_key_expression}), '\\n', "
        f"lowerUTF8(toString({payload_sha256_expression}))"
        "))))"
    )


def clickhouse_observation_uid_sql(
    *,
    source_record_uid_expression: str,
    observation_kind: str,
    natural_key_expression: str,
) -> str:
    _validate_sql_token(observation_kind)
    return (
        "lower(hex(SHA256(concat("
        f"'{SOURCE_RECORD_IDENTITY_VERSION}\\nobservation\\n', "
        f"toString({source_record_uid_expression}), "
        f"'\\n{observation_kind}\\n', toString({natural_key_expression})"
        "))))"
    )


def _uid(*parts: str) -> str:
    body = "\n".join((SOURCE_RECORD_IDENTITY_VERSION, *parts))
    return sha256(body.encode("utf-8")).hexdigest()


def _validated_sha256(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("Company source-record content hash must be SHA-256 hex")
    return normalized


def _validate_sql_token(value: str) -> None:
    if _SQL_TOKEN.fullmatch(value) is None:
        raise ValueError(f"Unsafe company source-record SQL identity token: {value!r}")
