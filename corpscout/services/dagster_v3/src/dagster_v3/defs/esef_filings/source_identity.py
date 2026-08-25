from hashlib import sha256


SOURCE_RECORD_IDENTITY_VERSION = "company-source-record-v1"


def file_source_record_uid(*, record_kind: str, content_sha256: str) -> str:
    normalized_hash = _validated_sha256(content_sha256)
    identity = "\n".join(
        (SOURCE_RECORD_IDENTITY_VERSION, "file", record_kind, normalized_hash)
    )
    return sha256(identity.encode("utf-8")).hexdigest()


def _validated_sha256(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("ESEF source content hash must be SHA-256 hex")
    return normalized
