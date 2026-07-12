"""Frozen binary primitives used by logical catalog identities."""

import hashlib
import re
from typing import Any


_MAGIC = b"CCWIB-ID\x00"
_ENCODING_VERSION = 1
_SHA256 = re.compile(r"[0-9a-f]{64}")


def new_identity_digest(kind: str) -> Any:
    digest = hashlib.sha256()
    digest.update(_MAGIC)
    update_text(digest, kind)
    digest.update(_ENCODING_VERSION.to_bytes(2, byteorder="big", signed=False))
    return digest


def update_text(digest: Any, value: str) -> None:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFFFFFF:
        raise ValueError("identity text field exceeds the uint32 byte limit")
    digest.update(len(encoded).to_bytes(4, byteorder="big", signed=False))
    digest.update(encoded)


def decode_sha256(value: str, name: str) -> bytes:
    if _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 value")
    return bytes.fromhex(value)
