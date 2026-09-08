"""Pure helpers the address rules share with the retired information merge.

`merge_company_info`, the field values and the rest of the old publisher's rules went with
basic-info slice 4 (2026-09-08); the address model that once imported these helpers
(`address_rules.py`, `address_legacy.py`) retired in address slice 4b (2026-09-08). What
stays is the artifact row shape, the evidence-set hash and the text normaliser, still used by
`tests/test_se_company_info_rules.py`.
"""

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ArtifactRow:
    source: str
    source_record_uid: str
    evidence_hash: str
    observed_at: datetime
    values: Mapping[str, Any]


def evidence_set_hash_for(evidence_hashes: Sequence[str]) -> str:
    """Sha256 hex of the sorted hashes joined by ``\\n``.

    Must equal the address final's MATERIALIZED ``evidence_set_hash`` column:
    ``lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x),
    evidence_hashes)), '\\n'))))``. ``sorted()`` on strings matches ClickHouse's
    default ascending ``arraySort``, and ``hexdigest()`` is already lowercase.
    """
    return hashlib.sha256("\n".join(sorted(evidence_hashes)).encode()).hexdigest()


def _text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None
