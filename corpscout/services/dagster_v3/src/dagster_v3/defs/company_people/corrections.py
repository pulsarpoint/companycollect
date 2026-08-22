"""Human corrections and model suggestions for Sweden company people.

The ledger is append-only input to normalization and role materialization.
Nothing in this module edits published rows.
"""

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

DATABASE = "corpscout"
GROUP_NAME = "company_people"

CORRECTION_TABLE = "se_company_person_correction"
SUGGESTION_TABLE = "se_company_person_enrichment_observation"
QUALIFIED_CORRECTION_TABLE = f"{DATABASE}.{CORRECTION_TABLE}"
QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"

CORRECTION_COLUMNS = (
    "correction_id",
    "company_id",
    "correction_kind",
    "subject_person_id",
    "target_person_id",
    "draft_ids",
    "payload",
    "evidence_hash",
    "reason",
    "decided_by",
    "supersedes_correction_id",
    "created_at",
)

SUGGESTION_COLUMNS = (
    "suggestion_id",
    "company_id",
    "person_id",
    "input_hash",
    "draft_ids",
    "suggestion",
    "raw_response",
    "model_provider",
    "model_name",
    "prompt_version",
    "prompt_tokens",
    "completion_tokens",
    "source_run_id",
    "created_at",
)

PERSON_CORRECTION_KINDS = (
    "merge_persons",
    "reassign_draft",
    "split_person",
    "approve_suggestion",
    "reject_suggestion",
    "override_field",
)
ROLE_CORRECTION_KINDS = ("set_role", "remove_role")
UNDO_KIND = "undo"
CORRECTION_KINDS = frozenset((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, UNDO_KIND))
KIND_ORDER = {
    kind: index
    for index, kind in enumerate((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS))
}
ZERO_HASH = "0" * 64
