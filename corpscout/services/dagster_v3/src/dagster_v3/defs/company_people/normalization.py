"""Publish Sweden company-person profiles from immutable source observations.

The processing boundary is a company. A changed company backed by one source is
resolved deterministically. A changed company backed by several sources is sent
to the LLM in one request unless its observation count exceeds the configured
request size. Large companies are partitioned by role and oversized role groups
are chunked; no source observation is dropped.
"""

import hashlib
import json
import re
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from openai import OpenAI
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_people.corrections import (
    CORRECTION_TABLE,
    PERSON_CORRECTION_KINDS,
    QUALIFIED_SUGGESTION_TABLE,
    SUGGESTION_COLUMNS,
    SUGGESTION_TABLE,
    ZERO_HASH,
    PersonCorrection,
    StoredSuggestion,
    build_company_corrections_sql,
    build_company_suggestions_sql,
    correction_from_row,
    effective_company_corrections_cte,
    effective_corrections,
    suggestion_from_row,
)
from dagster_v3.defs.company_people.identity_eval import (
    PersonObservationRow,
    identity_key_k2,
    k3_merge_groups,
)
from dagster_v3.defs.company_people.roles import (
    canonical_role_code,
    source_role_code,
)
from dagster_v3.defs.company_people.source_views import (
    SE_COMPANY_PERSON_BOLAGSVERKET_VIEW,
    SE_COMPANY_PERSON_ESEF_VIEW,
    SE_COMPANY_PERSON_WIKIDATA_VIEW,
    build_se_company_person_blank_full_name_count_sql,
    build_se_company_person_source_observations_sql,
    normalized_company_ids,
)
from dagster_v3.defs.esef_filings.llm_enrichment import deepseek_settings

DATABASE = "corpscout"
GROUP_NAME = "se_company_person"

PERSON_TABLE = "se_company_person"
QUALIFIED_PERSON_TABLE = f"{DATABASE}.{PERSON_TABLE}"

# Unqualified names of the three source views normalization reads (source_views.py,
# migrations 000330/000331) -- se_company_person_draft is retired from this read path
# (Task 3); assert_clickhouse_tables_exist checks system.tables by bare name, and views
# appear there exactly like tables.
SOURCE_VIEW_TABLES = tuple(
    view.removeprefix(f"{DATABASE}.")
    for view in (
        SE_COMPANY_PERSON_BOLAGSVERKET_VIEW,
        SE_COMPANY_PERSON_ESEF_VIEW,
        SE_COMPANY_PERSON_WIKIDATA_VIEW,
    )
)

PERSON_COLUMNS = (
    "person_id",
    "company_id",
    "name",
    "description",
    "draft_ids",
    "correction_ids",
    "suggestion_id",
    "merged_into_person_id",
    "model_provider",
    "model_name",
    "prompt_version",
    "source_run_id",
    "created_at",
    "updated_at",
)

PROMPT_VERSION = "se-company-people-v2"
DIRECT_PROMPT_VERSION = "single-source-copy-v2"
MAX_OUTPUT_TOKENS = 4_000
MAX_CONTRACT_ATTEMPTS = 3

_ROLE_SEPARATOR_PATTERN = re.compile(r"[^a-z0-9]+")

_ROLE_ALIASES = {
    "audit_partner": "audit_partner",
    "auditor": "auditor",
    "board_chair": "board_chair",
    "board_chairman": "board_chair",
    "board_member": "board_member",
    "ceo": "chief_executive_officer",
    "chair": "board_chair",
    "chairman": "board_chair",
    "chief_executive": "chief_executive_officer",
    "chief_executive_officer": "chief_executive_officer",
    "chief_financial_officer": "chief_financial_officer",
    "cfo": "chief_financial_officer",
    "deputy_board_member": "deputy_board_member",
    "deputy_chief_executive": "deputy_chief_executive_officer",
    "deputy_chief_executive_officer": "deputy_chief_executive_officer",
    "director": "board_member",
    "executive": "executive",
    "founder": "founder",
    "liquidator": "liquidator",
    "owner": "owner",
}


@dataclass(frozen=True)
class DraftPersonObservation:
    draft_id: uuid.UUID
    source: str
    source_record_uid: str
    fiscal_year: int | None
    source_observed_at: datetime
    source_value: Mapping[str, Any]


@dataclass(frozen=True)
class ExistingPersonProfile:
    person_id: uuid.UUID
    name: str
    description: str | None
    draft_ids: tuple[uuid.UUID, ...]
    created_at: datetime
    draft_set_hash: str = ""
    merged_into_person_id: uuid.UUID | None = None
    correction_ids: tuple[uuid.UUID, ...] = ()
    suggestion_id: uuid.UUID | None = None


@dataclass(frozen=True)
class CompanyPersonStatus:
    company_id: str
    source_count: int
    observation_count: int
    draft_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True)
class CompanyPersonWork:
    status: CompanyPersonStatus
    observations: tuple[DraftPersonObservation, ...]
    previous_profiles: tuple[ExistingPersonProfile, ...]
    suggestions: tuple[StoredSuggestion, ...] = ()
    corrections: tuple[PersonCorrection, ...] = ()

    @property
    def requires_llm(self) -> bool:
        return self.status.source_count > 1


@dataclass(frozen=True)
class CompanyObservationBatch:
    role_bucket: str
    batch_index: int
    batch_count: int
    observations: tuple[DraftPersonObservation, ...]


@dataclass(frozen=True)
class LlmCompanyPeopleResult:
    response: "LlmCompanyPeopleResponse"
    model_provider: str
    model_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    contract_retry_count: int = 0
    input_hash: str = ""
    raw_response: str = ""
    reused: bool = False


@dataclass(frozen=True)
class PersonProfileWrite:
    person_id: uuid.UUID
    company_id: str
    name: str
    description: str | None
    draft_ids: tuple[uuid.UUID, ...]
    model_provider: str
    model_name: str
    prompt_version: str
    created_at: datetime
    suggestion_id: uuid.UUID | None = None
    correction_ids: tuple[uuid.UUID, ...] = ()
    merged_into_person_id: uuid.UUID | None = None


@dataclass(frozen=True)
class SuggestionWrite:
    suggestion_id: uuid.UUID
    company_id: str
    person_id: uuid.UUID
    input_hash: str
    draft_ids: tuple[uuid.UUID, ...]
    suggestion_json: str
    raw_response: str
    model_provider: str
    model_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    created_at: datetime


@dataclass
class _ProfileAccumulator:
    person_id: uuid.UUID
    name: str
    description: str | None
    draft_ids: set[uuid.UUID]
    created_at: datetime
    model_provider: str
    model_name: str
    prompt_version: str
    touched: bool = False
    suggestion_id: uuid.UUID | None = None
    merged_into_person_id: uuid.UUID | None = None
    correction_ids: list[uuid.UUID] = field(default_factory=list)


class LlmCompanyPersonSuggestion(BaseModel):
    """One normalized person supported by draft IDs from the current request."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    existing_person_id: uuid.UUID | None = None
    name: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=1_000)
    draft_ids: list[uuid.UUID] = Field(min_length=1)

    @field_validator("draft_ids")
    @classmethod
    def draft_ids_are_unique(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(value) != len(set(value)):
            raise ValueError("draft_ids must be unique within a person")
        return value


class LlmCompanyPeopleResponse(BaseModel):
    """Validated top-level response returned by company-person normalization."""

    model_config = ConfigDict(extra="forbid")

    people: list[LlmCompanyPersonSuggestion] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def assignments_are_unique(self) -> "LlmCompanyPeopleResponse":
        draft_ids = [
            draft_id for person in self.people for draft_id in person.draft_ids
        ]
        if len(draft_ids) != len(set(draft_ids)):
            raise ValueError("a draft_id may be assigned to only one person")
        existing_ids = [
            person.existing_person_id
            for person in self.people
            if person.existing_person_id is not None
        ]
        if len(existing_ids) != len(set(existing_ids)):
            raise ValueError("an existing_person_id may appear only once per response")
        return self


# (company_id, batch, previous_profiles, request) -> result. The fourth argument
# is the request the caller already built and hashed; the suggester forwards it
# unchanged so the recorded input_hash is exactly the one looked up.
CompanyLlmSuggester = Callable[
    [str, CompanyObservationBatch, tuple[ExistingPersonProfile, ...], dict[str, Any]],
    LlmCompanyPeopleResult,
]


def _qualified(table: str) -> str:
    return f"`{DATABASE}`.`{table}`"


def _insert_columns(columns: Sequence[str]) -> str:
    return ",\n    ".join(columns)


def _company_status_ctes() -> str:
    """Company evidence: the source-view observations plus the live ledger rows applied to it.

    A company is unchanged only when both match what is already published, so an
    appended correction is itself changed evidence for exactly that company. The observation
    read is the shared ``source_observations`` CTE (source_views.py) over the three SE person
    views -- the ``se_company_person_draft`` inbox this used to read is retired from this
    path (Task 3); ``draft_id`` is now computed inline from each view row's own identity
    fields rather than looked up from a materialized draft table.
    """
    return f"""{build_se_company_person_source_observations_sql()},
draft_companies AS (
    SELECT
        company_id,
        uniqExact(source) AS source_count,
        count() AS observation_count,
        arraySort(groupUniqArray(draft_id)) AS draft_ids
    FROM source_observations
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
published_companies AS (
    SELECT
        company_id,
        arraySort(arrayDistinct(arrayFlatten(groupArray(draft_ids)))) AS draft_ids,
        arraySort(arrayDistinct(arrayFlatten(groupArray(
            arrayMap(id -> toString(id), correction_ids)
        )))) AS correction_ids
    FROM corpscout.se_company_person FINAL
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
{effective_company_corrections_cte()},
company_status AS (
    SELECT
        -- Named one by one on purpose. After the second `LEFT JOIN … USING`, a
        -- star projection of the left side stops carrying the join key into the
        -- outer scope, and selecting company_id from company_status then dies
        -- with UNKNOWN_IDENTIFIER on ClickHouse 26.5.
        drafts.company_id AS company_id,
        drafts.source_count AS source_count,
        drafts.observation_count AS observation_count,
        drafts.draft_ids AS draft_ids,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids
            AND published.correction_ids = corrections.correction_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
    LEFT JOIN effective_company_corrections AS corrections USING (company_id)
)"""


def build_company_statistics_sql() -> str:
    return f"""WITH {_company_status_ctes()}
SELECT
    count() AS company_count,
    countIf(is_unchanged) AS skipped_company_count,
    countIf(NOT is_unchanged AND source_count = 1) AS pending_direct_company_count,
    countIf(NOT is_unchanged AND source_count > 1) AS pending_llm_company_count
FROM company_status"""


def build_pending_companies_sql() -> str:
    return f"""WITH {_company_status_ctes()}
SELECT company_id, source_count, observation_count, draft_ids
FROM company_status
WHERE NOT is_unchanged
  AND company_id > %(after_company_id)s
ORDER BY company_id
LIMIT %(max_companies)s"""


def build_company_observations_sql() -> str:
    return f"""WITH {build_se_company_person_source_observations_sql()}
SELECT
    draft_id,
    company_id,
    source,
    source_record_uid,
    fiscal_year,
    source_observed_at,
    source_value_json
FROM source_observations
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, source, fiscal_year, source_observed_at, draft_id"""


def build_existing_profiles_sql() -> str:
    return """SELECT
    person_id,
    company_id,
    name,
    description,
    draft_ids,
    created_at,
    toString(draft_set_hash),
    merged_into_person_id,
    correction_ids,
    suggestion_id
FROM corpscout.se_company_person FINAL
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, person_id"""


def _source_value(value: object) -> Mapping[str, Any]:
    parsed = json.loads(str(value))
    if not isinstance(parsed, dict):
        raise ValueError("Company-person draft source_value_json must be an object")
    return parsed


def _status_from_row(row: Sequence[Any]) -> CompanyPersonStatus:
    return CompanyPersonStatus(
        company_id=str(row[0]),
        source_count=int(row[1]),
        observation_count=int(row[2]),
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[3])),
    )


def _observation_from_row(row: Sequence[Any]) -> tuple[str, DraftPersonObservation]:
    company_id = str(row[1])
    return company_id, DraftPersonObservation(
        draft_id=uuid.UUID(str(row[0])),
        source=str(row[2]),
        source_record_uid=str(row[3]),
        fiscal_year=int(row[4]) if row[4] is not None else None,
        source_observed_at=row[5],
        source_value=_source_value(row[6]),
    )


def _profile_from_row(row: Sequence[Any]) -> tuple[str, ExistingPersonProfile]:
    company_id = str(row[1])
    return company_id, ExistingPersonProfile(
        person_id=uuid.UUID(str(row[0])),
        name=str(row[2]),
        description=str(row[3]) if row[3] is not None else None,
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[4])),
        created_at=row[5],
        draft_set_hash=str(row[6]),
        merged_into_person_id=None if row[7] is None else uuid.UUID(str(row[7])),
        correction_ids=tuple(uuid.UUID(str(value)) for value in row[8]),
        suggestion_id=None if row[9] is None else uuid.UUID(str(row[9])),
    )


def _role_token(value: object) -> str:
    return _ROLE_SEPARATOR_PATTERN.sub("_", str(value).strip().casefold()).strip("_")


def observation_role_bucket(observation: DraftPersonObservation) -> str:
    """Return a stable canonical-ish role bucket for request partitioning."""
    value = observation.source_value
    mapped_role_code = canonical_role_code(observation.source, value)
    if mapped_role_code is not None:
        return mapped_role_code

    candidates = (
        source_role_code(observation.source, value),
        value.get("role_kind"),
        value.get("role_category"),
        value.get("role_property"),
        value.get("role_label"),
        value.get("role"),
        value.get("role_original"),
    )
    tokens = [_role_token(candidate) for candidate in candidates if candidate]
    for token in tokens:
        if token in _ROLE_ALIASES:
            return _ROLE_ALIASES[token]

    combined = "_".join(tokens)
    if "audit" in combined:
        return "auditor"
    if "chief_executive" in combined or "ceo" in combined:
        return "chief_executive_officer"
    if "chief_financial" in combined or "cfo" in combined:
        return "chief_financial_officer"
    if "deputy" in combined and "board" in combined:
        return "deputy_board_member"
    if "chair" in combined:
        return "board_chair"
    if "board" in combined or "director" in combined:
        return "board_member"
    if "executive" in combined or "management" in combined:
        return "executive"
    if "liquidat" in combined:
        return "liquidator"
    if "founder" in combined:
        return "founder"
    if "owner" in combined:
        return "owner"
    return "other"


def _observation_sort_key(
    observation: DraftPersonObservation,
) -> tuple[str, str, int, datetime, str]:
    return (
        observation_role_bucket(observation),
        observation.source,
        observation.fiscal_year or 0,
        observation.source_observed_at,
        str(observation.draft_id),
    )


def batch_company_observations(
    observations: Sequence[DraftPersonObservation],
    *,
    maximum_observations_per_request: int,
) -> tuple[CompanyObservationBatch, ...]:
    """Create bounded requests while retaining every observation exactly once."""
    if maximum_observations_per_request < 1:
        raise ValueError("maximum_observations_per_request must be positive")
    if not observations:
        raise ValueError("A company must have at least one draft observation")

    ordered = tuple(sorted(observations, key=_observation_sort_key))
    if len(ordered) <= maximum_observations_per_request:
        return (
            CompanyObservationBatch(
                role_bucket="all",
                batch_index=1,
                batch_count=1,
                observations=ordered,
            ),
        )

    by_role: dict[str, list[DraftPersonObservation]] = defaultdict(list)
    for observation in ordered:
        by_role[observation_role_bucket(observation)].append(observation)

    raw_batches: list[tuple[str, tuple[DraftPersonObservation, ...]]] = []
    for role_bucket in sorted(by_role):
        role_observations = by_role[role_bucket]
        for offset in range(
            0, len(role_observations), maximum_observations_per_request
        ):
            raw_batches.append(
                (
                    role_bucket,
                    tuple(
                        role_observations[
                            offset : offset + maximum_observations_per_request
                        ]
                    ),
                )
            )

    batch_count = len(raw_batches)
    return tuple(
        CompanyObservationBatch(
            role_bucket=role_bucket,
            batch_index=index,
            batch_count=batch_count,
            observations=batch_observations,
        )
        for index, (role_bucket, batch_observations) in enumerate(raw_batches, start=1)
    )


def build_company_people_request(
    *,
    company_id: str,
    batch: CompanyObservationBatch,
    previous_profiles: Sequence[ExistingPersonProfile],
    model: str,
) -> dict[str, Any]:
    """Format one JSON-only DeepSeek request for a company observation batch."""
    request_input = {
        "company_id": company_id,
        "batch": {
            "role_bucket": batch.role_bucket,
            "batch_index": batch.batch_index,
            "batch_count": batch.batch_count,
        },
        "previous_profiles": [
            {
                "person_id": str(profile.person_id),
                "name": profile.name,
                "description": profile.description,
                "draft_ids": [str(draft_id) for draft_id in profile.draft_ids],
            }
            for profile in sorted(
                previous_profiles, key=lambda item: str(item.person_id)
            )
        ],
        "source_observations": [
            {
                "draft_id": str(observation.draft_id),
                "source": observation.source,
                "role_bucket": observation_role_bucket(observation),
                "fiscal_year": observation.fiscal_year,
                "source_observed_at": observation.source_observed_at.isoformat(),
                "value": observation.source_value,
            }
            for observation in batch.observations
        ],
    }
    return {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Resolve people for one company using only source_observations. "
                    "Return exactly one JSON object shaped as "
                    '{"people":[{"existing_person_id":uuid-or-null,'
                    '"name":string,"description":string-or-null,'
                    '"draft_ids":[uuid,...]}]}. Group observations that describe '
                    "the same natural person. Every input draft_id must occur exactly "
                    "once in the response, with no additional IDs. Set "
                    "existing_person_id only when the person matches a supplied "
                    "previous profile, and copy that UUID exactly. Normalize the best "
                    "supported full name. Write a concise English description using "
                    "only supplied source facts, or null. Previous profiles help with "
                    "identity continuity but are not evidence for new facts. Do not "
                    "return unsupported previous profiles and never invent facts."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    request_input,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            },
        ],
        "temperature": 0,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "response_format": {"type": "json_object"},
        "extra_body": {"thinking": {"type": "disabled"}},
    }


def request_input_hash(request: Mapping[str, Any]) -> str:
    """Hash the exact model, prompt version and messages of one request."""
    payload = json.dumps(
        {
            "model": request["model"],
            "prompt_version": PROMPT_VERSION,
            "messages": request["messages"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def validate_company_people_response(
    response: LlmCompanyPeopleResponse,
    *,
    batch: CompanyObservationBatch,
    previous_profiles: Sequence[ExistingPersonProfile],
) -> None:
    expected_draft_ids = {observation.draft_id for observation in batch.observations}
    returned_draft_ids = {
        draft_id for person in response.people for draft_id in person.draft_ids
    }
    if returned_draft_ids != expected_draft_ids:
        missing = sorted(
            str(value) for value in expected_draft_ids - returned_draft_ids
        )
        unexpected = sorted(
            str(value) for value in returned_draft_ids - expected_draft_ids
        )
        raise ValueError(
            "LLM draft assignment does not match request evidence: "
            f"missing={missing} unexpected={unexpected}"
        )

    allowed_person_ids = {profile.person_id for profile in previous_profiles}
    unexpected_person_ids = sorted(
        str(person.existing_person_id)
        for person in response.people
        if person.existing_person_id is not None
        and person.existing_person_id not in allowed_person_ids
    )
    if unexpected_person_ids:
        raise ValueError(
            f"LLM returned unknown existing_person_id values: {unexpected_person_ids}"
        )


def request_company_people(
    client: OpenAI,
    *,
    company_id: str,
    batch: CompanyObservationBatch,
    previous_profiles: Sequence[ExistingPersonProfile],
    model: str,
    model_provider: str = "deepseek",
    request: dict[str, Any] | None = None,
    maximum_contract_attempts: int = MAX_CONTRACT_ATTEMPTS,
) -> LlmCompanyPeopleResult:
    """Resolve one batch with the model, repairing contract failures in place.

    A prebuilt ``request`` is used verbatim so the recorded ``input_hash`` is the
    one the caller used to look up stored suggestions. Repair turns are appended
    after the hash is taken and therefore never change it.
    """
    if maximum_contract_attempts < 1:
        raise ValueError("maximum_contract_attempts must be positive")
    if request is None:
        request = build_company_people_request(
            company_id=company_id,
            batch=batch,
            previous_profiles=previous_profiles,
            model=model,
        )
    input_hash = request_input_hash(request)
    prompt_tokens = 0
    completion_tokens = 0

    for attempt in range(1, maximum_contract_attempts + 1):
        api_response = client.chat.completions.create(**request)
        usage = getattr(api_response, "usage", None)
        prompt_tokens += _usage_value(usage, "prompt_tokens")
        completion_tokens += _usage_value(usage, "completion_tokens")
        content = api_response.choices[0].message.content
        try:
            response = _parse_company_people_response(content)
            validate_company_people_response(
                response,
                batch=batch,
                previous_profiles=previous_profiles,
            )
        except (RuntimeError, ValidationError, ValueError) as exc:
            if attempt == maximum_contract_attempts:
                raise ValueError(
                    "LLM failed the company-person response contract after "
                    f"{maximum_contract_attempts} attempts: {exc}"
                ) from exc
            required_draft_ids = sorted(
                str(observation.draft_id) for observation in batch.observations
            )
            if content is not None:
                request["messages"].append({"role": "assistant", "content": content})
            request["messages"].append(
                {
                    "role": "user",
                    "content": (
                        "Your previous response failed validation: "
                        f"{exc}. Return a corrected complete JSON object. The draft_ids "
                        "across all people must contain each of these IDs exactly once "
                        f"and no others: {json.dumps(required_draft_ids)}"
                    ),
                }
            )
            continue

        return LlmCompanyPeopleResult(
            response=response,
            model_provider=model_provider,
            model_name=model,
            prompt_version=PROMPT_VERSION,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            contract_retry_count=attempt - 1,
            input_hash=input_hash,
            raw_response=content or "",
        )

    raise AssertionError("unreachable company-person LLM attempt loop")


def _parse_company_people_response(content: str | None) -> LlmCompanyPeopleResponse:
    if content is None:
        raise RuntimeError("Company-person normalization returned no content")
    json_start = content.find("{")
    json_end = content.rfind("}")
    if json_start < 0 or json_end < json_start:
        raise RuntimeError("Company-person normalization did not return a JSON object")
    return LlmCompanyPeopleResponse.model_validate_json(
        content[json_start : json_end + 1]
    )


def _usage_value(usage: object, name: str) -> int:
    if usage is None:
        return 0
    value = getattr(usage, name, 0)
    return int(value) if value is not None else 0


PERSON_ID_HASH_DOMAIN = "se-company-person-v2"


def person_id_for(company_id: str, group_key: str) -> uuid.UUID:
    """The deterministic person id: company scope plus an ALREADY-CANONICAL group key.

    ``group_key`` is not derived here -- the caller resolves it first:

    - Both the deterministic single-source path (``_normalize_single_source_company``) and
      the LLM multi-source path (``_normalize_multi_source_company``) run the full K3
      reconciliation over a company's current observations
      (``_company_person_group_keys``, reusing ``identity_eval.k3_merge_groups`` -- the
      production key derivation, not a fork of it) ONCE per company and use its canonical
      keys. This is company-scoped and source-agnostic: a person's group key -- and
      therefore their person_id -- no longer depends on how many sources happened to observe
      them (fix round: the two paths used to disagree, the same human getting a different
      person_id depending on source count -- a v2 regression from v1, where both paths
      happened to agree because K1 was cheap enough to inline everywhere).
    - Only a name with NO matching observation at all in the current company (an LLM-invented
      name, or a human-typed one in the ``split_person``/``override_field`` correction
      handlers) falls back to ``identity_key_k2(name)`` directly -- there is nothing in
      ``_company_person_group_keys``' map to look up. A singleton group's canonical key would
      be trivially itself anyway, so this is not a different rule, just K3 with nothing else
      in the group to compare against.

    See ``_company_person_group_keys`` for how the canonical key itself is chosen within one
    K3 group (CONTROLLER RULING: stability over minimality).
    """
    digest = hashlib.sha256(
        f"{PERSON_ID_HASH_DOMAIN}\n{company_id}\n{group_key}".encode()
    ).hexdigest()
    return uuid.UUID(hex=digest[:32])


def _company_person_group_keys(
    company_id: str,
    observations: Sequence[DraftPersonObservation],
    previous_profiles: Sequence[ExistingPersonProfile] = (),
) -> dict[uuid.UUID, str]:
    """K3-resolved canonical group key per non-blank-name observation, one company at a time.

    CANONICAL KEY (CONTROLLER RULING, amending the original shortest-member-only rule): a
    group's canonical key is a PREVIOUSLY-PUBLISHED member's canonical key when one exists,
    else the group's shortest member K2 key. Stability beats minimality -- without this, a
    K3 group's person_id could churn merely because a later run adds a newer, shorter-named
    observation to an already-published group (e.g. "Anna Maria Svensson" published first,
    "Anna Svensson" observed later: the group's person_id must stay the one already
    published, not silently move to the new shorter name's hash).

    The ledger stores each published person's ``person_id`` (a UUID), not the string key that
    produced it, so "does this group already have a published identity" is answered by
    RE-HASHING each candidate member key with ``person_id_for`` and testing membership
    against ``previous_profiles``' person_ids -- there is no way to recover "the" prior key
    except by testing candidates this way. A TOMBSTONED (merged-away) profile's id is
    excluded from that membership test: reusing it would resurrect a merged-away identity
    instead of letting the group resolve fresh (or, if its evidence rejoins the surviving
    target's group some other way, stay with the target). If more than one member key
    matches a (different) previously-published person_id -- reachable only if K3 now merges
    two groups that were published separately in an earlier run -- the alphabetically-first
    matching key wins: a deterministic tie-break for an edge case with no currently-reachable
    test scenario.

    Member K2 keys are recomputed directly from each row's ``full_name``
    (``{identity_key_k2(row.full_name) for row in decision.rows}``), NOT recovered by
    splitting ``MergeDecision.k3_person_key`` on ``"|"`` -- a literal ``|`` character
    somewhere in a name would corrupt that split.

    ``min(..., key=lambda key: (len(key), key))`` is a deterministic TIE-BREAK, not a
    meaningful "base name" rule: it reads as "drop the middle name" for K3 rule (a)
    (superset-of-tokens merges), but K3 rule (b) (a shared Wikidata QID) can merge two
    completely unrelated spellings with no superset relationship at all -- the tie-break
    still produces SOME deterministic answer there, it just is not "the base name" in any
    meaningful sense for that case.

    Correlated back to ``draft_id`` by object identity (``id()``), not value equality: two
    DIFFERENT observations can be value-identical ``PersonObservationRow`` instances (e.g. two
    bolagsverket signatories sharing one filing's ``source_record_uid``, name and role --
    ``PersonObservationRow`` does not carry the row-level disambiguator ``draft_id`` itself
    now folds in, see ``source_views.build_se_company_person_source_observations_sql``'s
    docstring), and a value-keyed lookup would silently collapse them onto one group.
    """
    rows: list[PersonObservationRow] = []
    observation_by_row_id: dict[int, DraftPersonObservation] = {}
    for observation in observations:
        name = _source_name(observation)
        if not name.strip():
            continue
        wikidata_id = observation.source_value.get("person_wikidata_id") or ""
        row = PersonObservationRow(
            company_id=company_id,
            source=observation.source,
            source_record_uid=observation.source_record_uid,
            full_name=name,
            person_wikidata_id=str(wikidata_id),
        )
        rows.append(row)
        observation_by_row_id[id(row)] = observation

    # A tombstoned (merged-away) profile's id is deliberately excluded: reusing it here would
    # resurrect a merged-away identity if K3 later regroups its observations under a key that
    # happens to match the tombstone's own key, rather than the surviving target's -- exactly
    # the outcome apply_person_corrections' merge_persons handling exists to prevent.
    previous_person_ids = {
        profile.person_id
        for profile in previous_profiles
        if profile.merged_into_person_id is None
    }
    group_key_by_draft_id: dict[uuid.UUID, str] = {}
    for decision in k3_merge_groups(rows):
        member_keys = sorted({identity_key_k2(row.full_name) for row in decision.rows})
        published_keys = [
            key for key in member_keys if person_id_for(company_id, key) in previous_person_ids
        ]
        canonical_key = (
            published_keys[0]
            if published_keys
            else min(member_keys, key=lambda key: (len(key), key))
        )
        for row in decision.rows:
            draft_observation = observation_by_row_id[id(row)]
            group_key_by_draft_id[draft_observation.draft_id] = canonical_key
    return group_key_by_draft_id


def _group_key_for_draft_ids(
    draft_ids: Iterable[uuid.UUID],
    group_key_by_draft_id: Mapping[uuid.UUID, str],
    *,
    fallback_name: str,
) -> str:
    """The K3 canonical group key shared by a set of draft_ids (a profile's evidence, or an
    LLM suggestion's), or K2 of ``fallback_name`` when NONE of them resolve to a known
    observation -- an LLM-invented name, or a human-typed one with no company-wide K3 context
    (``person_id_for``'s docstring). If the draft_ids span more than one K3 group (a profile
    the LLM or a correction has already made deviate from K3 -- itself a pre-existing,
    orthogonal compromise of this multi-source path, not something this helper introduces),
    the alphabetically-first group key is the deterministic tie-break.
    """
    keys = {
        group_key_by_draft_id[draft_id]
        for draft_id in draft_ids
        if draft_id in group_key_by_draft_id
    }
    if keys:
        return min(keys)
    return identity_key_k2(fallback_name)


def _source_name(observation: DraftPersonObservation) -> str:
    value = observation.source_value
    if observation.source == "bolagsverket":
        return " ".join(
            part.strip()
            for part in (
                str(value.get("first_name", "")),
                str(value.get("last_name", "")),
            )
            if part.strip()
        )
    return str(value.get("name", "")).strip()


def _source_description(observation: DraftPersonObservation) -> str | None:
    value = observation.source_value.get("description")
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


def _profiles_for_request(
    profiles: Mapping[uuid.UUID, _ProfileAccumulator],
) -> tuple[ExistingPersonProfile, ...]:
    """Live profiles only: a merge tombstone is history, not identity evidence."""
    return tuple(
        ExistingPersonProfile(
            person_id=profile.person_id,
            name=profile.name,
            description=profile.description,
            draft_ids=tuple(sorted(profile.draft_ids)),
            created_at=profile.created_at,
        )
        for profile in sorted(profiles.values(), key=lambda item: str(item.person_id))
        if profile.draft_ids and profile.merged_into_person_id is None
    )


def _sorted_correction_ids(
    correction_ids: Sequence[uuid.UUID],
) -> tuple[uuid.UUID, ...]:
    """Sort by string like the ClickHouse correction_set_hash does."""
    return tuple(sorted(set(correction_ids), key=str))


def _profile_changed(
    profile: _ProfileAccumulator,
    previous: ExistingPersonProfile | None,
) -> bool:
    if previous is None:
        return True
    return (
        profile.name != previous.name
        or profile.description != previous.description
        or tuple(sorted(profile.draft_ids)) != previous.draft_ids
        or _sorted_correction_ids(profile.correction_ids)
        != _sorted_correction_ids(previous.correction_ids)
        or profile.suggestion_id != previous.suggestion_id
        or profile.merged_into_person_id != previous.merged_into_person_id
    )


@dataclass(frozen=True)
class CorrectionOutcome:
    applied: tuple[PersonCorrection, ...]
    stale: tuple[PersonCorrection, ...]


@dataclass(frozen=True)
class _MultiSourceOutcome:
    profiles: dict[uuid.UUID, _ProfileAccumulator]
    suggestion_writes: list[SuggestionWrite]
    metrics: dict[str, int]
    current_input_hashes: frozenset[str]
    emptied_person_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True)
class CompanyNormalizationNotes:
    """What one company skipped, by id, so the run log can name it."""

    company_id: str
    stale_correction_ids: tuple[uuid.UUID, ...]
    emptied_person_ids: tuple[uuid.UUID, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.stale_correction_ids and not self.emptied_person_ids


def _evidence_is_current(
    correction: PersonCorrection,
    profile: _ProfileAccumulator | None,
    previous: ExistingPersonProfile | None,
) -> bool:
    """True when the reviewer's evidence still matches the published row and this run.

    ``draft_set_hash`` is never recomputed in Python: it is compared as loaded
    from the published row, together with an in-run equality check that the
    person's evidence has not moved since that row was written.
    """
    if correction.evidence_hash == ZERO_HASH:
        return profile is not None
    if profile is None or previous is None:
        return False
    return (
        previous.draft_set_hash == correction.evidence_hash
        and tuple(sorted(profile.draft_ids)) == previous.draft_ids
    )


def _deterministic_name(
    profile: _ProfileAccumulator,
    observations_by_id: Mapping[uuid.UUID, DraftPersonObservation],
) -> str:
    """The name the source observations support, ignoring model suggestions."""
    ordered = sorted(
        (
            observations_by_id[draft_id]
            for draft_id in profile.draft_ids
            if draft_id in observations_by_id
        ),
        key=lambda item: (
            item.source_observed_at,
            item.fiscal_year or 0,
            str(item.draft_id),
        ),
        reverse=True,
    )
    for observation in ordered:
        name = _source_name(observation)
        if name:
            return name
    return profile.name


def _nullable_payload_uuid(value: object) -> uuid.UUID | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return None


def apply_person_corrections(
    profiles: dict[uuid.UUID, _ProfileAccumulator],
    *,
    company: CompanyPersonWork,
    current_input_hashes: frozenset[str],
    created_at: datetime,
) -> CorrectionOutcome:
    """Apply live person corrections in kind order; never apply a stale one.

    A correction is decided before anything is mutated: a stale or inapplicable
    row leaves every profile exactly as it was, is counted, and stays in the
    ledger for the reviewer to re-decide.
    """
    previous_by_id = {item.person_id: item for item in company.previous_profiles}
    observations_by_id = {item.draft_id: item for item in company.observations}
    suggestions_by_id = {item.suggestion_id: item for item in company.suggestions}
    applied: list[PersonCorrection] = []
    stale: list[PersonCorrection] = []

    def ensure_profile(person_id: uuid.UUID) -> _ProfileAccumulator | None:
        """The in-run profile for a person, restored from the published row.

        Published provenance strings are not loaded back (the profile query
        reads content and evidence only), so a restored row is re-labelled as
        what actually produced it: the correction engine. The published
        ``suggestion_id`` is carried so approving nothing does not silently drop
        the suggestion this profile was published from, and the tombstone
        pointer starts clear because only a live merge may set it.
        """
        profile = profiles.get(person_id)
        if profile is not None:
            return profile
        previous = previous_by_id.get(person_id)
        if previous is None:
            return None
        profile = _ProfileAccumulator(
            person_id=previous.person_id,
            name=previous.name,
            description=previous.description,
            # Empty on purpose: this person has no observation in this run, so
            # its evidence is only whatever a correction moves onto it.
            draft_ids=set(),
            created_at=previous.created_at,
            model_provider="deterministic",
            model_name="correction",
            prompt_version=DIRECT_PROMPT_VERSION,
            suggestion_id=previous.suggestion_id,
            merged_into_person_id=None,
        )
        profiles[person_id] = profile
        return profile

    for correction in effective_corrections(company.corrections):
        if correction.kind not in PERSON_CORRECTION_KINDS:
            continue
        subject = profiles.get(correction.subject_person_id)
        previous = previous_by_id.get(correction.subject_person_id)
        if subject is None or not _evidence_is_current(correction, subject, previous):
            stale.append(correction)
            continue

        if correction.kind == "merge_persons":
            if (
                correction.target_person_id is None
                or correction.target_person_id == subject.person_id
            ):
                stale.append(correction)
                continue
            target = ensure_profile(correction.target_person_id)
            if target is None:
                stale.append(correction)
                continue
            target.draft_ids.update(subject.draft_ids)
            target.correction_ids.append(correction.correction_id)
            target.touched = True
            subject.merged_into_person_id = target.person_id
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        elif correction.kind == "reassign_draft":
            moved = set(correction.draft_ids)
            if (
                correction.target_person_id is None
                or correction.target_person_id == subject.person_id
                or len(moved) != 1
                or not moved <= subject.draft_ids
                or subject.draft_ids == moved
            ):
                stale.append(correction)
                continue
            target = ensure_profile(correction.target_person_id)
            if target is None:
                stale.append(correction)
                continue
            subject.draft_ids -= moved
            target.draft_ids |= moved
            for profile in (subject, target):
                profile.correction_ids.append(correction.correction_id)
                profile.touched = True

        elif correction.kind == "split_person":
            moved = set(correction.draft_ids)
            name = str(correction.payload.get("name", "")).strip()
            if not moved or not moved < subject.draft_ids or name == "":
                stale.append(correction)
                continue
            new_id = person_id_for(company.status.company_id, identity_key_k2(name))
            if new_id == subject.person_id:
                stale.append(correction)
                continue
            target = ensure_profile(new_id)
            if target is None:
                target = _ProfileAccumulator(
                    person_id=new_id,
                    name=name,
                    description=None,
                    draft_ids=set(),
                    created_at=created_at,
                    model_provider="deterministic",
                    model_name="correction",
                    prompt_version=DIRECT_PROMPT_VERSION,
                )
                profiles[new_id] = target
            subject.draft_ids -= moved
            target.draft_ids |= moved
            target.name = name
            for profile in (subject, target):
                profile.correction_ids.append(correction.correction_id)
                profile.touched = True

        elif correction.kind in ("approve_suggestion", "reject_suggestion"):
            suggestion_id = _nullable_payload_uuid(
                correction.payload.get("suggestion_id")
            )
            suggestion = suggestions_by_id.get(suggestion_id) if suggestion_id else None
            if suggestion is None or suggestion.input_hash not in current_input_hashes:
                stale.append(correction)
                continue
            if correction.kind == "approve_suggestion":
                # Approving takes the suggestion's drafts from whoever holds
                # them. A person the steal would leave with no evidence cannot
                # be published at all (000291's CHECK notEmpty(draft_ids)), so
                # the correction is stale instead of producing a rejected row.
                moved = set(suggestion.draft_ids)
                emptied = [
                    other.person_id
                    for other in profiles.values()
                    if other is not subject
                    and other.draft_ids
                    and not other.draft_ids - moved
                ]
                if emptied or not subject.draft_ids | moved:
                    stale.append(correction)
                    continue
                for other in profiles.values():
                    if other is not subject:
                        other.draft_ids -= moved
                subject.draft_ids |= moved
                subject.name = suggestion.name
                subject.description = suggestion.description
                subject.suggestion_id = suggestion.suggestion_id
            else:
                subject.name = _deterministic_name(subject, observations_by_id)
                subject.description = None
                subject.suggestion_id = None
                subject.model_provider = "deterministic"
                subject.model_name = "rejected-suggestion"
                subject.prompt_version = DIRECT_PROMPT_VERSION
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        elif correction.kind == "override_field":
            name = None
            if "name" in correction.payload:
                name = str(correction.payload["name"]).strip()
                if name == "":
                    stale.append(correction)
                    continue
            if name is not None:
                subject.name = name
            if "description" in correction.payload:
                value = correction.payload["description"]
                subject.description = (
                    None if value is None else str(value).strip() or None
                )
            subject.correction_ids.append(correction.correction_id)
            subject.touched = True

        applied.append(correction)

    return CorrectionOutcome(applied=tuple(applied), stale=tuple(stale))


def _finalize_company_profiles(
    company: CompanyPersonWork,
    profiles: dict[uuid.UUID, _ProfileAccumulator],
    *,
    current_input_hashes: frozenset[str],
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], dict[str, int], tuple[uuid.UUID, ...]]:
    """Apply the ledger, then write only the profiles that actually changed.

    The ids of the corrections that were too stale to apply are returned so the
    caller can name them in the run log instead of only counting them.
    """
    previous_by_id = {item.person_id: item for item in company.previous_profiles}
    outcome = apply_person_corrections(
        profiles,
        company=company,
        current_input_hashes=current_input_hashes,
        created_at=created_at,
    )
    writes: list[PersonProfileWrite] = []
    unchanged_profile_count = 0
    invalid_profile_count = 0
    for profile in sorted(profiles.values(), key=lambda item: str(item.person_id)):
        if not profile.touched:
            continue
        if not profile.draft_ids:
            # Defensive: every kind that moves evidence refuses to empty a
            # person, so this is unreachable by design. Skipping the write keeps
            # a bug from failing the run on the main table's notEmpty check.
            invalid_profile_count += 1
            continue
        previous = previous_by_id.get(profile.person_id)
        if not _profile_changed(profile, previous):
            unchanged_profile_count += 1
            continue
        writes.append(
            PersonProfileWrite(
                person_id=profile.person_id,
                company_id=company.status.company_id,
                name=profile.name,
                description=profile.description,
                draft_ids=tuple(sorted(profile.draft_ids)),
                model_provider=profile.model_provider,
                model_name=profile.model_name,
                prompt_version=profile.prompt_version,
                created_at=profile.created_at,
                suggestion_id=profile.suggestion_id,
                correction_ids=_sorted_correction_ids(profile.correction_ids),
                merged_into_person_id=profile.merged_into_person_id,
            )
        )
    return (
        writes,
        {
            "unchanged_profile_count": unchanged_profile_count,
            "invalid_profile_count": invalid_profile_count,
            "applied_correction_count": len(outcome.applied),
            "stale_correction_count": len(outcome.stale),
        },
        tuple(correction.correction_id for correction in outcome.stale),
    )


def _normalize_single_source_company(
    company: CompanyPersonWork,
    *,
    created_at: datetime,
) -> dict[uuid.UUID, _ProfileAccumulator]:
    """Group a single source's observations by K3, not K1 (spec 3.2's production rule).

    K3 replaces the previous K1 (first|last token) grouping wholesale here: every observation
    in a single-source company is visible at once, exactly the scope K3's deterministic
    reconciliation pass is designed for -- no LLM involved, so there is no continuity
    mechanism to defer to (contrast the multi-source path, see ``person_id_for``'s
    docstring).
    """
    group_key_by_draft_id = _company_person_group_keys(
        company.status.company_id,
        company.observations,
        previous_profiles=company.previous_profiles,
    )
    by_group: dict[str, list[DraftPersonObservation]] = defaultdict(list)
    for observation in company.observations:
        group_key = group_key_by_draft_id.get(observation.draft_id)
        if group_key is not None:
            by_group[group_key].append(observation)

    previous_by_id = {item.person_id: item for item in company.previous_profiles}
    profiles: dict[uuid.UUID, _ProfileAccumulator] = {}
    source = next(iter({item.source for item in company.observations}))

    for group_key, observations in sorted(by_group.items()):
        ordered = sorted(
            observations,
            key=lambda item: (
                item.source_observed_at,
                item.fiscal_year or 0,
                str(item.draft_id),
            ),
            reverse=True,
        )
        representative = ordered[0]
        name = _source_name(representative)
        description = next(
            (
                value
                for value in (_source_description(item) for item in ordered)
                if value is not None
            ),
            None,
        )
        person_id = person_id_for(company.status.company_id, group_key)
        previous = previous_by_id.get(person_id)
        if description is None and previous is not None:
            description = previous.description
        profiles[person_id] = _ProfileAccumulator(
            person_id=person_id,
            name=name,
            description=description,
            draft_ids={item.draft_id for item in observations},
            created_at=previous.created_at if previous is not None else created_at,
            model_provider="deterministic",
            model_name=f"single-source:{source}",
            prompt_version=DIRECT_PROMPT_VERSION,
            touched=True,
        )
    return profiles


def _newest_stored_by_person(
    stored: Sequence[StoredSuggestion],
) -> tuple[StoredSuggestion, ...]:
    """Keep the newest stored row per person, ordered by person id."""
    newest_by_person: dict[uuid.UUID, StoredSuggestion] = {}
    for row in sorted(
        stored, key=lambda item: (item.created_at, str(item.suggestion_id))
    ):
        newest_by_person[row.person_id] = row
    return tuple(
        newest_by_person[person_id] for person_id in sorted(newest_by_person, key=str)
    )


def _reuse_stored_suggestions(
    stored: Sequence[StoredSuggestion],
    *,
    batch: CompanyObservationBatch,
    previous_profiles: Sequence[ExistingPersonProfile],
    input_hash: str,
) -> LlmCompanyPeopleResult | None:
    """Rebuild a validated response from stored rows, or None to call the model."""
    if not stored:
        return None
    provenance = max(
        stored, key=lambda item: (item.created_at, str(item.suggestion_id))
    )
    try:
        response = LlmCompanyPeopleResponse(
            people=[
                LlmCompanyPersonSuggestion(
                    existing_person_id=row.existing_person_id,
                    name=row.name,
                    description=row.description,
                    draft_ids=list(row.draft_ids),
                )
                for row in stored
            ]
        )
        validate_company_people_response(
            response, batch=batch, previous_profiles=previous_profiles
        )
    except (ValidationError, ValueError):
        return None
    return LlmCompanyPeopleResult(
        response=response,
        model_provider=provenance.model_provider,
        model_name=provenance.model_name,
        prompt_version=provenance.prompt_version,
        prompt_tokens=0,
        completion_tokens=0,
        input_hash=input_hash,
        reused=True,
    )


def _normalize_multi_source_company(
    company: CompanyPersonWork,
    *,
    llm_suggester: CompanyLlmSuggester,
    llm_model: str | None,
    maximum_observations_per_request: int,
    created_at: datetime,
) -> "_MultiSourceOutcome":
    # Computed ONCE per company, source-agnostic (fix round, Important): the same K3 group
    # keys the deterministic single-source path uses. Without this, the multi-source path
    # used to key its own name-based person lookups by raw K2 -- a fork from the
    # single-source path's K3 keys that let the same human end up with a DIFFERENT person_id
    # depending purely on how many sources happened to observe them (a v2 regression from v1,
    # where both paths agreed because K1 was cheap enough to inline everywhere). See
    # person_id_for's docstring.
    group_key_by_draft_id = _company_person_group_keys(
        company.status.company_id,
        company.observations,
        previous_profiles=company.previous_profiles,
    )
    # A merge tombstone keeps the drafts it was merged away with, and so does
    # the person it was merged into. Seeding both would hand the model the same
    # evidence twice and leave whichever row it does not re-populate empty, so
    # tombstones stay out of this run entirely: their published row is history
    # and is left exactly as it is.
    profiles = {
        previous.person_id: _ProfileAccumulator(
            person_id=previous.person_id,
            name=previous.name,
            description=previous.description,
            draft_ids=set(previous.draft_ids),
            created_at=previous.created_at,
            model_provider="deepseek",
            model_name="",
            prompt_version=PROMPT_VERSION,
        )
        for previous in company.previous_profiles
        if previous.merged_into_person_id is None
    }
    batches = batch_company_observations(
        company.observations,
        maximum_observations_per_request=maximum_observations_per_request,
    )
    prompt_tokens = 0
    completion_tokens = 0
    contract_retry_count = 0
    suggestion_writes: list[SuggestionWrite] = []
    reused_batch_count = 0
    request_count = 0
    input_hashes: set[str] = set()
    stored_by_hash: dict[str, list[StoredSuggestion]] = defaultdict(list)
    for stored in company.suggestions:
        stored_by_hash[stored.input_hash].append(stored)
    if llm_model is None:
        raise RuntimeError("A multi-source company requires an LLM model name")

    for batch in batches:
        request_profiles = _profiles_for_request(profiles)
        request = build_company_people_request(
            company_id=company.status.company_id,
            batch=batch,
            previous_profiles=request_profiles,
            model=llm_model,
        )
        input_hash = request_input_hash(request)
        input_hashes.add(input_hash)
        stored_rows = _newest_stored_by_person(stored_by_hash.get(input_hash, ()))
        result = _reuse_stored_suggestions(
            stored_rows,
            batch=batch,
            previous_profiles=request_profiles,
            input_hash=input_hash,
        )
        stored_by_draft_ids: dict[tuple[uuid.UUID, ...], StoredSuggestion] = {}
        if result is None:
            result = llm_suggester(
                company.status.company_id,
                batch,
                request_profiles,
                request,
            )
            if result.input_hash and result.input_hash != input_hash:
                raise ValueError(
                    "LLM input hash mismatch: the suggester answered a different "
                    f"request than the one looked up: expected={input_hash} "
                    f"got={result.input_hash}"
                )
            validate_company_people_response(
                result.response,
                batch=batch,
                previous_profiles=request_profiles,
            )
        else:
            stored_by_draft_ids = {
                tuple(sorted(row.draft_ids)): row for row in stored_rows
            }
        if result.reused:
            reused_batch_count += 1
        else:
            request_count += 1
        prompt_tokens += result.prompt_tokens
        completion_tokens += result.completion_tokens
        contract_retry_count += result.contract_retry_count

        batch_draft_ids = {observation.draft_id for observation in batch.observations}
        for profile in profiles.values():
            removed_ids = profile.draft_ids & batch_draft_ids
            if removed_ids:
                profile.draft_ids -= removed_ids
                profile.touched = True

        # K3-keyed (fix round, Important): profiles_by_name and the fallback below both key
        # off the SAME company-wide group_key_by_draft_id computed once above -- the LLM's
        # own existing_person_id continuity is still the primary "same person" mechanism,
        # this is only the fallback for a suggestion it did not attribute to a supplied
        # previous profile. Raw K2 applies only when a profile/suggestion's draft_ids don't
        # resolve to any known observation (see _group_key_for_draft_ids).
        profiles_by_name = {
            _group_key_for_draft_ids(
                profile.draft_ids, group_key_by_draft_id, fallback_name=profile.name
            ): profile.person_id
            for profile in profiles.values()
        }
        for suggestion in result.response.people:
            stored_row = stored_by_draft_ids.get(tuple(sorted(suggestion.draft_ids)))
            person_id = (
                stored_row.person_id
                if stored_row is not None
                else suggestion.existing_person_id
            )
            if person_id is None:
                suggestion_group_key = _group_key_for_draft_ids(
                    suggestion.draft_ids,
                    group_key_by_draft_id,
                    fallback_name=suggestion.name,
                )
                person_id = profiles_by_name.get(suggestion_group_key)
                if person_id is None:
                    person_id = person_id_for(
                        company.status.company_id, suggestion_group_key
                    )

            profile = profiles.get(person_id)
            if profile is None:
                profile = _ProfileAccumulator(
                    person_id=person_id,
                    name=suggestion.name,
                    description=suggestion.description,
                    draft_ids=set(),
                    created_at=created_at,
                    model_provider=result.model_provider,
                    model_name=result.model_name,
                    prompt_version=result.prompt_version,
                )
                profiles[person_id] = profile
            profile.name = suggestion.name
            if suggestion.description is not None:
                profile.description = suggestion.description
            profile.draft_ids.update(suggestion.draft_ids)
            profile.model_provider = result.model_provider
            profile.model_name = result.model_name
            profile.prompt_version = result.prompt_version
            profile.touched = True

            if stored_row is not None:
                profile.suggestion_id = stored_row.suggestion_id
                continue
            suggestion_id = uuid.uuid4()
            suggestion_writes.append(
                SuggestionWrite(
                    suggestion_id=suggestion_id,
                    company_id=company.status.company_id,
                    person_id=person_id,
                    input_hash=input_hash,
                    draft_ids=tuple(sorted(suggestion.draft_ids)),
                    suggestion_json=suggestion.model_dump_json(),
                    raw_response=result.raw_response,
                    model_provider=result.model_provider,
                    model_name=result.model_name,
                    prompt_version=result.prompt_version,
                    prompt_tokens=result.prompt_tokens,
                    completion_tokens=result.completion_tokens,
                    created_at=created_at,
                )
            )
            profile.suggestion_id = suggestion_id

    # A model that hands every draft of an existing person to somebody else
    # leaves that person with no evidence, and the main table's
    # `CHECK notEmpty(draft_ids)` cannot represent that. One such profile used to
    # abort the materialization for every company in the batch. It is now
    # dropped from this run instead: nothing is written for it, so its published
    # row stays exactly as it is, and the count plus the ids reach the run log.
    emptied_person_ids = tuple(
        sorted(
            (
                profile.person_id
                for profile in profiles.values()
                if profile.touched and not profile.draft_ids
            ),
            key=str,
        )
    )
    for person_id in emptied_person_ids:
        del profiles[person_id]

    return _MultiSourceOutcome(
        profiles=profiles,
        suggestion_writes=suggestion_writes,
        metrics={
            "llm_request_count": request_count,
            "llm_reused_batch_count": reused_batch_count,
            "llm_role_batch_count": len(batches) if len(batches) > 1 else 0,
            "llm_observation_count": len(company.observations),
            "llm_prompt_tokens": prompt_tokens,
            "llm_completion_tokens": completion_tokens,
            "llm_contract_retry_count": contract_retry_count,
            "emptied_profile_count": len(emptied_person_ids),
        },
        current_input_hashes=frozenset(input_hashes),
        emptied_person_ids=emptied_person_ids,
    )


def normalize_companies(
    companies: Sequence[CompanyPersonWork],
    *,
    llm_suggester: CompanyLlmSuggester | None,
    llm_model: str | None,
    maximum_observations_per_request: int,
    created_at: datetime,
) -> tuple[
    list[PersonProfileWrite],
    list[SuggestionWrite],
    dict[str, int],
    list[CompanyNormalizationNotes],
]:
    """Normalize changed companies and return writes only after all LLM calls pass.

    The fourth return value names, per company, what was skipped: the stale
    ledger rows and the profiles a model emptied. Counting them is not enough for
    a reviewer to find them again.
    """
    writes: list[PersonProfileWrite] = []
    suggestion_writes: list[SuggestionWrite] = []
    notes: list[CompanyNormalizationNotes] = []
    metrics = {
        "direct_company_count": 0,
        "llm_company_count": 0,
        "directly_inserted_count": 0,
        "llm_inserted_count": 0,
        "llm_request_count": 0,
        "llm_reused_batch_count": 0,
        "llm_role_batch_count": 0,
        "llm_observation_count": 0,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
        "llm_contract_retry_count": 0,
        "unchanged_profile_count": 0,
        "invalid_profile_count": 0,
        "applied_correction_count": 0,
        "stale_correction_count": 0,
        "emptied_profile_count": 0,
    }

    for company in companies:
        emptied_person_ids: tuple[uuid.UUID, ...] = ()
        if company.requires_llm:
            if llm_suggester is None:
                raise RuntimeError("A multi-source company requires an LLM suggester")
            metrics["llm_company_count"] += 1
            outcome = _normalize_multi_source_company(
                company,
                llm_suggester=llm_suggester,
                llm_model=llm_model,
                maximum_observations_per_request=maximum_observations_per_request,
                created_at=created_at,
            )
            emptied_person_ids = outcome.emptied_person_ids
            (
                company_writes,
                finalize_metrics,
                stale_correction_ids,
            ) = _finalize_company_profiles(
                company,
                outcome.profiles,
                current_input_hashes=outcome.current_input_hashes,
                created_at=created_at,
            )
            metrics["llm_inserted_count"] += len(company_writes)
            suggestion_writes.extend(outcome.suggestion_writes)
            for name, value in (*outcome.metrics.items(), *finalize_metrics.items()):
                metrics[name] += value
        else:
            metrics["direct_company_count"] += 1
            profiles = _normalize_single_source_company(
                company,
                created_at=created_at,
            )
            (
                company_writes,
                finalize_metrics,
                stale_correction_ids,
            ) = _finalize_company_profiles(
                company,
                profiles,
                current_input_hashes=frozenset(),
                created_at=created_at,
            )
            metrics["directly_inserted_count"] += len(company_writes)
            for name, value in finalize_metrics.items():
                metrics[name] += value
        writes.extend(company_writes)
        notes.append(
            CompanyNormalizationNotes(
                company_id=company.status.company_id,
                stale_correction_ids=stale_correction_ids,
                emptied_person_ids=emptied_person_ids,
            )
        )

    return writes, suggestion_writes, metrics, notes


def _load_company_work(
    *,
    clickhouse: ClickhouseResource,
    statuses: Sequence[CompanyPersonStatus],
) -> list[CompanyPersonWork]:
    if not statuses:
        return []
    selected_company_ids = tuple(status.company_id for status in statuses)
    observations_by_company: dict[str, list[DraftPersonObservation]] = defaultdict(list)
    profiles_by_company: dict[str, list[ExistingPersonProfile]] = defaultdict(list)
    suggestions_by_company: dict[str, list[StoredSuggestion]] = defaultdict(list)
    corrections_by_company: dict[str, list[PersonCorrection]] = defaultdict(list)

    with clickhouse.get_connection() as client:
        parameters = {"selected_company_ids": selected_company_ids}
        for row in client.execute(build_company_observations_sql(), parameters):
            company_id, observation = _observation_from_row(row)
            observations_by_company[company_id].append(observation)
        for row in client.execute(build_existing_profiles_sql(), parameters):
            company_id, profile = _profile_from_row(row)
            profiles_by_company[company_id].append(profile)
        for row in client.execute(build_company_suggestions_sql(), parameters):
            company_id, suggestion = suggestion_from_row(row)
            suggestions_by_company[company_id].append(suggestion)
        for row in client.execute(build_company_corrections_sql(), parameters):
            company_id, correction = correction_from_row(row)
            corrections_by_company[company_id].append(correction)

    result: list[CompanyPersonWork] = []
    for status in statuses:
        observations = tuple(observations_by_company[status.company_id])
        observed_draft_ids = tuple(sorted(item.draft_id for item in observations))
        if observed_draft_ids != status.draft_ids:
            raise ValueError(
                f"Draft rows changed while loading company {status.company_id}"
            )
        observed_sources = {item.source for item in observations}
        if len(observations) != status.observation_count:
            raise ValueError(
                f"Draft observation count changed for company {status.company_id}"
            )
        if len(observed_sources) != status.source_count:
            raise ValueError(
                f"Draft source count changed for company {status.company_id}"
            )
        result.append(
            CompanyPersonWork(
                status=status,
                observations=observations,
                previous_profiles=tuple(profiles_by_company[status.company_id]),
                suggestions=tuple(suggestions_by_company[status.company_id]),
                corrections=tuple(corrections_by_company[status.company_id]),
            )
        )
    return result


def _insert_person_writes(
    *,
    clickhouse: ClickhouseResource,
    writes: Sequence[PersonProfileWrite],
    source_run_id: str,
    updated_at: datetime,
) -> int:
    qualified_target_table = _qualified(PERSON_TABLE)
    if not writes:
        with clickhouse.get_connection() as client:
            return int(
                client.execute(f"SELECT count() FROM {qualified_target_table} FINAL")[
                    0
                ][0]
            )

    qualified_stage_table = _qualified(f"_tmp_{PERSON_TABLE}_{uuid.uuid4().hex}")
    insert_columns = _insert_columns(PERSON_COLUMNS)
    rows = [
        (
            write.person_id,
            write.company_id,
            write.name,
            write.description,
            list(write.draft_ids),
            list(write.correction_ids),
            write.suggestion_id,
            write.merged_into_person_id,
            write.model_provider,
            write.model_name,
            write.prompt_version,
            source_run_id,
            write.created_at,
            updated_at,
        )
        for write in writes
    ]
    with clickhouse.get_connection() as client:
        stage_created = False
        primary_error: Exception | None = None
        try:
            client.execute(
                f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
            )
            stage_created = True
            client.execute(
                f"INSERT INTO {qualified_stage_table} ({insert_columns}) VALUES",
                rows,
            )
            staged_count, invalid_count = client.execute(
                f"""SELECT count(), countIf(trim(name) = '' OR empty(draft_ids))
                FROM {qualified_stage_table}"""
            )[0]
            if int(staged_count) != len(writes) or int(invalid_count) != 0:
                raise ValueError(
                    "Company-person stage validation failed: "
                    f"expected={len(writes)} staged={staged_count} invalid={invalid_count}"
                )
            client.execute(
                f"""INSERT INTO {qualified_target_table} ({insert_columns})
                SELECT {insert_columns} FROM {qualified_stage_table}"""
            )
            return int(
                client.execute(f"SELECT count() FROM {qualified_target_table} FINAL")[
                    0
                ][0]
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            if stage_created:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
                except Exception:
                    if primary_error is None:
                        raise


def _insert_suggestion_writes(
    *,
    clickhouse: ClickhouseResource,
    writes: Sequence[SuggestionWrite],
    source_run_id: str,
) -> int:
    """Append one enrichment observation row per suggestion in a single INSERT."""
    if not writes:
        return 0
    insert_columns = _insert_columns(SUGGESTION_COLUMNS)
    rows = [
        (
            write.suggestion_id,
            write.company_id,
            write.person_id,
            write.input_hash,
            list(write.draft_ids),
            write.suggestion_json,
            write.raw_response,
            write.model_provider,
            write.model_name,
            write.prompt_version,
            write.prompt_tokens,
            write.completion_tokens,
            source_run_id,
            write.created_at,
        )
        for write in writes
    ]
    with clickhouse.get_connection() as client:
        client.execute(
            f"INSERT INTO {QUALIFIED_SUGGESTION_TABLE} ({insert_columns}) VALUES",
            rows,
        )
    return len(rows)


def materialize_se_company_people(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    updated_at: datetime,
    company_ids: Sequence[str],
    max_companies: int,
    company_batch_size: int,
    maximum_observations_per_request: int,
    timeout_seconds: int,
    llm_client: OpenAI | None,
    llm_model: str | None,
    log: Callable[..., object] | None,
) -> dict[str, object]:
    company_scope = normalized_company_ids(company_ids)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=(
            *SOURCE_VIEW_TABLES,
            PERSON_TABLE,
            SUGGESTION_TABLE,
            CORRECTION_TABLE,
        ),
    )
    query_parameters = {
        "all_companies": not company_scope,
        "company_ids": company_scope or ("",),
    }
    with clickhouse.get_connection() as client:
        statistics = client.execute(build_company_statistics_sql(), query_parameters)[0]
        excluded_blank_full_name_count = int(
            client.execute(build_se_company_person_blank_full_name_count_sql())[0][0]
        )

    pending_company_count = int(statistics[2]) + int(statistics[3])
    if log is not None:
        log(
            "Evaluated Sweden company-person companies: total=%s pending=%s "
            "direct=%s llm=%s skipped=%s selected=%s",
            statistics[0],
            pending_company_count,
            statistics[2],
            statistics[3],
            statistics[1],
            min(pending_company_count, max_companies),
        )

    deepseek_client = llm_client
    selected_model = llm_model
    # The provider label rides with the rest of the endpoint configuration:
    # a DeepSeek-compatible endpoint that is not DeepSeek must not be recorded
    # as one. Filled in below when the settings are loaded for a real call.
    model_provider = "deepseek"
    normalization_metrics = {
        "direct_company_count": 0,
        "llm_company_count": 0,
        "directly_inserted_count": 0,
        "llm_inserted_count": 0,
        "llm_request_count": 0,
        "llm_reused_batch_count": 0,
        "llm_role_batch_count": 0,
        "llm_observation_count": 0,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
        "llm_contract_retry_count": 0,
        "unchanged_profile_count": 0,
        "invalid_profile_count": 0,
        "applied_correction_count": 0,
        "stale_correction_count": 0,
        "emptied_profile_count": 0,
        "settled_company_count": 0,
        "suggestion_inserted_count": 0,
    }
    selected_company_count = 0
    inserted_count = 0
    publish_batch_count = 0
    total_person_count = 0
    # Batches walk company_id order. A company whose only change is a stale
    # correction publishes nothing, so without this cursor the next pending
    # query would hand it back forever.
    after_company_id = ""

    while selected_company_count < max_companies:
        current_batch_size = min(
            company_batch_size,
            max_companies - selected_company_count,
        )
        with clickhouse.get_connection() as client:
            statuses = [
                _status_from_row(row)
                for row in client.execute(
                    build_pending_companies_sql(),
                    {
                        **query_parameters,
                        "max_companies": current_batch_size,
                        "after_company_id": after_company_id,
                    },
                )
            ]
        if not statuses:
            break

        companies = _load_company_work(clickhouse=clickhouse, statuses=statuses)
        llm_suggester: CompanyLlmSuggester | None = None
        if any(company.requires_llm for company in companies):
            if deepseek_client is None:
                settings = deepseek_settings()
                selected_model = settings.model
                model_provider = settings.provider
                deepseek_client = OpenAI(
                    base_url=settings.base_url.rstrip("/"),
                    api_key=settings.api_key,
                    timeout=float(timeout_seconds),
                    max_retries=2,
                )
            if selected_model is None or selected_model.strip() == "":
                raise ValueError(
                    "LLM model must be provided for multi-source companies"
                )

            def suggest_company_people(
                company_id: str,
                batch: CompanyObservationBatch,
                previous_profiles: tuple[ExistingPersonProfile, ...],
                request: dict[str, Any],
            ) -> LlmCompanyPeopleResult:
                return request_company_people(
                    deepseek_client,
                    company_id=company_id,
                    batch=batch,
                    previous_profiles=previous_profiles,
                    model=selected_model,
                    model_provider=model_provider,
                    request=request,
                )

            llm_suggester = suggest_company_people

        writes, suggestion_writes, batch_metrics, notes = normalize_companies(
            companies,
            llm_suggester=llm_suggester,
            llm_model=selected_model,
            maximum_observations_per_request=maximum_observations_per_request,
            created_at=updated_at,
        )
        if log is not None:
            for note in notes:
                if note.is_empty:
                    continue
                log(
                    "Stale corrections skipped: company=%s ids=%s "
                    "emptied_people=%s",
                    note.company_id,
                    [str(value) for value in note.stale_correction_ids],
                    [str(value) for value in note.emptied_person_ids],
                )
        normalization_metrics["suggestion_inserted_count"] += _insert_suggestion_writes(
            clickhouse=clickhouse,
            writes=suggestion_writes,
            source_run_id=source_run_id,
        )
        selected_company_count += len(companies)
        after_company_id = statuses[-1].company_id
        for name, value in batch_metrics.items():
            normalization_metrics[name] += value

        if not writes:
            normalization_metrics["settled_company_count"] += len(companies)
            if log is not None:
                log(
                    "Sweden company-person batch produced no new profiles "
                    "(stale corrections or unchanged evidence): companies=%s",
                    [status.company_id for status in statuses[:10]],
                )
            continue

        total_person_count = _insert_person_writes(
            clickhouse=clickhouse,
            writes=writes,
            source_run_id=source_run_id,
            updated_at=updated_at,
        )
        inserted_count += len(writes)
        publish_batch_count += 1

        if log is not None:
            log(
                "Published Sweden company-person batch: batch=%s companies=%s "
                "processed_companies=%s inserted=%s total_people=%s",
                publish_batch_count,
                len(companies),
                selected_company_count,
                len(writes),
                total_person_count,
            )

    if publish_batch_count == 0:
        with clickhouse.get_connection() as client:
            total_person_count = int(
                client.execute(f"SELECT count() FROM {_qualified(PERSON_TABLE)} FINAL")[
                    0
                ][0]
            )

    metadata: dict[str, object] = {
        "company_count": int(statistics[0]),
        "skipped_company_count": int(statistics[1]),
        "pending_direct_company_count": int(statistics[2]),
        "pending_llm_company_count": int(statistics[3]),
        "selected_company_count": selected_company_count,
        "deferred_company_count": pending_company_count - selected_company_count,
        "publish_batch_count": publish_batch_count,
        "company_batch_size": company_batch_size,
        "inserted_count": inserted_count,
        **normalization_metrics,
        "total_person_count": total_person_count,
        "excluded_blank_full_name_count": excluded_blank_full_name_count,
        "source_run_id": source_run_id,
        "company_scope": list(company_scope),
    }
    if log is not None:
        log(
            "Published Sweden company-person profiles: companies=%s inserted=%s "
            "direct=%s llm_profiles=%s llm_requests=%s llm_observations=%s "
            "skipped_companies=%s deferred_companies=%s total=%s",
            metadata["selected_company_count"],
            metadata["inserted_count"],
            metadata["directly_inserted_count"],
            metadata["llm_inserted_count"],
            metadata["llm_request_count"],
            metadata["llm_observation_count"],
            metadata["skipped_company_count"],
            metadata["deferred_company_count"],
            metadata["total_person_count"],
        )
    return metadata


class SECompanyPersonConfig(dg.Config):
    company_ids: list[str] = Field(default_factory=list)
    max_companies: int = Field(default=1_000_000, ge=1, le=1_000_000)
    company_batch_size: int = Field(default=5_000, ge=1, le=25_000)
    maximum_observations_per_request: int = Field(default=50, ge=1, le=500)
    timeout_seconds: int = Field(default=180, ge=1, le=600)


# The transitive read footprint of the three source views (source_views.py) --
# se_company_person_draft_clickhouse is retired from this path (Task 3); this asset now
# reads the views directly, so its deps are the same upstream tables identity_eval.py's
# evaluation asset already depends on for the same views.
_SOURCE_ASSET_DEPS = (
    dg.AssetKey("se_financial_report_signatories_clickhouse"),
    dg.AssetKey("esef_document_people_clickhouse"),
    dg.AssetKey("company_identifier_clickhouse"),
    dg.AssetKey("wikidata_company_identifiers"),
    dg.AssetKey("wikidata_company_people"),
    dg.AssetKey("wikidata_persons"),
)


@dg.asset(
    name="se_company_person_clickhouse",
    deps=_SOURCE_ASSET_DEPS,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python", "llm", "deepseek"},
    metadata={"table": QUALIFIED_PERSON_TABLE},
    description=(
        "Publishes changed Sweden company-person profiles company by company, "
        "copying single-source companies directly and resolving multi-source "
        "companies with bounded, lossless DeepSeek requests."
    ),
)
def se_company_person_clickhouse(
    context: dg.AssetExecutionContext,
    config: SECompanyPersonConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = materialize_se_company_people(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        updated_at=datetime.now(UTC),
        company_ids=config.company_ids,
        max_companies=config.max_companies,
        company_batch_size=config.company_batch_size,
        maximum_observations_per_request=(config.maximum_observations_per_request),
        timeout_seconds=config.timeout_seconds,
        llm_client=None,
        llm_model=None,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata={**metadata, "table": QUALIFIED_PERSON_TABLE})


se_company_person_job = dg.define_asset_job(
    "se_company_person_job",
    selection=dg.AssetSelection.assets(
        "se_company_person_role_draft_clickhouse",
        "se_company_person_clickhouse",
        "se_company_person_role_clickhouse",
    ),
)

se_company_person_publish_job = dg.define_asset_job(
    "se_company_person_publish_job",
    selection=dg.AssetSelection.assets("se_company_person_clickhouse"),
)


defs = dg.Definitions(
    assets=[se_company_person_clickhouse],
    jobs=[se_company_person_job, se_company_person_publish_job],
)
