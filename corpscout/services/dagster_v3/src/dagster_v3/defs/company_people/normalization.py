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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_people.draft import normalized_company_ids
from dagster_v3.defs.esef_filings.llm_enrichment import deepseek_settings

DATABASE = "corpscout"
GROUP_NAME = "company_people"

PERSON_DRAFT_TABLE = "se_company_person_draft"
PERSON_TABLE = "se_company_person"
QUALIFIED_PERSON_TABLE = f"{DATABASE}.{PERSON_TABLE}"

PERSON_COLUMNS = (
    "person_id",
    "company_id",
    "name",
    "description",
    "draft_ids",
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

_WHITESPACE_PATTERN = re.compile(r"\s+")
_ROLE_SEPARATOR_PATTERN = re.compile(r"[^a-z0-9]+")

_ROLE_ALIASES = {
    "p112": "founder",
    "p127": "owner",
    "p169": "chief_executive_officer",
    "p3320": "board_member",
    "p488": "board_chair",
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


CompanyLlmSuggester = Callable[
    [str, CompanyObservationBatch, tuple[ExistingPersonProfile, ...]],
    LlmCompanyPeopleResult,
]


def _qualified(table: str) -> str:
    return f"`{DATABASE}`.`{table}`"


def _insert_columns(columns: Sequence[str]) -> str:
    return ",\n    ".join(columns)


def _company_status_ctes() -> str:
    return """draft_companies AS (
    SELECT
        company_id,
        uniqExact(source) AS source_count,
        count() AS observation_count,
        arraySort(groupUniqArray(draft_id)) AS draft_ids
    FROM corpscout.se_company_person_draft FINAL
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
published_companies AS (
    SELECT
        company_id,
        arraySort(arrayDistinct(arrayFlatten(groupArray(draft_ids)))) AS draft_ids
    FROM corpscout.se_company_person FINAL
    WHERE (%(all_companies)s OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
company_status AS (
    SELECT
        drafts.*,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
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
ORDER BY company_id
LIMIT %(max_companies)s"""


def build_company_observations_sql() -> str:
    return """SELECT
    draft_id,
    company_id,
    source,
    fiscal_year,
    source_observed_at,
    source_value_json
FROM corpscout.se_company_person_draft FINAL
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, source, fiscal_year, source_observed_at, draft_id"""


def build_existing_profiles_sql() -> str:
    return """SELECT
    person_id,
    company_id,
    name,
    description,
    draft_ids,
    created_at
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
        fiscal_year=int(row[3]) if row[3] is not None else None,
        source_observed_at=row[4],
        source_value=_source_value(row[5]),
    )


def _profile_from_row(row: Sequence[Any]) -> tuple[str, ExistingPersonProfile]:
    company_id = str(row[1])
    return company_id, ExistingPersonProfile(
        person_id=uuid.UUID(str(row[0])),
        name=str(row[2]),
        description=str(row[3]) if row[3] is not None else None,
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[4])),
        created_at=row[5],
    )


def _role_token(value: object) -> str:
    return _ROLE_SEPARATOR_PATTERN.sub("_", str(value).strip().casefold()).strip("_")


def observation_role_bucket(observation: DraftPersonObservation) -> str:
    """Return a stable canonical-ish role bucket for request partitioning."""
    value = observation.source_value
    candidates = (
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
) -> LlmCompanyPeopleResult:
    request = build_company_people_request(
        company_id=company_id,
        batch=batch,
        previous_profiles=previous_profiles,
        model=model,
    )
    api_response = client.chat.completions.create(**request)
    content = api_response.choices[0].message.content
    if content is None:
        raise RuntimeError("Company-person normalization returned no content")
    json_start = content.find("{")
    json_end = content.rfind("}")
    if json_start < 0 or json_end < json_start:
        raise RuntimeError("Company-person normalization did not return a JSON object")

    response = LlmCompanyPeopleResponse.model_validate_json(
        content[json_start : json_end + 1]
    )
    validate_company_people_response(
        response,
        batch=batch,
        previous_profiles=previous_profiles,
    )
    usage = getattr(api_response, "usage", None)
    return LlmCompanyPeopleResult(
        response=response,
        model_provider="deepseek",
        model_name=model,
        prompt_version=PROMPT_VERSION,
        prompt_tokens=_usage_value(usage, "prompt_tokens"),
        completion_tokens=_usage_value(usage, "completion_tokens"),
    )


def _usage_value(usage: object, name: str) -> int:
    if usage is None:
        return 0
    value = getattr(usage, name, 0)
    return int(value) if value is not None else 0


def _normalized_name(value: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", value.strip()).casefold()


def _name_match_key(value: str) -> str:
    tokens = _normalized_name(value).split()
    if len(tokens) < 2:
        return tokens[0] if tokens else ""
    return f"{tokens[0]}|{tokens[-1]}"


def _person_id(company_id: str, name: str) -> uuid.UUID:
    digest = hashlib.sha256(
        f"se-company-person-v1\n{company_id}\n{_name_match_key(name)}".encode()
    ).hexdigest()
    return uuid.UUID(hex=digest[:32])


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


def _previous_profile_by_name(
    profiles: Sequence[ExistingPersonProfile],
) -> dict[str, ExistingPersonProfile]:
    result: dict[str, ExistingPersonProfile] = {}
    for profile in sorted(profiles, key=lambda item: str(item.person_id)):
        result.setdefault(_name_match_key(profile.name), profile)
    return result


def _profiles_for_request(
    profiles: Mapping[uuid.UUID, _ProfileAccumulator],
) -> tuple[ExistingPersonProfile, ...]:
    return tuple(
        ExistingPersonProfile(
            person_id=profile.person_id,
            name=profile.name,
            description=profile.description,
            draft_ids=tuple(sorted(profile.draft_ids)),
            created_at=profile.created_at,
        )
        for profile in sorted(profiles.values(), key=lambda item: str(item.person_id))
        if profile.draft_ids
    )


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
    )


def _normalize_single_source_company(
    company: CompanyPersonWork,
    *,
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], int]:
    by_name: dict[str, list[DraftPersonObservation]] = defaultdict(list)
    for observation in company.observations:
        source_name = _source_name(observation)
        if source_name:
            by_name[_name_match_key(source_name)].append(observation)

    previous_by_name = _previous_profile_by_name(company.previous_profiles)
    previous_by_id = {
        profile.person_id: profile for profile in company.previous_profiles
    }
    writes: list[PersonProfileWrite] = []
    unchanged_profile_count = 0
    source = next(iter({item.source for item in company.observations}))

    for name_key, observations in sorted(by_name.items()):
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
        previous = previous_by_name.get(name_key)
        if description is None and previous is not None:
            description = previous.description
        person_id = (
            previous.person_id
            if previous is not None
            else _person_id(company.status.company_id, name)
        )
        draft_ids = tuple(sorted(item.draft_id for item in observations))
        profile = _ProfileAccumulator(
            person_id=person_id,
            name=name,
            description=description,
            draft_ids=set(draft_ids),
            created_at=previous.created_at if previous is not None else created_at,
            model_provider="deterministic",
            model_name=f"single-source:{source}",
            prompt_version=DIRECT_PROMPT_VERSION,
        )
        if not _profile_changed(profile, previous_by_id.get(person_id)):
            unchanged_profile_count += 1
            continue
        writes.append(
            PersonProfileWrite(
                person_id=profile.person_id,
                company_id=company.status.company_id,
                name=profile.name,
                description=profile.description,
                draft_ids=draft_ids,
                model_provider=profile.model_provider,
                model_name=profile.model_name,
                prompt_version=profile.prompt_version,
                created_at=profile.created_at,
            )
        )
    return writes, unchanged_profile_count


def _normalize_multi_source_company(
    company: CompanyPersonWork,
    *,
    llm_suggester: CompanyLlmSuggester,
    maximum_observations_per_request: int,
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], dict[str, int]]:
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
    }
    previous_by_id = {
        previous.person_id: previous for previous in company.previous_profiles
    }
    batches = batch_company_observations(
        company.observations,
        maximum_observations_per_request=maximum_observations_per_request,
    )
    prompt_tokens = 0
    completion_tokens = 0

    for batch in batches:
        request_profiles = _profiles_for_request(profiles)
        result = llm_suggester(
            company.status.company_id,
            batch,
            request_profiles,
        )
        validate_company_people_response(
            result.response,
            batch=batch,
            previous_profiles=request_profiles,
        )
        prompt_tokens += result.prompt_tokens
        completion_tokens += result.completion_tokens

        batch_draft_ids = {observation.draft_id for observation in batch.observations}
        for profile in profiles.values():
            removed_ids = profile.draft_ids & batch_draft_ids
            if removed_ids:
                profile.draft_ids -= removed_ids
                profile.touched = True

        profiles_by_name = {
            _name_match_key(profile.name): profile.person_id
            for profile in profiles.values()
        }
        for suggestion in result.response.people:
            person_id = suggestion.existing_person_id
            if person_id is None:
                person_id = profiles_by_name.get(_name_match_key(suggestion.name))
            if person_id is None:
                person_id = _person_id(company.status.company_id, suggestion.name)

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

    empty_profiles = [
        str(profile.person_id)
        for profile in profiles.values()
        if profile.touched and not profile.draft_ids
    ]
    if empty_profiles:
        raise ValueError(
            "LLM merge would remove all evidence from existing profiles, which the "
            f"append-only main table cannot represent: {sorted(empty_profiles)}"
        )

    writes: list[PersonProfileWrite] = []
    unchanged_profile_count = 0
    for profile in sorted(profiles.values(), key=lambda item: str(item.person_id)):
        if not profile.touched:
            continue
        if not _profile_changed(profile, previous_by_id.get(profile.person_id)):
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
            )
        )

    return writes, {
        "llm_request_count": len(batches),
        "llm_role_batch_count": len(batches) if len(batches) > 1 else 0,
        "llm_observation_count": len(company.observations),
        "llm_prompt_tokens": prompt_tokens,
        "llm_completion_tokens": completion_tokens,
        "unchanged_profile_count": unchanged_profile_count,
    }


def normalize_companies(
    companies: Sequence[CompanyPersonWork],
    *,
    llm_suggester: CompanyLlmSuggester | None,
    maximum_observations_per_request: int,
    created_at: datetime,
) -> tuple[list[PersonProfileWrite], dict[str, int]]:
    """Normalize changed companies and return writes only after all LLM calls pass."""
    writes: list[PersonProfileWrite] = []
    metrics = {
        "direct_company_count": 0,
        "llm_company_count": 0,
        "directly_inserted_count": 0,
        "llm_inserted_count": 0,
        "llm_request_count": 0,
        "llm_role_batch_count": 0,
        "llm_observation_count": 0,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
        "unchanged_profile_count": 0,
    }

    for company in companies:
        if company.requires_llm:
            if llm_suggester is None:
                raise RuntimeError("A multi-source company requires an LLM suggester")
            metrics["llm_company_count"] += 1
            company_writes, company_metrics = _normalize_multi_source_company(
                company,
                llm_suggester=llm_suggester,
                maximum_observations_per_request=maximum_observations_per_request,
                created_at=created_at,
            )
            metrics["llm_inserted_count"] += len(company_writes)
            for name, value in company_metrics.items():
                metrics[name] += value
        else:
            metrics["direct_company_count"] += 1
            company_writes, unchanged_count = _normalize_single_source_company(
                company,
                created_at=created_at,
            )
            metrics["directly_inserted_count"] += len(company_writes)
            metrics["unchanged_profile_count"] += unchanged_count
        writes.extend(company_writes)

    return writes, metrics


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

    with clickhouse.get_connection() as client:
        parameters = {"selected_company_ids": selected_company_ids}
        for row in client.execute(build_company_observations_sql(), parameters):
            company_id, observation = _observation_from_row(row)
            observations_by_company[company_id].append(observation)
        for row in client.execute(build_existing_profiles_sql(), parameters):
            company_id, profile = _profile_from_row(row)
            profiles_by_company[company_id].append(profile)

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
        tables=(PERSON_DRAFT_TABLE, PERSON_TABLE),
    )
    query_parameters = {
        "all_companies": not company_scope,
        "company_ids": company_scope or ("",),
    }
    with clickhouse.get_connection() as client:
        statistics = client.execute(build_company_statistics_sql(), query_parameters)[0]

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
    normalization_metrics = {
        "direct_company_count": 0,
        "llm_company_count": 0,
        "directly_inserted_count": 0,
        "llm_inserted_count": 0,
        "llm_request_count": 0,
        "llm_role_batch_count": 0,
        "llm_observation_count": 0,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
        "unchanged_profile_count": 0,
    }
    selected_company_count = 0
    inserted_count = 0
    publish_batch_count = 0
    total_person_count = 0

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
                    {**query_parameters, "max_companies": current_batch_size},
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
            ) -> LlmCompanyPeopleResult:
                return request_company_people(
                    deepseek_client,
                    company_id=company_id,
                    batch=batch,
                    previous_profiles=previous_profiles,
                    model=selected_model,
                )

            llm_suggester = suggest_company_people

        writes, batch_metrics = normalize_companies(
            companies,
            llm_suggester=llm_suggester,
            maximum_observations_per_request=maximum_observations_per_request,
            created_at=updated_at,
        )
        if not writes:
            raise RuntimeError(
                "Company-person processing made no publish progress for pending "
                f"companies: {[status.company_id for status in statuses[:10]]}"
            )
        total_person_count = _insert_person_writes(
            clickhouse=clickhouse,
            writes=writes,
            source_run_id=source_run_id,
            updated_at=updated_at,
        )
        selected_company_count += len(companies)
        inserted_count += len(writes)
        publish_batch_count += 1
        for name, value in batch_metrics.items():
            normalization_metrics[name] += value

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


@dg.asset(
    name="se_company_person_clickhouse",
    deps=[dg.AssetKey("se_company_person_draft_clickhouse")],
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
        "se_company_person_draft_clickhouse",
        "se_company_person_clickhouse",
    ),
)


defs = dg.Definitions(
    assets=[se_company_person_clickhouse],
    jobs=[se_company_person_job],
)
