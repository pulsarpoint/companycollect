"""Standards-compliant ESEF report-package parsing for company enrichment.

This module deliberately does not publish into the canonical ESEF fact tables. It
produces a versioned, auditable JSON artifact that can be reviewed before any
downstream LLM or company-profile integration is introduced.
"""

import json
import shutil
import stat
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Any

from arelle import XbrlConst
from arelle.ModelInstanceObject import ModelFact
from arelle.ModelXbrl import ModelXbrl
from arelle.RuntimeOptions import RuntimeOptions
from arelle.api.Session import Session
from lxml import etree

from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.contact_candidates import (
    EsefContactCandidate,
    TaggedContactValue,
    extract_contact_candidates,
)
from dagster_v3.defs.esef_filings.website_candidates import (
    EsefWebsiteCandidate,
    TaggedWebsiteValue,
    extract_website_candidates,
)

ARELLE_VERSION = version("arelle-release")
CANDIDATE_EXTRACTOR_VERSION = "2"
EXTRACTOR_VERSIONS = {
    dependency: version(dependency)
    for dependency in ("email-validator", "lxml", "phonenumbers", "tldextract")
}
EXTRACTOR_VERSIONS["corpscout-contact-candidates"] = CANDIDATE_EXTRACTOR_VERSION
ARTIFACT_PREFIX = "esef_filings/ixbrl_segments"
MAX_PACKAGE_FILES = 10_000
MAX_PACKAGE_UNCOMPRESSED_BYTES = 1_500_000_000
MAX_PACKAGE_MEMBER_BYTES = 750_000_000
_ARELLE_SESSION_LOCK = Lock()

_STANDARD_NAMESPACE_PREFIXES = (
    "https://xbrl.ifrs.org/",
    "http://xbrl.ifrs.org/",
    "https://www.esma.europa.eu/",
    "http://www.esma.europa.eu/",
    "http://www.xbrl.org/",
    "https://xbrl.org/",
    "http://xbrl.org/",
)

_SEGMENT_CONCEPTS: dict[str, frozenset[str]] = {
    "identity": frozenset(
        {
            "NameOfReportingEntityOrOtherMeansOfIdentification",
            "NameOfReportingEntity",
            "LegalFormOfEntity",
            "CountryOfIncorporation",
            "DomicileOfEntity",
            "ExplanationOfChangeInNameOfReportingEntityOrOtherMeansOfIdentificationFromEndOfPrecedingReportingPeriod",
            "ChangeInNameOfReportingEntityOrOtherMeansOfIdentificationFromEndOfPrecedingReportingPeriod",
            "NameOfParentEntity",
            "IDOfParentEntity",
            "LEIOfParentEntity",
            "NameOfUltimateParentOfGroup",
            "IDOfUltimateParentOfGroup",
            "LEIOfUltimateParentOfGroup",
            "NameOfMostSeniorParentEntityProducingPubliclyAvailableFinancialStatements",
            "IDOfMostSeniorParentEntityProducingPubliclyAvailableFinancialStatements",
            "LEIOfMostSeniorParentEntityProducingPubliclyAvailableFinancialStatements",
            "WhetherFinancialStatementsAreOfGroupOfEntities",
        }
    ),
    "business_profile": frozenset(
        {"DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities"}
    ),
    "locations_and_contacts": frozenset(
        {
            "AddressOfRegisteredOfficeOfEntity",
            "PrincipalPlaceOfBusinessOfEntity",
            "WebsitesOfLegalEntity",
            "WebsiteAtWhichTheFinancialStatementsOfTheEntityAreDisclosedTogetherWithTheAuditorsReport",
            "EMailAddress",
            "ContactPhoneNumber",
            "AffiliatesOfReportingEntityAddressesAndPhones",
        }
    ),
    "products_markets_and_segments": frozenset(
        {
            "DisclosureOfEntitysReportableSegmentsExplanatory",
            "FactorsUsedToIdentifyEntitysReportableSegments",
            "DescriptionOfTypesOfProductsAndServicesFromWhichEachReportableSegmentDerivesItsRevenues",
            "ReportableSegment1",
            "ReportableSegment2",
            "ReportableSegment3",
            "ReportableSegment4",
            "ReportableSegment5",
            "ReportableSegment6",
            "ReportableSegment7",
        }
    ),
    "people_and_audit": frozenset(
        {
            "DisclosureOfInformationAboutKeyManagementPersonnelExplanatory",
            "KeyManagementPersonnelOfEntityOrParent",
            "DisclosureOfDirectorsRemuneration",
            "DisclosureOfDirectorsRemunerationExplanatory",
            "DisclosureOfDirectorRemuneration",
            "DisclosureOfDirectorsEmoluments",
            "DisclosureOfDirectorsEmolumentsExplanatory",
            "DisclosureOfAuditorsRemunerationExplanatory",
            "WebsiteOfTheAuditEntity",
            "KeyPartnerWhoSignedTheAuditorsReportThatIsDisclosedTogetherWithTheFinancialStatementsAtTheWebAddressIndicatedInThisForm",
            "AuditReportNameOfKeyAuditPartner",
            "RegistrationNumberInRegisterOfAuditorsAndAuditingEntities",
            "AuditReportSectionOfRegisterOfAuditorsAndAuditEntities",
            "AuditorResponsibilityForMeetingInlineXBRLReportingRequirements",
        }
    ),
    "group_structure": frozenset(
        {
            "DisclosureOfSignificantInvestmentsInSubsidiariesExplanatory",
            "DisclosureOfInformationOnSubsidiariesTextBlock",
            "DisclosureOfInterestsInSubsidiariesExplanatory",
            "Subsidiaries",
            "NameOfSubsidiary",
            "IDOfSubsidiary",
            "IdentificationCodeOfSubsidiaryLegalEntity",
            "CountryOfIncorporationOrResidenceOfSubsidiary",
            "PrincipalPlaceOfBusinessOfSubsidiary",
            "ProportionOfOwnershipInterestInSubsidiary",
            "ProportionOfVotingPowerHeldInSubsidiary",
            "IDOfUnconsolidatedSubsidiary",
            "CountryOfIncorporationOrResidenceOfAssociate",
        }
    ),
    "workforce": frozenset(
        {
            "DisclosureOfInformationAboutEmployeesExplanatory",
            "DisclosureOfEmployeeBenefitsExplanatory",
            "AverageNumberOfEmployees",
            "NumberOfEmployees",
            "EmployeeBenefitsExpense",
        }
    ),
    "securities_and_listing": frozenset(
        {
            "NameOfExchangeOnWhichEntitysSecuritiesAreListed",
            "TickerSymbol",
            "DescriptionOfClassOfShareCapital",
            "NumberOfSharesIssued",
            "NumberOfSharesOutstanding",
        }
    ),
    "financial_highlights": frozenset(
        {
            "Revenue",
            "ProfitLoss",
            "ProfitLossBeforeTax",
            "Assets",
            "Liabilities",
            "Equity",
            "CashAndCashEquivalents",
            "CashFlowsFromUsedInOperatingActivities",
            "DividendsRecognisedAsDistributionsToOwnersPerShare",
        }
    ),
}


@dataclass(frozen=True)
class EsefArtifactSource:
    fxo_id: str
    country: str = ""
    source_url: str = ""
    object_key: str = ""
    company_id: str = ""
    source_run_id: str = ""
    expected_package_sha256: str = ""


@dataclass(frozen=True)
class EsefConcept:
    qname: str
    namespace: str
    local_name: str
    labels: dict[str, str]
    documentation: dict[str, str]
    type_qname: str
    base_xsd_type: str
    period_type: str
    balance: str
    is_numeric: bool
    is_text_block: bool
    is_extension: bool
    anchors: list[str]
    presentation_roles: list[str]
    presentation_parents: list[str]


@dataclass(frozen=True)
class EsefFactLink:
    target_fact_key: str
    arcrole: str
    linkrole: str


@dataclass(frozen=True)
class EsefFact:
    fact_key: str
    source_fact_id: str
    ordinal: int
    report_member: str
    concept_qname: str
    canonical_value: str | None
    language: str
    decimals: int | None
    unit: dict[str, list[str]] | None
    is_nil: bool
    is_numeric: bool
    entity: dict[str, str]
    period: dict[str, str]
    dimensions: dict[str, str]
    oim_dimensions: dict[str, str]
    links: list[EsefFactLink]


@dataclass(frozen=True)
class EsefSegmentReference:
    fact_key: str
    selection_reason: str


@dataclass(frozen=True)
class EsefDocument:
    report_members: list[str]
    namespaces: dict[str, str]
    entities: list[dict[str, str]]
    periods: list[dict[str, str]]
    languages: list[str]
    units: list[dict[str, list[str]]]
    taxonomy_entrypoints: list[str]


@dataclass(frozen=True)
class EsefValidationMessage:
    code: str
    level: str
    message: str


@dataclass(frozen=True)
class EsefQuality:
    fact_count: int
    text_fact_count: int
    numeric_fact_count: int
    nil_fact_count: int
    duplicate_fact_count: int
    error_count: int
    warning_count: int
    unclassified_concepts: list[str]
    validation_messages: list[EsefValidationMessage]


@dataclass(frozen=True)
class EsefParser:
    engine: str = "Arelle"
    version: str = ARELLE_VERSION
    validation_profile: str = "ESEF"
    candidate_extractors: str = "deterministic XHTML and tagged-fact extraction"
    candidate_extractor_versions: dict[str, str] = field(
        default_factory=lambda: dict(EXTRACTOR_VERSIONS)
    )


@dataclass(frozen=True)
class EsefSegmentArtifact:
    schema_version: int
    package_sha256: str
    source: EsefArtifactSource
    parser: EsefParser
    document: EsefDocument
    concepts: dict[str, EsefConcept]
    facts: dict[str, EsefFact]
    segments: dict[str, list[EsefSegmentReference]]
    contact_candidates: list[EsefContactCandidate]
    website_candidates: list[EsefWebsiteCandidate]
    quality: EsefQuality


@dataclass
class _ParsedModels:
    concepts: dict[str, EsefConcept] = field(default_factory=dict)
    facts: dict[str, EsefFact] = field(default_factory=dict)
    validation_messages: list[EsefValidationMessage] = field(default_factory=list)
    taxonomy_entrypoints: set[str] = field(default_factory=set)
    namespaces: dict[str, str] = field(default_factory=dict)


def parse_esef_report_package(
    package_path: str | Path,
    *,
    source: EsefArtifactSource,
    validate_esef: bool = True,
) -> EsefSegmentArtifact:
    """Parse one ESEF report package into a deterministic enrichment artifact."""
    resolved_package_path = Path(package_path).resolve(strict=True)
    package_sha256 = sha256(resolved_package_path.read_bytes()).hexdigest()
    if (
        source.expected_package_sha256 != ""
        and source.expected_package_sha256 != package_sha256
    ):
        raise ValueError(
            "ESEF package SHA-256 does not match the expected source hash: "
            f"expected={source.expected_package_sha256} actual={package_sha256}"
        )

    with tempfile.TemporaryDirectory(prefix="esef_ixbrl_segments_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        report_members = _extract_report_package(resolved_package_path, temp_dir)
        parsed = _parse_report_members(
            package_path=resolved_package_path,
            temp_dir=temp_dir,
            report_members=report_members,
            package_sha256=package_sha256,
            validate_esef=validate_esef,
        )
        report_paths = {
            report_member: temp_dir.joinpath(*PurePosixPath(report_member).parts)
            for report_member in report_members
        }
        report_languages = _report_languages(report_paths.values())
        tagged_facts = [
            (
                fact.report_member,
                parsed.concepts[fact.concept_qname].local_name,
                fact.canonical_value,
            )
            for fact in parsed.facts.values()
            if fact.canonical_value is not None
            and fact.concept_qname in parsed.concepts
        ]
        contact_candidates = extract_contact_candidates(
            report_paths,
            tagged_values=(
                TaggedContactValue(
                    report_member=report_member,
                    concept_local_name=concept_local_name,
                    value=value,
                )
                for report_member, concept_local_name, value in tagged_facts
            ),
            default_region=source.country,
        )
        website_candidates = extract_website_candidates(
            report_paths,
            tagged_values=(
                TaggedWebsiteValue(
                    report_member=report_member,
                    concept_local_name=concept_local_name,
                    value=value,
                )
                for report_member, concept_local_name, value in tagged_facts
            ),
            known_email_domains=(
                candidate.normalized_value.rpartition("@")[2]
                for candidate in contact_candidates
                if candidate.kind == "email"
            ),
        )

    segments = _select_segments(parsed.facts, parsed.concepts)
    classified_fact_keys = {
        reference.fact_key
        for references in segments.values()
        for reference in references
    }
    duplicate_count = len(parsed.facts) - len(
        {_fact_signature(fact) for fact in parsed.facts.values()}
    )
    return EsefSegmentArtifact(
        schema_version=ARTIFACT_SCHEMA_VERSION,
        package_sha256=package_sha256,
        source=source,
        parser=EsefParser(
            validation_profile="ESEF" if validate_esef else "XBRL load only"
        ),
        document=_document_from_facts(
            report_members=report_members,
            facts=parsed.facts.values(),
            taxonomy_entrypoints=parsed.taxonomy_entrypoints,
            namespaces=parsed.namespaces,
            report_languages=report_languages,
        ),
        concepts=dict(sorted(parsed.concepts.items())),
        facts=dict(sorted(parsed.facts.items())),
        segments=segments,
        contact_candidates=contact_candidates,
        website_candidates=website_candidates,
        quality=EsefQuality(
            fact_count=len(parsed.facts),
            text_fact_count=sum(
                not fact.is_numeric and not fact.is_nil
                for fact in parsed.facts.values()
            ),
            numeric_fact_count=sum(fact.is_numeric for fact in parsed.facts.values()),
            nil_fact_count=sum(fact.is_nil for fact in parsed.facts.values()),
            duplicate_fact_count=max(duplicate_count, 0),
            error_count=sum(
                message.level == "error" for message in parsed.validation_messages
            ),
            warning_count=sum(
                message.level == "warning" for message in parsed.validation_messages
            ),
            unclassified_concepts=sorted(
                {
                    fact.concept_qname
                    for fact in parsed.facts.values()
                    if fact.fact_key not in classified_fact_keys
                    and fact.concept_qname != "xbrl:note"
                }
            ),
            validation_messages=parsed.validation_messages,
        ),
    )


def artifact_json_bytes(artifact: EsefSegmentArtifact) -> bytes:
    """Serialize an artifact canonically for stable hashes and S3 idempotency."""
    return json.dumps(
        asdict(artifact),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def artifact_object_key(package_sha256: str) -> str:
    if len(package_sha256) != 64:
        raise ValueError("ESEF package SHA-256 must contain 64 hexadecimal characters")
    try:
        int(package_sha256, 16)
    except ValueError as exc:
        raise ValueError("ESEF package SHA-256 must be hexadecimal") from exc
    return (
        f"{ARTIFACT_PREFIX}/schema=v{ARTIFACT_SCHEMA_VERSION}/"
        f"parser=arelle-{ARELLE_VERSION}/candidates=v{CANDIDATE_EXTRACTOR_VERSION}/"
        f"package_sha256={package_sha256}/artifact.json"
    )


def compare_artifact_to_oim(
    artifact: EsefSegmentArtifact,
    reference: Mapping[str, Any],
) -> dict[str, int]:
    """Compare parsed facts with an OIM xBRL-JSON reference as multisets."""
    reference_facts = reference.get("facts")
    if not isinstance(reference_facts, Mapping):
        raise ValueError("OIM reference must contain a facts object")

    artifact_signatures = sorted(
        _oim_signature(
            value=fact.canonical_value,
            decimals=fact.decimals,
            dimensions=fact.oim_dimensions,
        )
        for fact in artifact.facts.values()
    )
    reference_signatures = sorted(
        _oim_signature(
            value=entry.get("value"),
            decimals=entry.get("decimals"),
            dimensions=entry.get("dimensions"),
        )
        for entry in reference_facts.values()
        if isinstance(entry, Mapping)
    )
    artifact_counts = _signature_counts(artifact_signatures)
    reference_counts = _signature_counts(reference_signatures)
    matched = sum(
        min(count, reference_counts.get(signature, 0))
        for signature, count in artifact_counts.items()
    )
    return {
        "artifact_fact_count": len(artifact_signatures),
        "reference_fact_count": len(reference_signatures),
        "matched_fact_count": matched,
        "artifact_only_count": len(artifact_signatures) - matched,
        "reference_only_count": len(reference_signatures) - matched,
    }


def _extract_report_package(package_path: Path, temp_dir: Path) -> list[str]:
    with zipfile.ZipFile(package_path) as package:
        members = package.infolist()
        if len(members) > MAX_PACKAGE_FILES:
            raise ValueError(
                f"ESEF package contains too many files: {len(members)} > {MAX_PACKAGE_FILES}"
            )
        total_size = sum(member.file_size for member in members)
        if total_size > MAX_PACKAGE_UNCOMPRESSED_BYTES:
            raise ValueError(
                "ESEF package expands beyond the configured limit: "
                f"{total_size} > {MAX_PACKAGE_UNCOMPRESSED_BYTES}"
            )

        report_members: list[str] = []
        for member in members:
            normalized_name = member.filename.replace("\\", "/")
            member_path = PurePosixPath(normalized_name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"ESEF package contains an unsafe path: {member.filename}"
                )
            if member.file_size > MAX_PACKAGE_MEMBER_BYTES:
                raise ValueError(
                    "ESEF package member expands beyond the configured limit: "
                    f"{member.filename}"
                )
            mode = member.external_attr >> 16
            if mode != 0 and stat.S_ISLNK(mode):
                raise ValueError(
                    f"ESEF package contains an unsupported symbolic link: {member.filename}"
                )
            if member.is_dir():
                continue

            target_path = temp_dir.joinpath(*member_path.parts)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with (
                package.open(member) as source_file,
                target_path.open("wb") as target_file,
            ):
                shutil.copyfileobj(source_file, target_file)
            lowered = f"/{normalized_name.lower()}"
            if "/reports/" in lowered and lowered.endswith((".xhtml", ".html")):
                report_members.append(normalized_name)

    if not report_members:
        raise ValueError("ESEF report package contains no report XHTML members")
    return sorted(report_members)


def _parse_report_members(
    *,
    package_path: Path,
    temp_dir: Path,
    report_members: list[str],
    package_sha256: str,
    validate_esef: bool,
) -> _ParsedModels:
    parsed = _ParsedModels()
    parse_targets = (
        [(package_path, report_members[0], True)]
        if validate_esef
        else [
            (
                temp_dir.joinpath(*PurePosixPath(report_member).parts),
                report_member,
                False,
            )
            for report_member in report_members
        ]
    )
    for report_index, (entrypoint_path, report_member, run_validation) in enumerate(
        parse_targets,
        start=1,
    ):
        options = RuntimeOptions(
            entrypointFile=str(entrypoint_path),
            packages=[str(package_path)],
            plugins="validate/ESEF" if run_validation else None,
            disclosureSystemName="esef" if run_validation else None,
            validate=run_validation,
            validateDuplicateFacts="all" if run_validation else None,
            keepOpen=True,
            disablePersistentConfig=True,
            internetConnectivity="online",
            internetRecheck="never",
            logFile="logToBuffer",
            logLevel="WARNING",
            logTextMaxLength=1_000_000,
        )
        with _ARELLE_SESSION_LOCK, Session() as session:
            session.run(options, logFileName="logToBuffer")
            models = session.get_models()
            parsed.validation_messages.extend(
                _validation_messages(session.get_logs("json"), temp_dir)
            )
            if not models:
                raise ValueError(
                    f"Arelle could not load ESEF report member: {report_member}"
                )
            model = models[-1]
            _merge_model(
                parsed=parsed,
                model=model,
                temp_dir=temp_dir,
                report_member=report_member,
                report_index=report_index,
                package_sha256=package_sha256,
                package_path=package_path,
            )

    parsed.validation_messages.sort(
        key=lambda message: (message.level, message.code, message.message)
    )
    return parsed


def _merge_model(
    *,
    parsed: _ParsedModels,
    model: ModelXbrl,
    temp_dir: Path,
    report_member: str,
    report_index: int,
    package_sha256: str,
    package_path: Path,
) -> None:
    parsed.namespaces.update(
        {
            str(prefix): str(namespace)
            for prefix, namespace in model.prefixedNamespaces.items()
            if prefix is not None
        }
    )
    fact_languages = {
        _fact_language(fact)
        for fact in model.factsInInstance
        if _fact_language(fact) != ""
    }
    taxonomy_label_languages = {
        str(resource.xmlLang).lower()
        for relationship in model.relationshipSet(
            XbrlConst.conceptLabel
        ).modelRelationships
        if (resource := relationship.toModelObject) is not None
        and getattr(resource, "xmlLang", None) is not None
        and str(resource.xmlLang).strip() != ""
    }
    label_languages = sorted(fact_languages | taxonomy_label_languages | {"en"})
    relationships = _relationship_metadata(model)

    ordered_facts = sorted(model.factsInInstance, key=_fact_sort_key)
    model_fact_keys: dict[int, str] = {}
    for ordinal, fact in enumerate(ordered_facts, start=1):
        if fact.concept is None or fact.context is None:
            continue
        fact_report_member = _report_member(
            str(fact.modelDocument.uri),
            package_path=package_path,
            default=report_member,
        )
        source_fact_id = fact.id or f"report-{report_index}-fact-{ordinal}"
        fact_key = sha256(
            (
                f"{package_sha256}|{fact_report_member}|{source_fact_id}|{ordinal}"
            ).encode()
        ).hexdigest()
        if fact_key in parsed.facts:
            raise ValueError(f"Duplicate deterministic ESEF fact key: {fact_key}")

        concept_qname = str(fact.qname)
        unit = _unit(fact.unit)
        entity = {
            "scheme": str(fact.context.entityIdentifier[0]),
            "identifier": str(fact.context.entityIdentifier[1]),
        }
        parsed.namespaces.setdefault("scheme", entity["scheme"])
        period = _period(fact.context)
        dimensions = _dimensions(fact.context)
        language = _fact_language(fact)
        parsed.facts[fact_key] = EsefFact(
            fact_key=fact_key,
            source_fact_id=source_fact_id,
            ordinal=ordinal,
            report_member=fact_report_member,
            concept_qname=concept_qname,
            canonical_value=_canonical_value(fact),
            language=language,
            decimals=_decimals(fact.decimals),
            unit=unit,
            is_nil=bool(fact.isNil),
            is_numeric=bool(fact.isNumeric),
            entity=entity,
            period=period,
            dimensions=dimensions,
            oim_dimensions=_oim_dimensions(
                model=model,
                concept_qname=concept_qname,
                entity=entity,
                period=period,
                unit=unit,
                language=language,
                dimensions=dimensions,
            ),
            links=[],
        )
        model_fact_keys[id(fact)] = fact_key
        if concept_qname not in parsed.concepts:
            parsed.concepts[concept_qname] = _concept(
                fact.concept,
                label_languages=label_languages,
                relationships=relationships.get(concept_qname, {}),
            )

    _merge_footnotes(
        parsed=parsed,
        model=model,
        model_fact_keys=model_fact_keys,
        report_member=report_member,
        report_index=report_index,
        first_ordinal=len(ordered_facts) + 1,
        package_sha256=package_sha256,
        package_path=package_path,
    )

    model_document = model.modelDocument
    if model_document is not None:
        for referenced_document in model_document.referencesDocument:
            parsed.taxonomy_entrypoints.add(
                _portable_uri(
                    str(referenced_document.uri),
                    temp_dir=temp_dir,
                    package_path=package_path,
                )
            )


def _merge_footnotes(
    *,
    parsed: _ParsedModels,
    model: ModelXbrl,
    model_fact_keys: Mapping[int, str],
    report_member: str,
    report_index: int,
    first_ordinal: int,
    package_sha256: str,
    package_path: Path,
) -> None:
    relationships = list(model.relationshipSet("XBRL-footnotes").modelRelationships)
    text_resources_by_identity = {
        id(relationship.toModelObject): relationship.toModelObject
        for relationship in relationships
        if not isinstance(relationship.toModelObject, ModelFact)
    }
    note_keys: dict[int, str] = {}
    for offset, resource in enumerate(
        sorted(text_resources_by_identity.values(), key=_footnote_sort_key),
        start=0,
    ):
        ordinal = first_ordinal + offset
        note_text = str(resource.viewText())
        note_report_member = _report_member(
            str(resource.modelDocument.uri),
            package_path=package_path,
            default=report_member,
        )
        source_fact_id = resource.id or f"report-{report_index}-note-{offset + 1}"
        fact_key = sha256(
            (
                f"{package_sha256}|{note_report_member}|footnote|"
                f"{source_fact_id}|{ordinal}|{sha256(note_text.encode()).hexdigest()}"
            ).encode()
        ).hexdigest()
        language = str(resource.xmlLang or "").lower()
        note_keys[id(resource)] = fact_key
        parsed.facts[fact_key] = EsefFact(
            fact_key=fact_key,
            source_fact_id=source_fact_id,
            ordinal=ordinal,
            report_member=note_report_member,
            concept_qname="xbrl:note",
            canonical_value=note_text,
            language=language,
            decimals=None,
            unit=None,
            is_nil=False,
            is_numeric=False,
            entity={},
            period={},
            dimensions={},
            oim_dimensions={
                "concept": "xbrl:note",
                "noteId": source_fact_id,
                **({"language": language} if language else {}),
            },
            links=[],
        )

    if note_keys:
        parsed.namespaces.setdefault("xbrl", "https://xbrl.org/2021")
        parsed.concepts.setdefault("xbrl:note", _note_concept())

    links_by_source: dict[str, list[EsefFactLink]] = {}
    for relationship in relationships:
        source_key = model_fact_keys.get(id(relationship.fromModelObject))
        if source_key is None:
            continue
        target_key = model_fact_keys.get(id(relationship.toModelObject))
        if target_key is None:
            target_key = note_keys.get(id(relationship.toModelObject))
        if target_key is None:
            continue
        links_by_source.setdefault(source_key, []).append(
            EsefFactLink(
                target_fact_key=target_key,
                arcrole=str(relationship.arcrole or ""),
                linkrole=str(relationship.linkrole or ""),
            )
        )

    for source_key, links in links_by_source.items():
        parsed.facts[source_key] = replace(
            parsed.facts[source_key],
            links=sorted(
                links,
                key=lambda link: (
                    link.arcrole,
                    link.linkrole,
                    link.target_fact_key,
                ),
            ),
        )


def _footnote_sort_key(resource: Any) -> tuple[str, str, str]:
    return (
        str(resource.id or ""),
        str(getattr(resource, "sourceline", "") or ""),
        str(resource.viewText()),
    )


def _note_concept() -> EsefConcept:
    return EsefConcept(
        qname="xbrl:note",
        namespace="https://xbrl.org/2021",
        local_name="note",
        labels={"en": "XBRL footnote"},
        documentation={},
        type_qname="",
        base_xsd_type="string",
        period_type="",
        balance="",
        is_numeric=False,
        is_text_block=True,
        is_extension=False,
        anchors=[],
        presentation_roles=[],
        presentation_parents=[],
    )


def _fact_sort_key(fact: ModelFact) -> tuple[str, ...]:
    return (
        str(fact.id or ""),
        str(fact.qname or ""),
        str(getattr(fact, "contextID", "") or ""),
        str(getattr(fact, "unitID", "") or ""),
        str(fact.xmlLang or ""),
        str(fact.value or ""),
        str(getattr(fact, "sourceline", "") or ""),
    )


def _relationship_metadata(model: ModelXbrl) -> dict[str, dict[str, set[str]]]:
    result: dict[str, dict[str, set[str]]] = {}
    anchoring = model.relationshipSet(XbrlConst.widerNarrower)
    presentation = model.relationshipSet(XbrlConst.parentChild)
    for concept in model.qnameConcepts.values():
        concept_qname = str(concept.qname)
        anchors = {
            str(relationship.toModelObject.qname)
            for relationship in anchoring.fromModelObject(concept)
            if getattr(relationship.toModelObject, "qname", None) is not None
        }
        anchors.update(
            str(relationship.fromModelObject.qname)
            for relationship in anchoring.toModelObject(concept)
            if getattr(relationship.fromModelObject, "qname", None) is not None
        )
        parent_relationships = presentation.toModelObject(concept)
        parents = {
            str(relationship.fromModelObject.qname)
            for relationship in parent_relationships
            if getattr(relationship.fromModelObject, "qname", None) is not None
        }
        roles = {
            str(relationship.linkrole)
            for relationship in [
                *presentation.fromModelObject(concept),
                *parent_relationships,
            ]
            if relationship.linkrole
        }
        if anchors or parents or roles:
            result[concept_qname] = {
                "anchors": anchors,
                "parents": parents,
                "roles": roles,
            }
    return result


def _concept(
    concept: Any,
    *,
    label_languages: list[str],
    relationships: Mapping[str, set[str]],
) -> EsefConcept:
    labels: dict[str, str] = {}
    documentation: dict[str, str] = {}
    for language in label_languages:
        label = concept.label(lang=language, fallbackToQname=False, strip=True)
        if label:
            labels[language] = str(label)
        description = concept.label(
            preferredLabel=XbrlConst.documentationLabel,
            lang=language,
            fallbackToQname=False,
            strip=True,
        )
        if description:
            documentation[language] = str(description)
    namespace = str(concept.qname.namespaceURI or "")
    return EsefConcept(
        qname=str(concept.qname),
        namespace=namespace,
        local_name=str(concept.qname.localName),
        labels=dict(sorted(labels.items())),
        documentation=dict(sorted(documentation.items())),
        type_qname=str(concept.typeQname or ""),
        base_xsd_type=str(concept.baseXsdType or ""),
        period_type=str(concept.periodType or ""),
        balance=str(concept.balance or ""),
        is_numeric=bool(concept.isNumeric),
        is_text_block=bool(concept.isTextBlock),
        is_extension=not namespace.startswith(_STANDARD_NAMESPACE_PREFIXES),
        anchors=sorted(relationships.get("anchors", set())),
        presentation_roles=sorted(relationships.get("roles", set())),
        presentation_parents=sorted(relationships.get("parents", set())),
    )


def _select_segments(
    facts: Mapping[str, EsefFact],
    concepts: Mapping[str, EsefConcept],
) -> dict[str, list[EsefSegmentReference]]:
    categories_by_local_name = {
        local_name: category
        for category, local_names in _SEGMENT_CONCEPTS.items()
        for local_name in local_names
    }
    result: dict[str, list[EsefSegmentReference]] = {
        category: [] for category in _SEGMENT_CONCEPTS
    }
    for fact in sorted(
        facts.values(),
        key=lambda item: (item.report_member, item.ordinal, item.fact_key),
    ):
        concept = concepts[fact.concept_qname]
        category = categories_by_local_name.get(concept.local_name)
        reason = f"concept:{concept.local_name}"
        if category is None:
            for anchor in concept.anchors:
                anchor_local_name = anchor.rsplit(":", 1)[-1]
                category = categories_by_local_name.get(anchor_local_name)
                if category is not None:
                    reason = f"anchor:{anchor}"
                    break
        if category is None:
            for parent in concept.presentation_parents:
                parent_local_name = parent.rsplit(":", 1)[-1]
                category = categories_by_local_name.get(parent_local_name)
                if category is not None:
                    reason = f"presentation-parent:{parent}"
                    break
        if category is not None:
            result[category].append(
                EsefSegmentReference(fact_key=fact.fact_key, selection_reason=reason)
            )
    return result


def _document_from_facts(
    *,
    report_members: list[str],
    facts: Iterable[EsefFact],
    taxonomy_entrypoints: Iterable[str],
    namespaces: Mapping[str, str],
    report_languages: Iterable[str],
) -> EsefDocument:
    fact_list = list(facts)
    entities = sorted(
        {json.dumps(fact.entity, sort_keys=True) for fact in fact_list if fact.entity}
    )
    periods = sorted(
        {json.dumps(fact.period, sort_keys=True) for fact in fact_list if fact.period}
    )
    units = sorted(
        {
            json.dumps(fact.unit, sort_keys=True)
            for fact in fact_list
            if fact.unit is not None
        }
    )
    return EsefDocument(
        report_members=report_members,
        namespaces=dict(sorted(namespaces.items())),
        entities=[json.loads(value) for value in entities],
        periods=[json.loads(value) for value in periods],
        languages=sorted(
            {fact.language for fact in fact_list if fact.language != ""}
            | {language for language in report_languages if language != ""}
        ),
        units=[json.loads(value) for value in units],
        taxonomy_entrypoints=sorted(taxonomy_entrypoints),
    )


def _report_languages(report_paths: Iterable[Path]) -> set[str]:
    """Read the submitted XHTML language without loading full reports in memory."""
    languages: set[str] = set()
    xml_language = "{http://www.w3.org/XML/1998/namespace}lang"
    for report_path in report_paths:
        try:
            for _event, element in etree.iterparse(
                str(report_path),
                events=("start",),
                huge_tree=True,
                recover=True,
            ):
                language = str(element.get(xml_language) or element.get("lang") or "")
                normalized = language.strip().lower()
                if normalized != "":
                    languages.add(normalized)
                break
        except OSError, etree.XMLSyntaxError:
            continue
    return languages


def _unit(unit: Any) -> dict[str, list[str]] | None:
    if unit is None:
        return None
    numerator, denominator = unit.measures
    return {
        "numerators": sorted(str(measure) for measure in numerator),
        "denominators": sorted(str(measure) for measure in denominator),
    }


def _period(context: Any) -> dict[str, str]:
    if context.isForeverPeriod:
        return {"forever": "true"}
    if context.isInstantPeriod:
        return {"instant": _iso_value(context.instantDatetime)}
    return {
        "start": _iso_value(context.startDatetime),
        "end": _iso_value(context.endDatetime),
    }


def _dimensions(context: Any) -> dict[str, str]:
    dimensions: dict[str, str] = {}
    for dimension_qname, dimension_value in context.qnameDims.items():
        if dimension_value.isExplicit:
            value = str(dimension_value.memberQname)
        else:
            typed_member = dimension_value.typedMember
            value = str(typed_member.stringValue if typed_member is not None else "")
        dimensions[str(dimension_qname)] = value
    return dict(sorted(dimensions.items()))


def _canonical_value(fact: Any) -> str | None:
    if fact.isNil:
        return None
    value = fact.xValue
    if value is None:
        value = fact.value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        if value.is_infinite():
            return "-INF" if value < 0 else "INF"
        if value.is_nan():
            return "NaN"
        if value == value.to_integral():
            return f"{value:.1f}"
        integer_part, fractional_part = f"{value:f}".split(".")
        return f"{integer_part}.{fractional_part.rstrip('0') or '0'}"
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _fact_language(fact: ModelFact) -> str:
    concept_type = fact.concept.type if fact.concept is not None else None
    if (
        concept_type is not None
        and concept_type.isOimTextFactType
        and fact.xmlLang is not None
    ):
        return str(fact.xmlLang).lower()
    return ""


def _decimals(value: Any) -> int | None:
    if value is None or str(value).upper() == "INF":
        return None
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def _oim_dimensions(
    *,
    model: ModelXbrl,
    concept_qname: str,
    entity: Mapping[str, str],
    period: Mapping[str, str],
    unit: Mapping[str, list[str]] | None,
    language: str,
    dimensions: Mapping[str, str],
) -> dict[str, str]:
    result = {
        "concept": concept_qname,
        "entity": _prefixed_name(
            model,
            entity["scheme"],
            entity["identifier"],
            preferred_prefix="scheme",
            force_preferred_prefix=True,
        ),
        "period": _oim_period(period),
    }
    if unit is not None:
        result["unit"] = _oim_unit(unit)
    if language != "":
        result["language"] = language
    result.update(dimensions)
    return dict(sorted(result.items()))


def _prefixed_name(
    model: ModelXbrl,
    namespace: str,
    local_name: str,
    *,
    preferred_prefix: str = "",
    force_preferred_prefix: bool = False,
) -> str:
    prefixes = sorted(
        prefix
        for prefix, uri in model.prefixedNamespaces.items()
        if uri == namespace and prefix is not None
    )
    if force_preferred_prefix or preferred_prefix in prefixes:
        return f"{preferred_prefix}:{local_name}"
    if prefixes:
        return f"{prefixes[0]}:{local_name}"
    return f"{namespace}:{local_name}"


def _oim_period(period: Mapping[str, str]) -> str:
    if "instant" in period:
        return period["instant"]
    if "start" in period and "end" in period:
        return f"{period['start']}/{period['end']}"
    return "forever"


def _oim_unit(unit: Mapping[str, list[str]]) -> str:
    numerators = "*".join(unit["numerators"])
    denominators = "*".join(unit["denominators"])
    return f"{numerators}/{denominators}" if denominators else numerators


def _validation_messages(log_json: str, temp_dir: Path) -> list[EsefValidationMessage]:
    payload = json.loads(log_json)
    records = payload.get("log", []) if isinstance(payload, Mapping) else []
    result: list[EsefValidationMessage] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        message = record.get("message", {})
        text = message.get("text", "") if isinstance(message, Mapping) else message
        portable_text = str(text)
        for temporary_prefix in {str(temp_dir), str(temp_dir.resolve())}:
            portable_text = portable_text.replace(temporary_prefix, "<package>")
        result.append(
            EsefValidationMessage(
                code=str(record.get("code", "")),
                level=str(record.get("level", "")),
                message=portable_text,
            )
        )
    return result


def _portable_uri(uri: str, *, temp_dir: Path, package_path: Path) -> str:
    package_prefix = f"{package_path}/"
    if uri.startswith(package_prefix):
        return f"package:/{uri.removeprefix(package_prefix)}"
    uri_path = Path(uri)
    resolved_temp_dir = temp_dir.resolve()
    try:
        relative_path = uri_path.resolve().relative_to(resolved_temp_dir)
    except OSError, ValueError:
        return uri
    else:
        return "package:/" + relative_path.as_posix()


def _report_member(uri: str, *, package_path: Path, default: str) -> str:
    package_prefix = f"{package_path}/"
    if uri.startswith(package_prefix):
        return uri.removeprefix(package_prefix)
    return default


def _iso_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _fact_signature(fact: EsefFact) -> str:
    return json.dumps(
        {
            "concept": fact.concept_qname,
            "value": fact.canonical_value,
            "dimensions": fact.oim_dimensions,
        },
        sort_keys=True,
    )


def _oim_signature(
    *,
    value: Any,
    decimals: Any,
    dimensions: Any,
) -> str:
    normalized_decimals = _decimals(decimals)
    normalized_dimensions = dict(dimensions) if isinstance(dimensions, Mapping) else {}
    if normalized_dimensions.get("concept") == "xbrl:note":
        normalized_dimensions.pop("noteId", None)
    return json.dumps(
        {
            "value": _comparison_value(
                value,
                numeric=(
                    normalized_decimals is not None or "unit" in normalized_dimensions
                ),
            ),
            "decimals": normalized_decimals,
            "dimensions": normalized_dimensions,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _comparison_value(value: Any, *, numeric: bool) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not numeric:
        return text
    try:
        decimal_value = Decimal(text)
    except ArithmeticError, ValueError:
        return text
    if not decimal_value.is_finite():
        return str(decimal_value)
    if decimal_value == 0:
        return "0"
    return str(decimal_value.normalize())


def _signature_counts(signatures: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for signature in signatures:
        counts[signature] = counts.get(signature, 0) + 1
    return counts
