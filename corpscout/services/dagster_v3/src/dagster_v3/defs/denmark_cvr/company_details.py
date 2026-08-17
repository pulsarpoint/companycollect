import hashlib
import json
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from random import randint
from typing import Any, Literal, Self
from urllib.parse import urlencode, urlparse

import dagster as dg
import duckdb
from cloakbrowser import launch
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from pydantic import model_validator

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.assets import DENMARK_CVR_BUCKET
from dagster_v3.defs.denmark_cvr.company_detail_catalog import (
    DENMARK_CVR_COMPANY_DETAIL_CATALOG_PILOT_PARTITION,
    DenmarkCvrCompanyDetailCatalogEntry,
    DenmarkCvrCompanyDetailCatalogReference,
    DenmarkCvrCompanyDetailObjectKind,
    bootstrap_company_detail_catalog,
    catalog_entry_from_body,
    load_optional_company_detail_catalog,
    publish_company_detail_catalog,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_PATH,
    DENMARK_CVR_DUCKDB_SCHEMA,
)
from dagster_v3.defs.denmark_cvr.partitions import DENMARK_CVR_ACTIVE_PARTITIONS
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_BASE_URL,
    SAFE_RESPONSE_HEADERS,
)

DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT = 128
DENMARK_CVR_COMPANY_DETAIL_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:03d}"
        for bucket_index in range(DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT)
    ]
)
DENMARK_CVR_COMPANY_DETAIL_PREFIX = "denmark_cvr/company_details"
DENMARK_CVR_COMPANY_DETAIL_MAPPING_VERSION = 8
DENMARK_CVR_COMPANY_DETAIL_POOL = "denmark_cvr_company_details"
DENMARK_CVR_COMPANY_DETAIL_FAILURE_DB_PATH = Path(
    "data/denmark_cvr_company_detail_failures.sqlite3"
)
DENMARK_CVR_COMPANY_DETAIL_FAILURES_CLICKHOUSE_TABLE = (
    "corpscout.dk_cvr_company_detail_failures"
)
DATACVR_COMPANY_DETAIL_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
DATACVR_COMPANY_DETAIL_SUPPRESSIBLE_STATUSES = frozenset({500, 502, 503, 504})

type DenmarkCvrCompanyDetailFailureDecision = Literal["ignore_company"]

INSERT_COMPANY_DETAIL_FAILURE_SQL = f"""
INSERT INTO {DENMARK_CVR_COMPANY_DETAIL_FAILURES_CLICKHOUSE_TABLE} (
    cvr,
    http_status,
    first_failed_at,
    failed_at,
    failure_count,
    decision,
    source_asset,
    source_partition_key,
    source_url,
    source_run_id,
    failure_object_key
) VALUES
"""

DATACVR_COMPANY_DETAIL_SCRIPT = """
async ({ url }) => {
  const response = await fetch(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json, text/plain, */*",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        "X-Requested-With": "XMLHttpRequest",
      },
      credentials: "include",
    },
  );

  return {
    ok: response.ok,
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    body: await response.text(),
  };
}
"""

# DataCVR detail payload keys are Danish even when locale=en. Values intentionally
# remain untouched; this mapping changes only object keys in the companion _en file.
DENMARK_CVR_COMPANY_DETAIL_KEY_MAP: dict[str, str] = {
    "aktiveEjerforholdDto": "activeOwnershipRelationsDto",
    "aktiveLegaleEjere": "activeLegalOwners",
    "aktiveProduktionsenheder": "activeProductionUnits",
    "aktiveReelleEjere": "activeBeneficialOwners",
    "aktiviteterOmfattetAfHvidvaskloven": "activitiesCoveredByAntiMoneyLaunderingAct",
    "antalAarsvaerk": "fullTimeEquivalentCount",
    "antalAnsatte": "employeeCount",
    "aar": "year",
    "adresse": "address",
    "begunstigetGruppeNavn": "beneficiaryGroupName",
    "begunstigetGruppeRetskrav": "beneficiaryGroupLegalClaim",
    "betydeligIndflydelseViaRolle": "significantInfluenceThroughRole",
    "betydeligIndflydelseViaRolleList": "significantInfluencesThroughRole",
    "bibranche": "secondaryIndustry",
    "bibrancher": "secondaryIndustries",
    "binavne": "secondaryNames",
    "begrundelseForTilbagetraekning": "withdrawalReason",
    "bestyrelseAnsesSomReelleEjere": "boardConsideredBeneficialOwners",
    "boersnoteret": "listedOnStockExchange",
    "branchekode": "industryCode",
    "bygningsnummer": "buildingNumber",
    "cvrNummer": "companyRegistrationNumber",
    "cvrnummer": "companyRegistrationNumber",
    "datoForTilbageTrukket": "withdrawalDate",
    "declaringClass": "declaringClass",
    "delvistIndbetaltKapital": "partiallyPaidCapital",
    "dirigentNavn": "meetingChairName",
    "dokumentId": "documentId",
    "dokumentReferencer": "documentReferences",
    "dokumentType": "documentType",
    "dokumentreferencer": "documentReferences",
    "dokumenttype": "documentType",
    "ejerborgLink": "ownerCitizenLink",
    "ejerforhold": "ownershipRelations",
    "ejerregistreringUnderFemProcent": "ownershipRegistrationBelowFivePercent",
    "ekstraData": "extraData",
    "ekstraDataList": "extraDataList",
    "email": "email",
    "enhedsNummer": "entityNumber",
    "enhedsnummer": "entityNumber",
    "enhedstype": "entityType",
    "fax": "fax",
    "foersteRegnskabsperiodeSlut": "firstAccountingPeriodEnd",
    "foersteRegnskabsperiodeStart": "firstAccountingPeriodStart",
    "foreningsrepraesentanter": "associationRepresentatives",
    "formaal": "purpose",
    "funktionsVaerdi": "functionValue",
    "godkendelsesdato": "approvalDate",
    "groenlandskRegistreringsnummer": "greenlandRegistrationNumber",
    "gyldigFra": "validFrom",
    "gyldigTil": "validUntil",
    "harManuelSignering": "hasManualSigning",
    "harPseudoCvr": "hasPseudoCvr",
    "helligdagsaabent": "openOnPublicHolidays",
    "historiskStamdata": "historicalMasterData",
    "hjemsted": "registeredOffice",
    "hovedbranche": "primaryIndustry",
    "hovednavn": "primaryName",
    "hovedselskab": "parentCompany",
    "hovedType": "mainType",
    "id": "id",
    "indberetningstype": "filingType",
    "indholdstype": "contentType",
    "indtraadtDato": "joinedDate",
    "intervalKodeAntalAarsvaerk": "fullTimeEquivalentCountIntervalCode",
    "intervalKodeAntalAnsatte": "employeeCountIntervalCode",
    "kanHaveLegaleEjere": "canHaveLegalOwners",
    "kanHaveReelleEjere": "canHaveBeneficialOwners",
    "kapitalklasser": "capitalClasses",
    "kommune": "municipality",
    "kommunekode": "municipalityCode",
    "koncessionsdato": "concessionDate",
    "kontaktperson": "contactPerson",
    "kreditoplysningskode": "creditInformationCode",
    "kvartal": "quarter",
    "kvartalsbeskaeftigelse": "quarterlyEmployment",
    "liberaleErhvervRegistreringsstatus": "liberalProfessionRegistrationStatus",
    "maaned": "month",
    "maanedsbeskaeftigelse": "monthlyEmployment",
    "modervirksomhederVedFranchise": "parentCompaniesByFranchise",
    "mneNummer": "mneNumber",
    "name": "name",
    "navn": "name",
    "netvaerk": "network",
    "offentliggoerelseId": "publicationId",
    "offentliggoerelseTidsstempel": "publicationTimestamp",
    "offentliggoerelseTidsstempelFormateret": "formattedPublicationTimestamp",
    "omfattetAfHvidvaskloven": "coveredByAntiMoneyLaunderingAct",
    "omgjort": "reversed",
    "omlaegningsperiodeSlut": "conversionPeriodEnd",
    "omlaegningsperiodeStart": "conversionPeriodStart",
    "ophoersdato": "cessationDate",
    "ophoerteEjerforholdDto": "ceasedOwnershipRelationsDto",
    "ophoerteFad": "ceasedFad",
    "ophoerteLegaleEjere": "ceasedLegalOwners",
    "ophoerteProduktionsenheder": "ceasedProductionUnits",
    "ophoerteReelleEjere": "ceasedBeneficialOwners",
    "oplysningerOmRevisionsvirksomhed": "auditFirmInformation",
    "ordinal": "ordinal",
    "overskrift": "heading",
    "periode": "period",
    "periodeFormateret": "formattedPeriod",
    "personkreds": "companyPeople",
    "personkredser": "personGroups",
    "personRoller": "personRoles",
    "personType": "personType",
    "pNummer": "productionUnitNumber",
    "pnummer": "productionUnitNumber",
    "postadresse": "postalAddress",
    "postnummerOgBy": "postalCodeAndCity",
    "produktionsenheder": "productionUnits",
    "regnummer": "registrationNumber",
    "registreretIHvidvaskregistret": "registeredInAntiMoneyLaunderingRegister",
    "registreretKapital": "registeredCapital",
    "registreretMyndighed": "registrationAuthority",
    "registreringsnummer": "registrationNumber",
    "registreringstype": "registrationType",
    "registreringsTekst": "registrationText",
    "regnskaber": "financialStatements",
    "regnskabsaarSlut": "financialYearEnd",
    "regnskabsaarStart": "financialYearStart",
    "regnskabsperiodeFra": "accountingPeriodFrom",
    "regnskabsperiodeSlut": "accountingPeriodEnd",
    "regnskabsperiodeStart": "accountingPeriodStart",
    "regnskabsperiodeTil": "accountingPeriodTo",
    "regnskabsType": "financialStatementType",
    "reklamebeskyttet": "advertisingProtected",
    "revisionsvirksomhed": "auditFirm",
    "rolle": "role",
    "rolleTekstnogle": "roleTextKey",
    "sagsnummer": "caseNumber",
    "sammenhaengendeRegnskaber": "consecutiveFinancialStatements",
    "samledeStemmeandel": "totalVotingShare",
    "senesteNavn": "latestName",
    "senesteVedtaegtsdato": "latestArticlesOfAssociationDate",
    "senesteVedtaegtsdatoFoer1900": "latestArticlesOfAssociationDateBefore1900",
    "skjulEjerforhold": "hideOwnershipRelations",
    "skjulOevrigeDokumenter": "hideOtherDocuments",
    "socialoekonomiskVirksomhed": "socialEnterprise",
    "sorteringsVaredi": "sortingValue",
    "sorteringsVaerdi": "sortingValue",
    "stadfaestelsesdato": "confirmationDate",
    "stadfaestetAf": "confirmedBy",
    "stamdata": "masterData",
    "startdato": "startDate",
    "statsligVirksomhed": "stateOwnedCompany",
    "status": "status",
    "stiftetFor1900Tekstnogle": "foundedBefore1900TextKey",
    "tegnetKapital": "subscribedCapital",
    "tegningsberettiget": "authorizedSignatories",
    "tegningsregel": "signingRule",
    "tekstMedLink": "textWithLink",
    "tekstnogle": "textKey",
    "tekstUdenLink": "textWithoutLink",
    "telefon": "phone",
    "telefonSekundaert": "secondaryPhone",
    "tilbagetrukket": "withdrawn",
    "tilknytning": "affiliation",
    "tilknyttedeRevisorer": "affiliatedAuditors",
    "titel": "title",
    "titelTekstnogler": "titleTextKeys",
    "titlePrefix": "titlePrefix",
    "udvidedeOplysninger": "extendedInformation",
    "udenlandskAdresse": "foreignAddress",
    "udenlandskAdresseLand": "foreignAddressCountry",
    "udenlandskAdresseLandekode": "foreignAddressCountryCode",
    "udenlandskPostadresse": "foreignPostalAddress",
    "udenlandskPostadresseLand": "foreignPostalAddressCountry",
    "udenlandskPostadresseLandekode": "foreignPostalAddressCountryCode",
    "vaerdi": "value",
    "vaerdiTekstnogle": "valueTextKey",
    "virkningsdato": "effectiveDate",
    "virksomhedHarIkkeKunnetIdentificereReelleEjereLedelseErIndsat": (
        "companyCouldNotIdentifyBeneficialOwnersAndManagementWasRegistered"
    ),
    "virksomhedHarIkkeReelleEjereOgLedelseErIndsat": (
        "companyHasNoBeneficialOwnersAndManagementWasRegistered"
    ),
    "virksomhedRegistreringer": "companyRegistrations",
    "virksomhedsFormTilladerReelleEjerOplysninger": (
        "legalFormAllowsBeneficialOwnerInformation"
    ),
    "virksomhedsform": "legalForm",
    "virksomhedsformKode": "legalFormCode",
    "virksomhedsMeddelelser": "companyNotices",
    "virksomhedsnavn": "companyName",
    "virksomhedstype": "companyType",
    "visNavnPostfix": "showNamePostfix",
    "webadresse": "webAddress",
}


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailDownload:
    cvr: str
    source_url: str
    raw_body: str
    payload: dict[str, Any]
    status: int
    response_headers: dict[str, str]

    @property
    def downloaded_size_bytes(self) -> int:
        return len(self.raw_body.encode("utf-8"))


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailHttpFailure:
    cvr: str
    source_url: str
    status: int
    attempt_count: int
    response_headers: dict[str, str]


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailFailureRecord:
    cvr: str
    http_status: int
    first_failed_at: datetime
    failed_at: datetime
    failure_count: int
    request_attempt_count: int
    decision: DenmarkCvrCompanyDetailFailureDecision
    source_asset: str
    source_partition_key: str
    source_url: str
    source_run_id: str
    failure_object_key: str
    response_headers: dict[str, str]


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailSummary:
    partition_key: str
    selected_company_count: int
    complete_company_count: int
    skipped_company_count: int
    already_skipped_company_count: int
    skipped_request_attempt_count: int
    already_complete_company_count: int
    translated_existing_company_count: int
    downloaded_company_count: int
    written_object_count: int
    downloaded_size_bytes: int

    @property
    def resolved_company_count(self) -> int:
        return (
            self.complete_company_count
            + self.skipped_company_count
            + self.already_skipped_company_count
        )


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCatalogWriteResult:
    detail_summary: DenmarkCvrCompanyDetailSummary
    catalog_reference: DenmarkCvrCompanyDetailCatalogReference
    catalog_bootstrapped: bool
    catalog_reused: bool
    bootstrap_object_read_count: int
    catalog_object_count: int


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailPartitionSnapshot:
    bucket: str
    partition_key: str
    catalog_reference: DenmarkCvrCompanyDetailCatalogReference | None


@dataclass(frozen=True)
class _DenmarkCvrCompanyDetailWriteResult:
    summary: DenmarkCvrCompanyDetailSummary
    written_object_keys: tuple[str, ...]


class DenmarkCvrCompanyDetailRequestError(RuntimeError):
    pass


class DenmarkCvrCompanyDetailKeyError(ValueError):
    pass


class DenmarkCvrCompanyDetailResource(dg.ConfigurableResource):
    detail_base_url: str = DATACVR_BASE_URL
    locale: str = "en"
    min_delay_ms: int = 100
    max_delay_ms: int = 800
    max_attempts: int = 3
    retry_base_delay_seconds: float = 30.0
    retry_max_delay_seconds: float = 120.0
    failure_database_path: str = str(DENMARK_CVR_COMPANY_DETAIL_FAILURE_DB_PATH)

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        _validate_https_base_url(self.detail_base_url)
        if self.locale != "en":
            raise ValueError("DataCVR company details must use locale='en'")
        if self.min_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("request delays must not be negative")
        if self.min_delay_ms > self.max_delay_ms:
            raise ValueError("min_delay_ms must not exceed max_delay_ms")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero")
        if self.retry_base_delay_seconds < 0 or self.retry_max_delay_seconds < 0:
            raise ValueError("retry delays must not be negative")
        if self.retry_base_delay_seconds > self.retry_max_delay_seconds:
            raise ValueError(
                "retry_base_delay_seconds must not exceed retry_max_delay_seconds"
            )
        if self.failure_database_path.strip() == "":
            raise ValueError("failure_database_path must not be blank")
        return self

    def iter_company_details(
        self,
        cvrs: Sequence[str],
        *,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[DenmarkCvrCompanyDetailDownload | DenmarkCvrCompanyDetailHttpFailure]:
        selected_cvrs = tuple(cvrs)
        if not selected_cvrs:
            return
        for cvr in selected_cvrs:
            _validate_cvr(cvr)

        try:
            browser = launcher()
        except Exception:
            raise DenmarkCvrCompanyDetailRequestError(
                "DataCVR company-detail browser failed to start; verify Chromium "
                "runtime dependencies"
            ) from None

        try:
            page = browser.new_page()
            page.goto(
                company_detail_page_url(self.detail_base_url, selected_cvrs[0]),
                wait_until="networkidle",
            )
            for company_index, cvr in enumerate(selected_cvrs):
                if company_index > 0:
                    sleep(self._request_delay_seconds())
                source_url = company_detail_api_url(self.detail_base_url, cvr)
                result = self._request_with_retry(
                    page,
                    cvr=cvr,
                    source_url=source_url,
                    sleep=sleep,
                )
                if _is_suppressible_company_detail_result(result):
                    yield _company_detail_http_failure(
                        cvr=cvr,
                        source_url=source_url,
                        result=result,
                        attempt_count=self.max_attempts,
                    )
                    continue
                yield _validated_company_detail_download(
                    cvr=cvr,
                    source_url=source_url,
                    result=result,
                )
        finally:
            browser.close()

    def _request_delay_seconds(self) -> float:
        return randint(self.min_delay_ms, self.max_delay_ms) / 1_000

    def _request_with_retry(
        self,
        page: Any,
        *,
        cvr: str,
        source_url: str,
        sleep: Callable[[float], None],
    ) -> Any:
        result: Any = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = page.evaluate(
                    DATACVR_COMPANY_DETAIL_SCRIPT,
                    {"url": source_url},
                )
            except Exception:
                raise DenmarkCvrCompanyDetailRequestError(
                    f"DataCVR company-detail request failed for CVR {cvr}"
                ) from None
            if not _is_retryable_company_detail_result(result):
                return result
            if attempt < self.max_attempts:
                sleep(
                    _company_detail_retry_delay_seconds(
                        result,
                        attempt=attempt,
                        base_delay_seconds=self.retry_base_delay_seconds,
                        max_delay_seconds=self.retry_max_delay_seconds,
                    )
                )
        return result


def company_detail_api_url(base_url: str, cvr: str) -> str:
    normalized_base_url = _validate_https_base_url(base_url)
    _validate_cvr(cvr)
    query = urlencode({"cvrnummer": cvr, "locale": "en"})
    return f"{normalized_base_url}/gateway/virksomhed/hentVirksomhed?{query}"


def company_detail_page_url(base_url: str, cvr: str) -> str:
    normalized_base_url = _validate_https_base_url(base_url)
    _validate_cvr(cvr)
    return f"{normalized_base_url}/enhed/virksomhed/{cvr}?locale=en"


def company_detail_bucket_key(cvr: str) -> str:
    _validate_cvr(cvr)
    digest = hashlib.md5(cvr.encode("ascii"), usedforsecurity=False).digest()
    duckdb_lower_md5 = int.from_bytes(digest[-8:], byteorder="little")
    bucket_index = duckdb_lower_md5 % DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT
    return f"bucket_{bucket_index:03d}"


def company_detail_partition_cvrs(
    denmark_cvr_duckdb: DuckDBResource,
    partition_key: str,
) -> tuple[str, ...]:
    bucket_index = _company_detail_bucket_index(partition_key)
    try:
        with denmark_cvr_duckdb.get_connection() as connection:
            invalid_count = connection.execute(
                f"""
                SELECT count(*)
                FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
                WHERE cvr IS NULL OR NOT regexp_full_match(cvr, '[0-9]{{8}}')
                """
            ).fetchone()[0]
            if invalid_count:
                raise ValueError(
                    "Denmark CVR companies table contains "
                    f"{invalid_count} invalid CVR numbers"
                )
            rows = connection.execute(
                f"""
                SELECT cvr
                FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
                WHERE md5_number_lower(cvr) % ? = ?
                ORDER BY cvr
                """,
                [DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT, bucket_index],
            ).fetchall()
    except duckdb.CatalogException:
        raise RuntimeError(
            "Denmark CVR company details require the "
            "denmark_cvr_companies_duckdb asset; materialize it first"
        ) from None
    return tuple(str(row[0]) for row in rows)


def company_detail_update_cvrs(
    denmark_cvr_duckdb: DuckDBResource,
    update_date: str,
) -> tuple[str, ...]:
    _validate_update_date(update_date)
    try:
        with denmark_cvr_duckdb.get_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT cvr
                FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
                WHERE source_capture_type = 'active'
                  AND source_partition_key = ?
                ORDER BY cvr
                """,
                [update_date],
            ).fetchall()
    except duckdb.CatalogException:
        raise RuntimeError(
            "Denmark CVR company-detail updates require the "
            "denmark_cvr_companies_duckdb asset; materialize it first"
        ) from None
    cvrs = tuple(str(row[0]) for row in rows)
    for cvr in cvrs:
        _validate_cvr(cvr)
    return cvrs


def company_detail_object_key(
    partition_key: str,
    cvr: str,
    *,
    english_keys: bool,
) -> str:
    _company_detail_bucket_index(partition_key)
    _validate_cvr(cvr)
    if company_detail_bucket_key(cvr) != partition_key:
        raise ValueError(f"CVR {cvr} does not belong to partition {partition_key}")
    filename = "company_en.json" if english_keys else "company.json"
    return f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/cvr={cvr}/{filename}"


def company_detail_failure_object_key(
    partition_key: str,
    cvr: str,
) -> str:
    _company_detail_bucket_index(partition_key)
    _validate_cvr(cvr)
    if company_detail_bucket_key(cvr) != partition_key:
        raise ValueError(f"CVR {cvr} does not belong to partition {partition_key}")
    return (
        f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/"
        f"cvr={cvr}/company_error.json"
    )


def company_detail_update_object_key(
    update_date: str,
    cvr: str,
    *,
    english_keys: bool,
) -> str:
    _validate_update_date(update_date)
    _validate_cvr(cvr)
    filename = "company_en.json" if english_keys else "company.json"
    return (
        f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/updates/date={update_date}/"
        f"{company_detail_bucket_key(cvr)}/cvr={cvr}/{filename}"
    )


def company_detail_update_failure_object_key(
    update_date: str,
    cvr: str,
) -> str:
    _validate_update_date(update_date)
    _validate_cvr(cvr)
    return (
        f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/updates/date={update_date}/"
        f"{company_detail_bucket_key(cvr)}/cvr={cvr}/company_error.json"
    )


def translate_company_detail_keys(payload: Mapping[str, Any]) -> dict[str, Any]:
    unmapped_paths = company_detail_unmapped_key_paths(payload)
    if unmapped_paths:
        raise DenmarkCvrCompanyDetailKeyError(
            "Unmapped DataCVR company-detail keys at " + ", ".join(unmapped_paths)
        )
    translated = _translate_value(payload, path=())
    if not isinstance(translated, dict):
        raise TypeError("Translated DataCVR company detail must be an object")
    return translated


def company_detail_unmapped_key_paths(
    payload: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return every source-key path that has no English mapping."""
    return tuple(_unmapped_key_paths(payload, path=()))


def record_company_detail_http_failure(
    database_path: Path,
    *,
    failure: DenmarkCvrCompanyDetailHttpFailure,
    failed_at: datetime,
    source_asset: str,
    source_partition_key: str,
    source_run_id: str,
    failure_object_key: str,
) -> DenmarkCvrCompanyDetailFailureRecord:
    if failed_at.utcoffset() is None:
        raise ValueError("Company-detail failure timestamp must include a timezone")
    if failure.attempt_count <= 0:
        raise ValueError("Company-detail failure attempt count must be positive")
    if source_asset.strip() == "":
        raise ValueError("Company-detail failure source asset must not be blank")
    if source_partition_key.strip() == "":
        raise ValueError("Company-detail failure partition key must not be blank")
    if source_run_id.strip() == "":
        raise ValueError("Company-detail failure run ID must not be blank")
    if failure.status not in DATACVR_COMPANY_DETAIL_SUPPRESSIBLE_STATUSES:
        raise ValueError(
            f"HTTP {failure.status} is not a suppressible company-detail failure"
        )

    normalized_failed_at = failed_at.astimezone(UTC)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path, timeout=30) as connection:
        _ensure_company_detail_failure_table(connection)
        connection.execute("BEGIN IMMEDIATE")
        previous = connection.execute(
            """
            SELECT min(failed_at), count(*)
            FROM company_detail_http_failures
            WHERE cvr = ? AND http_status = ?
            """,
            [failure.cvr, failure.status],
        ).fetchone()
        previous_first_failed_at = previous[0]
        previous_failure_count = int(previous[1])
        first_failed_at = (
            datetime.fromisoformat(str(previous_first_failed_at)).astimezone(UTC)
            if previous_first_failed_at is not None
            else normalized_failed_at
        )
        failure_count = previous_failure_count + 1
        decision: DenmarkCvrCompanyDetailFailureDecision = "ignore_company"
        connection.execute(
            """
            INSERT INTO company_detail_http_failures (
                cvr,
                http_status,
                failed_at,
                request_attempt_count,
                decision,
                source_asset,
                source_partition_key,
                source_url,
                source_run_id,
                failure_object_key,
                response_headers
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                failure.cvr,
                failure.status,
                normalized_failed_at.isoformat(timespec="microseconds"),
                failure.attempt_count,
                decision,
                source_asset,
                source_partition_key,
                failure.source_url,
                source_run_id,
                failure_object_key,
                json.dumps(
                    failure.response_headers,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ],
        )

    return DenmarkCvrCompanyDetailFailureRecord(
        cvr=failure.cvr,
        http_status=failure.status,
        first_failed_at=first_failed_at,
        failed_at=normalized_failed_at,
        failure_count=failure_count,
        request_attempt_count=failure.attempt_count,
        decision=decision,
        source_asset=source_asset,
        source_partition_key=source_partition_key,
        source_url=failure.source_url,
        source_run_id=source_run_id,
        failure_object_key=failure_object_key,
        response_headers=failure.response_headers,
    )


def clear_company_detail_failure_history(
    database_path: Path,
    cvr: str,
) -> None:
    _validate_cvr(cvr)
    if not database_path.is_file():
        return
    with sqlite3.connect(database_path, timeout=30) as connection:
        _ensure_company_detail_failure_table(connection)
        connection.execute(
            "DELETE FROM company_detail_http_failures WHERE cvr = ?",
            [cvr],
        )


def insert_company_detail_failure_record(
    client: Any,
    record: DenmarkCvrCompanyDetailFailureRecord,
) -> None:
    client.execute(
        INSERT_COMPANY_DETAIL_FAILURE_SQL,
        [
            (
                record.cvr,
                record.http_status,
                record.first_failed_at,
                record.failed_at,
                record.failure_count,
                record.decision,
                record.source_asset,
                record.source_partition_key,
                record.source_url,
                record.source_run_id,
                record.failure_object_key,
            )
        ],
    )


def publish_company_detail_failure_record(
    clickhouse: ClickhouseResource,
    record: DenmarkCvrCompanyDetailFailureRecord,
) -> None:
    with clickhouse.get_connection() as client:
        insert_company_detail_failure_record(client, record)


def write_company_detail_partition(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    partition_key: str,
    cvrs: Sequence[str],
    failure_database_path: Path,
    observed_at: datetime,
    source_run_id: str,
    record_failure: Callable[[DenmarkCvrCompanyDetailFailureRecord], object],
    log_info: Callable[..., object] | None = None,
    log_warning: Callable[..., object] | None = None,
) -> DenmarkCvrCompanyDetailSummary:
    _company_detail_bucket_index(partition_key)
    selected_cvrs = tuple(cvrs)
    for cvr in selected_cvrs:
        if company_detail_bucket_key(cvr) != partition_key:
            raise ValueError(f"CVR {cvr} does not belong to partition {partition_key}")

    partition_prefix = f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/"
    object_keys = {
        cvr: (
            company_detail_object_key(partition_key, cvr, english_keys=False),
            company_detail_object_key(partition_key, cvr, english_keys=True),
            company_detail_failure_object_key(partition_key, cvr),
        )
        for cvr in selected_cvrs
    }
    return _write_company_details(
        object_store=object_store,
        details=details,
        source_asset="denmark_cvr_company_details_s3",
        result_partition_key=partition_key,
        object_prefix=partition_prefix,
        object_keys=object_keys,
        failure_database_path=failure_database_path,
        observed_at=observed_at,
        source_run_id=source_run_id,
        record_failure=record_failure,
        log_info=log_info,
        log_warning=log_warning,
    )


def write_company_detail_catalog_partition(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    partition_key: str,
    cvrs: Sequence[str],
    failure_database_path: Path,
    observed_at: datetime,
    source_run_id: str,
    record_failure: Callable[[DenmarkCvrCompanyDetailFailureRecord], object],
    log_info: Callable[..., object] | None = None,
    log_warning: Callable[..., object] | None = None,
) -> DenmarkCvrCompanyDetailCatalogWriteResult:
    """Resolve one partition through an exact-key object catalog contract."""
    _company_detail_bucket_index(partition_key)
    selected_cvrs = tuple(cvrs)
    for cvr in selected_cvrs:
        if company_detail_bucket_key(cvr) != partition_key:
            raise ValueError(f"CVR {cvr} does not belong to partition {partition_key}")

    object_keys = {
        cvr: (
            company_detail_object_key(partition_key, cvr, english_keys=False),
            company_detail_object_key(partition_key, cvr, english_keys=True),
            company_detail_failure_object_key(partition_key, cvr),
        )
        for cvr in selected_cvrs
    }
    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    existing_catalog = load_optional_company_detail_catalog(
        object_store=object_store,
        partition_key=partition_key,
    )
    if existing_catalog is None:
        bootstrap = bootstrap_company_detail_catalog(
            object_store=object_store,
            partition_key=partition_key,
            object_keys=object_keys,
        )
        initial_entries = bootstrap.entries
        bootstrap_object_read_count = bootstrap.object_read_count
    else:
        initial_entries = existing_catalog.entries
        bootstrap_object_read_count = 0

    existing_keys = {entry.object_key for entry in initial_entries}
    write_result = _write_company_details_from_existing_keys(
        object_store=object_store,
        details=details,
        source_asset="denmark_cvr_company_details_s3",
        result_partition_key=partition_key,
        object_keys=object_keys,
        existing_keys=existing_keys,
        failure_database_path=failure_database_path,
        observed_at=observed_at,
        source_run_id=source_run_id,
        record_failure=record_failure,
        log_info=log_info,
        log_warning=log_warning,
    )
    if existing_catalog is not None and not write_result.written_object_keys:
        catalog = existing_catalog
        catalog_reused = True
    else:
        catalog = publish_company_detail_catalog(
            object_store=object_store,
            partition_key=partition_key,
            entries=_updated_company_detail_catalog_entries(
                object_store=object_store,
                partition_key=partition_key,
                object_keys=object_keys,
                initial_entries=initial_entries,
                written_object_keys=write_result.written_object_keys,
            ),
            source_run_id=source_run_id,
            created_at=observed_at,
        )
        catalog_reused = False
    return DenmarkCvrCompanyDetailCatalogWriteResult(
        detail_summary=write_result.summary,
        catalog_reference=catalog.reference,
        catalog_bootstrapped=existing_catalog is None,
        catalog_reused=catalog_reused,
        bootstrap_object_read_count=bootstrap_object_read_count,
        catalog_object_count=len(catalog.entries),
    )


def write_company_detail_updates(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    update_date: str,
    cvrs: Sequence[str],
    failure_database_path: Path,
    observed_at: datetime,
    source_run_id: str,
    record_failure: Callable[[DenmarkCvrCompanyDetailFailureRecord], object],
    log_info: Callable[..., object] | None = None,
    log_warning: Callable[..., object] | None = None,
) -> DenmarkCvrCompanyDetailSummary:
    _validate_update_date(update_date)
    selected_cvrs = tuple(cvrs)
    for cvr in selected_cvrs:
        _validate_cvr(cvr)
    object_prefix = f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/updates/date={update_date}/"
    object_keys = {
        cvr: (
            company_detail_update_object_key(
                update_date,
                cvr,
                english_keys=False,
            ),
            company_detail_update_object_key(
                update_date,
                cvr,
                english_keys=True,
            ),
            company_detail_update_failure_object_key(update_date, cvr),
        )
        for cvr in selected_cvrs
    }
    return _write_company_details(
        object_store=object_store,
        details=details,
        source_asset="denmark_cvr_company_detail_updates_s3",
        result_partition_key=update_date,
        object_prefix=object_prefix,
        object_keys=object_keys,
        failure_database_path=failure_database_path,
        observed_at=observed_at,
        source_run_id=source_run_id,
        record_failure=record_failure,
        log_info=log_info,
        log_warning=log_warning,
    )


def _write_company_details(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    source_asset: str,
    result_partition_key: str,
    object_prefix: str,
    object_keys: Mapping[str, tuple[str, str, str]],
    failure_database_path: Path,
    observed_at: datetime,
    source_run_id: str,
    record_failure: Callable[[DenmarkCvrCompanyDetailFailureRecord], object],
    log_info: Callable[..., object] | None,
    log_warning: Callable[..., object] | None,
) -> DenmarkCvrCompanyDetailSummary:
    if observed_at.utcoffset() is None:
        raise ValueError("Company-detail observation timestamp must include a timezone")
    if source_run_id.strip() == "":
        raise ValueError("Company-detail source run ID must not be blank")

    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    existing_keys = set(
        object_store.list_keys(object_prefix, bucket=DENMARK_CVR_BUCKET)
    )
    return _write_company_details_from_existing_keys(
        object_store=object_store,
        details=details,
        source_asset=source_asset,
        result_partition_key=result_partition_key,
        object_keys=object_keys,
        existing_keys=existing_keys,
        failure_database_path=failure_database_path,
        observed_at=observed_at,
        source_run_id=source_run_id,
        record_failure=record_failure,
        log_info=log_info,
        log_warning=log_warning,
    ).summary


def _write_company_details_from_existing_keys(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    source_asset: str,
    result_partition_key: str,
    object_keys: Mapping[str, tuple[str, str, str]],
    existing_keys: set[str],
    failure_database_path: Path,
    observed_at: datetime,
    source_run_id: str,
    record_failure: Callable[[DenmarkCvrCompanyDetailFailureRecord], object],
    log_info: Callable[..., object] | None,
    log_warning: Callable[..., object] | None,
) -> _DenmarkCvrCompanyDetailWriteResult:
    if observed_at.utcoffset() is None:
        raise ValueError("Company-detail observation timestamp must include a timezone")
    if source_run_id.strip() == "":
        raise ValueError("Company-detail source run ID must not be blank")

    failure_cvrs = _company_detail_failure_cvrs(failure_database_path)

    already_complete_count = 0
    already_skipped_count = 0
    translated_existing_count = 0
    written_object_count = 0
    written_object_keys: list[str] = []
    cvrs_to_download: list[str] = []
    for cvr, (original_key, english_key, failure_key) in object_keys.items():
        if original_key in existing_keys and english_key in existing_keys:
            already_complete_count += 1
            if cvr in failure_cvrs:
                clear_company_detail_failure_history(failure_database_path, cvr)
                failure_cvrs.remove(cvr)
            continue
        if (
            failure_key in existing_keys
            and original_key not in existing_keys
            and english_key not in existing_keys
        ):
            already_skipped_count += 1
            continue
        if original_key in existing_keys:
            raw_body = object_store.read_bytes(
                original_key,
                bucket=DENMARK_CVR_BUCKET,
            ).decode("utf-8")
            payload = _parse_company_detail_payload(raw_body, cvr=cvr)
            object_store.write_bytes(
                english_key,
                _translated_json_bytes(payload),
                bucket=DENMARK_CVR_BUCKET,
            )
            existing_keys.add(english_key)
            translated_existing_count += 1
            written_object_count += 1
            written_object_keys.append(english_key)
            if cvr in failure_cvrs:
                clear_company_detail_failure_history(failure_database_path, cvr)
                failure_cvrs.remove(cvr)
            continue
        cvrs_to_download.append(cvr)

    downloaded_count = 0
    skipped_count = 0
    skipped_request_attempt_count = 0
    downloaded_size_bytes = 0
    for result in details.iter_company_details(tuple(cvrs_to_download)):
        original_key, english_key, failure_key = object_keys[result.cvr]
        if isinstance(result, DenmarkCvrCompanyDetailHttpFailure):
            failure_record = record_company_detail_http_failure(
                failure_database_path,
                failure=result,
                failed_at=observed_at,
                source_asset=source_asset,
                source_partition_key=result_partition_key,
                source_run_id=source_run_id,
                failure_object_key=failure_key,
            )
            record_failure(failure_record)
            marker_body = _company_detail_failure_marker_bytes(failure_record)
            object_store.write_bytes(
                failure_key,
                marker_body,
                bucket=DENMARK_CVR_BUCKET,
            )
            skipped_count += 1
            skipped_request_attempt_count += result.attempt_count
            written_object_count += 1
            existing_keys.add(failure_key)
            written_object_keys.append(failure_key)
            if log_warning is not None:
                log_warning(
                    "Skipping DataCVR company detail after exhausted retries: "
                    "partition=%s cvr=%s http_status=%s request_attempts=%s "
                    "failure_count=%s "
                    "first_failed_at=%s failed_at=%s marker=%s",
                    result_partition_key,
                    result.cvr,
                    result.status,
                    result.attempt_count,
                    failure_record.failure_count,
                    failure_record.first_failed_at.isoformat(),
                    failure_record.failed_at.isoformat(),
                    failure_key,
                )
            continue

        download = result
        object_store.write_bytes(
            original_key,
            download.raw_body.encode("utf-8"),
            bucket=DENMARK_CVR_BUCKET,
        )
        object_store.write_bytes(
            english_key,
            _translated_json_bytes(download.payload),
            bucket=DENMARK_CVR_BUCKET,
        )
        downloaded_count += 1
        downloaded_size_bytes += download.downloaded_size_bytes
        written_object_count += 2
        existing_keys.update((original_key, english_key))
        written_object_keys.extend((original_key, english_key))
        if download.cvr in failure_cvrs:
            clear_company_detail_failure_history(
                failure_database_path,
                download.cvr,
            )
            failure_cvrs.remove(download.cvr)
        if log_info is not None and (
            downloaded_count == 1
            or downloaded_count % 100 == 0
            or downloaded_count == len(cvrs_to_download)
        ):
            log_info(
                "DataCVR company-detail progress: partition=%s downloaded=%s/%s "
                "downloaded_bytes=%s",
                result_partition_key,
                downloaded_count,
                len(cvrs_to_download),
                downloaded_size_bytes,
            )

    complete_company_count = (
        already_complete_count + translated_existing_count + downloaded_count
    )
    resolved_company_count = (
        complete_company_count + already_skipped_count + skipped_count
    )
    if resolved_company_count != len(object_keys):
        raise DenmarkCvrCompanyDetailRequestError(
            "DataCVR company-detail resource did not resolve every selected company: "
            f"selected={len(object_keys)} resolved={resolved_company_count}"
        )
    return _DenmarkCvrCompanyDetailWriteResult(
        summary=DenmarkCvrCompanyDetailSummary(
            partition_key=result_partition_key,
            selected_company_count=len(object_keys),
            complete_company_count=complete_company_count,
            skipped_company_count=skipped_count,
            already_skipped_company_count=already_skipped_count,
            skipped_request_attempt_count=skipped_request_attempt_count,
            already_complete_company_count=already_complete_count,
            translated_existing_company_count=translated_existing_count,
            downloaded_company_count=downloaded_count,
            written_object_count=written_object_count,
            downloaded_size_bytes=downloaded_size_bytes,
        ),
        written_object_keys=tuple(written_object_keys),
    )


def _updated_company_detail_catalog_entries(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    object_keys: Mapping[str, tuple[str, str, str]],
    initial_entries: tuple[DenmarkCvrCompanyDetailCatalogEntry, ...],
    written_object_keys: tuple[str, ...],
) -> tuple[DenmarkCvrCompanyDetailCatalogEntry, ...]:
    object_kinds: tuple[DenmarkCvrCompanyDetailObjectKind, ...] = (
        "original",
        "english",
        "failure",
    )
    identity_by_key = {
        object_key: (cvr, object_kind)
        for cvr, keys in object_keys.items()
        for object_key, object_kind in zip(
            keys,
            object_kinds,
            strict=True,
        )
    }
    entries_by_key = {entry.object_key: entry for entry in initial_entries}
    for object_key in written_object_keys:
        if object_key not in identity_by_key:
            raise ValueError(
                "Denmark CVR company-detail writer produced an unexpected object: "
                f"partition={partition_key} key={object_key}"
            )
        cvr, object_kind = identity_by_key[object_key]
        entries_by_key[object_key] = catalog_entry_from_body(
            cvr=cvr,
            object_kind=object_kind,
            object_key=object_key,
            body=object_store.read_bytes(
                object_key,
                bucket=DENMARK_CVR_BUCKET,
            ),
        )

    selected_cvrs = set(object_keys)
    active_keys = {
        entry.object_key for entry in initial_entries if entry.cvr not in selected_cvrs
    }
    available_keys = set(entries_by_key)
    for cvr, (original_key, english_key, failure_key) in object_keys.items():
        if original_key in available_keys and english_key in available_keys:
            active_keys.update((original_key, english_key))
            continue
        if failure_key in available_keys:
            active_keys.add(failure_key)
            continue
        raise DenmarkCvrCompanyDetailRequestError(
            "Denmark CVR company-detail catalog cannot represent resolved company: "
            f"partition={partition_key} cvr={cvr}"
        )
    return tuple(entries_by_key[key] for key in sorted(active_keys))


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name="denmark_cvr_company_details",
    kinds={"python", "browser", "clickhouse", "duckdb", "json", "s3", "sqlite"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "virksomhed",
        "layer": "raw_detail",
    },
    partitions_def=DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_COMPANY_DETAIL_POOL,
    description=(
        "Reads one stable hash bucket of CVR numbers from the Denmark company "
        "DuckDB table, downloads each HTTPS DataCVR company-detail response, and "
        "checkpoints an original Danish-key JSON object plus an _en object whose "
        "keys are translated to English without changing values. Company-specific "
        "server errors are skipped and checkpointed after three failed attempts. "
        "The bucket_000 pilot uses an exact-key v2 Parquet catalog instead of "
        "enumerating the object prefix."
    ),
)
def denmark_cvr_company_details_s3(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    denmark_cvr_company_details: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult[DenmarkCvrCompanyDetailPartitionSnapshot]:
    partition_key = context.partition_key
    observed_at = datetime.now(UTC)
    cvrs = company_detail_partition_cvrs(denmark_cvr_duckdb, partition_key)
    context.log.info(
        "DataCVR company-detail partition selected: partition=%s companies=%s",
        partition_key,
        len(cvrs),
    )
    catalog_result: DenmarkCvrCompanyDetailCatalogWriteResult | None = None
    if partition_key == DENMARK_CVR_COMPANY_DETAIL_CATALOG_PILOT_PARTITION:
        catalog_result = write_company_detail_catalog_partition(
            object_store=object_store,
            details=denmark_cvr_company_details,
            partition_key=partition_key,
            cvrs=cvrs,
            failure_database_path=Path(
                denmark_cvr_company_details.failure_database_path
            ),
            observed_at=observed_at,
            source_run_id=context.run_id,
            record_failure=lambda record: publish_company_detail_failure_record(
                clickhouse,
                record,
            ),
            log_info=context.log.info,
            log_warning=context.log.warning,
        )
        summary = catalog_result.detail_summary
    else:
        summary = write_company_detail_partition(
            object_store=object_store,
            details=denmark_cvr_company_details,
            partition_key=partition_key,
            cvrs=cvrs,
            failure_database_path=Path(
                denmark_cvr_company_details.failure_database_path
            ),
            observed_at=observed_at,
            source_run_id=context.run_id,
            record_failure=lambda record: publish_company_detail_failure_record(
                clickhouse,
                record,
            ),
            log_info=context.log.info,
            log_warning=context.log.warning,
        )
    metadata = {
        "partition_key": summary.partition_key,
        "hash_bucket_count": DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT,
        "selected_company_count": summary.selected_company_count,
        "complete_company_count": summary.complete_company_count,
        "resolved_company_count": summary.resolved_company_count,
        "skipped_company_count": summary.skipped_company_count,
        "already_skipped_company_count": summary.already_skipped_company_count,
        "skipped_request_attempt_count": summary.skipped_request_attempt_count,
        "already_complete_company_count": summary.already_complete_company_count,
        "translated_existing_company_count": (
            summary.translated_existing_company_count
        ),
        "downloaded_company_count": summary.downloaded_company_count,
        "written_object_count": summary.written_object_count,
        "downloaded_size_bytes": summary.downloaded_size_bytes,
        "failure_database_path": str(denmark_cvr_company_details.failure_database_path),
        "max_request_attempts": denmark_cvr_company_details.max_attempts,
        "retry_base_delay_seconds": (
            denmark_cvr_company_details.retry_base_delay_seconds
        ),
        "retry_max_delay_seconds": (
            denmark_cvr_company_details.retry_max_delay_seconds
        ),
        "failure_clickhouse_table": (
            DENMARK_CVR_COMPANY_DETAIL_FAILURES_CLICKHOUSE_TABLE
        ),
        "key_mapping_version": DENMARK_CVR_COMPANY_DETAIL_MAPPING_VERSION,
        "s3_bucket": DENMARK_CVR_BUCKET,
        "s3_prefix": f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/",
        "source_url": company_detail_api_url(
            denmark_cvr_company_details.detail_base_url,
            "00000000",
        ).replace("00000000", "{cvr}"),
        "object_catalog_mode": "v2" if catalog_result is not None else "legacy",
    }
    if catalog_result is not None:
        metadata.update(
            {
                "object_catalog_commit_key": (
                    catalog_result.catalog_reference.commit_key
                ),
                "object_catalog_source_run_id": (
                    catalog_result.catalog_reference.source_run_id
                ),
                "object_catalog_object_count": catalog_result.catalog_object_count,
                "object_catalog_bootstrapped": catalog_result.catalog_bootstrapped,
                "object_catalog_reused": catalog_result.catalog_reused,
                "object_catalog_bootstrap_object_read_count": (
                    catalog_result.bootstrap_object_read_count
                ),
            }
        )
    return dg.MaterializeResult(
        value=DenmarkCvrCompanyDetailPartitionSnapshot(
            bucket=DENMARK_CVR_BUCKET,
            partition_key=partition_key,
            catalog_reference=(
                catalog_result.catalog_reference if catalog_result is not None else None
            ),
        ),
        metadata=metadata,
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name="denmark_cvr_company_details",
    kinds={"python", "browser", "clickhouse", "duckdb", "json", "s3", "sqlite"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "virksomhed",
        "layer": "raw_detail_update",
    },
    partitions_def=DENMARK_CVR_ACTIVE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_COMPANY_DETAIL_POOL,
    description=(
        "Reads companies assigned to one active DuckDB source date, downloads "
        "their current HTTPS DataCVR details in one browser session, and writes "
        "date-versioned original and English-key JSON objects. Company-specific "
        "server errors are skipped and checkpointed after three failed attempts."
    ),
)
def denmark_cvr_company_detail_updates_s3(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    denmark_cvr_company_details: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    update_date = context.partition_key
    observed_at = datetime.now(UTC)
    cvrs = company_detail_update_cvrs(denmark_cvr_duckdb, update_date)
    summary = write_company_detail_updates(
        object_store=object_store,
        details=denmark_cvr_company_details,
        update_date=update_date,
        cvrs=cvrs,
        failure_database_path=Path(denmark_cvr_company_details.failure_database_path),
        observed_at=observed_at,
        source_run_id=context.run_id,
        record_failure=lambda record: publish_company_detail_failure_record(
            clickhouse,
            record,
        ),
        log_info=context.log.info,
        log_warning=context.log.warning,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_key": summary.partition_key,
            "selected_company_count": summary.selected_company_count,
            "complete_company_count": summary.complete_company_count,
            "resolved_company_count": summary.resolved_company_count,
            "skipped_company_count": summary.skipped_company_count,
            "already_skipped_company_count": summary.already_skipped_company_count,
            "skipped_request_attempt_count": (summary.skipped_request_attempt_count),
            "already_complete_company_count": (summary.already_complete_company_count),
            "translated_existing_company_count": (
                summary.translated_existing_company_count
            ),
            "downloaded_company_count": summary.downloaded_company_count,
            "written_object_count": summary.written_object_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
            "failure_database_path": str(
                denmark_cvr_company_details.failure_database_path
            ),
            "max_request_attempts": denmark_cvr_company_details.max_attempts,
            "retry_base_delay_seconds": (
                denmark_cvr_company_details.retry_base_delay_seconds
            ),
            "retry_max_delay_seconds": (
                denmark_cvr_company_details.retry_max_delay_seconds
            ),
            "failure_clickhouse_table": (
                DENMARK_CVR_COMPANY_DETAIL_FAILURES_CLICKHOUSE_TABLE
            ),
            "key_mapping_version": DENMARK_CVR_COMPANY_DETAIL_MAPPING_VERSION,
            "s3_bucket": DENMARK_CVR_BUCKET,
            "s3_prefix": (
                f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/updates/date={update_date}/"
            ),
        }
    )


def _validate_https_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or parsed.netloc == "":
        raise ValueError("DataCVR company-detail base URL must be an HTTPS URL")
    return normalized


def _validate_cvr(cvr: str) -> None:
    if len(cvr) != 8 or not cvr.isascii() or not cvr.isdigit():
        raise ValueError("DataCVR company-detail CVR number must contain eight digits")


def _validate_update_date(value: str) -> None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValueError(
            f"Invalid DataCVR company-detail update date: {value!r}"
        ) from None
    if parsed.isoformat() != value:
        raise ValueError(f"Invalid DataCVR company-detail update date: {value!r}")


def _company_detail_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid DataCVR company-detail partition: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT:
        raise ValueError(
            f"DataCVR company-detail partition is out of range: {partition_key}"
        )
    expected_key = f"bucket_{bucket_index:03d}"
    if partition_key != expected_key:
        raise ValueError(f"Invalid DataCVR company-detail partition: {partition_key!r}")
    return bucket_index


def _company_detail_http_failure(
    *,
    cvr: str,
    source_url: str,
    result: Any,
    attempt_count: int,
) -> DenmarkCvrCompanyDetailHttpFailure:
    if not isinstance(result, Mapping):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned an invalid browser result for CVR {cvr}"
        )
    status = result.get("status")
    if (
        not isinstance(status, int)
        or status not in DATACVR_COMPANY_DETAIL_SUPPRESSIBLE_STATUSES
    ):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned an invalid retryable result for CVR {cvr}"
        )
    if attempt_count <= 0:
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned an invalid attempt count for CVR {cvr}"
        )
    headers = result.get("headers")
    response_headers = (
        {
            str(name).lower(): str(value)
            for name, value in headers.items()
            if str(name).lower() in SAFE_RESPONSE_HEADERS
        }
        if isinstance(headers, Mapping)
        else {}
    )
    return DenmarkCvrCompanyDetailHttpFailure(
        cvr=cvr,
        source_url=source_url,
        status=status,
        attempt_count=attempt_count,
        response_headers=response_headers,
    )


def _validated_company_detail_download(
    *,
    cvr: str,
    source_url: str,
    result: Any,
) -> DenmarkCvrCompanyDetailDownload:
    if not isinstance(result, dict):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned an invalid browser result for CVR {cvr}"
        )
    status = result.get("status")
    raw_body = result.get("body")
    headers = result.get("headers")
    if not isinstance(status, int) or not isinstance(raw_body, str):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned an invalid browser result for CVR {cvr}"
        )
    if result.get("ok") is not True:
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR company-detail request returned HTTP {status} for CVR {cvr}"
        )
    payload = _parse_company_detail_payload(raw_body, cvr=cvr)
    response_headers = (
        {
            str(name).lower(): str(value)
            for name, value in headers.items()
            if str(name).lower() in SAFE_RESPONSE_HEADERS
        }
        if isinstance(headers, dict)
        else {}
    )
    return DenmarkCvrCompanyDetailDownload(
        cvr=cvr,
        source_url=source_url,
        raw_body=raw_body,
        payload=payload,
        status=status,
        response_headers=response_headers,
    )


def _is_retryable_company_detail_result(result: Any) -> bool:
    return (
        isinstance(result, Mapping)
        and result.get("status") in DATACVR_COMPANY_DETAIL_RETRYABLE_STATUSES
    )


def _is_suppressible_company_detail_result(result: Any) -> bool:
    return (
        isinstance(result, Mapping)
        and result.get("status") in DATACVR_COMPANY_DETAIL_SUPPRESSIBLE_STATUSES
    )


def _company_detail_retry_delay_seconds(
    result: Mapping[str, Any],
    *,
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> float:
    headers = result.get("headers")
    if isinstance(headers, Mapping):
        retry_after = next(
            (
                value
                for key, value in headers.items()
                if str(key).lower() == "retry-after"
            ),
            None,
        )
        if isinstance(retry_after, str):
            try:
                parsed_retry_after = float(retry_after)
            except ValueError:
                parsed_retry_after = -1
            if parsed_retry_after >= 0:
                return min(parsed_retry_after, max_delay_seconds)
    return min(base_delay_seconds * (2 ** (attempt - 1)), max_delay_seconds)


def _company_detail_failure_cvrs(database_path: Path) -> set[str]:
    if not database_path.is_file():
        return set()
    with sqlite3.connect(database_path, timeout=30) as connection:
        _ensure_company_detail_failure_table(connection)
        rows = connection.execute(
            "SELECT DISTINCT cvr FROM company_detail_http_failures"
        ).fetchall()
    return {str(row[0]) for row in rows}


def _ensure_company_detail_failure_table(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS company_detail_http_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cvr TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            failed_at TEXT NOT NULL,
            request_attempt_count INTEGER NOT NULL,
            decision TEXT NOT NULL,
            source_asset TEXT NOT NULL,
            source_partition_key TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            failure_object_key TEXT NOT NULL,
            response_headers TEXT NOT NULL,
            CHECK (length(cvr) = 8),
            CHECK (http_status BETWEEN 100 AND 599),
            CHECK (request_attempt_count > 0),
            CHECK (decision = 'ignore_company')
        )
        """
    )
    columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(company_detail_http_failures)"
        ).fetchall()
    }
    if "request_attempt_count" not in columns:
        connection.execute(
            """
            ALTER TABLE company_detail_http_failures
            ADD COLUMN request_attempt_count INTEGER NOT NULL DEFAULT 1
            CHECK (request_attempt_count > 0)
            """
        )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS
            company_detail_http_failures_cvr_status_failed_at
        ON company_detail_http_failures (cvr, http_status, failed_at)
        """
    )


def _company_detail_failure_marker_bytes(
    record: DenmarkCvrCompanyDetailFailureRecord,
) -> bytes:
    return json.dumps(
        {
            "cvr": record.cvr,
            "http_status": record.http_status,
            "first_failed_at": record.first_failed_at.isoformat(),
            "last_failed_at": record.failed_at.isoformat(),
            "failure_count": record.failure_count,
            "request_attempt_count": record.request_attempt_count,
            "decision": record.decision,
            "source_asset": record.source_asset,
            "source_partition_key": record.source_partition_key,
            "source_url": record.source_url,
            "source_run_id": record.source_run_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _parse_company_detail_payload(raw_body: str, *, cvr: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned invalid company-detail JSON for CVR {cvr}"
        ) from None
    if not isinstance(payload, dict):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR company-detail JSON must be an object for CVR {cvr}"
        )
    master_data = payload.get("stamdata")
    response_cvr = (
        master_data.get("cvrnummer") if isinstance(master_data, dict) else None
    )
    if response_cvr != cvr:
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR company-detail JSON CVR mismatch for requested CVR {cvr}"
        )
    return payload


def _translated_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        translate_company_detail_keys(payload),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _translate_value(value: Any, *, path: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        translated: dict[str, Any] = {}
        for source_key, child in value.items():
            if not isinstance(source_key, str):
                raise DenmarkCvrCompanyDetailKeyError(
                    f"DataCVR company-detail key at {_display_path(path)} is not text"
                )
            if source_key not in DENMARK_CVR_COMPANY_DETAIL_KEY_MAP:
                raise DenmarkCvrCompanyDetailKeyError(
                    "Unmapped DataCVR company-detail key "
                    f"{source_key!r} at {_display_path((*path, source_key))}"
                )
            target_key = DENMARK_CVR_COMPANY_DETAIL_KEY_MAP[source_key]
            if target_key in translated:
                raise DenmarkCvrCompanyDetailKeyError(
                    "DataCVR company-detail key mapping collision at "
                    f"{_display_path(path)} for English key {target_key!r}"
                )
            translated[target_key] = _translate_value(
                child,
                path=(*path, source_key),
            )
        return translated
    if isinstance(value, list):
        return [
            _translate_value(item, path=(*path, f"[{index}]"))
            for index, item in enumerate(value)
        ]
    return value


def _unmapped_key_paths(value: Any, *, path: tuple[str, ...]) -> Iterator[str]:
    if isinstance(value, Mapping):
        for source_key, child in value.items():
            child_path = (*path, str(source_key))
            if (
                not isinstance(source_key, str)
                or source_key not in DENMARK_CVR_COMPANY_DETAIL_KEY_MAP
            ):
                yield _display_path(child_path)
            yield from _unmapped_key_paths(child, path=child_path)
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            yield from _unmapped_key_paths(child, path=(*path, f"[{index}]"))


def _display_path(path: tuple[str, ...]) -> str:
    return ".".join(path) if path else "<root>"


defs = dg.Definitions(
    assets=[
        denmark_cvr_company_details_s3,
        denmark_cvr_company_detail_updates_s3,
    ],
    resources={
        "denmark_cvr_company_details": DenmarkCvrCompanyDetailResource(),
        "denmark_cvr_duckdb": duckdb_resource(DENMARK_CVR_DUCKDB_PATH),
    },
)
