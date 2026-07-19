import hashlib
import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from random import randint
from typing import Any, Self
from urllib.parse import urlencode, urlparse

import dagster as dg
import duckdb
from cloakbrowser import launch
from dagster_duckdb import DuckDBResource
from pydantic import model_validator

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.assets import DENMARK_CVR_BUCKET
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
class DenmarkCvrCompanyDetailSummary:
    partition_key: str
    selected_company_count: int
    complete_company_count: int
    already_complete_company_count: int
    translated_existing_company_count: int
    downloaded_company_count: int
    written_object_count: int
    downloaded_size_bytes: int


class DenmarkCvrCompanyDetailRequestError(RuntimeError):
    pass


class DenmarkCvrCompanyDetailKeyError(ValueError):
    pass


class DenmarkCvrCompanyDetailResource(dg.ConfigurableResource):
    detail_base_url: str = DATACVR_BASE_URL
    locale: str = "en"
    min_delay_ms: int = 100
    max_delay_ms: int = 800

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        _validate_https_base_url(self.detail_base_url)
        if self.locale != "en":
            raise ValueError("DataCVR company details must use locale='en'")
        if self.min_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("request delays must not be negative")
        if self.min_delay_ms > self.max_delay_ms:
            raise ValueError("min_delay_ms must not exceed max_delay_ms")
        return self

    def iter_company_details(
        self,
        cvrs: Sequence[str],
        *,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[DenmarkCvrCompanyDetailDownload]:
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
                try:
                    result = page.evaluate(
                        DATACVR_COMPANY_DETAIL_SCRIPT,
                        {"url": source_url},
                    )
                except Exception:
                    raise DenmarkCvrCompanyDetailRequestError(
                        f"DataCVR company-detail request failed for CVR {cvr}"
                    ) from None
                yield _validated_company_detail_download(
                    cvr=cvr,
                    source_url=source_url,
                    result=result,
                )
        finally:
            browser.close()

    def _request_delay_seconds(self) -> float:
        return randint(self.min_delay_ms, self.max_delay_ms) / 1_000


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


def write_company_detail_partition(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    partition_key: str,
    cvrs: Sequence[str],
    log_info: Callable[..., object] | None = None,
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
        )
        for cvr in selected_cvrs
    }
    return _write_company_details(
        object_store=object_store,
        details=details,
        result_partition_key=partition_key,
        object_prefix=partition_prefix,
        object_keys=object_keys,
        log_info=log_info,
    )


def write_company_detail_updates(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    update_date: str,
    cvrs: Sequence[str],
    log_info: Callable[..., object] | None = None,
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
        )
        for cvr in selected_cvrs
    }
    return _write_company_details(
        object_store=object_store,
        details=details,
        result_partition_key=update_date,
        object_prefix=object_prefix,
        object_keys=object_keys,
        log_info=log_info,
    )


def _write_company_details(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    result_partition_key: str,
    object_prefix: str,
    object_keys: Mapping[str, tuple[str, str]],
    log_info: Callable[..., object] | None,
) -> DenmarkCvrCompanyDetailSummary:
    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    existing_keys = set(
        object_store.list_keys(object_prefix, bucket=DENMARK_CVR_BUCKET)
    )

    already_complete_count = 0
    translated_existing_count = 0
    written_object_count = 0
    cvrs_to_download: list[str] = []
    for cvr, (original_key, english_key) in object_keys.items():
        if original_key in existing_keys and english_key in existing_keys:
            already_complete_count += 1
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
            continue
        cvrs_to_download.append(cvr)

    downloaded_count = 0
    downloaded_size_bytes = 0
    for download in details.iter_company_details(tuple(cvrs_to_download)):
        original_key, english_key = object_keys[download.cvr]
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
    return DenmarkCvrCompanyDetailSummary(
        partition_key=result_partition_key,
        selected_company_count=len(object_keys),
        complete_company_count=complete_company_count,
        already_complete_company_count=already_complete_count,
        translated_existing_company_count=translated_existing_count,
        downloaded_company_count=downloaded_count,
        written_object_count=written_object_count,
        downloaded_size_bytes=downloaded_size_bytes,
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name="denmark_cvr_company_details",
    kinds={"python", "browser", "duckdb", "json", "s3"},
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
        "keys are translated to English without changing values."
    ),
)
def denmark_cvr_company_details_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_company_details: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    cvrs = company_detail_partition_cvrs(denmark_cvr_duckdb, partition_key)
    context.log.info(
        "DataCVR company-detail partition selected: partition=%s companies=%s",
        partition_key,
        len(cvrs),
    )
    summary = write_company_detail_partition(
        object_store=object_store,
        details=denmark_cvr_company_details,
        partition_key=partition_key,
        cvrs=cvrs,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_key": summary.partition_key,
            "hash_bucket_count": DENMARK_CVR_COMPANY_DETAIL_BUCKET_COUNT,
            "selected_company_count": summary.selected_company_count,
            "complete_company_count": summary.complete_company_count,
            "already_complete_company_count": summary.already_complete_company_count,
            "translated_existing_company_count": (
                summary.translated_existing_company_count
            ),
            "downloaded_company_count": summary.downloaded_company_count,
            "written_object_count": summary.written_object_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
            "key_mapping_version": DENMARK_CVR_COMPANY_DETAIL_MAPPING_VERSION,
            "s3_bucket": DENMARK_CVR_BUCKET,
            "s3_prefix": (f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/"),
            "source_url": company_detail_api_url(
                denmark_cvr_company_details.detail_base_url,
                "00000000",
            ).replace("00000000", "{cvr}"),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name="denmark_cvr_company_details",
    kinds={"python", "browser", "duckdb", "json", "s3"},
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
        "date-versioned original and English-key JSON objects."
    ),
)
def denmark_cvr_company_detail_updates_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_company_details: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    update_date = context.partition_key
    cvrs = company_detail_update_cvrs(denmark_cvr_duckdb, update_date)
    summary = write_company_detail_updates(
        object_store=object_store,
        details=denmark_cvr_company_details,
        update_date=update_date,
        cvrs=cvrs,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_key": summary.partition_key,
            "selected_company_count": summary.selected_company_count,
            "complete_company_count": summary.complete_company_count,
            "already_complete_company_count": (summary.already_complete_company_count),
            "translated_existing_company_count": (
                summary.translated_existing_company_count
            ),
            "downloaded_company_count": summary.downloaded_company_count,
            "written_object_count": summary.written_object_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
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
