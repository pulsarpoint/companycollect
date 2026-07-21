from __future__ import annotations

import json
from collections.abc import Iterator
from hashlib import sha256
from typing import Any

import dagster as dg
import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers.requests import Client as DltRequestsClient
from pydantic import field_validator

from dagster_v3.defs.wikidata.registry_seed import WIKIDATA_REGISTRY_NUMBER_PROPERTY_IDS

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_RAW_BUCKET = "source-wikidata-company-seed"
# Registry-number pulls are treated as pseudo-exchanges (id "registry_<PID>") so the
# raw-object layout, manifests, page numbering, augmentation batching, and provenance
# keys — all keyed on "exchange_id" — work unchanged for a company-discovery source that
# isn't actually an exchange listing. See defs/wikidata/registry_seed.py for where the
# property set comes from.
WIKIDATA_REGISTRY_PSEUDO_EXCHANGE_PREFIX = "registry_"
WIKIDATA_DUCKDB_PIPELINE_NAME = "wikidata_listed_companies"
WIKIDATA_DUCKDB_DATASET_NAME = "wikidata_stage"
WIKIDATA_LISTED_COMPANIES_TABLE = "listed_companies"
DEFAULT_WIKIDATA_PAGE_SIZE = 5_000
DEFAULT_WIKIDATA_AUGMENTATION_BATCH_SIZE = 100
DEFAULT_WIKIDATA_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_WIKIDATA_REQUEST_DELAY_SECONDS = 1.0
DEFAULT_WIKIDATA_USER_AGENT = "corpscout-dagster-v3-wikidata/0.1"
WIKIDATA_REQUEST_MAX_ATTEMPTS = 5
WIKIDATA_RETRY_INITIAL_DELAY_SECONDS = 10.0
WIKIDATA_RETRY_MAX_DELAY_SECONDS = 120.0
WIKIDATA_LISTED_COMPANIES_COLUMNS: dict[str, dict[str, Any]] = {
    "source_run_id": {"data_type": "text"},
    "retrieved_at": {"data_type": "timestamp"},
    "exchange_wikidata_id": {"data_type": "text", "nullable": False},
    "exchange_name": {"data_type": "text"},
    "listed_company_count_on_exchange": {"data_type": "bigint"},
    "page_number": {"data_type": "bigint"},
    "page_offset": {"data_type": "bigint"},
    "page_row_number": {"data_type": "bigint"},
    "company_wikidata_id": {"data_type": "text", "nullable": False},
    "company_url": {"data_type": "text"},
    "company_label": {"data_type": "text"},
    "company_description": {"data_type": "text"},
    "official_name": {"data_type": "text"},
    "listing_statement_id": {"data_type": "text"},
    "listing_url": {"data_type": "text"},
    "ticker": {"data_type": "text"},
    "isin": {"data_type": "text"},
    "website_url": {"data_type": "text"},
    "cik": {"data_type": "text"},
    "lei": {"data_type": "text"},
    "headquarters_wikidata_id": {"data_type": "text"},
    "headquarters_label": {"data_type": "text"},
    "headquarters_country_wikidata_id": {"data_type": "text"},
    "headquarters_country_label": {"data_type": "text"},
    "headquarters_country_iso2": {"data_type": "text"},
    "inception_date": {"data_type": "text"},
    "legal_form_wikidata_id": {"data_type": "text"},
    "legal_form_label": {"data_type": "text"},
    "employee_count": {"data_type": "text"},
    "employee_count_point_in_time": {"data_type": "text"},
    "logo_image": {"data_type": "text"},
    "logo_image_url": {"data_type": "text"},
    "industry_wikidata_id": {"data_type": "text"},
    "industry_label": {"data_type": "text"},
    "opencorporates_company_id": {"data_type": "text"},
    "eu_vat_number": {"data_type": "text"},
    "duns_number": {"data_type": "text"},
    "permid": {"data_type": "text"},
    "bloomberg_company_id": {"data_type": "text"},
    "linkedin_company_id": {"data_type": "text"},
    "se_orgnr": {"data_type": "text"},
    "no_orgnr": {"data_type": "text"},
    "dk_cvr": {"data_type": "text"},
    "fi_business_id": {"data_type": "text"},
    "uk_company_number": {"data_type": "text"},
    "fr_siren": {"data_type": "text"},
    "cz_ico": {"data_type": "text"},
    "lv_regcode": {"data_type": "text"},
    "br_cnpj": {"data_type": "text"},
    "parent_organization_statement_id": {"data_type": "text"},
    "parent_organization_wikidata_id": {"data_type": "text"},
    "parent_organization_label": {"data_type": "text"},
    "parent_organization_start_date": {"data_type": "text"},
    "parent_organization_end_date": {"data_type": "text"},
    "child_organization_statement_id": {"data_type": "text"},
    "child_organization_wikidata_id": {"data_type": "text"},
    "child_organization_label": {"data_type": "text"},
    "child_organization_start_date": {"data_type": "text"},
    "child_organization_end_date": {"data_type": "text"},
    "owned_by_statement_id": {"data_type": "text"},
    "owned_by_wikidata_id": {"data_type": "text"},
    "owned_by_label": {"data_type": "text"},
    "owned_by_start_date": {"data_type": "text"},
    "owned_by_end_date": {"data_type": "text"},
    "owner_of_statement_id": {"data_type": "text"},
    "owner_of_wikidata_id": {"data_type": "text"},
    "owner_of_label": {"data_type": "text"},
    "owner_of_start_date": {"data_type": "text"},
    "owner_of_end_date": {"data_type": "text"},
    "person_wikidata_id": {"data_type": "text"},
    "person_url": {"data_type": "text"},
    "person_label": {"data_type": "text"},
    "person_description": {"data_type": "text"},
    "person_image": {"data_type": "text"},
    "person_image_url": {"data_type": "text"},
    "person_birth_year": {"data_type": "text"},
    "role_property": {"data_type": "text"},
    "role_start_date": {"data_type": "text"},
    "role_end_date": {"data_type": "text"},
    "query_hash": {"data_type": "text"},
    "source_record_id": {"data_type": "text", "nullable": False},
    "source_payload_hash": {"data_type": "text", "nullable": False},
    "raw_binding_json": {"data_type": "text"},
}

WIKIDATA_AUGMENTATION_FIELD_NAMES = (
    "inception_date",
    "legal_form_wikidata_id",
    "legal_form_label",
    "employee_count",
    "employee_count_point_in_time",
    "logo_image",
    "logo_image_url",
    "opencorporates_company_id",
    "eu_vat_number",
    "duns_number",
    "permid",
    "bloomberg_company_id",
    "linkedin_company_id",
    "se_orgnr",
    "no_orgnr",
    "dk_cvr",
    "fi_business_id",
    "uk_company_number",
    "fr_siren",
    "cz_ico",
    "lv_regcode",
    "br_cnpj",
    "parent_organization_statement_id",
    "parent_organization_wikidata_id",
    "parent_organization_label",
    "parent_organization_start_date",
    "parent_organization_end_date",
    "child_organization_statement_id",
    "child_organization_wikidata_id",
    "child_organization_label",
    "child_organization_start_date",
    "child_organization_end_date",
    "owned_by_statement_id",
    "owned_by_wikidata_id",
    "owned_by_label",
    "owned_by_start_date",
    "owned_by_end_date",
    "owner_of_statement_id",
    "owner_of_wikidata_id",
    "owner_of_label",
    "owner_of_start_date",
    "owner_of_end_date",
    "person_wikidata_id",
    "person_url",
    "person_label",
    "person_description",
    "person_image",
    "person_image_url",
    "person_birth_year",
    "role_property",
    "role_start_date",
    "role_end_date",
)


class WikidataRawPullConfig(dg.Config):
    exchange_ids_csv: str | None = None
    page_size: int = DEFAULT_WIKIDATA_PAGE_SIZE
    augmentation_batch_size: int = DEFAULT_WIKIDATA_AUGMENTATION_BATCH_SIZE
    max_pages: int | None = None
    max_exchanges: int | None = None
    request_timeout_seconds: int = DEFAULT_WIKIDATA_REQUEST_TIMEOUT_SECONDS
    request_delay_seconds: float = DEFAULT_WIKIDATA_REQUEST_DELAY_SECONDS
    user_agent: str = DEFAULT_WIKIDATA_USER_AGENT
    # Registry-number seed: every Wikidata item carrying a national registry-number
    # property (SE orgnr, NO orgnr, DK CVR, ...), not just exchange-listed companies.
    # Pulled as pseudo-exchanges after the real exchanges above. Property set defaults
    # to the union of every country module's WikidataRegistrySeedSpec (see
    # defs/wikidata/registry_seed.py) — override with registry_property_ids_csv for a
    # narrower run (e.g. backfilling one country).
    include_registry_seed: bool = True
    registry_property_ids_csv: str | None = None

    @field_validator("exchange_ids_csv")
    @classmethod
    def validate_optional_exchange_ids(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("exchange_ids_csv must not be blank when provided")
        return stripped

    @field_validator("registry_property_ids_csv")
    @classmethod
    def validate_optional_registry_property_ids(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("registry_property_ids_csv must not be blank when provided")
        return stripped

    @field_validator("user_agent")
    @classmethod
    def validate_required_string(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("page_size", "augmentation_batch_size", "request_timeout_seconds")
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("max_pages", "max_exchanges")
    @classmethod
    def validate_optional_positive_int(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_non_negative_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_delay_seconds must be zero or greater")
        return value

    def configured_exchange_ids(self) -> tuple[str, ...] | None:
        if self.exchange_ids_csv is None:
            return None
        exchange_ids = tuple(
            exchange_id.strip()
            for exchange_id in self.exchange_ids_csv.split(",")
            if exchange_id.strip()
        )
        if not exchange_ids:
            raise ValueError("exchange_ids_csv must contain at least one exchange ID")
        return exchange_ids

    def configured_registry_property_ids(self) -> tuple[str, ...]:
        if self.registry_property_ids_csv is None:
            return WIKIDATA_REGISTRY_NUMBER_PROPERTY_IDS
        property_ids = tuple(
            property_id.strip()
            for property_id in self.registry_property_ids_csv.split(",")
            if property_id.strip()
        )
        if not property_ids:
            raise ValueError(
                "registry_property_ids_csv must contain at least one property ID"
            )
        return property_ids


class WikidataListedCompaniesConfig(dg.Config):
    raw_run_id: str | None = None
    max_pages: int | None = None
    max_exchanges: int | None = None

    @field_validator("raw_run_id")
    @classmethod
    def validate_optional_run_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("raw_run_id must not be blank when provided")
        return stripped

    @field_validator("max_pages", "max_exchanges")
    @classmethod
    def validate_optional_positive_int(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("value must be greater than zero")
        return value


class WikidataSparqlClient:
    def __init__(self, *, timeout_seconds: int) -> None:
        self._timeout_seconds = timeout_seconds
        self._session = DltRequestsClient(
            request_timeout=timeout_seconds,
            request_max_attempts=WIKIDATA_REQUEST_MAX_ATTEMPTS,
            request_backoff_factor=WIKIDATA_RETRY_INITIAL_DELAY_SECONDS,
            request_max_retry_delay=WIKIDATA_RETRY_MAX_DELAY_SECONDS,
            respect_retry_after_header=True,
        )

    def fetch(self, query: str, *, user_agent: str) -> dict[str, Any]:
        headers = getattr(self._session, "headers", None)
        if isinstance(headers, dict):
            headers["User-Agent"] = user_agent

        response = self._session.get(
            WIKIDATA_SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Wikidata SPARQL response was not a JSON object")
        return payload


def build_listed_company_query(
    *,
    exchange_id: str,
    limit: int,
    offset: int,
) -> str:
    return f"""
SELECT DISTINCT
  ?company
  ?companyLabel
  ?companyDescription
  ?officialName
  ?listing
  ?exchange
  ?exchangeLabel
  ?ticker
  ?isin
  ?website
  ?cik
  ?lei
  ?headquarters
  ?headquartersLabel
  ?headquartersCountry
  ?headquartersCountryLabel
  ?headquartersCountryIso2
  ?industry
  ?industryLabel
WHERE {{
  ?company p:P414 ?listing .
  ?listing ps:P414 ?exchange .

  VALUES ?exchange {{ wd:{exchange_id} }}

  OPTIONAL {{ ?company wdt:P1448 ?officialName . }}
  OPTIONAL {{ ?listing pq:P249 ?ticker . }}
  OPTIONAL {{ ?listing pq:P946 ?isin . }}
  OPTIONAL {{ ?company wdt:P856 ?website . }}
  OPTIONAL {{ ?company wdt:P5531 ?cik . }}
  OPTIONAL {{ ?company wdt:P1278 ?lei . }}
  OPTIONAL {{
    ?company wdt:P159 ?headquarters .
    OPTIONAL {{ ?headquarters wdt:P131*/wdt:P17 ?headquartersCountry . }}
    OPTIONAL {{ ?headquartersCountry wdt:P297 ?headquartersCountryIso2 . }}
  }}
  OPTIONAL {{ ?company wdt:P452 ?industry . }}

  FILTER NOT EXISTS {{ ?listing pq:P582 ?listingEndDate . }}
  FILTER NOT EXISTS {{ ?company wdt:P576 ?dissolvedDate . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?company ?listing ?website ?ticker ?isin ?cik ?lei ?headquarters ?headquartersCountry ?headquartersCountryIso2 ?industry
LIMIT {limit}
OFFSET {offset}
""".strip()


def registry_pseudo_exchange_id(property_id: str) -> str:
    """The pseudo-exchange id a registry-number property pulls under, e.g.
    ``registry_P6460`` for Sweden's orgnr. Keeps raw-object keys, manifests, and page
    numbering identical in shape to a real exchange pull (all keyed on "exchange_id")."""
    return f"{WIKIDATA_REGISTRY_PSEUDO_EXCHANGE_PREFIX}{property_id}"


def is_registry_pseudo_exchange_id(exchange_id: str) -> bool:
    return exchange_id.startswith(WIKIDATA_REGISTRY_PSEUDO_EXCHANGE_PREFIX)


def registry_property_id_from_pseudo_exchange_id(exchange_id: str) -> str:
    if not is_registry_pseudo_exchange_id(exchange_id):
        raise ValueError(f"{exchange_id!r} is not a registry pseudo-exchange id")
    return exchange_id[len(WIKIDATA_REGISTRY_PSEUDO_EXCHANGE_PREFIX) :]


def build_registry_number_company_query(
    *,
    property_id: str,
    limit: int,
    offset: int,
) -> str:
    """Mirrors build_listed_company_query's SELECT bindings and OPTIONAL blocks so the
    page-row parser (listed_company_row_from_binding) works unchanged, EXCEPT the WHERE
    clause anchors on ``?company wdt:<property_id> ?registryValue .`` instead of a
    p:P414 listing — there is no exchange/ticker/isin triple, so ?listing/?exchange/
    ?exchangeLabel/?ticker/?isin simply come back unbound (empty) for every row, same as
    any other absent OPTIONAL binding. The registry value itself is not selected: this
    seed's job is only discovering company ids, the identifier augmentation query
    (build_company_identifier_augmentation_query) already extracts the value per
    company."""
    return f"""
SELECT DISTINCT
  ?company
  ?companyLabel
  ?companyDescription
  ?officialName
  ?listing
  ?exchange
  ?exchangeLabel
  ?ticker
  ?isin
  ?website
  ?cik
  ?lei
  ?headquarters
  ?headquartersLabel
  ?headquartersCountry
  ?headquartersCountryLabel
  ?headquartersCountryIso2
  ?industry
  ?industryLabel
WHERE {{
  ?company wdt:{property_id} ?registryValue .

  OPTIONAL {{ ?company wdt:P1448 ?officialName . }}
  OPTIONAL {{ ?company wdt:P856 ?website . }}
  OPTIONAL {{ ?company wdt:P5531 ?cik . }}
  OPTIONAL {{ ?company wdt:P1278 ?lei . }}
  OPTIONAL {{
    ?company wdt:P159 ?headquarters .
    OPTIONAL {{ ?headquarters wdt:P131*/wdt:P17 ?headquartersCountry . }}
    OPTIONAL {{ ?headquartersCountry wdt:P297 ?headquartersCountryIso2 . }}
  }}
  OPTIONAL {{ ?company wdt:P452 ?industry . }}

  FILTER NOT EXISTS {{ ?company wdt:P576 ?dissolvedDate . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?company ?website ?cik ?lei ?headquarters ?headquartersCountry ?headquartersCountryIso2 ?industry
LIMIT {limit}
OFFSET {offset}
""".strip()


def build_company_profile_augmentation_query(company_ids: tuple[str, ...]) -> str:
    values = company_values_clause(company_ids)
    return f"""
SELECT DISTINCT
  ?company
  ?inceptionDate
  ?legalForm
  ?legalFormLabel
  ?employeeCount
  ?employeeCountPointInTime
  ?logoImage
  ?logoImageUrl
WHERE {{
  VALUES ?company {{ {values} }}

  OPTIONAL {{ ?company wdt:P571 ?inceptionDate . }}
  OPTIONAL {{ ?company wdt:P1454 ?legalForm . }}
  OPTIONAL {{
    ?company p:P1128 ?employeeCountStatement .
    ?employeeCountStatement ps:P1128 ?employeeCount .
    OPTIONAL {{ ?employeeCountStatement pq:P585 ?employeeCountPointInTime . }}
  }}
  OPTIONAL {{
    ?company wdt:P154 ?logoImage .
    BIND(IRI(CONCAT(
      "https://commons.wikimedia.org/wiki/Special:FilePath/",
      ENCODE_FOR_URI(STR(?logoImage))
    )) AS ?logoImageUrl)
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?company ?legalForm ?employeeCountStatement
""".strip()


def build_company_identifier_augmentation_query(company_ids: tuple[str, ...]) -> str:
    values = company_values_clause(company_ids)
    return f"""
SELECT DISTINCT
  ?company
  ?openCorporatesId
  ?euVatNumber
  ?dunsNumber
  ?permId
  ?bloombergCompanyId
  ?linkedinCompanyId
  ?seOrgnr
  ?noOrgnr
  ?dkCvr
  ?fiBusinessId
  ?ukCompanyNumber
  ?frSiren
  ?czIco
  ?lvRegcode
  ?brCnpj
WHERE {{
  VALUES ?company {{ {values} }}

  {{
    ?company wdt:P1320 ?openCorporatesId .
  }}
  UNION
  {{
    ?company wdt:P3608 ?euVatNumber .
  }}
  UNION
  {{
    ?company wdt:P2771 ?dunsNumber .
  }}
  UNION
  {{
    ?company wdt:P3347 ?permId .
  }}
  UNION
  {{
    ?company wdt:P3377 ?bloombergCompanyId .
  }}
  UNION
  {{
    ?company wdt:P4264 ?linkedinCompanyId .
  }}
  UNION
  {{
    ?company wdt:P6460 ?seOrgnr .
  }}
  UNION
  {{
    ?company wdt:P2333 ?noOrgnr .
  }}
  UNION
  {{
    ?company wdt:P1059 ?dkCvr .
  }}
  UNION
  {{
    ?company wdt:P12980 ?fiBusinessId .
  }}
  UNION
  {{
    ?company wdt:P2622 ?ukCompanyNumber .
  }}
  UNION
  {{
    ?company wdt:P1616 ?frSiren .
  }}
  UNION
  {{
    ?company wdt:P4156 ?czIco .
  }}
  UNION
  {{
    ?company wdt:P8053 ?lvRegcode .
  }}
  UNION
  {{
    ?company wdt:P6204 ?brCnpj .
  }}
}}
ORDER BY ?company ?openCorporatesId ?euVatNumber ?dunsNumber ?permId ?bloombergCompanyId ?linkedinCompanyId ?seOrgnr ?noOrgnr ?dkCvr ?fiBusinessId ?ukCompanyNumber ?frSiren ?czIco ?lvRegcode ?brCnpj
""".strip()


def build_company_relationship_augmentation_query(company_ids: tuple[str, ...]) -> str:
    values = company_values_clause(company_ids)
    return f"""
SELECT DISTINCT
  ?company
  ?parentOrganizationStatement
  ?parentOrganization
  ?parentOrganizationLabel
  ?parentOrganizationStartDate
  ?parentOrganizationEndDate
  ?childOrganizationStatement
  ?childOrganization
  ?childOrganizationLabel
  ?childOrganizationStartDate
  ?childOrganizationEndDate
  ?ownedByStatement
  ?ownedBy
  ?ownedByLabel
  ?ownedByStartDate
  ?ownedByEndDate
  ?ownerOfStatement
  ?ownerOf
  ?ownerOfLabel
  ?ownerOfStartDate
  ?ownerOfEndDate
WHERE {{
  VALUES ?company {{ {values} }}

  {{
    ?company p:P749 ?parentOrganizationStatement .
    ?parentOrganizationStatement ps:P749 ?parentOrganization .
    OPTIONAL {{ ?parentOrganizationStatement pq:P580 ?parentOrganizationStartDate . }}
    OPTIONAL {{ ?parentOrganizationStatement pq:P582 ?parentOrganizationEndDate . }}
  }}
  UNION
  {{
    ?company p:P355 ?childOrganizationStatement .
    ?childOrganizationStatement ps:P355 ?childOrganization .
    OPTIONAL {{ ?childOrganizationStatement pq:P580 ?childOrganizationStartDate . }}
    OPTIONAL {{ ?childOrganizationStatement pq:P582 ?childOrganizationEndDate . }}
  }}
  UNION
  {{
    ?company p:P127 ?ownedByStatement .
    ?ownedByStatement ps:P127 ?ownedBy .
    OPTIONAL {{ ?ownedByStatement pq:P580 ?ownedByStartDate . }}
    OPTIONAL {{ ?ownedByStatement pq:P582 ?ownedByEndDate . }}
  }}
  UNION
  {{
    ?company p:P1830 ?ownerOfStatement .
    ?ownerOfStatement ps:P1830 ?ownerOf .
    OPTIONAL {{ ?ownerOfStatement pq:P580 ?ownerOfStartDate . }}
    OPTIONAL {{ ?ownerOfStatement pq:P582 ?ownerOfEndDate . }}
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?company ?parentOrganizationStatement ?childOrganizationStatement ?ownedByStatement ?ownerOfStatement
""".strip()


def build_company_people_augmentation_query(company_ids: tuple[str, ...]) -> str:
    """Company-anchored person links: chief executive officer (P169), founder (P112),
    chairperson (P488), board member (P3320), and person-valued owned-by (P127, filtered
    to ?person wdt:P31 wd:Q5 -- P127 usually points at another company, so without the
    human filter this branch would pull in corporate owners too). Every branch shares the
    same ?roleStatement/?person/?startDate/?endDate variable names (unlike
    build_company_relationship_augmentation_query's per-branch names) since all five
    represent the same shape (a person link with an optional date range) -- that keeps
    the DuckDB pivot a single flat group-by instead of nine UNION branches. The company
    anchor (?company, bound from the VALUES clause) is the only way a person enters this
    table; there is no name-based lookup anywhere in this query or its consumers."""
    values = company_values_clause(company_ids)
    return f"""
SELECT DISTINCT
  ?company
  ?person
  ?personLabel
  ?personDescription
  ?personImage
  ?personImageUrl
  ?personBirthYear
  ?roleProperty
  ?startDate
  ?endDate
WHERE {{
  VALUES ?company {{ {values} }}

  {{
    ?company p:P169 ?roleStatement .
    ?roleStatement ps:P169 ?person .
    BIND("P169" AS ?roleProperty)
    OPTIONAL {{ ?roleStatement pq:P580 ?startDate . }}
    OPTIONAL {{ ?roleStatement pq:P582 ?endDate . }}
  }}
  UNION
  {{
    ?company p:P112 ?roleStatement .
    ?roleStatement ps:P112 ?person .
    BIND("P112" AS ?roleProperty)
    OPTIONAL {{ ?roleStatement pq:P580 ?startDate . }}
    OPTIONAL {{ ?roleStatement pq:P582 ?endDate . }}
  }}
  UNION
  {{
    ?company p:P488 ?roleStatement .
    ?roleStatement ps:P488 ?person .
    BIND("P488" AS ?roleProperty)
    OPTIONAL {{ ?roleStatement pq:P580 ?startDate . }}
    OPTIONAL {{ ?roleStatement pq:P582 ?endDate . }}
  }}
  UNION
  {{
    ?company p:P3320 ?roleStatement .
    ?roleStatement ps:P3320 ?person .
    BIND("P3320" AS ?roleProperty)
    OPTIONAL {{ ?roleStatement pq:P580 ?startDate . }}
    OPTIONAL {{ ?roleStatement pq:P582 ?endDate . }}
  }}
  UNION
  {{
    ?company p:P127 ?roleStatement .
    ?roleStatement ps:P127 ?person .
    ?person wdt:P31 wd:Q5 .
    BIND("P127" AS ?roleProperty)
    OPTIONAL {{ ?roleStatement pq:P580 ?startDate . }}
    OPTIONAL {{ ?roleStatement pq:P582 ?endDate . }}
  }}

  OPTIONAL {{
    ?person wdt:P569 ?personBirthDate .
    BIND(YEAR(?personBirthDate) AS ?personBirthYear)
  }}
  OPTIONAL {{
    ?person wdt:P18 ?personImage .
    BIND(IRI(CONCAT(
      "https://commons.wikimedia.org/wiki/Special:FilePath/",
      ENCODE_FOR_URI(STR(?personImage))
    )) AS ?personImageUrl)
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?company ?roleProperty ?person
""".strip()


def company_values_clause(company_ids: tuple[str, ...]) -> str:
    if not company_ids:
        raise ValueError("company_ids must contain at least one Wikidata ID")
    return " ".join(f"wd:{company_id}" for company_id in company_ids)


def build_active_listed_exchanges_query() -> str:
    return """
SELECT ?exchange ?exchangeLabel (COUNT(DISTINCT ?company) AS ?listedCompanyCount)
WHERE {
  ?company p:P414 ?listing .
  ?listing ps:P414 ?exchange .

  FILTER NOT EXISTS { ?listing pq:P582 ?listingEndDate . }
  FILTER NOT EXISTS { ?company wdt:P576 ?dissolvedDate . }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
  }
}
GROUP BY ?exchange ?exchangeLabel
ORDER BY DESC(?listedCompanyCount) ?exchange
""".strip()


@dlt.source(name="wikidata_company_seed")
def wikidata_listed_companies_source(
    *,
    object_store: Any | None = None,
    raw_run_id: str | None = None,
    max_pages: int | None = None,
    max_exchanges: int | None = None,
    source_run_id: str = "",
) -> DltResource:
    return wikidata_listed_companies_resource(
        object_store=object_store,
        raw_run_id=raw_run_id,
        max_pages=max_pages,
        max_exchanges=max_exchanges,
        source_run_id=source_run_id,
    )


def wikidata_listed_companies_resource(
    *,
    object_store: Any | None = None,
    raw_run_id: str | None = None,
    max_pages: int | None = None,
    max_exchanges: int | None = None,
    source_run_id: str = "",
) -> DltResource:
    def rows() -> Iterator[dict[str, Any]]:
        yield from iter_wikidata_listed_company_rows(
            object_store=object_store,
            raw_run_id=raw_run_id,
            max_pages=max_pages,
            max_exchanges=max_exchanges,
            source_run_id=source_run_id,
        )

    return dlt.resource(
        rows,
        name=WIKIDATA_LISTED_COMPANIES_TABLE,
        table_name=WIKIDATA_LISTED_COMPANIES_TABLE,
        write_disposition="replace",
        primary_key=["exchange_wikidata_id", "source_record_id"],
        columns=WIKIDATA_LISTED_COMPANIES_COLUMNS,
    )


def iter_wikidata_listed_company_rows(
    *,
    object_store: Any | None = None,
    raw_run_id: str | None = None,
    max_pages: int | None = None,
    max_exchanges: int | None = None,
    source_run_id: str = "",
) -> Iterator[dict[str, Any]]:
    if object_store is None:
        raise ValueError(
            "Wikidata listed-company dlt source requires object_store; "
            "materialize wikidata_company_seed_raw_objects first"
        )
    effective_raw_run_id = raw_run_id or source_run_id
    if not effective_raw_run_id:
        raise ValueError("Wikidata listed-company dlt source requires a raw run id")

    manifest_keys = wikidata_raw_manifest_keys(
        object_store=object_store,
        run_id=effective_raw_run_id,
        fallback_to_latest=raw_run_id is None,
    )
    if not manifest_keys:
        raise ValueError(
            "No Wikidata raw manifest objects found for "
            f"run_id={effective_raw_run_id}; materialize "
            "wikidata_company_seed_raw_objects first"
        )

    for exchange_number, manifest_key in enumerate(manifest_keys, start=1):
        if max_exchanges is not None and exchange_number > max_exchanges:
            break
        yield from iter_wikidata_listed_company_rows_from_manifest(
            object_store=object_store,
            manifest_key=manifest_key,
            max_pages=max_pages,
            fallback_source_run_id=source_run_id,
        )


def wikidata_raw_manifest_keys(
    *,
    object_store: Any,
    run_id: str,
    fallback_to_latest: bool,
) -> list[str]:
    all_manifest_keys = [
        key
        for key in sorted(object_store.list_keys("raw/", bucket=WIKIDATA_RAW_BUCKET))
        if key.endswith("/manifest.json")
    ]
    run_prefix = f"raw/run_id={run_id}/"
    run_manifest_keys = [key for key in all_manifest_keys if key.startswith(run_prefix)]
    if run_manifest_keys or not fallback_to_latest or not all_manifest_keys:
        return run_manifest_keys

    return latest_wikidata_raw_manifest_keys(
        object_store=object_store,
        manifest_keys=all_manifest_keys,
    )


def latest_wikidata_raw_manifest_keys(
    *,
    object_store: Any,
    manifest_keys: list[str],
) -> list[str]:
    manifest_keys_by_run: dict[str, list[str]] = {}
    for manifest_key in manifest_keys:
        run_prefix, separator, _ = manifest_key.partition("retrieved_date=")
        if separator:
            manifest_keys_by_run.setdefault(run_prefix, []).append(manifest_key)

    if not manifest_keys_by_run:
        return []

    return max(
        manifest_keys_by_run.values(),
        key=lambda keys: str(
            read_wikidata_raw_json(
                object_store=object_store,
                object_key=keys[0],
            ).get("started_at")
            or ""
        ),
    )


def iter_wikidata_listed_company_rows_from_manifest(
    *,
    object_store: Any,
    manifest_key: str,
    max_pages: int | None,
    fallback_source_run_id: str,
) -> Iterator[dict[str, Any]]:
    manifest = read_wikidata_raw_json(object_store=object_store, object_key=manifest_key)
    exchange = exchange_row_from_manifest(
        manifest,
        source_run_id=fallback_source_run_id,
    )
    source_run_id = str(manifest.get("run_id") or fallback_source_run_id)
    retrieved_at = str(manifest.get("started_at") or "")
    page_size = manifest_page_size(manifest)
    augmentation_rows_by_company_id = wikidata_company_augmentation_rows_by_company_id(
        object_store=object_store,
        manifest=manifest,
    )

    for object_number, object_key in enumerate(manifest_object_keys(manifest), start=1):
        if max_pages is not None and object_number > max_pages:
            break
        page_number = page_number_from_object_key(object_key) or object_number
        page_offset = (page_number - 1) * page_size
        query = build_listed_company_query(
            exchange_id=exchange["exchange_wikidata_id"],
            limit=page_size,
            offset=page_offset,
        )
        payload = read_wikidata_raw_json(object_store=object_store, object_key=object_key)
        for row_number, binding in enumerate(response_bindings(payload), start=1):
            base_row = listed_company_row_from_binding(
                binding,
                exchange=exchange,
                page_number=page_number,
                page_offset=page_offset,
                page_row_number=row_number,
                query=query,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
            )
            company_augmentation_rows = augmentation_rows_by_company_id.get(
                base_row["company_wikidata_id"],
                [],
            )
            if not company_augmentation_rows:
                yield base_row
                continue

            for augmentation_row_number, augmentation_row in enumerate(
                company_augmentation_rows,
                start=1,
            ):
                row = merged_listed_company_augmentation_row(
                    base_row,
                    augmentation_row=augmentation_row,
                    augmentation_row_number=augmentation_row_number,
                )
                yield row


def read_wikidata_raw_json(
    *,
    object_store: Any,
    object_key: str,
) -> dict[str, Any]:
    payload = json.loads(
        object_store.read_bytes(object_key, bucket=WIKIDATA_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Wikidata raw object {object_key} was not a JSON object")
    return payload


def exchange_row_from_manifest(
    manifest: dict[str, Any],
    *,
    source_run_id: str,
) -> dict[str, Any]:
    exchange_id = str(manifest.get("exchange_id") or "")
    if not exchange_id:
        raise ValueError("Wikidata raw manifest is missing exchange_id")
    return {
        "exchange_wikidata_id": exchange_id,
        "exchange_name": str(manifest.get("exchange_name") or ""),
        "listed_company_count_on_exchange": int(
            manifest.get("listed_company_count_on_exchange") or 0
        ),
        "source_run_id": str(manifest.get("run_id") or source_run_id),
        "retrieved_at": str(manifest.get("started_at") or ""),
        "source_row_number": 0,
    }


def manifest_page_size(manifest: dict[str, Any]) -> int:
    page_size = int(manifest.get("page_size") or DEFAULT_WIKIDATA_PAGE_SIZE)
    if page_size <= 0:
        raise ValueError("Wikidata raw manifest page_size must be greater than zero")
    return page_size


def manifest_object_keys(manifest: dict[str, Any]) -> list[str]:
    objects = manifest.get("objects")
    if not isinstance(objects, list):
        raise ValueError("Wikidata raw manifest is missing objects list")
    return [str(object_key) for object_key in objects]


def manifest_augmentation_object_keys(manifest: dict[str, Any]) -> list[str]:
    objects = manifest.get("augmentation_objects", [])
    if not isinstance(objects, list):
        raise ValueError("Wikidata raw manifest augmentation_objects must be a list")
    return [str(object_key) for object_key in objects]


def wikidata_company_augmentation_rows_by_company_id(
    *,
    object_store: Any,
    manifest: dict[str, Any],
) -> dict[str, list[dict[str, str]]]:
    rows_by_company_id: dict[str, list[dict[str, str]]] = {}
    for object_key in manifest_augmentation_object_keys(manifest):
        payload = read_wikidata_raw_json(object_store=object_store, object_key=object_key)
        for binding in response_bindings(payload):
            augmentation_row = company_augmentation_row_from_binding(binding)
            company_id = augmentation_row["company_wikidata_id"]
            if company_id:
                rows_by_company_id.setdefault(company_id, []).append(augmentation_row)
    return rows_by_company_id


def page_number_from_object_key(object_key: str) -> int | None:
    marker = "/page="
    if marker not in object_key:
        return None
    page_part = object_key.rsplit(marker, 1)[1].removesuffix(".json")
    if not page_part.isdigit():
        return None
    return int(page_part)


def active_listed_exchange_row_from_binding(
    binding: dict[str, Any],
    *,
    source_run_id: str,
    retrieved_at: str,
    source_row_number: int,
) -> dict[str, Any]:
    exchange_url = binding_value(binding, "exchange")
    exchange_id = wikidata_id_from_url(exchange_url)
    return {
        "exchange_wikidata_id": exchange_id,
        "exchange_name": binding_value(binding, "exchangeLabel"),
        "listed_company_count_on_exchange": binding_int(binding, "listedCompanyCount"),
        "source_run_id": source_run_id,
        "retrieved_at": retrieved_at,
        "source_row_number": source_row_number,
    }


def listed_company_row_from_binding(
    binding: dict[str, Any],
    *,
    exchange: dict[str, Any],
    page_number: int,
    page_offset: int,
    page_row_number: int,
    query: str,
    source_run_id: str,
    retrieved_at: str,
) -> dict[str, Any]:
    raw_binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"))
    company_url = binding_value(binding, "company")
    listing_url = binding_value(binding, "listing")
    company_id = wikidata_id_from_url(company_url)
    listing_statement_id = wikidata_id_from_url(listing_url)
    exchange_id = exchange["exchange_wikidata_id"]
    return {
        "source_run_id": source_run_id,
        "retrieved_at": retrieved_at,
        "exchange_wikidata_id": exchange_id,
        "exchange_name": exchange["exchange_name"],
        "listed_company_count_on_exchange": exchange["listed_company_count_on_exchange"],
        "page_number": page_number,
        "page_offset": page_offset,
        "page_row_number": page_row_number,
        "company_wikidata_id": company_id,
        "company_url": company_url,
        "company_label": binding_value(binding, "companyLabel"),
        "company_description": binding_value(binding, "companyDescription"),
        "official_name": binding_value(binding, "officialName"),
        "listing_statement_id": listing_statement_id,
        "listing_url": listing_url,
        "ticker": binding_value(binding, "ticker"),
        "isin": binding_value(binding, "isin"),
        "website_url": binding_value(binding, "website"),
        "cik": binding_value(binding, "cik"),
        "lei": binding_value(binding, "lei"),
        "headquarters_wikidata_id": wikidata_id_from_url(binding_value(binding, "headquarters")),
        "headquarters_label": binding_value(binding, "headquartersLabel"),
        "headquarters_country_wikidata_id": wikidata_id_from_url(
            binding_value(binding, "headquartersCountry")
        ),
        "headquarters_country_label": binding_value(binding, "headquartersCountryLabel"),
        "headquarters_country_iso2": binding_value(binding, "headquartersCountryIso2"),
        "inception_date": binding_value(binding, "inceptionDate"),
        "legal_form_wikidata_id": wikidata_id_from_url(binding_value(binding, "legalForm")),
        "legal_form_label": binding_value(binding, "legalFormLabel"),
        "employee_count": binding_value(binding, "employeeCount"),
        "employee_count_point_in_time": binding_value(binding, "employeeCountPointInTime"),
        "logo_image": binding_value(binding, "logoImage"),
        "logo_image_url": binding_value(binding, "logoImageUrl"),
        "industry_wikidata_id": wikidata_id_from_url(binding_value(binding, "industry")),
        "industry_label": binding_value(binding, "industryLabel"),
        "opencorporates_company_id": binding_value(binding, "openCorporatesId"),
        "eu_vat_number": binding_value(binding, "euVatNumber"),
        "duns_number": binding_value(binding, "dunsNumber"),
        "permid": binding_value(binding, "permId"),
        "bloomberg_company_id": binding_value(binding, "bloombergCompanyId"),
        "linkedin_company_id": binding_value(binding, "linkedinCompanyId"),
        "se_orgnr": binding_value(binding, "seOrgnr"),
        "no_orgnr": binding_value(binding, "noOrgnr"),
        "dk_cvr": binding_value(binding, "dkCvr"),
        "fi_business_id": binding_value(binding, "fiBusinessId"),
        "uk_company_number": binding_value(binding, "ukCompanyNumber"),
        "fr_siren": binding_value(binding, "frSiren"),
        "cz_ico": binding_value(binding, "czIco"),
        "lv_regcode": binding_value(binding, "lvRegcode"),
        "br_cnpj": binding_value(binding, "brCnpj"),
        "parent_organization_statement_id": wikidata_id_from_url(
            binding_value(binding, "parentOrganizationStatement")
        ),
        "parent_organization_wikidata_id": wikidata_id_from_url(
            binding_value(binding, "parentOrganization")
        ),
        "parent_organization_label": binding_value(binding, "parentOrganizationLabel"),
        "parent_organization_start_date": binding_value(
            binding,
            "parentOrganizationStartDate",
        ),
        "parent_organization_end_date": binding_value(binding, "parentOrganizationEndDate"),
        "child_organization_statement_id": wikidata_id_from_url(
            binding_value(binding, "childOrganizationStatement")
        ),
        "child_organization_wikidata_id": wikidata_id_from_url(
            binding_value(binding, "childOrganization")
        ),
        "child_organization_label": binding_value(binding, "childOrganizationLabel"),
        "child_organization_start_date": binding_value(binding, "childOrganizationStartDate"),
        "child_organization_end_date": binding_value(binding, "childOrganizationEndDate"),
        "owned_by_statement_id": wikidata_id_from_url(binding_value(binding, "ownedByStatement")),
        "owned_by_wikidata_id": wikidata_id_from_url(binding_value(binding, "ownedBy")),
        "owned_by_label": binding_value(binding, "ownedByLabel"),
        "owned_by_start_date": binding_value(binding, "ownedByStartDate"),
        "owned_by_end_date": binding_value(binding, "ownedByEndDate"),
        "owner_of_statement_id": wikidata_id_from_url(binding_value(binding, "ownerOfStatement")),
        "owner_of_wikidata_id": wikidata_id_from_url(binding_value(binding, "ownerOf")),
        "owner_of_label": binding_value(binding, "ownerOfLabel"),
        "owner_of_start_date": binding_value(binding, "ownerOfStartDate"),
        "owner_of_end_date": binding_value(binding, "ownerOfEndDate"),
        "query_hash": query_hash(query),
        "source_record_id": (
            f"{exchange_id}:{page_number:06d}:{page_row_number:06d}:"
            f"{company_id}:{listing_statement_id}"
        ),
        "source_payload_hash": sha256(raw_binding_json.encode("utf-8")).hexdigest(),
        "raw_binding_json": raw_binding_json,
    }


def company_augmentation_row_from_binding(binding: dict[str, Any]) -> dict[str, str]:
    raw_binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"))
    return {
        "company_wikidata_id": wikidata_id_from_url(binding_value(binding, "company")),
        "inception_date": binding_value(binding, "inceptionDate"),
        "legal_form_wikidata_id": wikidata_id_from_url(binding_value(binding, "legalForm")),
        "legal_form_label": binding_value(binding, "legalFormLabel"),
        "employee_count": binding_value(binding, "employeeCount"),
        "employee_count_point_in_time": binding_value(binding, "employeeCountPointInTime"),
        "logo_image": binding_value(binding, "logoImage"),
        "logo_image_url": binding_value(binding, "logoImageUrl"),
        "opencorporates_company_id": binding_value(binding, "openCorporatesId"),
        "eu_vat_number": binding_value(binding, "euVatNumber"),
        "duns_number": binding_value(binding, "dunsNumber"),
        "permid": binding_value(binding, "permId"),
        "bloomberg_company_id": binding_value(binding, "bloombergCompanyId"),
        "linkedin_company_id": binding_value(binding, "linkedinCompanyId"),
        "se_orgnr": binding_value(binding, "seOrgnr"),
        "no_orgnr": binding_value(binding, "noOrgnr"),
        "dk_cvr": binding_value(binding, "dkCvr"),
        "fi_business_id": binding_value(binding, "fiBusinessId"),
        "uk_company_number": binding_value(binding, "ukCompanyNumber"),
        "fr_siren": binding_value(binding, "frSiren"),
        "cz_ico": binding_value(binding, "czIco"),
        "lv_regcode": binding_value(binding, "lvRegcode"),
        "br_cnpj": binding_value(binding, "brCnpj"),
        "parent_organization_statement_id": wikidata_id_from_url(
            binding_value(binding, "parentOrganizationStatement")
        ),
        "parent_organization_wikidata_id": wikidata_id_from_url(
            binding_value(binding, "parentOrganization")
        ),
        "parent_organization_label": binding_value(binding, "parentOrganizationLabel"),
        "parent_organization_start_date": binding_value(
            binding,
            "parentOrganizationStartDate",
        ),
        "parent_organization_end_date": binding_value(binding, "parentOrganizationEndDate"),
        "child_organization_statement_id": wikidata_id_from_url(
            binding_value(binding, "childOrganizationStatement")
        ),
        "child_organization_wikidata_id": wikidata_id_from_url(
            binding_value(binding, "childOrganization")
        ),
        "child_organization_label": binding_value(binding, "childOrganizationLabel"),
        "child_organization_start_date": binding_value(binding, "childOrganizationStartDate"),
        "child_organization_end_date": binding_value(binding, "childOrganizationEndDate"),
        "owned_by_statement_id": wikidata_id_from_url(binding_value(binding, "ownedByStatement")),
        "owned_by_wikidata_id": wikidata_id_from_url(binding_value(binding, "ownedBy")),
        "owned_by_label": binding_value(binding, "ownedByLabel"),
        "owned_by_start_date": binding_value(binding, "ownedByStartDate"),
        "owned_by_end_date": binding_value(binding, "ownedByEndDate"),
        "owner_of_statement_id": wikidata_id_from_url(binding_value(binding, "ownerOfStatement")),
        "owner_of_wikidata_id": wikidata_id_from_url(binding_value(binding, "ownerOf")),
        "owner_of_label": binding_value(binding, "ownerOfLabel"),
        "owner_of_start_date": binding_value(binding, "ownerOfStartDate"),
        "owner_of_end_date": binding_value(binding, "ownerOfEndDate"),
        "person_wikidata_id": wikidata_id_from_url(binding_value(binding, "person")),
        "person_url": binding_value(binding, "person"),
        "person_label": binding_value(binding, "personLabel"),
        "person_description": binding_value(binding, "personDescription"),
        "person_image": binding_value(binding, "personImage"),
        "person_image_url": binding_value(binding, "personImageUrl"),
        "person_birth_year": binding_value(binding, "personBirthYear"),
        "role_property": binding_value(binding, "roleProperty"),
        "role_start_date": binding_value(binding, "startDate"),
        "role_end_date": binding_value(binding, "endDate"),
        "augmentation_raw_binding_json": raw_binding_json,
    }


def merged_listed_company_augmentation_row(
    base_row: dict[str, Any],
    *,
    augmentation_row: dict[str, str],
    augmentation_row_number: int,
) -> dict[str, Any]:
    row = dict(base_row)
    for field_name in WIKIDATA_AUGMENTATION_FIELD_NAMES:
        row[field_name] = augmentation_row.get(field_name, "")

    augmentation_raw_binding_json = augmentation_row.get("augmentation_raw_binding_json", "")
    row["raw_binding_json"] = json.dumps(
        {
            "listing": base_row["raw_binding_json"],
            "augmentation": augmentation_raw_binding_json,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    row["source_record_id"] = f"{base_row['source_record_id']}:aug:{augmentation_row_number:06d}"
    row["source_payload_hash"] = sha256(row["raw_binding_json"].encode("utf-8")).hexdigest()
    return row


def response_bindings(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = payload.get("results")
    if not isinstance(results, dict):
        raise ValueError("Wikidata SPARQL response is missing results object")
    bindings = results.get("bindings")
    if not isinstance(bindings, list):
        raise ValueError("Wikidata SPARQL response is missing results.bindings list")
    return bindings


def binding_value(binding: dict[str, Any], name: str) -> str:
    value = binding.get(name)
    if not isinstance(value, dict):
        return ""
    return str(value.get("value", ""))


def binding_int(binding: dict[str, Any], name: str) -> int:
    raw_value = binding_value(binding, name)
    if not raw_value:
        return 0
    return int(raw_value)


def wikidata_id_from_url(url: str) -> str:
    if not url:
        return ""
    return url.rstrip("/").rsplit("/", 1)[-1]


def query_hash(query: str) -> str:
    return sha256(query.encode("utf-8")).hexdigest()


def page_object_key(
    *,
    retrieved_date: str,
    run_id: str,
    exchange_id: str,
    page_number: int,
) -> str:
    return (
        f"raw/run_id={run_id}/retrieved_date={retrieved_date}/"
        f"exchange_id={exchange_id}/page={page_number:06d}.json"
    )


def augmentation_object_key(
    *,
    retrieved_date: str,
    run_id: str,
    exchange_id: str,
    augmentation_kind: str,
    page_number: int,
    batch_number: int,
) -> str:
    return (
        f"raw/run_id={run_id}/retrieved_date={retrieved_date}/"
        f"exchange_id={exchange_id}/augmentation_kind={augmentation_kind}/"
        f"page={page_number:06d}_batch={batch_number:06d}.json"
    )


def manifest_object_key(
    *,
    retrieved_date: str,
    run_id: str,
    exchange_id: str,
) -> str:
    return (
        f"raw/run_id={run_id}/retrieved_date={retrieved_date}/"
        f"exchange_id={exchange_id}/manifest.json"
    )


def active_exchanges_object_key(
    *,
    retrieved_date: str,
    run_id: str,
) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/active_exchanges.json"
