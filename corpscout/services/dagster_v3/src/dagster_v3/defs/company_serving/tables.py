from dataclasses import dataclass

CLICKHOUSE_DATABASE = "corpscout"
COUNTRY_CODE = "SE"
PUBLISH_POOL = "company_serving_clickhouse"


@dataclass(frozen=True)
class CurrentTable:
    name: str
    build_model: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    partitioned: bool = True
    required: bool = False

    @property
    def qualified_name(self) -> str:
        return f"{CLICKHOUSE_DATABASE}.{self.name}"

    @property
    def qualified_build_model(self) -> str:
        return f"{CLICKHOUSE_DATABASE}.{self.build_model}"


SHARED_KEY = ("country_code", "company_id")

EXTERNAL_IDENTIFIERS = CurrentTable(
    "company_external_identifier_current",
    "company_external_identifier_current_build",
    (
        "country_code",
        "company_id",
        "identifier_scheme",
        "identifier_value",
        "is_primary",
        "match_method",
        "match_confidence",
        "first_seen_date",
        "last_seen_date",
        "resolved_at",
    ),
    (*SHARED_KEY, "identifier_scheme", "is_primary", "identifier_value"),
    required=True,
)
GLEIF = CurrentTable(
    "company_gleif_current",
    "company_gleif_current_build",
    (
        "country_code",
        "company_id",
        "lei",
        "is_primary",
        "legal_name",
        "entity_status",
        "registration_status",
        "category",
        "legal_form_id",
        "jurisdiction",
        "legal_address_country",
        "headquarters_country",
        "headquarters_abroad",
        "ownership_exception_reasons",
        "initial_registration_date",
        "last_update_date",
        "next_renewal_date",
        "resolved_at",
    ),
    (*SHARED_KEY, "is_primary", "lei"),
)
GLEIF_RELATIONSHIPS = CurrentTable(
    "company_gleif_relationship_current",
    "company_gleif_relationship_current_build",
    (
        "country_code",
        "company_id",
        "relationship_id",
        "direction",
        "relationship_type",
        "other_lei",
        "other_country_code",
        "other_company_id",
        "other_name",
        "relationship_status",
        "valid_from",
        "valid_to",
        "resolved_at",
    ),
    (*SHARED_KEY, "direction", "relationship_type", "relationship_id"),
)
WIKIDATA = CurrentTable(
    "company_wikidata_current",
    "company_wikidata_current_build",
    (
        "country_code",
        "company_id",
        "wikidata_id",
        "is_primary",
        "wikidata_url",
        "description",
        "official_name",
        "inception_date",
        "employee_count",
        "employee_count_as_of",
        "industry_label",
        "legal_form_label",
        "headquarters",
        "headquarters_country",
        "logo_url",
        "has_current_listing",
        "listings",
        "websites",
        "linkedin_id",
        "resolved_at",
    ),
    (*SHARED_KEY, "is_primary", "wikidata_id"),
)
MANAGEMENT = CurrentTable(
    "company_management_current",
    "company_management_current_build",
    (
        "country_code",
        "company_id",
        "management_id",
        "person_id",
        "external_person_scheme",
        "external_person_value",
        "display_name",
        "first_name",
        "last_name",
        "person_description",
        "birth_year",
        "image_url",
        "external_url",
        "role_kind",
        "role_label",
        "signatory_kind",
        "start_date",
        "end_date",
        "latest_fiscal_year",
        "is_current",
        "confidence",
        "source_systems",
        "resolved_at",
    ),
    (*SHARED_KEY, "is_current", "role_kind", "management_id"),
)
DESCRIPTIONS = CurrentTable(
    "company_description_current",
    "company_description_current_build",
    (
        "country_code",
        "company_id",
        "description_id",
        "description_kind",
        "text_original",
        "language_original",
        "text_en",
        "source_date",
        "extraction_method",
        "confidence",
        "extracted_at",
        "resolved_at",
    ),
    (*SHARED_KEY, "description_kind", "description_id"),
)
CONTACTS = CurrentTable(
    "company_contact_current",
    "company_contact_current_build",
    (
        "country_code",
        "company_id",
        "contact_id",
        "contact_type",
        "contact_value",
        "registrable_domain",
        "fiscal_year",
        "confidence",
        "resolved_at",
    ),
    (*SHARED_KEY, "contact_type", "contact_id"),
)
DOMAINS = CurrentTable(
    "company_domains",
    "company_domains_build",
    (
        "country_code",
        "company_id",
        "root_domain",
        "website_url",
        "website_host",
        "source_names",
        "source_confidences",
        "source_record_ids",
        "source_urls",
        "confidence_bases",
        "suggested_confidence",
        "suggested_primary",
        "evidence_fingerprint",
        "review_status",
        "review_note",
        "reviewed_by",
        "reviewed_at",
        "reviewed_evidence_fingerprint",
        "is_active",
        "first_seen_at",
        "last_seen_at",
        "resolved_at",
    ),
    (*SHARED_KEY, "root_domain"),
)
CONTRACTS = CurrentTable(
    "company_contract_current",
    "company_contract_current_build",
    (
        "country_code",
        "company_id",
        "contract_ref",
        "source",
        "notice_ref",
        "contract_date",
        "buyer_name",
        "title",
        "agreement_type",
        "cpv_code",
        "supplier_count",
        "amount_original",
        "amount_usd",
        "currency",
        "notice_amount_original",
        "notice_amount_usd",
        "notice_currency",
        "source_url",
        "resolved_at",
    ),
    (*SHARED_KEY, "contract_date", "contract_ref"),
)
CONTRACT_SUMMARY = CurrentTable(
    "company_contract_summary_current",
    "company_contract_summary_current_build",
    (
        "country_code",
        "company_id",
        "contract_count",
        "last_contract_date",
        "total_attributable_value_usd",
        "valued_contract_count",
        "source_systems",
        "resolved_at",
    ),
    SHARED_KEY,
)
INDUSTRIES = CurrentTable(
    "se_company_industry_display_current",
    "se_company_industry_display_current_build",
    (
        "company_id",
        "classification_system",
        "classification_code",
        "classification_level",
        "label_sv",
        "label_en",
        "is_primary",
        "source",
        "source_record_uid",
        "resolved_at",
    ),
    ("company_id", "is_primary", "classification_system", "classification_code"),
    partitioned=False,
)
ADDRESSES = CurrentTable(
    "se_company_address_display_current",
    "se_company_address_display_current_build",
    (
        "company_id",
        "address_key",
        "address_type",
        "source",
        "raw_address",
        "display_address",
        "normalized_address",
        "street_address",
        "care_of",
        "postal_code",
        "post_town",
        "resolved_country_code",
        "is_foreign",
        "latitude",
        "longitude",
        "geocode_status",
        "geocode_provider",
        "geocode_precision",
        "geocoded_at",
        "source_record_uid",
        "resolved_at",
    ),
    ("company_id", "address_type", "source", "address_key"),
    partitioned=False,
)
SOURCE_LINKS = CurrentTable(
    "company_section_item_source_links",
    "company_section_item_source_links_build",
    (
        "country_code",
        "company_id",
        "section",
        "item_key",
        "source_record_uid",
        "relationship_kind",
        "match_method",
        "match_confidence",
        "source_run_id",
        "linked_at",
    ),
    (*SHARED_KEY, "section", "item_key", "source_record_uid"),
)
PRESENCE = CurrentTable(
    "company_section_presence_current",
    "company_section_presence_current_build",
    (
        "country_code",
        "company_id",
        "section",
        "item_count",
        "latest_observed_at",
        "resolved_at",
    ),
    (*SHARED_KEY, "section"),
    required=True,
)

CURRENT_TABLES = (
    EXTERNAL_IDENTIFIERS,
    GLEIF,
    GLEIF_RELATIONSHIPS,
    WIKIDATA,
    MANAGEMENT,
    DESCRIPTIONS,
    CONTACTS,
    CONTRACTS,
    CONTRACT_SUMMARY,
    INDUSTRIES,
    ADDRESSES,
    SOURCE_LINKS,
    DOMAINS,
    PRESENCE,
)
HISTORY_TABLES = {
    EXTERNAL_IDENTIFIERS.name: "company_external_identifier_observations",
    GLEIF.name: "company_gleif_observations",
    GLEIF_RELATIONSHIPS.name: "company_gleif_relationship_observations",
    WIKIDATA.name: "company_wikidata_observations",
    MANAGEMENT.name: "company_management_observations",
}
VALID_SECTIONS = (
    "gleif",
    "wikidata",
    "management",
    "descriptions",
    "domains",
    "contracts",
    "financials",
    "industries",
    "addresses",
    "sources",
    "technology",
)
SYNTHETIC_SOURCE_MODELS = {
    "company_source_records": "company_serving_source_records_build",
    "company_source_record_origins": "company_serving_source_origins_build",
    "company_source_record_links": "company_serving_source_links_build",
}
