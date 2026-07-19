"""Table and column contracts for the Finland Hilma procurement export source.

Column orders are load-bearing: the raw CSV positional names, the typed DuckDB
tables, the ClickHouse migration (000147) and the export subsets must all agree.
See docs/finland_hilma-design.md.
"""

from __future__ import annotations

COUNTRY_ISO2 = "FI"
SOURCE_SLUG = "finland_hilma"

DLT_DATASET_NAME = "finland_hilma"
DUCKDB_FILE_NAME = "finland_hilma_source.duckdb"

S3_BUCKET = "source-finland-hilma"
S3_EXPORTS_PREFIX = "exports/"

FINLAND_HILMA_DATABASE = "corpscout"
FI_HILMA_NOTICES_TABLE = "fi_hilma_notices"
FI_HILMA_NOTICE_WINNERS_TABLE = "fi_hilma_notice_winners"
QUALIFIED_FI_HILMA_NOTICES_TABLE = f"{FINLAND_HILMA_DATABASE}.{FI_HILMA_NOTICES_TABLE}"
QUALIFIED_FI_HILMA_NOTICE_WINNERS_TABLE = (
    f"{FINLAND_HILMA_DATABASE}.{FI_HILMA_NOTICE_WINNERS_TABLE}"
)

RAW_EXPORTS_TABLE = "raw_exports"
NOTICES_TABLE = "notices"
NOTICE_WINNERS_TABLE = "notice_winners"

# Positional names for the 58-column "Hilma search results" CSV export
# (portal export with the full column set; cp1252, ';', quoted multiline).
RAW_COLUMNS = (
    "notice_number",
    "ted_number",
    "lot_id",
    "published_utc",
    "procurement_project_id",
    "procedure_id",
    "notice_type",
    "notice_name_fi",
    "notice_name_en",
    "notice_name_sv",
    "notice_name_other",
    "lot_name_fi",
    "lot_name_en",
    "lot_name_sv",
    "lot_name_other",
    "notice_cpv_code",
    "lot_cpv_code",
    "organisation_name_fi",
    "organisation_name_en",
    "organisation_name_sv",
    "organisation_name_other",
    "organisation_department",
    "organisation_registration_number",
    "organisation_address",
    "nuts_code",
    "place_of_performance_info",
    "deadline_utc",
    "dynamic_purchasing_system",
    "framework_agreement",
    "notice_estimated_value",
    "notice_estimated_currency",
    "lot_estimated_value",
    "lot_estimated_currency",
    "survey_innovation_considered",
    "survey_new_to_buyer",
    "survey_new_to_market",
    "survey_energy_efficiency",
    "survey_low_carbon",
    "survey_circular_economy",
    "survey_biodiversity",
    "survey_sustainable_food",
    "survey_gpp_criteria",
    "survey_fair_working_conditions",
    "survey_partial_work_capacity",
    "survey_jobs_created",
    "survey_code_of_conduct",
    "survey_sme_participation",
    "survey_user_participation",
    "procedure_type",
    "main_nature",
    "lot_main_nature",
    "lot_winner",
    "procurement_value",
    "procurement_currency",
    "lots_value",
    "lots_value_currency",
    "notice_additional_info_fi",
    "lot_additional_info_fi",
)

# The exact portal header titles (whitespace-normalized), used to refuse
# exports made with a different column selection. Index-aligned with
# RAW_COLUMNS.
EXPECTED_HEADER_TITLES = (
    "Notice Hilma-number",
    "TED number",
    "Lot Id",
    "Published (UTC)",
    "Procurement project Id",
    "Procedure Id",
    "Notice type",
    "Notice name (fi)",
    "Notice name (en)",
    "Notice name (sv)",
    "Notice name (other)",
    "Lot name (fi)",
    "Lot name (en)",
    "Lot name (sv)",
    "Lot name (other)",
    "Notice Common Procurement Vocabulary (CPV code)",
    "Lot Common Procurement Vocabulary (CPV code)",
    "Organisation name (fi)",
    "Organisation name (en)",
    "Organisation name (sv)",
    "Organisation name (other)",
    "Department / sub-organisation",
    "Registration number",
    "Organisation address",
    "Place of performance NUTS-code",
    "Additional information on the place of performance",
    "Deadline (UTC)",
    "Dynamic purchasing system",
    "Framework agreement",
    "Notice Estimated value",
    "Notice Currency",
    "Lot Estimated value",
    "Lot Currency",
    "In preparation, need and opportunities for innovation was considered.",
    "The solution sought after, or part of it, is new to us as a buyer.",
    "The solution sought after, or part of it, is new to the market or industry.",
    "Procurement takes into account energy efficiency",
    "This procurement promotes low carbon emissions.",
    "This procurement promotes circular economy.",
    "This procurement promotes biodiversity.",
    "This procurement promotes a sustainable food system.",
    "Are Motiva's, ecolabels' or EU GPP criteria used in the procurement?",
    "This procurement promotes fair working conditions.",
    "This procurement includes a requirement to provide employment for people with partial work capacity.",
    "How many jobs and apprenticeships are expected to be created as a result of the procurement?",
    "Code of Conduct is used in this procurement.",
    "Procurement allows for participation of small and medium-sized enterprises.",
    "The participation of service users or their representatives in the preparation of the procurement has been taken into account in this procurement",
    "Notice Procedure Type",
    "Notice Main Nature",
    "Lot Main Nature",
    "Lot winner",
    "Procurement value",
    "Currency",
    "Lots value",
    "Lots value currency",
    "Notice Additional information(fi)",
    "Lot Additional information(fi)",
)

# Monetary amount name -> (raw value column, raw currency column).
AMOUNT_SOURCE_COLUMNS = {
    "notice_estimated_value": ("notice_estimated_value", "notice_estimated_currency"),
    "lot_estimated_value": ("lot_estimated_value", "lot_estimated_currency"),
    "procurement_value": ("procurement_value", "procurement_currency"),
    "lots_value": ("lots_value", "lots_value_currency"),
}
AMOUNT_NAMES = tuple(AMOUNT_SOURCE_COLUMNS)

_AMOUNT_COLUMNS = tuple(
    col
    for name in AMOUNT_NAMES
    for col in (
        f"{name}_amount_original",
        f"{name}_amount_usd",
        f"{name}_currency",
    )
)

# Column order must match the DuckDB notices table and the 000147 migration.
FI_HILMA_NOTICES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "notice_number",
    "ted_number",
    "lot_id",
    "published_at",
    "notice_type",
    "is_award",
    "procurement_project_id",
    "procedure_id",
    "notice_name_fi",
    "notice_name_en",
    "notice_name_sv",
    "lot_name_fi",
    "lot_name_en",
    "lot_name_sv",
    "notice_cpv_code",
    "lot_cpv_code",
    "buyer_name_fi",
    "buyer_name_en",
    "buyer_name_sv",
    "buyer_department",
    "buyer_business_id",
    "buyer_address",
    "nuts_code",
    "deadline_at",
    "dynamic_purchasing_system",
    "framework_agreement",
    "procedure_type",
    "main_nature",
    "lot_main_nature",
    "winners_raw",
    *_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_key",
    "resolved_at",
)

# Column order must match the DuckDB winners table and the 000147 migration.
FI_HILMA_NOTICE_WINNERS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "notice_number",
    "lot_id",
    "winner_ordinal",
    "winner_name",
    "winner_business_id",
    "buyer_business_id",
    "is_award",
    "published_at",
    "resolved_at",
)
