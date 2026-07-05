DLT_DATASET_NAME = "brazil_pgfn"
COMPANY_DEBTS_TABLE = "company_debts"

BRAZIL_COMP_PGFN_DATABASE = "corpscout"
BR_PGFN_COMPANY_DEBTS_TABLE_CH = "br_pgfn_company_debts"
QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE = (
    f"{BRAZIL_COMP_PGFN_DATABASE}.{BR_PGFN_COMPANY_DEBTS_TABLE_CH}"
)

BR_PGFN_COMPANY_DEBTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_year",
    "snapshot_quarter",
    "snapshot_month",
    "snapshot_reference_date",
    "source_system",
    "source_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "cnpj",
    "cnpj_basico",
    "person_type",
    "debtor_role",
    "debtor_name",
    "debtor_state",
    "responsible_unit",
    "responsible_entity",
    "inscription_unit",
    "inscription_number",
    "inscription_situation_type",
    "inscription_situation",
    "main_revenue",
    "inscription_date",
    "is_lawsuit",
    "consolidated_amount_brl",
    "resolved_at",
)
BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS = BR_PGFN_COMPANY_DEBTS_COLUMNS
