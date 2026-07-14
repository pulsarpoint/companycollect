from dataclasses import dataclass

DLT_DATASET_NAME = "brazil_cgu"

CEIS_COMPANY_SANCTIONS_TABLE = "ceis_company_sanctions"
CNEP_COMPANY_SANCTIONS_TABLE = "cnep_company_sanctions"
CEPIM_BLOCKED_ENTITIES_TABLE = "cepim_blocked_entities"
LENIENCY_AGREEMENTS_TABLE = "leniency_agreements"
LENIENCY_AGREEMENT_EFFECTS_TABLE = "leniency_agreement_effects"

BRAZIL_COMP_CGU_DATABASE = "corpscout"
BR_CGU_CEIS_COMPANY_SANCTIONS_TABLE_CH = "br_cgu_ceis_company_sanctions"
BR_CGU_CNEP_COMPANY_SANCTIONS_TABLE_CH = "br_cgu_cnep_company_sanctions"
BR_CGU_CEPIM_BLOCKED_ENTITIES_TABLE_CH = "br_cgu_cepim_blocked_entities"
BR_CGU_LENIENCY_AGREEMENTS_TABLE_CH = "br_cgu_leniency_agreements"
BR_CGU_LENIENCY_AGREEMENT_EFFECTS_TABLE_CH = "br_cgu_leniency_agreement_effects"

SANCTION_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_date",
    "source_dataset",
    "source_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "registry",
    "sanction_id",
    "cnpj",
    "cnpj_basico",
    "person_type",
    "sanctioned_name",
    "sanctioning_agency_reported_name",
    "receita_legal_name",
    "receita_trade_name",
    "process_number",
    "sanction_category",
    "sanction_start_date",
    "sanction_end_date",
    "publication_date",
    "publication",
    "publication_detail",
    "final_judgment_date",
    "sanction_scope",
    "sanctioning_agency",
    "sanctioning_agency_state",
    "sanctioning_agency_sphere",
    "legal_basis",
    "source_information_date",
    "information_origin",
    "notes",
    "source_payload_hash",
    "resolved_at",
)
CNEP_COMPANY_SANCTIONS_COLUMNS = (
    SANCTION_COLUMNS[:21] + ("fine_amount_brl",) + SANCTION_COLUMNS[21:]
)

CEPIM_BLOCKED_ENTITIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_date",
    "source_dataset",
    "source_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "cnpj",
    "cnpj_basico",
    "entity_name",
    "agreement_number",
    "granting_agency",
    "impediment_reason",
    "source_payload_hash",
    "resolved_at",
)

LENIENCY_AGREEMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_date",
    "source_dataset",
    "source_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "agreement_id",
    "sanctioned_document_raw",
    "cnpj",
    "cnpj_basico",
    "legal_name",
    "trade_name",
    "agreement_start_date",
    "agreement_end_date",
    "agreement_status",
    "information_date",
    "process_number",
    "agreement_terms",
    "sanctioning_agency",
    "source_payload_hash",
    "resolved_at",
)

LENIENCY_AGREEMENT_EFFECTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_date",
    "source_dataset",
    "source_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "agreement_id",
    "agreement_effect",
    "effect_complement",
    "source_payload_hash",
    "resolved_at",
)


@dataclass(frozen=True)
class CguTable:
    duckdb_table: str
    clickhouse_table: str
    columns: tuple[str, ...]


CGU_TABLES = {
    CEIS_COMPANY_SANCTIONS_TABLE: CguTable(
        duckdb_table=CEIS_COMPANY_SANCTIONS_TABLE,
        clickhouse_table=BR_CGU_CEIS_COMPANY_SANCTIONS_TABLE_CH,
        columns=SANCTION_COLUMNS,
    ),
    CNEP_COMPANY_SANCTIONS_TABLE: CguTable(
        duckdb_table=CNEP_COMPANY_SANCTIONS_TABLE,
        clickhouse_table=BR_CGU_CNEP_COMPANY_SANCTIONS_TABLE_CH,
        columns=CNEP_COMPANY_SANCTIONS_COLUMNS,
    ),
    CEPIM_BLOCKED_ENTITIES_TABLE: CguTable(
        duckdb_table=CEPIM_BLOCKED_ENTITIES_TABLE,
        clickhouse_table=BR_CGU_CEPIM_BLOCKED_ENTITIES_TABLE_CH,
        columns=CEPIM_BLOCKED_ENTITIES_COLUMNS,
    ),
    LENIENCY_AGREEMENTS_TABLE: CguTable(
        duckdb_table=LENIENCY_AGREEMENTS_TABLE,
        clickhouse_table=BR_CGU_LENIENCY_AGREEMENTS_TABLE_CH,
        columns=LENIENCY_AGREEMENTS_COLUMNS,
    ),
    LENIENCY_AGREEMENT_EFFECTS_TABLE: CguTable(
        duckdb_table=LENIENCY_AGREEMENT_EFFECTS_TABLE,
        clickhouse_table=BR_CGU_LENIENCY_AGREEMENT_EFFECTS_TABLE_CH,
        columns=LENIENCY_AGREEMENT_EFFECTS_COLUMNS,
    ),
}

CLICKHOUSE_TABLES = tuple(table.clickhouse_table for table in CGU_TABLES.values())
