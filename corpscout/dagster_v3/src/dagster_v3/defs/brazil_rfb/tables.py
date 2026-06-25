DLT_DATASET_NAME = "brazil_rfb"
SNAPSHOT_FILES_TABLE = "snapshot_files"

BRAZIL_RFB_DATABASE = "corpscout"

RAW_TABLE_BY_FAMILY = {
    "empresas": "empresas_raw",
    "estabelecimentos": "estabelecimentos_raw",
    "simples": "simples_raw",
    "cnaes": "cnaes_raw",
    "naturezas": "naturezas_raw",
    "municipios": "municipios_raw",
    "paises": "paises_raw",
    "qualificacoes": "qualificacoes_raw",
    "motivos": "motivos_raw",
}

RAW_COLUMNS_BY_FAMILY = {
    "empresas": (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    ),
    "estabelecimentos": (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ),
    "simples": (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ),
    "cnaes": ("code", "description_pt"),
    "naturezas": ("code", "description_pt"),
    "municipios": ("code", "description_pt"),
    "paises": ("code", "description_pt"),
    "qualificacoes": ("code", "description_pt"),
    "motivos": ("code", "description_pt"),
}

RAW_PROVENANCE_COLUMNS = (
    "source_file_family",
    "source_archive_url",
    "source_csv_member",
    "source_run_id",
    "loaded_at",
)

SNAPSHOT_FILE_COLUMNS = {
    "family": {"data_type": "text", "nullable": False},
    "archive_url": {"data_type": "text", "nullable": False},
    "archive_name": {"data_type": "text", "nullable": False},
    "archive_sha256": {"data_type": "text", "nullable": False},
    "csv_member_name": {"data_type": "text", "nullable": False},
    "csv_path": {"data_type": "text", "nullable": False},
    "source_run_id": {"data_type": "text", "nullable": False},
    "retrieved_at": {"data_type": "timestamp", "nullable": False},
}

COMPANIES_TABLE = "companies"
ESTABLISHMENTS_TABLE = "establishments"

BR_COMPANIES_TABLE_CH = "br_companies"
BR_ESTABLISHMENTS_TABLE_CH = "br_establishments"
QUALIFIED_BR_COMPANIES_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_COMPANIES_TABLE_CH}"
QUALIFIED_BR_ESTABLISHMENTS_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_ESTABLISHMENTS_TABLE_CH}"

BR_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj_basico",
    "headquarters_cnpj",
    "legal_name",
    "trade_name",
    "legal_nature_code",
    "legal_nature_description_pt",
    "company_size_code",
    "company_size_en",
    "share_capital_amount_original",
    "status_code",
    "status_en",
    "is_active",
    "status_date",
    "activity_start_date",
    "street_type",
    "street_name",
    "street_number",
    "address_complement",
    "district",
    "postal_code",
    "state",
    "municipality_code",
    "municipality_name",
    "is_simples",
    "is_mei",
    "resolved_at",
)
BR_COMPANIES_EXPORT_COLUMNS = BR_COMPANIES_COLUMNS

BR_ESTABLISHMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj",
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "is_headquarters",
    "trade_name",
    "status_code",
    "status_en",
    "status_date",
    "status_reason_code",
    "activity_start_date",
    "primary_cnae_code",
    "secondary_cnae_codes",
    "street_type",
    "street_name",
    "street_number",
    "address_complement",
    "district",
    "postal_code",
    "state",
    "municipality_code",
    "municipality_name",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
    "resolved_at",
)
BR_ESTABLISHMENTS_EXPORT_COLUMNS = BR_ESTABLISHMENTS_COLUMNS
