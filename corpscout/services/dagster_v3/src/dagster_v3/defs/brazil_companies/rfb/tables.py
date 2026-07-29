from dagster_v3.contact_extraction import (
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
)
from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

DLT_DATASET_NAME = "brazil_rfb"
SNAPSHOT_FILES_TABLE = "snapshot_files"

BRAZIL_COMP_RFB_DATABASE = "corpscout"

# Wikidata P6204 = Brazilian CNPJ (BR). Aggregated by defs/wikidata/registry_seed.py;
# see WikidataRegistrySeedSpec for why this lives here instead of a central list in
# defs/wikidata/. The asset key is hardcoded (not imported from rfb/assets.py) to avoid
# a circular import — it must match CLICKHOUSE_COMPANIES_ASSET_KEY in rfb/assets.py.
WIKIDATA_REGISTRY_SEED_SPEC = WikidataRegistrySeedSpec(
    property_id="P6204",
    country_iso2="BR",
    spine_asset_key="brazil_comp_rfb_clickhouse_companies",
)

RAW_TABLE_BY_FAMILY = {
    "empresas": "empresas_raw",
    "estabelecimentos": "estabelecimentos_raw",
    "socios": "socios_raw",
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
    "socios": (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
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

COMPANY_RELATIONS_TABLE = "company_relations"

BR_COMPANY_RELATIONS_TABLE_CH = "br_company_relations"
QUALIFIED_BR_COMPANY_RELATIONS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_RELATIONS_TABLE_CH}"
)

BR_COMPANY_RELATIONS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
    "first_seen_snapshot",
    "last_seen_snapshot",
    "start_at",
    "end_at",
    "is_current",
    "observations",
    "resolved_at",
)
BR_COMPANY_RELATIONS_EXPORT_COLUMNS = BR_COMPANY_RELATIONS_COLUMNS

# What a single monthly snapshot can actually supply -- BR_COMPANY_RELATIONS_COLUMNS
# minus the six columns only the history merge can compute (first_seen_snapshot,
# last_seen_snapshot, start_at, end_at, is_current, observations). relations.py's
# DuckDB build produces (at least) these; the merge step reads them to fold a
# snapshot into the SCD2 history rather than replacing it.
BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
    # resolved_at is produced by the DuckDB build (`now()` in relations.py) and
    # must be shipped. A column left out of this tuple is never inserted, so
    # ClickHouse fills it with the type default -- for DateTime64 that is the
    # epoch, silently, on every row.
    "resolved_at",
)

BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH = "br_company_relations_snapshots"
QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH}"
)
BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS = (
    "snapshot_year_month",
    "merged_at",
    "source_run_id",
    "edges_in_snapshot",
    "spells_opened",
    "spells_closed",
    "spells_total",
)

ESTABLISHMENTS_TABLE = "establishments"
COMPANY_CONTACT_INFO_TABLE = "company_contact_info"
WEBSITES_TABLE = "websites"

# Canonical (spec-standard) stage tables — company/domain-grain, deduped, column
# names matching dagster_v3.contact_extraction.COMPANY_CONTACTS_COLUMNS /
# COMPANY_DOMAINS_COLUMNS. Built alongside the legacy company_contact_info/websites
# stages above (which stay in place for the legacy ClickHouse export until Task 3).
COMPANY_CONTACTS_STAGE_TABLE = "company_contacts"
COMPANY_DOMAINS_STAGE_TABLE = "company_domains"

# Canonical ClickHouse export contract for the pair above. Column order/types are
# owned by the shared canonical standard (COMPANY_CONTACTS_COLUMNS/
# COMPANY_DOMAINS_COLUMNS), conformance-tested against the migration by
# tests/canonical_contact_tables.py — these are direct aliases (not copies), so an
# identity test can pin that the DuckDB stage, the export, and the migration DDL
# can never diverge.
BR_COMPANY_CONTACTS_TABLE_CH = "br_company_contacts"
BR_COMPANY_DOMAINS_TABLE_CH = "br_company_domains"
QUALIFIED_BR_COMPANY_CONTACTS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_CONTACTS_TABLE_CH}"
)
QUALIFIED_BR_COMPANY_DOMAINS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_DOMAINS_TABLE_CH}"
)
BR_COMPANY_CONTACTS_EXPORT_COLUMNS = COMPANY_CONTACTS_COLUMNS
BR_COMPANY_DOMAINS_EXPORT_COLUMNS = COMPANY_DOMAINS_COLUMNS

BR_COMPANIES_TABLE_CH = "br_companies"
BR_ESTABLISHMENTS_TABLE_CH = "br_establishments"
BR_COMPANY_CONTACT_INFO_TABLE_CH = "br_company_contact_info"
BR_WEBSITES_TABLE_CH = "br_websites"
QUALIFIED_BR_COMPANIES_TABLE = f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANIES_TABLE_CH}"
QUALIFIED_BR_ESTABLISHMENTS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_ESTABLISHMENTS_TABLE_CH}"
)
QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_CONTACT_INFO_TABLE_CH}"
)
QUALIFIED_BR_WEBSITES_TABLE = f"{BRAZIL_COMP_RFB_DATABASE}.{BR_WEBSITES_TABLE_CH}"

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

# Historical ClickHouse export contract for the now-dropped br_company_contact_info
# table (migrations 000054/000055 created it; 000092 dropped it in favor of the
# canonical br_company_contacts/br_company_domains pair above — no export function
# targets this table anymore). Kept only so the immutable historical migration DDL
# conformance test (test_clickhouse_migrations.py) and the internal
# company_contact_info DuckDB stage column-drift test (test_brazil_comp_rfb_transforms.py)
# keep working — the internal stage now carries one additional trailing
# `source_field` column (feeds the canonical company_contacts stage below) that
# is NOT part of this contract — the historical export selected this explicit
# column list rather than `select *`, so the extra stage column was invisible to it.
BR_COMPANY_CONTACT_INFO_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj",
    "cnpj_basico",
    "contact_type",
    "contact_type_en",
    "contact_area_code",
    "contact_value",
    "is_current",
    "root_domain",
    "domain_source",
    "resolved_at",
)
BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS = BR_COMPANY_CONTACT_INFO_COLUMNS

BR_WEBSITES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj_basico",
    "root_domain",
    "domain_source",
    "website_url",
    "website_normalized_url",
    "website_host",
    "is_current",
    "is_primary",
    "resolved_at",
)
BR_WEBSITES_EXPORT_COLUMNS = BR_WEBSITES_COLUMNS
