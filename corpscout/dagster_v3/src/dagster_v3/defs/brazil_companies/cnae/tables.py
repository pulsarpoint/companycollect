BRAZIL_COMP_CNAE_DATABASE = "corpscout"
BR_CNAE_TO_NACE_TABLE = "br_cnae_to_nace"
QUALIFIED_BR_CNAE_TO_NACE_TABLE = f"{BRAZIL_COMP_CNAE_DATABASE}.{BR_CNAE_TO_NACE_TABLE}"

BR_CNAE_TO_NACE_COLUMNS = (
    "cnae_version",
    "cnae_code",
    "cnae_normalized_code",
    "cnae_description_pt",
    "cnae_description_en",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_description_en",
    "mapping_source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
)
