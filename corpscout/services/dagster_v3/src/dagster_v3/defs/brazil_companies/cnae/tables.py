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

BR_CNAE_CATEGORIES_TABLE = "br_cnae_categories"

# Column order is the contract with migration 000221.
BR_CNAE_CATEGORIES_COLUMNS = (
    "classification_version",
    "code",
    "normalized_code",
    "level",
    "parent_normalized_code",
    "section_code",
    "division_code",
    "description_pt",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

# 1,332 subclasses plus their ancestors in CNAE 2.0. A floor, so a revision does
# not fail the load while a truncated download still does.
MIN_CNAE_CATEGORY_ROWS = 2_000
