"""Table and column contracts for France Annuaire enrichments."""

COUNTRY_ISO2 = "FR"
SOURCE_SLUG = "france_annuaire"

DLT_DATASET_NAME = "france_annuaire"
DUCKDB_FILE_NAME = "france_annuaire_source.duckdb"

DATAGOUV_DATASET_SLUG = (
    "donnees-des-entreprises-utilisees-dans-lannuaire-des-entreprises"
)
DATAGOUV_API_URL = f"https://www.data.gouv.fr/api/1/datasets/{DATAGOUV_DATASET_SLUG}/"
SOURCE_PAGE_URL = f"https://www.data.gouv.fr/datasets/{DATAGOUV_DATASET_SLUG}"

RAW_TABLE = "legal_units_raw"
ENRICHMENTS_TABLE = "company_enrichments"

CLICKHOUSE_DATABASE = "corpscout"
COMPANY_ENRICHMENTS_TABLE = "fr_company_enrichments"
QUALIFIED_COMPANY_ENRICHMENTS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{COMPANY_ENRICHMENTS_TABLE}"
)

RAW_SOURCE_COLUMNS = (
    "siren",
    "siret_siege",
    "date_mise_a_jour_insee",
    "date_mise_a_jour_rne",
    "egapro_renseignee",
    "est_achats_responsables",
    "est_alim_confiance",
    "est_association",
    "est_entrepreneur_individuel",
    "est_entrepreneur_spectacle",
    "est_patrimoine_vivant",
    "statut_entrepreneur_spectacle",
    "est_ess",
    "est_organisme_formation",
    "est_qualiopi",
    "est_administration",
    "est_societe_mission",
    "liste_id_organisme_formation",
    "liste_idcc",
    "est_siae",
    "type_siae",
    "liste_finess_juridique",
    "a_aide_ademe",
    "est_avocat",
)

FR_COMPANY_ENRICHMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "siren",
    "head_office_siret",
    "insee_updated_at",
    "rne_updated_at",
    "has_gender_equality_index",
    "has_responsible_purchasing_commitment",
    "has_alim_confiance_listing",
    "is_association",
    "is_individual_entrepreneur",
    "has_entertainment_entrepreneur_license",
    "is_living_heritage_company",
    "entertainment_entrepreneur_status",
    "is_social_solidarity_economy",
    "is_training_organization",
    "is_qualiopi_certified",
    "is_administration",
    "mission_company_status_code",
    "training_organization_ids",
    "collective_agreement_ids",
    "is_inclusion_structure",
    "inclusion_structure_type",
    "legal_finess_ids",
    "has_ademe_aid",
    "is_lawyer",
    "source_url",
    "resolved_at",
    "raw_entity",
    "source_payload_hash",
)

CLICKHOUSE_EXCLUDED_COLUMNS = frozenset({"raw_entity", "source_payload_hash"})
FR_COMPANY_ENRICHMENTS_EXPORT_COLUMNS = tuple(
    column
    for column in FR_COMPANY_ENRICHMENTS_COLUMNS
    if column not in CLICKHOUSE_EXCLUDED_COLUMNS
)
