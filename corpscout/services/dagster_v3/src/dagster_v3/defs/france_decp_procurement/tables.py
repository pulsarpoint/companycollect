COUNTRY_CODE = "FR"
SOURCE_SLUG = "france_decp_procurement"
GROUP_NAME = "france_decp_procurement"

DATASET_ID = "decp-2022-marches-valides"
SOURCE_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    f"{DATASET_ID}/exports/csv"
)
CATALOG_URL = "https://data.economie.gouv.fr/explore/dataset/decp-2022-marches-valides/"
SOURCE_LICENCE = "Licence Ouverte v2.0 (Etalab)"

S3_BUCKET = "source-france-decp-procurement"
S3_RAW_PREFIX = "raw"
S3_MANIFEST_PREFIX = "manifests"

DUCKDB_FILE_NAME = "france_decp_procurement_source.duckdb"
DUCKDB_SCHEMA = "france_decp_procurement"
DUCKDB_POOL = "france_decp_procurement_duckdb"
RAW_TABLE = "raw_contracts"
CANDIDATES_TABLE = "contract_holder_candidates"

CLICKHOUSE_DATABASE = "corpscout"
CONTRACT_HOLDERS_TABLE = "fr_decp_contract_holders"
QUALIFIED_CONTRACT_HOLDERS_TABLE = f"{CLICKHOUSE_DATABASE}.{CONTRACT_HOLDERS_TABLE}"

EXPECTED_SOURCE_COLUMNS = (
    "id",
    "nature",
    "objet",
    "codecpv",
    "procedure",
    "titulaire_id_1",
    "titulaire_typeidentifiant_1",
    "titulaire_id_2",
    "titulaire_typeidentifiant_2",
    "titulaire_id_3",
    "titulaire_typeidentifiant_3",
    "acheteur_id",
    "dureemois",
    "datenotification",
    "datepublicationdonnees",
    "montant",
    "formeprix",
    "attributionavance",
    "tauxavance",
    "offresrecues",
    "ccag",
    "soustraitancedeclaree",
    "typegroupementoperateurs",
    "idaccordcadre",
    "lieuexecution_code",
    "lieuexecution_typecode",
    "considerationssociales",
    "considerationsenvironnementales",
    "modalitesexecution",
    "techniques",
    "typesprix",
    "origineue",
    "originefrance",
    "marcheinnovant",
    "idmodification",
    "montantmodification",
    "dureemoismodification",
    "idtitulairemodification",
    "typeidentifianttitulairemodification",
    "datenotificationmodificationmodification",
    "datepublicationdonneesmodificationmodification",
    "idactesoustraitance",
    "dureemoisactesoustraitance",
    "datenotificationactesoustraitance",
    "datepublicationdonneesactesoustraitance",
    "montantactesoustraitance",
    "variationprixactesoustraitance",
    "idsoustraitant",
    "typeidentifiantsoustraitant",
    "idmodificationactesoustraitance",
    "dureemoismodificationactesoustraitance",
    "datenotificationmodificationsoustraitancemodificationactesoustraitance",
    "datepublicationdonneesmodificationactesoustraitance",
    "montantmodificationactesoustraitance",
    "source",
)

CANDIDATE_COLUMNS = (
    "source_slug",
    "source_run_id",
    "source_record_id",
    "contract_id",
    "holder_ordinal",
    "holder_id_raw",
    "holder_id_type",
    "holder_siren",
    "buyer_id_raw",
    "buyer_siren",
    "notification_date",
    "publication_date",
    "title",
    "nature",
    "procedure",
    "cpv_code",
    "duration_months",
    "contract_amount_eur",
    "contract_amount_usd",
    "contract_amount_attributable",
    "price_form",
    "offers_received",
    "framework_id",
    "modification_id",
    "modification_amount_eur",
    "modification_notification_date",
    "subcontract_id",
    "subcontract_amount_eur",
    "subcontractor_id_raw",
    "source_system",
    "source_url",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "match_eligibility",
)

CONTRACT_HOLDER_COLUMNS = (
    "company_id",
    "company_match_status",
    *CANDIDATE_COLUMNS,
)
