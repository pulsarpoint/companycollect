COUNTRY_CODE = "SE"
SOURCE_SLUG = "sweden_uhm_procurement"
GROUP_NAME = "sweden_uhm_procurement"

SOURCE_URL = "https://catalog.upphandlingsmyndigheten.se/store/12/resource/239"
SOURCE_CATALOG_URL = "https://www.upphandlingsmyndigheten.se/om-oss/var-oppna-data/"

S3_BUCKET = "source-sweden-uhm-procurement"
S3_RAW_PREFIX = "raw"
S3_MANIFEST_PREFIX = "manifests"

DUCKDB_FILE_NAME = "sweden_uhm_procurement_source.duckdb"
DUCKDB_SCHEMA = "sweden_uhm_procurement"
RAW_TABLE = "raw_awards"
CANDIDATES_TABLE = "award_candidates"

CLICKHOUSE_DATABASE = "corpscout"
AWARDS_TABLE = "se_uhm_procurement_awards"
QUALIFIED_AWARDS_TABLE = f"{CLICKHOUSE_DATABASE}.{AWARDS_TABLE}"

EXPECTED_SOURCE_COLUMNS = (
    "År",
    "Anbudsområdes-ID",
    "Anbudsområden",
    "Annonsdatabas",
    "Direktivstyrd",
    "Dynamiskt inköpssystem",
    "Elektronisk auktion",
    "Förfarande",
    "Innovationsupphandling",
    "Miljömässigt hållbar upphandling",
    "Socialt hållbar upphandling",
    "Reserverad upphandling",
    "Rättslig grund",
    "Samordnad upphandling",
    "Typ av avtal",
    "Överprövad",
    "CPV-Grupp",
    "CPV-Huvudgrupp",
    "CPV-Kategori",
    "CPV-Undergrupp",
    "CPV-Underkategori",
    "Huvudsaklig CPV-kod",
    "Publiceringsdatum",
    "Upphandlingens titel",
    "Upphandlings-ID",
    "Upphandlingsslag",
    "Delsektor för köpare",
    "Juridisk form för köpare",
    "Namn för köpare",
    "Organisationsnummer för köpare",
    "Sektor för köpare",
    "SNI-Avdelning för köpare",
    "Namn för leverantör",
    "Organisationsnummer för leverantör",
    "Företagsstorlek för leverantör",
    "Sektor för leverantör",
    "Juridisk form för leverantör",
    "SNI-Avdelning för leverantör",
    "SNI-Huvudgrupp för leverantör",
    "SNI-Grupp för leverantör",
    "SNI-Undergrupp för leverantör",
    "SNI-Detaljgrupp för leverantör",
    "Kontrakterad",
    "Antal kontrakterade anbud*",
)

RAW_METADATA_COLUMNS = (
    "source_run_id",
    "source_line_number",
    "source_object_key",
    "source_retrieved_at",
)

CANDIDATE_COLUMNS = (
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_line_number",
    "source_procurement_id",
    "source_lot_id",
    "publication_date",
    "title",
    "agreement_type",
    "contracted",
    "buyer_name",
    "buyer_id_normalized",
    "supplier_name",
    "supplier_id_normalized",
    "cpv_code",
    "advertising_database",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "match_eligibility",
)

AWARDS_COLUMNS = (
    "company_id",
    "company_match_status",
    "match_eligibility",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_line_number",
    "source_procurement_id",
    "source_lot_id",
    "publication_date",
    "title",
    "agreement_type",
    "contracted",
    "buyer_name",
    "buyer_id_normalized",
    "supplier_name",
    "supplier_id_normalized",
    "cpv_code",
    "advertising_database",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
)
