from pathlib import Path

DLT_DATASET_NAME = "sweden_company"
RAW_FILES_TABLE = "raw_files"
BOLAGSVERKET_RAW_TABLE = "bolagsverket_raw"
SCB_RAW_TABLE = "scb_raw"
COMPANIES_TABLE = "companies"
COMPANY_ADDRESSES_TABLE = "company_addresses"
COMPANY_INDUSTRY_CODES_TABLE = "company_industry_codes"

SWEDEN_COMPANY_DUCKDB_PATH = Path("data/sweden_company_source.duckdb")

BOLAGSVERKET_SOURCE_COLUMNS = (
    "organisationsidentitet",
    "namnskyddslopnummer",
    "registreringsland",
    "organisationsnamn",
    "organisationsform",
    "avregistreringsdatum",
    "avregistreringsorsak",
    "pagandeAvvecklingsEllerOmstruktureringsforfarande",
    "registreringsdatum",
    "verksamhetsbeskrivning",
    "postadress",
)

SCB_SOURCE_COLUMNS = (
    "ForAndrTyp",
    "COAdress",
    "Foretagsnamn",
    "FtgStat",
    "Gatuadress",
    "JEStat",
    "JurForm",
    "Namn",
    "Ng1",
    "Ng2",
    "Ng3",
    "Ng4",
    "Ng5",
    "PeOrgNr",
    "PostNr",
    "PostOrt",
    "RegDatKtid",
    "Reklamsparrtyp",
    "mCOAdress",
    "mForetagsnamn",
    "mFtgStat",
    "mGatuadress",
    "mJEStat",
    "mJurForm",
    "mNamn",
    "mNg1",
    "mNg2",
    "mNg3",
    "mNg4",
    "mNg5",
    "mPostNr",
    "mPostOrt",
    "mRegDatKtid",
    "mReklamsparrtyp",
)

RAW_PROVENANCE_COLUMNS = (
    "source_run_id",
    "source_line_number",
    "source_record_id",
    "source_payload_hash",
    "source_s3_key",
    "raw_record",
)
