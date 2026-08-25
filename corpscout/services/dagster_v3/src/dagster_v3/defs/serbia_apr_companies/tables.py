SOURCE_SLUG = "apr_companies"
SOURCE_NAME = "serbia_apr_companies"
SOURCE_URL = "https://openapi.apr.gov.rs/api/opendata/companies"
SOURCE_LICENSE = "sodl"

GROUP_NAME = "serbia_apr_companies"
S3_BUCKET = "source-serbia-apr-companies"
S3_RAW_PREFIX = "serbia_apr_companies/raw"
S3_MANIFEST_PREFIX = "serbia_apr_companies/manifests"

ASSET_TAGS = {
    "country": "serbia",
    "layer": "s3",
    "personal_data": "false",
    "source": SOURCE_SLUG,
    "source_name": SOURCE_NAME,
}
