from pathlib import Path

COUNTRY_CODE = "SE"
SOURCE_SLUG = "sweden_address_osm"
GROUP_NAME = "sweden_address_osm"

SOURCE_URL = "https://download.geofabrik.de/europe/sweden-latest.osm.pbf"
SOURCE_MD5_URL = f"{SOURCE_URL}.md5"
SOURCE_CATALOG_URL = "https://download.geofabrik.de/europe/sweden.html"
SOURCE_LICENSE_URL = "https://www.openstreetmap.org/copyright"

S3_BUCKET = "source-sweden-address-osm"
S3_RAW_PREFIX = "raw"
S3_MANIFEST_PREFIX = "manifests"

DUCKDB_PATH = Path("data") / "sweden_address_osm_source.duckdb"
DUCKDB_SCHEMA = "sweden_address_osm"
ADDRESS_TABLE = "address_points"
QUALIFIED_ADDRESS_TABLE = f"{DUCKDB_SCHEMA}.{ADDRESS_TABLE}"
STREET_SEGMENT_TABLE = "street_segments"
QUALIFIED_STREET_SEGMENT_TABLE = f"{DUCKDB_SCHEMA}.{STREET_SEGMENT_TABLE}"
DUCKDB_POOL = "sweden_address_osm_duckdb"

MINIMUM_SNAPSHOT_BYTES = 500_000_000
