"""Names, sources and column contracts of the IP registry reference data (migration 000449)."""

DATABASE = "corpscout"
SNAPSHOTS_TABLE = "ip_registry_snapshots"
IANA_TABLE = "ip_registry_iana_blocks"
SPECIAL_TABLE = "ip_registry_special_segments"
# Allocated/assigned records wide enough to cover an entire IANA block (source.HolderBlock).
HOLDER_TABLE = "ip_registry_holder_blocks"
SPECIAL_TRIE = "ip_registry_special_trie"
READY_VIEW = "ip_registry_ready"
IP_REGISTRY_POOL = "ip_registry"
# Snapshots kept per source after a load: the current one and the one before it.
SNAPSHOTS_KEPT = 2

IANA_SOURCES = {
    "iana_ipv4": "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv",
    "iana_ipv6": "https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv",
}
# Each RIR publishes the file daily next to a .md5 (BSD "MD5 (name) = hex" or, for ARIN,
# GNU "hex  name"). Only its available/reserved ipv4/ipv6 records are stored.
RIR_SOURCES = {
    "afrinic": "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
    "apnic": "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
    "arin": "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
    "lacnic": "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    "ripencc": "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
}
# The seven sources the readiness view requires before any classification is trusted.
SOURCES = (*IANA_SOURCES, *RIR_SOURCES)

SNAPSHOT_COLUMNS = (
    "source",
    "snapshot_date",
    "verified_at",
    "checksum",
    "serial",
    "records_ipv4",
    "records_ipv6",
    "segments_ipv4",
    "segments_ipv6",
    "source_url",
)
IANA_COLUMNS = (
    "source",
    "snapshot_date",
    "ip_version",
    "prefix",
    "first_ip",
    "last_ip",
    "designation",
    "rir",
    "status",
    "assigned_on",
    "whois",
    "rdap",
    "note",
    "loaded_at",
)
SPECIAL_COLUMNS = (
    "registry",
    "snapshot_date",
    "ip_version",
    "cc",
    "status",
    "start_address",
    "value",
    "first_ip",
    "last_ip",
    "cidrs",
    "loaded_at",
)
# Same shape as SPECIAL_COLUMNS; status is 'allocated' or 'assigned'.
HOLDER_COLUMNS = SPECIAL_COLUMNS
