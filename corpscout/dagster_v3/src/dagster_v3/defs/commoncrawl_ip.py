import dagster as dg


COMMONCRAWL_IP_BUCKET_COUNT = 256
COMMONCRAWL_IP_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:03d}"
        for bucket_index in range(COMMONCRAWL_IP_BUCKET_COUNT)
    ]
)
COMMONCRAWL_IP_ADDRESSES_ASSET = dg.AssetSpec(
    key="commoncrawl_ip_addresses",
    description=(
        "Canonical unique A/AAAA addresses incrementally aggregated from retry-safe "
        "CommonCrawl DNS record observations."
    ),
    group_name="commoncrawl_ip",
    kinds={"clickhouse", "dns"},
)


def commoncrawl_ip_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid CommonCrawl IP partition key: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < COMMONCRAWL_IP_BUCKET_COUNT:
        raise ValueError(f"CommonCrawl IP bucket index out of range: {bucket_index}")
    return bucket_index
