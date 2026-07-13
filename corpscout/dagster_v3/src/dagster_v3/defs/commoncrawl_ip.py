import dagster as dg


COMMONCRAWL_IP_BUCKET_COUNT = 256
COMMONCRAWL_IP_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:03d}"
        for bucket_index in range(COMMONCRAWL_IP_BUCKET_COUNT)
    ]
)
COMMONCRAWL_IP_ADDRESSES_KEY = dg.AssetKey("commoncrawl_ip_addresses")


def commoncrawl_ip_partition_key(bucket_index: int) -> str:
    if not isinstance(bucket_index, int) or isinstance(bucket_index, bool):
        raise TypeError(
            f"CommonCrawl IP bucket index must be an integer: {bucket_index!r}"
        )
    if not 0 <= bucket_index < COMMONCRAWL_IP_BUCKET_COUNT:
        raise ValueError(f"CommonCrawl IP bucket index out of range: {bucket_index}")
    return f"bucket_{bucket_index:03d}"


def commoncrawl_ip_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid CommonCrawl IP partition key: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < COMMONCRAWL_IP_BUCKET_COUNT:
        raise ValueError(f"CommonCrawl IP bucket index out of range: {bucket_index}")
    return bucket_index
