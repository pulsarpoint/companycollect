"""Fetch a single WARC record by byte range and parse it to (html, headers)."""
from io import BytesIO

from warcio.archiveiterator import ArchiveIterator

CC_BUCKET = "commoncrawl"


def fetch_warc_record(s3, warc_filename: str, offset: int, length: int, *,
                      bucket: str = CC_BUCKET) -> tuple[str, dict]:
    """Byte-range GET one WARC record -> (html, response-headers dict). `s3` has get_object."""
    resp = s3.get_object(Bucket=bucket, Key=warc_filename,
                         Range=f"bytes={offset}-{offset + length - 1}")
    data = resp["Body"].read()
    record = next(ArchiveIterator(BytesIO(data)))
    http = record.http_headers
    headers = {k: v for k, v in (http.headers if http else [])}
    html = record.content_stream().read().decode("utf-8", "replace")
    return html, headers
