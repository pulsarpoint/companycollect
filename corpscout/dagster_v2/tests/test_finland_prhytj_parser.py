import hashlib
import io

import pytest

from dagster_corpscout.sources.finland.prh_ytj.parser import parse_snapshot


def test_parse_snapshot_records_line_number_and_raw_line_hash():
    body = io.BytesIO(
        b'  {"businessId":{"value":"1234567-8"},"names":[]}  \n'
        b"\n"
        b'{"businessId":{"value":"8765432-1"},"names":[]}\n'
    )

    records = list(parse_snapshot(body))

    assert [record.line_number for record in records] == [1, 3]
    assert records[0].payload_hash == hashlib.sha256(
        b'  {"businessId":{"value":"1234567-8"},"names":[]}  '
    ).hexdigest()
    assert records[0].payload["businessId"]["value"] == "1234567-8"


def test_parse_snapshot_reports_malformed_line_number():
    with pytest.raises(ValueError, match="line 2"):
        list(parse_snapshot(io.BytesIO(b'{"businessId":{"value":"ok"}}\n{broken}\n')))
