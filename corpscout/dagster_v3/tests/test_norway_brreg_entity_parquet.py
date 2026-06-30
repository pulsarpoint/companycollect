from decimal import Decimal
from io import BytesIO
import json

import polars as pl

from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes


def test_entity_records_parquet_bytes_serializes_decimal_values_in_raw_payloads() -> None:
    parquet_body = entity_records_parquet_bytes(
        [
            {
                "org_number": "923609016",
                "change_type": "snapshot",
                "source_change_type": "snapshot",
                "updated_at": None,
                "update_id": None,
                "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016",
                "entity": {
                    "organisasjonsnummer": "923609016",
                    "kapital": {"belop": Decimal("12345.67")},
                },
                "raw_update": {"update_id": Decimal("10")},
            }
        ]
    )

    frame = pl.read_parquet(BytesIO(parquet_body))

    assert json.loads(frame["entity_json"][0])["kapital"]["belop"] == 12345.67
    assert json.loads(frame["raw_update_json"][0])["update_id"] == 10
