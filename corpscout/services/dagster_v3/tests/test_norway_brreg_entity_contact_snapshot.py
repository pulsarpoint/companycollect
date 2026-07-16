from __future__ import annotations

import csv
import gzip
from io import BytesIO, StringIO

import dagster as dg
import polars as pl

from dagster_v3.defs.norway_brreg import resolved_tables as no_tables
from dagster_v3.defs.norway_brreg.assets.entity_contact_snapshot import (
    norway_brreg_entities_snapshot_contact_parquets,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_NO_COMPANY_ADDRESSES,
    ENTITY_NORMALIZED_TABLE_NO_COMPANY_CONTACTS,
    normalized_snapshot_table_object_key,
)


class FakeContactSnapshotStorage:
    def __init__(self, csv_body: bytes) -> None:
        self.csv_body = csv_body
        self.uploads: dict[str, bytes] = {}

    @property
    def object_store(self):
        return self

    def download_file(self, key, target_path, bucket=None) -> None:
        target_path.write_bytes(self.csv_body)

    def upload_snapshot_table_file(self, table_name: str, source_path) -> str:
        key = normalized_snapshot_table_object_key(table_name)
        self.uploads[key] = source_path.read_bytes()
        return key


def test_bulk_csv_snapshot_preserves_all_contacts_and_both_address_types() -> None:
    storage = FakeContactSnapshotStorage(_bulk_csv_gzip())

    results = list(
        norway_brreg_entities_snapshot_contact_parquets(
            context=dg.build_asset_context(),
            norway_brreg_entity_storage=storage,
        )
    )

    assert {result.asset_key.path[-1] for result in results} == {
        "norway_brreg_entities_snapshot_no_company_contacts_parquet",
        "norway_brreg_entities_snapshot_no_company_addresses_parquet",
    }

    contacts = _uploaded_frame(storage, ENTITY_NORMALIZED_TABLE_NO_COMPANY_CONTACTS)
    assert contacts.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_COMPANY_CONTACTS_TABLE]
    )
    assert contacts.select(
        ["registry_id", "contact_type", "contact_value", "source_field"]
    ).sort(["registry_id", "contact_type"]).to_dicts() == [
        {
            "registry_id": "1000",
            "contact_type": "email",
            "contact_value": "post@activeone.no",
            "source_field": "epostadresse",
        },
        {
            "registry_id": "1000",
            "contact_type": "mobile",
            "contact_value": "900 00 001",
            "source_field": "mobil",
        },
        {
            "registry_id": "1000",
            "contact_type": "phone",
            "contact_value": "51 68 57 00",
            "source_field": "telefon",
        },
        {
            "registry_id": "1000",
            "contact_type": "website",
            "contact_value": "activeone.no",
            "source_field": "hjemmeside",
        },
        {
            "registry_id": "2000",
            "contact_type": "email",
            "contact_value": "owner@example.no",
            "source_field": "epostadresse",
        },
    ]

    addresses = _uploaded_frame(storage, ENTITY_NORMALIZED_TABLE_NO_COMPANY_ADDRESSES)
    assert addresses.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_COMPANY_ADDRESSES_TABLE]
    )
    assert addresses.select(
        [
            "registry_id",
            "address_type",
            "address_lines",
            "postal_code",
            "city",
            "municipality",
            "municipality_code",
            "country",
            "country_code",
            "source_field",
        ]
    ).to_dicts() == [
        {
            "registry_id": "1000",
            "address_type": "business",
            "address_lines": "Forusbeen 50",
            "postal_code": "4035",
            "city": "STAVANGER",
            "municipality": "STAVANGER",
            "municipality_code": "1103",
            "country": "Norge",
            "country_code": "NO",
            "source_field": "forretningsadresse",
        },
        {
            "registry_id": "1000",
            "address_type": "postal",
            "address_lines": "Postboks 100",
            "postal_code": "4001",
            "city": "STAVANGER",
            "municipality": "STAVANGER",
            "municipality_code": "1103",
            "country": "Norge",
            "country_code": "NO",
            "source_field": "postadresse",
        },
    ]


def _uploaded_frame(
    storage: FakeContactSnapshotStorage,
    table_name: str,
) -> pl.DataFrame:
    body = storage.uploads[normalized_snapshot_table_object_key(table_name)]
    return pl.read_parquet(BytesIO(body))


def _bulk_csv_gzip() -> bytes:
    fieldnames = [
        "organisasjonsnummer",
        "hjemmeside",
        "epostadresse",
        "telefon",
        "mobil",
        "forretningsadresse.adresse",
        "forretningsadresse.postnummer",
        "forretningsadresse.poststed",
        "forretningsadresse.kommune",
        "forretningsadresse.kommunenummer",
        "forretningsadresse.land",
        "forretningsadresse.landkode",
        "postadresse.adresse",
        "postadresse.postnummer",
        "postadresse.poststed",
        "postadresse.kommune",
        "postadresse.kommunenummer",
        "postadresse.land",
        "postadresse.landkode",
    ]
    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(
        {
            "organisasjonsnummer": "1000",
            "hjemmeside": "activeone.no",
            "epostadresse": "post@activeone.no",
            "telefon": "51 68 57 00",
            "mobil": "900 00 001",
            "forretningsadresse.adresse": "Forusbeen 50",
            "forretningsadresse.postnummer": "4035",
            "forretningsadresse.poststed": "STAVANGER",
            "forretningsadresse.kommune": "STAVANGER",
            "forretningsadresse.kommunenummer": "1103",
            "forretningsadresse.land": "Norge",
            "forretningsadresse.landkode": "NO",
            "postadresse.adresse": "Postboks 100",
            "postadresse.postnummer": "4001",
            "postadresse.poststed": "STAVANGER",
            "postadresse.kommune": "STAVANGER",
            "postadresse.kommunenummer": "1103",
            "postadresse.land": "Norge",
            "postadresse.landkode": "NO",
        }
    )
    writer.writerow(
        {
            "organisasjonsnummer": "2000",
            "epostadresse": "owner@example.no",
        }
    )
    return gzip.compress(buffer.getvalue().encode())
