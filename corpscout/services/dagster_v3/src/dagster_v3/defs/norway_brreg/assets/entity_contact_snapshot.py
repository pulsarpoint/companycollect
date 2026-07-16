from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import dagster as dg
import duckdb

from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    entries_snapshot_csv_raw_object_key,
)
from dagster_v3.defs.norway_brreg.constants import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_NO_COMPANY_ADDRESSES,
    ENTITY_NORMALIZED_TABLE_NO_COMPANY_CONTACTS,
    NorwayBrregEntityParquetStorageResource,
)

CONTACT_SNAPSHOT_ASSETS = (
    (
        "norway_brreg_entities_snapshot_no_company_contacts_parquet",
        ENTITY_NORMALIZED_TABLE_NO_COMPANY_CONTACTS,
        "Preserves BRREG bulk website, email, phone, and mobile contacts in parquet.",
    ),
    (
        "norway_brreg_entities_snapshot_no_company_addresses_parquet",
        ENTITY_NORMALIZED_TABLE_NO_COMPANY_ADDRESSES,
        "Preserves BRREG bulk business and postal addresses in parquet.",
    ),
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            asset_name,
            deps=[dg.AssetKey("norway_brreg_entries_snapshot_csv_raw_s3")],
            group_name=GROUP_NAME,
            kinds={"python", "s3", "csv", "parquet", "duckdb", "brreg"},
            description=description,
        )
        for asset_name, _table_name, description in CONTACT_SNAPSHOT_ASSETS
    ]
)
def norway_brreg_entities_snapshot_contact_parquets(
    context,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
) -> Iterator[dg.MaterializeResult]:
    source_run_id = context.op_execution_context.run_id
    resolved_at = datetime.now(UTC)
    with TemporaryDirectory() as temporary_directory:
        work_dir = Path(temporary_directory)
        csv_path = work_dir / "entities.csv.gz"
        database_path = work_dir / "contacts.duckdb"
        norway_brreg_entity_storage.object_store.download_file(
            entries_snapshot_csv_raw_object_key(),
            csv_path,
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )

        with duckdb.connect(str(database_path)) as connection:
            _build_contact_snapshot_tables(
                connection,
                csv_path=csv_path,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )
            for asset_name, table_name, _description in CONTACT_SNAPSHOT_ASSETS:
                row_count = connection.execute(
                    f"select count(*) from {_quote_identifier(table_name)}"
                ).fetchone()[0]
                parquet_path = work_dir / f"{table_name}.parquet"
                connection.execute(
                    f"copy {_quote_identifier(table_name)} to {_quote_string(str(parquet_path))} "
                    "(format parquet, compression zstd)"
                )
                s3_key = norway_brreg_entity_storage.upload_snapshot_table_file(
                    table_name,
                    parquet_path,
                )
                context.log.info(
                    "Norway Brreg bulk %s snapshot written: rows=%d key=%s",
                    table_name,
                    row_count,
                    s3_key,
                )
                yield dg.MaterializeResult(
                    asset_key=asset_name,
                    metadata={
                        "table_name": table_name,
                        "row_count": row_count,
                        "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
                        "s3_key": s3_key,
                    },
                )


def _build_contact_snapshot_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    csv_path: Path,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    connection.execute(
        """
        create table source as
        select
            organisasjonsnummer as registry_id,
            hjemmeside as website,
            epostadresse as email,
            telefon as phone,
            mobil as mobile,
            "forretningsadresse.adresse" as business_address_lines,
            "forretningsadresse.postnummer" as business_postal_code,
            "forretningsadresse.poststed" as business_city,
            "forretningsadresse.kommune" as business_municipality,
            "forretningsadresse.kommunenummer" as business_municipality_code,
            "forretningsadresse.land" as business_country,
            "forretningsadresse.landkode" as business_country_code,
            "postadresse.adresse" as postal_address_lines,
            "postadresse.postnummer" as postal_postal_code,
            "postadresse.poststed" as postal_city,
            "postadresse.kommune" as postal_municipality,
            "postadresse.kommunenummer" as postal_municipality_code,
            "postadresse.land" as postal_country,
            "postadresse.landkode" as postal_country_code
        from read_csv(?, header=true, all_varchar=true)
        """,
        [str(csv_path)],
    )
    connection.execute(
        "create table publish_context as "
        "select ?::varchar as source_run_id, ?::timestamptz as resolved_at",
        [source_run_id, resolved_at],
    )
    connection.execute(
        """
        create table no_company_contacts as
        select
            'NO'::varchar as country_iso2,
            'norway_brreg'::varchar as source_slug,
            publish_context.source_run_id,
            source.registry_id as source_record_id,
            source.registry_id,
            contact.contact_type,
            contact.source_field as contact_type_raw,
            trim(contact.contact_value) as contact_value,
            contact.source_field,
            true as is_current,
            null::date as valid_to,
            'https://data.brreg.no/enhetsregisteret/api/enheter/' || source.registry_id
                as source_url,
            publish_context.resolved_at
        from source
        cross join publish_context
        cross join lateral (
            values
                ('website', 'hjemmeside', source.website),
                ('email', 'epostadresse', source.email),
                ('phone', 'telefon', source.phone),
                ('mobile', 'mobil', source.mobile)
        ) as contact(contact_type, source_field, contact_value)
        where nullif(trim(source.registry_id), '') is not null
          and nullif(trim(contact.contact_value), '') is not null
        """
    )
    connection.execute(
        """
        create table no_company_addresses as
        select
            'NO'::varchar as country_iso2,
            'norway_brreg'::varchar as source_slug,
            publish_context.source_run_id,
            source.registry_id as source_record_id,
            source.registry_id,
            address.address_type,
            nullif(trim(address.address_lines), '') as address_lines,
            nullif(trim(address.postal_code), '') as postal_code,
            nullif(trim(address.city), '') as city,
            nullif(trim(address.municipality), '') as municipality,
            nullif(trim(address.municipality_code), '') as municipality_code,
            nullif(trim(address.country), '') as country,
            nullif(trim(address.country_code), '') as country_code,
            address.source_field,
            true as is_current,
            'https://data.brreg.no/enhetsregisteret/api/enheter/' || source.registry_id
                as source_url,
            publish_context.resolved_at
        from source
        cross join publish_context
        cross join lateral (
            values
                (
                    'business',
                    'forretningsadresse',
                    source.business_address_lines,
                    source.business_postal_code,
                    source.business_city,
                    source.business_municipality,
                    source.business_municipality_code,
                    source.business_country,
                    source.business_country_code
                ),
                (
                    'postal',
                    'postadresse',
                    source.postal_address_lines,
                    source.postal_postal_code,
                    source.postal_city,
                    source.postal_municipality,
                    source.postal_municipality_code,
                    source.postal_country,
                    source.postal_country_code
                )
        ) as address(
            address_type,
            source_field,
            address_lines,
            postal_code,
            city,
            municipality,
            municipality_code,
            country,
            country_code
        )
        where nullif(trim(source.registry_id), '') is not null
          and coalesce(
                nullif(trim(address.address_lines), ''),
                nullif(trim(address.postal_code), ''),
                nullif(trim(address.city), ''),
                nullif(trim(address.municipality), ''),
                nullif(trim(address.municipality_code), ''),
                nullif(trim(address.country), ''),
                nullif(trim(address.country_code), '')
              ) is not null
        """
    )


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
