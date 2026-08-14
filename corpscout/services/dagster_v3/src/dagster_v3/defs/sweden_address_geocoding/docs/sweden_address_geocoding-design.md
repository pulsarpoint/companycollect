# Sweden address geocoding design

## Country and source boundary

This is a country-specific Sweden pipeline. Address syntax, authoritative
reference data, licensing, and precision differ by country, so it is not a
universal country-partitioned geocoder.

The first delivery slice is deliberately only the authoritative raw source:
Lantmäteriet's **Belägenhetsadress Nedladdning, vektor**. Address matching,
country-specific DuckDB output, and ClickHouse publishing come after the raw
snapshot has been materialized successfully on the Dagster host.

## Official source contract

The public STAC collection
`https://api.lantmateriet.se/stac-vektor/v1/collections/belagenhetsadresser`
enumerates 290 municipality GeoPackage ZIP archives. Each item provides:

- the four-digit municipality code;
- county code, title, creation time, and source update time;
- EPSG 3006 as the source coordinate system;
- an authenticated `dl1.lantmateriet.se` ZIP URL; and
- the expected archive size.

The catalog is public. The ZIP downloads use HTTP Basic authentication and
require an approved Geotorget order for the product. The product is free but
requires legal review and acceptance of its special terms because the dataset
contains personal data.

## Raw asset and RustFS layout

`sweden_lantmateriet_address_archives_s3` discovers the current catalog and
requires all 290 municipality items before writing anything. Each source
version is stored immutably in RustFS/S3:

```text
source-sweden-lantmateriet-addresses/
  sweden_lantmateriet_addresses/raw/
    municipality_code=<code>/
      source_updated=<timestamp>/
        belagenhetsadresser_kn<code>.zip
        belagenhetsadresser_kn<code>.zip.metadata.json
  sweden_lantmateriet_addresses/manifests/
    retrieved_date=<date>/run_id=<dagster-run-id>/manifest.json
```

The downloader verifies the STAC size and ZIP central directory, computes
SHA-256, skips already archived source versions, and records source URLs,
timestamps, hashes, and object keys in the run manifest. Credentials are read
only from `LANTMATERIET_USERNAME` and `LANTMATERIET_PASSWORD` on the Dagster
host and are never written to metadata or logs.

## Scheduling and approval gate

`sweden_lantmateriet_addresses_weekly` is an unpartitioned weekly full-snapshot
schedule at Monday 07:40 Europe/Stockholm. It is deployed stopped until a
manual materialization proves the Geotorget product order has been approved.
Once validated, enabling the schedule is the only operational change needed.

## Downstream plan

After the raw asset succeeds:

1. build a Sweden-only DuckDB table by reading all municipal GeoPackages;
2. transform EPSG 3006 address points to WGS84 latitude/longitude;
3. normalize and match `corpscout.se_company_addresses_current` against the
   official street, house-number, postcode, and locality fields;
4. publish an auditable address-geocode lookup to ClickHouse; and
5. join that lookup in the company serving model.

GeoNames `SE.zip` remains a useful postal-code fallback for unmatched records,
but it must never override an official Lantmäteriet address-point match or be
presented as street-level precision.
