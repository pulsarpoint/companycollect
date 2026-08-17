# Sweden OpenStreetMap address design

## 1. Source overview

- **Country / source**: Sweden — OpenStreetMap extract published by Geofabrik.
- **Module**: `defs/sweden_address_osm/` · DuckDB file
  `data/sweden_address_osm_source.duckdb` · pool
  `sweden_address_osm_duckdb`.
- **Dataset**: `https://download.geofabrik.de/europe/sweden-latest.osm.pbf`,
  approximately 800 MB, refreshed daily, no authentication.
- **License**: OpenStreetMap data under ODbL 1.0. Products using or exposing
  the data must retain the required OpenStreetMap attribution and comply with
  the database-license obligations.
- **Record identity**: OSM object kind plus numeric ID, for example
  `node/123` or `way/456`.

## 2. Ingest mode and storage

The source is a non-partitioned bulk-file full refresh. Geofabrik publishes a
country PBF and a companion MD5 checksum, so paging an API or importing the
whole planet would add work without improving the Sweden slice.

`sweden_osm_pbf_s3` verifies the published checksum and content length before
storing one immutable object per MD5 in RustFS:

```text
source-sweden-address-osm/
  raw/md5=<source-md5>/sweden-latest.osm.pbf
  manifests/retrieved_at=<timestamp>/run_id=<run-id>.json
```

Repeated runs fetch the small checksum file, reuse an already archived PBF,
and still write a new run manifest. The manifest retains the stable and
resolved source URLs, MD5, SHA-256, content length, HTTP timestamps, license,
Dagster run ID, and retrieval time.

## 3. Address parsing

DuckDB's spatial extension `ST_ReadOSM` reads the compressed PBF directly with
multithreaded protobuf parsing. The build scans the PBF twice without retaining
the complete OSM graph:

1. select objects carrying `addr:housenumber` plus named `highway` ways;
2. select only the node coordinates referenced by those retained ways; and
3. emit direct address-node coordinates, a point-on-surface for closed address
   ways, or the midpoint along each named road segment.

The durable `sweden_address_osm.address_points` table contains WGS84 longitude
and latitude, normalized street/house-number/postcode matching fields, the OSM
record URL and raw tags, and complete snapshot provenance.

The durable `sweden_address_osm.street_segments` table contains one midpoint
per named OSM highway way. It is not building evidence. The matcher uses it
only after exact and address-point street matching fail, associates segments
with nearby OSM postcode centroids, and publishes the result as a lower-
confidence `matched_street` marker. Road groups wider than one kilometre remain
unmatched rather than selecting an arbitrary location.

Address-tagged relations are counted but omitted in this first implementation.
Resolving their nested multipolygon membership requires an additional relation
and way graph pass. The omission is visible in materialization metadata rather
than silently represented as complete coverage. Incomplete address ways at an
extract boundary are counted and omitted for the same reason.

## 4. Authority and matching strategy

OSM is the worldwide baseline, not Sweden's authoritative address register.
When Lantmäteriet access is approved, an exact Lantmäteriet address-point match
must outrank an OSM match. The current OSM resolver implements steps 2 and 3;
the full company-address resolver will apply:

1. Lantmäteriet exact address point;
2. OSM exact normalized postcode, street and house number;
3. OSM exact normalized city, street and house number when the OSM record has no
   postcode, returning every candidate when the key is ambiguous;
4. flagged same-postcode street location from OSM address points;
5. flagged nearby-postcode street location from named OSM road geometry; and
6. controlled locality fallback for non-physical postal addresses.

Every company match retains source, snapshot date, precision, method and
confidence. Approximate street markers never retain an OSM building record ID
and are explicitly presented as non-building locations in the application.

## 5. Scheduling and verification

The first live run (`7990c5d9-5d66-4297-91a2-1378469b9477`) completed on the
Dagster host on 2026-08-12. It produced 988,459 address rows (363,038 nodes and
625,421 ways), with no missing coordinates or incomplete ways; 1,139
address-tagged relations were explicitly counted and omitted. The resulting
DuckDB file was 110,374,912 bytes. `sweden_address_osm_weekly` is registered for
Tuesdays at 04:05 Europe/Stockholm and is default-stopped so enabling source
traffic remains an explicit operator decision.

Verification is covered by `tests/test_sweden_address_osm.py`: checksum and S3
reuse and transient-upload retry behavior, address-node/way geometry
construction, explicit relation omission counts, provenance-normalization
fields, and Dagster group/job/schedule wiring.
