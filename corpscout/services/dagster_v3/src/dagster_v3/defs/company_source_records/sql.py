from dagster_v3.defs.company_source_records import tables
from dagster_v3.defs.company_source_records.identity import (
    clickhouse_file_source_record_uid_sql,
    clickhouse_observation_uid_sql,
    clickhouse_structured_source_record_uid_sql,
)


BOLAGSVERKET_SOURCE_URL = (
    "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/"
    "bolagsverket_bulkfil.zip"
)
SCB_SOURCE_URL = "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip"


def sweden_registry_source_record_sql() -> tuple[str, ...]:
    bolagsverket_uid = clickhouse_structured_source_record_uid_sql(
        source_slug="sweden_bolagsverket",
        record_kind="registry_company",
        source_record_key_expression="bolagsverket_source_record_id",
        payload_sha256_expression="bolagsverket_source_payload_hash",
    )
    scb_uid = clickhouse_structured_source_record_uid_sql(
        source_slug="sweden_scb",
        record_kind="registry_company",
        source_record_key_expression="scb_source_record_id",
        payload_sha256_expression="scb_source_payload_hash",
    )
    source_rows = f"""
SELECT
    {bolagsverket_uid} AS source_record_uid,
    'sweden_bolagsverket' AS source_slug,
    bolagsverket_source_record_id AS source_record_key,
    bolagsverket_source_payload_hash AS payload_sha256,
    company_id,
    source_run_id,
    updated_from_raw_at AS observed_at
FROM corpscout.se_companies FINAL
WHERE ifNull(bolagsverket_source_record_id, '') != ''
  AND ifNull(bolagsverket_source_payload_hash, '') != ''
UNION ALL
SELECT
    {scb_uid} AS source_record_uid,
    'sweden_scb' AS source_slug,
    scb_source_record_id AS source_record_key,
    scb_source_payload_hash AS payload_sha256,
    company_id,
    source_run_id,
    updated_from_raw_at AS observed_at
FROM corpscout.se_companies FINAL
WHERE ifNull(scb_source_record_id, '') != ''
  AND ifNull(scb_source_payload_hash, '') != ''
""".strip()
    description_uid = clickhouse_observation_uid_sql(
        source_record_uid_expression="source_record_uid",
        observation_kind="company_description",
        natural_key_expression="'activity_description'",
    )
    return (
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
({", ".join(tables.SOURCE_RECORD_COLUMNS)})
WITH source_rows AS ({source_rows}),
existing AS (
    SELECT source_record_uid, min(first_seen_at) AS first_seen_at
    FROM {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
    GROUP BY source_record_uid
)
SELECT
    source_rows.source_record_uid,
    'structured',
    'registry_company',
    source_rows.payload_sha256,
    toUInt16(1),
    coalesce(existing.first_seen_at, source_rows.observed_at),
    source_rows.observed_at
FROM source_rows
LEFT JOIN existing USING (source_record_uid)""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE}
({", ".join(tables.SOURCE_RECORD_ORIGIN_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT
    source_record_uid,
    source_slug,
    source_record_key,
    if(source_slug = 'sweden_bolagsverket', '{BOLAGSVERKET_SOURCE_URL}', '{SCB_SOURCE_URL}'),
    '',
    payload_sha256,
    observed_at,
    source_run_id
FROM source_rows""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE}
({", ".join(tables.SOURCE_RECORD_LINK_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT
    source_record_uid,
    'SE',
    company_id,
    'registry_subject',
    'exact_registry_identifier',
    toFloat32(1),
    'se_orgnr',
    company_id,
    source_run_id,
    observed_at
FROM source_rows""",
        f"""INSERT INTO {tables.QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE}
({", ".join(tables.DESCRIPTION_OBSERVATION_COLUMNS)})
SELECT
    {description_uid},
    source_record_uid,
    'SE',
    company_id,
    'registered_activity',
    activity_description,
    'sv',
    nullIf(activity_description_en, ''),
    'source_field',
    toFloat32(1),
    [],
    'corpscout.se_companies.activity_description',
    CAST(NULL, 'Nullable(Date32)'),
    '',
    '',
    '',
    source_run_id,
    updated_from_raw_at
FROM (
    SELECT
        *,
        {bolagsverket_uid} AS source_record_uid
    FROM corpscout.se_companies_translated
)
WHERE ifNull(activity_description, '') != ''
  AND ifNull(bolagsverket_source_payload_hash, '') != ''""",
    )


def sweden_financial_source_record_sql() -> tuple[str, ...]:
    uid = clickhouse_structured_source_record_uid_sql(
        source_slug="sweden_financial",
        record_kind="annual_report_xhtml",
        source_record_key_expression="statement_key",
        payload_sha256_expression="statement_key",
    )
    source_rows = f"""
SELECT
    {uid} AS source_record_uid,
    source_record_id AS source_record_key,
    statement_key,
    company_id,
    xhtml_sha256 AS payload_sha256,
    source_archive_key,
    xhtml_object_key,
    source_run_id,
    resolved_at AS observed_at
FROM corpscout.se_financial_reports FINAL
WHERE statement_key != '' AND company_id != ''
""".strip()
    source_url = """concat(
        'https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/',
        extract(source_archive_key, 'year=([^/]+)'), '/',
        replaceRegexpOne(source_archive_key, '^.*/', '')
    )"""
    return (
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
({", ".join(tables.SOURCE_RECORD_COLUMNS)})
WITH source_rows AS ({source_rows}),
existing AS (
    SELECT source_record_uid, min(first_seen_at) AS first_seen_at
    FROM {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
    GROUP BY source_record_uid
)
SELECT source_rows.source_record_uid, 'structured', 'annual_report_xhtml',
       source_rows.payload_sha256, toUInt16(1),
       coalesce(existing.first_seen_at, source_rows.observed_at),
       source_rows.observed_at
FROM source_rows
LEFT JOIN existing USING (source_record_uid)""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE}
({", ".join(tables.SOURCE_RECORD_ORIGIN_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, 'sweden_financial', source_record_key,
       {source_url}, xhtml_object_key, payload_sha256, observed_at, source_run_id
FROM source_rows""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE}
({", ".join(tables.SOURCE_RECORD_LINK_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, 'SE', company_id, 'reported_entity',
       'xbrl_report_entity_identifier', toFloat32(1), 'se_orgnr', company_id,
       source_run_id, observed_at
FROM source_rows""",
    )


def finland_financial_source_record_sql() -> tuple[str, ...]:
    uid = clickhouse_file_source_record_uid_sql(
        record_kind="annual_report_xml",
        content_sha256_expression="xml_sha256",
    )
    source_rows = f"""
SELECT
    {uid} AS source_record_uid,
    statement_key AS source_record_key,
    business_id AS company_id,
    xml_sha256 AS payload_sha256,
    source_url,
    xml_object_key,
    source_run_id,
    resolved_at AS observed_at
FROM corpscout.fi_financial_statements FINAL
WHERE statement_key != '' AND business_id != '' AND xml_sha256 != ''
""".strip()
    return (
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
({", ".join(tables.SOURCE_RECORD_COLUMNS)})
WITH source_rows AS ({source_rows}),
existing AS (
    SELECT source_record_uid, min(first_seen_at) AS first_seen_at
    FROM {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
    GROUP BY source_record_uid
)
SELECT source_rows.source_record_uid, 'file', 'annual_report_xml',
       source_rows.payload_sha256, toUInt16(1),
       coalesce(existing.first_seen_at, source_rows.observed_at),
       source_rows.observed_at
FROM source_rows
LEFT JOIN existing USING (source_record_uid)""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE}
({", ".join(tables.SOURCE_RECORD_ORIGIN_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, 'finland_prh_xbrl', source_record_key, source_url,
       xml_object_key, payload_sha256, observed_at, source_run_id
FROM source_rows""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE}
({", ".join(tables.SOURCE_RECORD_LINK_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, 'FI', company_id, 'reported_entity',
       'xbrl_report_entity_identifier', toFloat32(1), 'fi_business_id', company_id,
       source_run_id, observed_at
FROM source_rows""",
    )


def esef_source_record_sql() -> tuple[str, ...]:
    uid = clickhouse_file_source_record_uid_sql(
        record_kind="esef_report_package",
        content_sha256_expression="filings.package_sha256",
    )
    source_rows = f"""
SELECT
    {uid} AS source_record_uid,
    filings.fxo_id AS source_record_key,
    lowerUTF8(filings.package_sha256) AS payload_sha256,
    filings.package_url AS package_url,
    concat(
        'esef_filings/report_packages/package_sha256=',
        lowerUTF8(filings.package_sha256),
        '/report-package.zip'
    ) AS package_object_key,
    upperUTF8(registry.country_iso2) AS country_iso2,
    registry.registry_id AS company_id,
    filings.lei AS lei,
    filings.source_run_id AS source_run_id,
    coalesce(parseDateTime64BestEffortOrNull(filings.processed_at), filings.resolved_at)
        AS observed_at
FROM corpscout.esef_filings AS filings FINAL
INNER JOIN corpscout.esef_entity_registry_map AS registry FINAL
    ON registry.lei = filings.lei
INNER JOIN (
    SELECT DISTINCT fxo_id FROM corpscout.esef_facts FINAL
) AS parsed_filings ON parsed_filings.fxo_id = filings.fxo_id
WHERE filings.package_sha256 != ''
  AND registry.country_iso2 != ''
  AND registry.registry_id != ''
""".strip()
    return (
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
({", ".join(tables.SOURCE_RECORD_COLUMNS)})
WITH source_rows AS ({source_rows}),
existing AS (
    SELECT source_record_uid, min(first_seen_at) AS first_seen_at
    FROM {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
    GROUP BY source_record_uid
)
SELECT source_rows.source_record_uid, 'file', 'esef_report_package',
       source_rows.payload_sha256, toUInt16(1),
       coalesce(existing.first_seen_at, source_rows.observed_at),
       source_rows.observed_at
FROM source_rows
LEFT JOIN existing USING (source_record_uid)""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE}
({", ".join(tables.SOURCE_RECORD_ORIGIN_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, 'esef_filings', source_record_key, package_url,
       package_object_key, payload_sha256, observed_at, source_run_id
FROM source_rows""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE}
({", ".join(tables.SOURCE_RECORD_LINK_COLUMNS)})
WITH source_rows AS ({source_rows})
SELECT source_record_uid, country_iso2, company_id, 'reported_entity',
       'verified_lei_registry_map', toFloat32(1), 'lei', lei,
       source_run_id, observed_at
FROM source_rows""",
    )


def _length_prefixed_clickhouse_row_hash(
    *, alias: str, columns: tuple[str, ...]
) -> str:
    values: list[str] = []
    for column in columns:
        value = f"coalesce(toString({alias}.{column}), '<null>')"
        values.extend((f"toString(length({value}))", "':'", value))
    return f"lower(hex(SHA256(concat({', '.join(values)}))))"


def _wikidata_company_snapshot_rows_sql() -> str:
    company_profile_hash = _length_prefixed_clickhouse_row_hash(
        alias="source",
        columns=(
            "wikidata_id",
            "wikidata_url",
            "name",
            "name_normalized",
            "company_description",
            "official_name",
            "headquarters_wikidata_id",
            "headquarters_label",
            "headquarters_country_wikidata_id",
            "headquarters_country_label",
            "headquarters_country_iso2",
            "country_resolution_method",
            "country_resolution_confidence",
            "inception_date",
            "legal_form_wikidata_id",
            "legal_form_label",
            "employee_count",
            "employee_count_point_in_time",
            "logo_image",
            "logo_image_url",
            "industry_wikidata_id",
            "industry_label",
            "has_current_listing",
            "listing_count",
        ),
    )
    child_tables = (
        (
            "wikidata_company_listings",
            "wikidata_id",
            (
                "wikidata_id",
                "listing_statement_id",
                "exchange_wikidata_id",
                "exchange_name",
                "ticker",
                "isin",
                "is_current",
            ),
        ),
        (
            "wikidata_company_identifiers",
            "wikidata_id",
            (
                "wikidata_id",
                "identifier_type",
                "wikidata_property_id",
                "identifier_value",
                "identifier_scope",
                "is_primary",
            ),
        ),
        (
            "wikidata_company_websites",
            "wikidata_id",
            (
                "wikidata_id",
                "website_url",
                "website_normalized_url",
                "website_host",
                "root_domain",
                "website_path",
                "website_kind",
                "confidence",
                "validation_status",
                "is_primary_candidate",
            ),
        ),
        (
            "wikidata_company_relationships",
            "subject_wikidata_id",
            (
                "subject_wikidata_id",
                "object_wikidata_id",
                "relationship_type",
                "wikidata_property_id",
                "relationship_statement_id",
                "object_name",
                "start_date",
                "end_date",
                "is_current",
            ),
        ),
        (
            "wikidata_company_people",
            "company_wikidata_id",
            (
                "company_wikidata_id",
                "person_wikidata_id",
                "role_property",
                "role_label",
                "start_date",
                "end_date",
                "is_current",
            ),
        ),
    )
    evidence_selects = [
        f"""SELECT source.wikidata_id, 'company_profile' AS record_kind,
       {company_profile_hash} AS row_payload_hash
FROM corpscout.wikidata_companies AS source FINAL"""
    ]
    evidence_selects.extend(
        f"""SELECT source.{company_key} AS wikidata_id, '{table_name}' AS record_kind,
       {_length_prefixed_clickhouse_row_hash(alias="source", columns=columns)} AS row_payload_hash
FROM corpscout.{table_name} AS source FINAL"""
        for table_name, company_key, columns in child_tables
    )
    evidence_rows = "\nUNION ALL\n".join(evidence_selects)
    source_record_uid = clickhouse_structured_source_record_uid_sql(
        source_slug="wikidata",
        record_kind="wikidata_company_item",
        source_record_key_expression="snapshots.wikidata_id",
        payload_sha256_expression="snapshots.source_payload_hash",
    )
    return f"""
WITH evidence_rows AS ({evidence_rows}),
snapshots AS (
    SELECT
        wikidata_id,
        lower(hex(SHA256(arrayStringConcat(
            arraySort(groupArray(concat(record_kind, ':', row_payload_hash))), '|'
        )))) AS source_payload_hash
    FROM evidence_rows
    WHERE wikidata_id != ''
    GROUP BY wikidata_id
)
SELECT
    {source_record_uid} AS source_record_uid,
    snapshots.wikidata_id,
    snapshots.source_payload_hash,
    companies.source_run_id,
    companies.retrieved_at,
    companies.wikidata_url,
    companies.company_description
FROM snapshots
INNER JOIN corpscout.wikidata_companies AS companies FINAL
    ON companies.wikidata_id = snapshots.wikidata_id
""".strip()


def wikidata_source_record_sql() -> tuple[str, ...]:
    snapshot_rows = _wikidata_company_snapshot_rows_sql()
    current_snapshot_rows = """
SELECT
    snapshots.source_record_uid,
    snapshots.wikidata_id,
    snapshots.source_payload_hash,
    snapshots.source_run_id,
    snapshots.retrieved_at,
    companies.wikidata_url,
    companies.company_description
FROM (
    SELECT *
    FROM corpscout.wikidata_company_source_snapshots FINAL
    ORDER BY retrieved_at DESC
    LIMIT 1 BY wikidata_id
) AS snapshots
INNER JOIN corpscout.wikidata_companies AS companies FINAL
    ON companies.wikidata_id = snapshots.wikidata_id
""".strip()
    person_uid = clickhouse_structured_source_record_uid_sql(
        source_slug="wikidata",
        record_kind="wikidata_person_item",
        source_record_key_expression="person_wikidata_id",
        payload_sha256_expression="source_payload_hash",
    )
    direct_links = """
SELECT
    ids.wikidata_id,
    'SE' AS country_code,
    companies.company_id,
    'wikidata_registry_identifier' AS match_method,
    toFloat32(1) AS match_confidence,
    'se_orgnr' AS matched_identifier_scheme,
    ids.identifier_value AS matched_identifier_value
FROM corpscout.wikidata_company_identifiers AS ids FINAL
INNER JOIN corpscout.se_companies AS companies FINAL
    ON companies.company_id = replaceRegexpAll(ids.identifier_value, '[^0-9]', '')
WHERE ids.identifier_type = 'se_orgnr'
UNION DISTINCT
SELECT
    ids.wikidata_id,
    company_ids.country_code,
    company_ids.company_id,
    'wikidata_verified_lei' AS match_method,
    toFloat32(1) AS match_confidence,
    'lei' AS matched_identifier_scheme,
    upperUTF8(ids.identifier_value) AS matched_identifier_value
FROM corpscout.wikidata_company_identifiers AS ids FINAL
INNER JOIN corpscout.company_identifier AS company_ids FINAL
    ON company_ids.issuer_scheme = 'lei'
   AND company_ids.issuer_id = upperUTF8(ids.identifier_value)
WHERE ids.identifier_type = 'lei'
""".strip()
    return (
        f"""INSERT INTO corpscout.wikidata_company_source_snapshots
(source_record_uid, wikidata_id, source_payload_hash, source_run_id, retrieved_at)
SELECT source_record_uid, wikidata_id, source_payload_hash, source_run_id, retrieved_at
FROM ({snapshot_rows})""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
({", ".join(tables.SOURCE_RECORD_COLUMNS)})
WITH source_rows AS (
    SELECT source_record_uid, source_payload_hash AS payload_sha256,
           retrieved_at AS observed_at, 'wikidata_company_item' AS record_kind
    FROM corpscout.wikidata_company_source_snapshots FINAL
    UNION ALL
    SELECT {person_uid}, source_payload_hash, retrieved_at, 'wikidata_person_item'
    FROM corpscout.wikidata_persons FINAL
    WHERE person_wikidata_id != '' AND source_payload_hash != ''
),
existing AS (
    SELECT source_record_uid, min(first_seen_at) AS first_seen_at
    FROM {tables.QUALIFIED_SOURCE_RECORDS_TABLE}
    GROUP BY source_record_uid
)
SELECT source_rows.source_record_uid, 'structured', source_rows.record_kind,
       source_rows.payload_sha256, toUInt16(1),
       coalesce(existing.first_seen_at, source_rows.observed_at),
       source_rows.observed_at
FROM source_rows
LEFT JOIN existing USING (source_record_uid)""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE}
({", ".join(tables.SOURCE_RECORD_ORIGIN_COLUMNS)})
SELECT source_record_uid, 'wikidata', wikidata_id, wikidata_url, '',
       source_payload_hash, retrieved_at, source_run_id
FROM ({current_snapshot_rows})
UNION ALL
SELECT {person_uid}, 'wikidata', person_wikidata_id, wikidata_url, '',
       source_payload_hash, retrieved_at, source_run_id
FROM corpscout.wikidata_persons FINAL
WHERE person_wikidata_id != '' AND source_payload_hash != ''""",
        f"""INSERT INTO {tables.QUALIFIED_SOURCE_RECORD_LINKS_TABLE}
({", ".join(tables.SOURCE_RECORD_LINK_COLUMNS)})
WITH company_links AS ({direct_links}),
snapshot_rows AS ({current_snapshot_rows})
SELECT snapshot_rows.source_record_uid, company_links.country_code,
       company_links.company_id, 'knowledge_graph_subject',
       company_links.match_method, company_links.match_confidence,
       company_links.matched_identifier_scheme,
       company_links.matched_identifier_value,
       snapshot_rows.source_run_id, snapshot_rows.retrieved_at
FROM company_links
INNER JOIN snapshot_rows
    ON snapshot_rows.wikidata_id = company_links.wikidata_id""",
        f"""INSERT INTO {tables.QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE}
({", ".join(tables.DESCRIPTION_OBSERVATION_COLUMNS)})
WITH company_links AS ({direct_links}),
snapshot_rows AS ({current_snapshot_rows})
SELECT
    {clickhouse_observation_uid_sql(source_record_uid_expression="snapshot_rows.source_record_uid", observation_kind="company_description", natural_key_expression="'company_description'")},
    snapshot_rows.source_record_uid,
    company_links.country_code,
    company_links.company_id,
    'knowledge_graph_description',
    snapshot_rows.company_description,
    'en',
    snapshot_rows.company_description,
    'source_field',
    toFloat32(0.85),
    [],
    'corpscout.wikidata_companies.company_description',
    toDate(snapshot_rows.retrieved_at),
    '',
    '',
    '',
    snapshot_rows.source_run_id,
    snapshot_rows.retrieved_at
FROM company_links
INNER JOIN snapshot_rows
    ON snapshot_rows.wikidata_id = company_links.wikidata_id
WHERE ifNull(snapshot_rows.company_description, '') != ''""",
    )
