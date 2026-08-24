{{ config(materialized='table', order_by=['country_code', 'company_id', 'root_domain']) }}

WITH companies AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),

existing AS (
    SELECT
        country_code AS country_code,
        domains.company_id AS company_id,
        root_domain AS root_domain,
        website_url,
        website_host,
        source_names,
        source_confidences,
        source_record_ids,
        source_urls,
        confidence_bases,
        suggested_confidence,
        suggested_primary,
        evidence_fingerprint,
        review_status,
        review_note,
        reviewed_by,
        reviewed_at,
        reviewed_evidence_fingerprint,
        is_active,
        first_seen_at,
        last_seen_at,
        resolved_at
    FROM {{ source('corpscout', 'company_domains') }} AS domains FINAL
    INNER JOIN companies
        ON companies.company_id = domains.company_id
    WHERE country_code = '{{ var("country_code") }}'
),

wikidata_sources AS (
    SELECT
        identifiers.country_code AS country_code,
        identifiers.company_id AS company_id,
        domains.domain AS root_domain,
        domains.website_url AS website_url,
        domains.website_host AS website_host,
        'wikidata' AS source_name,
        toFloat32(domains.confidence) AS source_confidence,
        domains.source_record_id AS source_record_id,
        wikidata.wikidata_url AS source_url,
        'official_website_claim' AS confidence_basis,
        domains.is_primary AS source_suggested_primary,
        domains.resolved_at AS observed_at
    FROM {{ ref('company_external_identifier_current_build') }} AS identifiers
    INNER JOIN companies
        ON companies.company_id = identifiers.company_id
    INNER JOIN {{ source('corpscout', 'wikidata_company_domains') }} AS domains FINAL
        ON domains.registry_id = identifiers.identifier_value
    INNER JOIN {{ ref('company_wikidata_current_build') }} AS wikidata
        ON wikidata.country_code = identifiers.country_code
       AND wikidata.company_id = identifiers.company_id
       AND wikidata.wikidata_id = identifiers.identifier_value
    WHERE identifiers.identifier_scheme = 'wikidata'
      AND domains.is_current = 1
      AND domains.domain != ''
),

esef_sources AS (
    SELECT
        candidates.country_iso2 AS country_code,
        candidates.company_id AS company_id,
        candidates.registrable_domain AS root_domain,
        concat('https://', candidates.registrable_domain) AS website_url,
        candidates.registrable_domain AS website_host,
        'esef_filing' AS source_name,
        toFloat32(multiIf(
            positionCaseInsensitiveUTF8(
                candidates.suggested_roles_json,
                'company_website'
            ) > 0,
            0.95,
            candidates.evidence_count >= 2,
            0.90,
            0.75
        )) AS source_confidence,
        candidates.source_document_id AS source_record_id,
        coalesce(
            nullIf(filings.viewer_url, ''),
            nullIf(filings.report_url, ''),
            nullIf(filings.package_url, ''),
            ''
        ) AS source_url,
        multiIf(
            positionCaseInsensitiveUTF8(
                candidates.suggested_roles_json,
                'company_website'
            ) > 0,
            'explicit_company_website',
            candidates.evidence_count >= 2,
            'repeated_filing_website',
            'filing_website_mention'
        ) AS confidence_basis,
        toUInt8(0) AS source_suggested_primary,
        candidates.resolved_at AS observed_at
    FROM {{ source('corpscout', 'esef_document_contact_candidates') }} AS candidates
    INNER JOIN companies
        ON companies.company_id = candidates.company_id
    LEFT ANY JOIN {{ source('corpscout', 'esef_filings') }} AS filings
        ON filings.fxo_id = candidates.source_document_id
    WHERE candidates.country_iso2 = '{{ var("country_code") }}'
      AND candidates.candidate_kind = 'website'
      AND candidates.company_id != ''
      AND candidates.registrable_domain != ''
),

common_crawl_evidence AS (
    SELECT
        evidence.country_iso2 AS country_code,
        evidence.company_id AS company_id,
        evidence.root_domain AS root_domain,
        evidence.discovery_run_id AS discovery_run_id,
        argMax(evidence.source_url, evidence.score_contribution) AS source_url
    FROM {{ source('corpscout', 'company_domain_suggestion_evidence_active') }} AS evidence
    GROUP BY
        evidence.country_iso2,
        evidence.company_id,
        evidence.root_domain,
        evidence.discovery_run_id
),

common_crawl_sources AS (
    SELECT
        suggestions.country_iso2 AS country_code,
        suggestions.company_id AS company_id,
        suggestions.root_domain AS root_domain,
        concat('https://', suggestions.root_domain) AS website_url,
        suggestions.root_domain AS website_host,
        'common_crawl_identity' AS source_name,
        toFloat32(least(greatest(suggestions.total_score / 100, 0), 1))
            AS source_confidence,
        concat(
            suggestions.discovery_run_id,
            ':',
            suggestions.company_id,
            ':',
            suggestions.root_domain
        ) AS source_record_id,
        ifNull(evidence.source_url, '') AS source_url,
        concat(
            suggestions.scoring_version,
            ':',
            arrayStringConcat(suggestions.candidate_sources, '+')
        ) AS confidence_basis,
        toUInt8(suggestions.rank = 1) AS source_suggested_primary,
        suggestions.suggested_at AS observed_at
    FROM {{ source('corpscout', 'company_domain_suggestions_active') }} AS suggestions
    INNER JOIN companies
        ON companies.company_id = suggestions.company_id
    LEFT JOIN common_crawl_evidence AS evidence
        ON evidence.country_code = suggestions.country_iso2
       AND evidence.company_id = suggestions.company_id
       AND evidence.root_domain = suggestions.root_domain
       AND evidence.discovery_run_id = suggestions.discovery_run_id
    WHERE suggestions.country_iso2 = '{{ var("country_code") }}'
),

source_rows AS (
    SELECT * FROM wikidata_sources
    UNION ALL
    SELECT * FROM esef_sources
    UNION ALL
    SELECT * FROM common_crawl_sources
),

grouped_sources AS (
    SELECT
        country_code,
        company_id,
        root_domain,
        argMax(
            website_url,
            tuple(source_confidence, observed_at)
        ) AS website_url,
        argMax(
            website_host,
            tuple(source_confidence, observed_at)
        ) AS website_host,
        arraySort(groupUniqArray(tuple(
            source_name,
            source_confidence,
            source_record_id,
            source_url,
            confidence_basis
        ))) AS source_evidence,
        toFloat32(max(source_confidence)) AS suggested_confidence,
        toUInt8(max(source_suggested_primary)) AS suggested_primary,
        min(observed_at) AS first_seen_at,
        max(observed_at) AS last_seen_at
    FROM source_rows
    GROUP BY
        country_code,
        company_id,
        root_domain
),

active_domains AS (
    SELECT
        grouped.country_code,
        grouped.company_id,
        grouped.root_domain,
        grouped.website_url,
        grouped.website_host,
        arrayMap(item -> item.1, grouped.source_evidence) AS source_names,
        arrayMap(item -> item.2, grouped.source_evidence) AS source_confidences,
        arrayMap(item -> item.3, grouped.source_evidence) AS source_record_ids,
        arrayMap(item -> item.4, grouped.source_evidence) AS source_urls,
        arrayMap(item -> item.5, grouped.source_evidence) AS confidence_bases,
        grouped.suggested_confidence,
        grouped.suggested_primary,
        lower(hex(SHA256(toString(grouped.source_evidence))))
            AS evidence_fingerprint,
        if(existing.root_domain != '', existing.review_status, 'unreviewed')
            AS review_status,
        if(existing.root_domain != '', existing.review_note, '') AS review_note,
        if(existing.root_domain != '', existing.reviewed_by, '') AS reviewed_by,
        if(existing.root_domain != '', existing.reviewed_at, NULL) AS reviewed_at,
        if(
            existing.root_domain != '',
            existing.reviewed_evidence_fingerprint,
            ''
        ) AS reviewed_evidence_fingerprint,
        toUInt8(1) AS is_active,
        if(
            existing.root_domain != '',
            least(existing.first_seen_at, grouped.first_seen_at),
            grouped.first_seen_at
        ) AS first_seen_at,
        if(
            existing.root_domain != '',
            greatest(existing.last_seen_at, grouped.last_seen_at),
            grouped.last_seen_at
        ) AS last_seen_at,
        now64(3, 'UTC') AS resolved_at
    FROM grouped_sources AS grouped
    LEFT JOIN existing
        ON existing.country_code = grouped.country_code
       AND existing.company_id = grouped.company_id
       AND existing.root_domain = grouped.root_domain
),

domains_without_current_source AS (
    SELECT
        existing.country_code,
        existing.company_id,
        existing.root_domain,
        existing.website_url,
        existing.website_host,
        existing.source_names,
        existing.source_confidences,
        existing.source_record_ids,
        existing.source_urls,
        existing.confidence_bases,
        existing.suggested_confidence,
        existing.suggested_primary,
        existing.evidence_fingerprint,
        existing.review_status,
        existing.review_note,
        existing.reviewed_by,
        existing.reviewed_at,
        existing.reviewed_evidence_fingerprint,
        toUInt8(existing.review_status IN ('confirmed_primary', 'confirmed_related'))
            AS is_active,
        existing.first_seen_at,
        existing.last_seen_at,
        now64(3, 'UTC') AS resolved_at
    FROM existing
    LEFT JOIN grouped_sources AS grouped
        ON grouped.country_code = existing.country_code
       AND grouped.company_id = existing.company_id
       AND grouped.root_domain = existing.root_domain
    WHERE grouped.root_domain = ''
)

SELECT * FROM active_domains
UNION ALL
SELECT * FROM domains_without_current_source
