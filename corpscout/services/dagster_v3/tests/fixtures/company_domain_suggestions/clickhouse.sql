CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE corpscout.se_companies
(
    company_id String,
    legal_name Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY company_id;

CREATE TABLE corpscout.se_financial_report_signatories
(
    company_id String,
    first_name String,
    last_name String,
    role_kind String
)
ENGINE = MergeTree
ORDER BY (company_id, first_name, last_name);

CREATE TABLE corpscout.se_industries
(
    company_id String,
    nace_rev2_class_code String
)
ENGINE = ReplacingMergeTree
ORDER BY (company_id, nace_rev2_class_code);

CREATE TABLE corpscout.gleif_lei_records
(
    registered_as Nullable(String),
    lei String,
    primary_country_iso2 Nullable(String),
    jurisdiction Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY lei;

CREATE TABLE corpscout.commoncrawl_page_jsonld
(
    root_domain String,
    country String,
    crawl_id String,
    page_url String,
    resolved_at DateTime64(3, 'UTC'),
    is_organization UInt8
)
ENGINE = MergeTree
ORDER BY (root_domain, crawl_id, page_url);

CREATE TABLE corpscout.commoncrawl_industries
(
    root_domain String,
    nace_code String,
    crawl_id String,
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (root_domain, nace_code, crawl_id);

CREATE TABLE corpscout.commoncrawl_domain_identifiers
(
    root_domain String,
    id_value String,
    id_type String,
    crawl_id String,
    source_url String,
    resolved_at DateTime64(3, 'UTC'),
    valid UInt8
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, id_type, id_value, crawl_id);

INSERT INTO corpscout.se_companies VALUES
    ('5590000000', 'Acme Security AB'),
    ('5590000001', 'Other Company AB'),
    ('5590000002', 'Bright Future Consulting AB'),
    ('5590000003', 'Nordic Global Services AB'),
    ('5590000004', 'Blue River Technology AB'),
    ('5590000005', 'Northern Security Solutions AB');

INSERT INTO corpscout.se_financial_report_signatories VALUES
    ('5590000000', 'Alice', 'Distinctive', 'board');

INSERT INTO corpscout.se_industries VALUES
    ('5590000000', '6201'),
    ('5590000001', '6201');

INSERT INTO corpscout.web_domain_identity_features VALUES
    (
        'domain_label', 'acmesecurity', 'acmesecurity.se', 'acmesecurity',
        'root_domain_label', 'https://acmesecurity.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000001', 'acmesecurity.se', 'SE559000000001',
        'vat', 'https://acmesecurity.se/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'domain_label', 'othercompany', 'othercompany.se', 'othercompany',
        'root_domain_label', 'https://othercompany.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'domain_label', 'bright', 'bright.se', 'bright',
        'root_domain_label', 'https://bright.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'domain_label', 'nordic', 'nordic.se', 'nordic',
        'root_domain_label', 'https://nordic.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000301', 'nordic.se', 'SE559000000301',
        'vat', 'https://nordic.se/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000301', 'nordic-services.se', 'SE559000000301',
        'vat', 'https://nordic-services.se/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'domain_label', 'brt', 'brt.se', 'brt',
        'root_domain_label', 'https://brt.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'domain_label', 'nss', 'nss.se', 'nss',
        'root_domain_label', 'https://nss.se', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000501', 'nss.se', 'SE559000000501',
        'vat', 'https://nss.se/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', '5493001kjtiigc8y1r17', 'brightfuture.se', '5493001KJTIIGC8Y1R17',
        'lei', 'https://brightfuture.se/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', '5590000004', 'blue.example', '5590000004',
        'registration_number', 'https://blue.example/about', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000001', 'registry.example', 'SE559000000001',
        'vat', 'https://registry.example/5590000000', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000101', 'registry.example', 'SE559000000101',
        'vat', 'https://registry.example/5590000001', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000201', 'registry.example', 'SE559000000201',
        'vat', 'https://registry.example/5590000002', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000301', 'registry.example', 'SE559000000301',
        'vat', 'https://registry.example/5590000003', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000401', 'registry.example', 'SE559000000401',
        'vat', 'https://registry.example/5590000004', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    ),
    (
        'identifier', 'se559000000501', 'registry.example', 'SE559000000501',
        'vat', 'https://registry.example/5590000005', 'CC-MAIN-2026-30',
        '2026-08-01 00:00:00.000', '2026-08-09 00:00:00.000'
    );

INSERT INTO corpscout.gleif_lei_records VALUES
    ('5590000002', '5493001KJTIIGC8Y1R17', 'SE', 'SE');

INSERT INTO corpscout.commoncrawl_page_jsonld VALUES
    (
        'acmesecurity.se', 'SE', 'CC-MAIN-2026-30', 'https://acmesecurity.se',
        '2026-08-01 00:00:00.000', 1
    ),
    (
        'othercompany.se', 'SE', 'CC-MAIN-2026-30', 'https://othercompany.se',
        '2026-08-01 00:00:00.000', 1
    );

INSERT INTO corpscout.commoncrawl_industries VALUES
    ('acmesecurity.se', '6201', 'CC-MAIN-2026-30', 'https://acmesecurity.se');

INSERT INTO corpscout.commoncrawl_domain_identifiers VALUES
    (
        'acmesecurity.se', 'SE559000000001', 'vat', 'CC-MAIN-2026-30',
        'https://acmesecurity.se/about', '2026-08-01 00:00:00.000', 1
    ),
    (
        'othercompany.se', 'SE556999999901', 'vat', 'CC-MAIN-2026-30',
        'https://othercompany.se/about', '2026-08-01 00:00:00.000', 1
    ),
    (
        'nordic.se', 'SE559000000301', 'vat', 'CC-MAIN-2026-30',
        'https://nordic.se/about', '2026-08-01 00:00:00.000', 1
    ),
    (
        'nss.se', 'SE559000000501', 'vat', 'CC-MAIN-2026-30',
        'https://nss.se/about', '2026-08-01 00:00:00.000', 1
    );

INSERT INTO corpscout.company_domain_suggestions VALUES
(
    'SE', '5590000000', 'acmesecurity.se', 1, 'Acme Security AB',
    ['domain_label', 'identifier'], 70, 0, 35, 0, 10, 5, 5, 0, 100,
    'se-domain-suggestions-v1', 'legacy-run', '2026-08-09 00:00:00.000'
);
