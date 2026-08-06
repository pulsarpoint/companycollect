CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.esef_source_documents
    MODIFY COLUMN source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(package_sha256)
    ))));

ALTER TABLE corpscout.esef_document_contact_candidates
    MODIFY COLUMN source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(package_sha256)
    ))));

ALTER TABLE corpscout.esef_document_company_information
    MODIFY COLUMN source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(package_sha256)
    ))));
