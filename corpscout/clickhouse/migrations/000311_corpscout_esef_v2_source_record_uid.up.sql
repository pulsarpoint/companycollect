CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.esef_source_documents_v2
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(toString(package_sha256))
    )))) AFTER source_document_id;

ALTER TABLE corpscout.esef_document_contact_candidates_v2
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(toString(package_sha256))
    )))) AFTER source_document_id;
