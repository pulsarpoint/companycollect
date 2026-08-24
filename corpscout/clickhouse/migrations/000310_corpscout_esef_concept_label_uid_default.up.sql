CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.esef_document_concept_labels_v2
    MODIFY COLUMN source_record_uid FixedString(64)
    DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(toString(package_sha256))
    ))));
