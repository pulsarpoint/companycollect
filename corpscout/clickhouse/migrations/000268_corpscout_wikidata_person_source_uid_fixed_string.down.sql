CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.wikidata_persons
    MODIFY COLUMN source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nwikidata\nwikidata_person_item\n',
        person_wikidata_id, '\n', lowerUTF8(source_payload_hash)
    ))));
