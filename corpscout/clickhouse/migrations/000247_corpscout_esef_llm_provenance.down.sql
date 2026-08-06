CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.esef_document_company_information
    DROP COLUMN IF EXISTS llm_response_sha256,
    DROP COLUMN IF EXISTS llm_response_text,
    DROP COLUMN IF EXISTS llm_request_sha256,
    DROP COLUMN IF EXISTS llm_request_object_key;
