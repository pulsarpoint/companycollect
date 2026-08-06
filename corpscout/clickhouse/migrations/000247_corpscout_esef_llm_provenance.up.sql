CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.esef_document_company_information
    ADD COLUMN IF NOT EXISTS llm_request_object_key String
        AFTER input_artifact_object_key,
    ADD COLUMN IF NOT EXISTS llm_request_sha256 String
        AFTER llm_request_object_key,
    ADD COLUMN IF NOT EXISTS llm_response_text String
        AFTER llm_request_sha256,
    ADD COLUMN IF NOT EXISTS llm_response_sha256 String
        AFTER llm_response_text;
