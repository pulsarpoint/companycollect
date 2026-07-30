CREATE DATABASE IF NOT EXISTS corpscout;

-- Split PNCP's nested domain values out of their JSON blobs, and keep the IBGE
-- municipality code.
--
-- The ingest stored `json ->> '$.tipoContrato'`, which extracts the whole nested
-- object as text, so all 116,226 rows carry the literal
-- '{"id":1,"nome":"Contrato (termo inicial)"}'. Unusable for grouping or
-- filtering, and it reached the contract page verbatim through the register
-- view's agreement_type. Distribution of that column today: 50,892 Empenho,
-- 35,939 Contrato (termo inicial), 29,012 Outros, and five rarer values.
--
-- It survived because the normalise test fixture set tipoContrato to a plain
-- STRING while the live API sends the nested object, so the stored shape was
-- never exercised. The new tests use the live shape, and the parser falls back to
-- the whole value when it is a plain string so older snapshots still yield a name.
--
-- The raw columns stay exactly as they are (§7a, store every value as the source
-- wrote it): tipo_contrato and categoria_processo remain the field as published,
-- and the parsed pair is added beside them.
--
-- The ids are Nullable rather than defaulted to 0, because PNCP has no domain
-- value 0 -- an absent object must read as "not stated" and not as a code that
-- does not exist.
--
-- buyer_municipality_ibge_code is the field the analysis found genuinely lost:
-- unidadeOrgao.codigoIbge was never read, and it is the only value on the
-- contract endpoint that cannot be derived from what we already keep. It is
-- Brazil's standard geographic key, so without it every join to population, GDP
-- or regional data goes through fuzzy matching on the municipality NAME. String
-- rather than an integer: it is an identifier, not a quantity.
--
-- Values appear as each month is re-published. Nothing needs re-downloading --
-- the raw pages are in S3.

ALTER TABLE corpscout.br_pncp_contracts
    ADD COLUMN IF NOT EXISTS tipo_contrato_id Nullable(UInt16) AFTER tipo_contrato,
    ADD COLUMN IF NOT EXISTS tipo_contrato_name String AFTER tipo_contrato_id,
    ADD COLUMN IF NOT EXISTS categoria_processo_id Nullable(UInt16)
        AFTER categoria_processo,
    ADD COLUMN IF NOT EXISTS categoria_processo_name String
        AFTER categoria_processo_id,
    ADD COLUMN IF NOT EXISTS buyer_municipality_ibge_code String
        AFTER buyer_municipality;
