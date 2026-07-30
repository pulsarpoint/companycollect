ALTER TABLE corpscout.br_pncp_contracts
    DROP COLUMN IF EXISTS buyer_municipality_ibge_code,
    DROP COLUMN IF EXISTS categoria_processo_name,
    DROP COLUMN IF EXISTS categoria_processo_id,
    DROP COLUMN IF EXISTS tipo_contrato_name,
    DROP COLUMN IF EXISTS tipo_contrato_id;
