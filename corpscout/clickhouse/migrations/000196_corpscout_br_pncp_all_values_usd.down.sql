ALTER TABLE corpscout.br_pncp_contracts
    DROP COLUMN IF EXISTS valor_acumulado_usd,
    DROP COLUMN IF EXISTS valor_parcela_usd,
    DROP COLUMN IF EXISTS valor_inicial_usd;
