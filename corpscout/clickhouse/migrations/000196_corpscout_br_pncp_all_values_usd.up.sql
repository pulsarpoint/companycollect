CREATE DATABASE IF NOT EXISTS corpscout;

-- Every value PNCP publishes gets a USD counterpart, not just the one the view
-- happens to read today.
--
-- br_pncp_contracts already stores all four native figures -- valor_inicial,
-- valor_parcela, valor_global, valor_acumulado -- because the register
-- publishes four and choosing a subset at ingest is the loss the whole design
-- exists to avoid. Converting only valor_global reintroduced exactly that loss
-- one layer down: the other three were unusable in any cross-country context,
-- so "which value is the contract value" became answerable only in BRL.
--
-- fi_hilma_notices is the shape being matched here: four value fields, each
-- with its own _original and _usd. One rate per contract still applies to all
-- four, so fx_rate_to_usd / fx_rate_date / fx_source stay single columns
-- rather than being repeated per figure.
--
-- Which figure a reader is shown is a presentation decision, made in the view
-- and the UI where it can be labelled. It is not a decision the pipeline is
-- allowed to make by discarding the alternatives.

ALTER TABLE corpscout.br_pncp_contracts
    ADD COLUMN IF NOT EXISTS valor_inicial_usd Nullable(Decimal(38, 2)) AFTER valor_global_usd,
    ADD COLUMN IF NOT EXISTS valor_parcela_usd Nullable(Decimal(38, 2)) AFTER valor_inicial_usd,
    ADD COLUMN IF NOT EXISTS valor_acumulado_usd Nullable(Decimal(38, 2)) AFTER valor_parcela_usd;
