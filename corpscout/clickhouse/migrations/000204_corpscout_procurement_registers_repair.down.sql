-- The inverse, which costs nothing here: procurement_registers is rebuilt in
-- full by procurement_registers_clickhouse on every materialization, so the
-- column's contents are derived rather than accumulated and a rollback loses
-- no history.
ALTER TABLE corpscout.procurement_registers
    DROP COLUMN IF EXISTS retrieval_method;
