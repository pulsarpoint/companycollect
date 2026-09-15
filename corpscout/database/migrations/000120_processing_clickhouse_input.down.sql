-- Input payloads cannot be reconstructed from progress records. Do not silently roll
-- back into an implementation that assumes PostgreSQL contains every input row.
DO $$ BEGIN
    RAISE EXCEPTION 'Input storage migration is forward-only; restore a pre-migration backup to roll back';
END $$;
