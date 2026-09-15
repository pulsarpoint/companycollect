DO $$ BEGIN RAISE EXCEPTION 'Brave archival is forward-only: restore from backups instead'; END $$;
