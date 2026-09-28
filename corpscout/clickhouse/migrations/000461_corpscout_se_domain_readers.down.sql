-- Deployed readers use this projection. Change it with a forward migration.
SELECT throwIf(1, 'Reader cutover is forward-only');
