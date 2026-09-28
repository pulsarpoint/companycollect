-- Readers join canonical domains using domain_id.
SELECT throwIf(1, 'Domain source reader cutover is forward-only');
