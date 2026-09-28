DROP VIEW IF EXISTS corpscout.se_company_domain_sources_resolved;
-- Keep identity columns and evidence when rolling back an application release.
SELECT throwIf(1, 'Website result references are forward-only. Keep publishers paused or use compatible writers without dropping evidence');
