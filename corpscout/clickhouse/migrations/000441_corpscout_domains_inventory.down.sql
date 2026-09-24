-- Keep the requested domains inventory name.
SELECT throwIf(1, 'Migration 441 is forward-only: domain_inventory was replaced by domains');
