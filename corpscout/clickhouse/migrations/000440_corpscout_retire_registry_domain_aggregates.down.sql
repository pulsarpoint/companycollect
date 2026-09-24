-- These aggregates and their producer were explicitly retired.
SELECT throwIf(1, 'Migration 440 is forward-only: registry domain aggregates were retired');
