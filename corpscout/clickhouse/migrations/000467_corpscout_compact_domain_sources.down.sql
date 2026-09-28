-- Never discard the copied index or revert readers to retired company columns.
SELECT throwIf(1, 'Compact domain sources are forward-only; retain both index layouts until coordinated cutover is verified');
