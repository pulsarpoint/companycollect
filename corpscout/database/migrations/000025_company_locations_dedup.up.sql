-- Remove duplicate location rows, keeping the oldest per (company_id, location_type, source).
DELETE FROM company_locations
WHERE id NOT IN (
    SELECT DISTINCT ON (company_id, location_type, source) id
    FROM company_locations
    ORDER BY company_id, location_type, source, created_at ASC
);

-- The old constraint only covered headquarters. Drop it and replace with a
-- broader partial unique index that covers both headquarters and registered_address,
-- keyed on (company_id, location_type, source) so different sources may each
-- provide one entry of each type for the same company.
DROP INDEX uq_company_locations_active_headquarters;

CREATE UNIQUE INDEX uq_company_locations_active_per_source
    ON company_locations (company_id, location_type, source)
    WHERE removed_at IS NULL
      AND location_type IN ('headquarters', 'registered_address');
