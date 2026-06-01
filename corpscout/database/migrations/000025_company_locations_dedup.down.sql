DROP INDEX IF EXISTS uq_company_locations_active_per_source;

CREATE UNIQUE INDEX uq_company_locations_active_headquarters
    ON company_locations (company_id, location_type)
    WHERE removed_at IS NULL
      AND location_type = 'headquarters';
