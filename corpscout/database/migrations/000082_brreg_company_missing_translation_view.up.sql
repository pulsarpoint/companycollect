CREATE INDEX IF NOT EXISTS idx_brreg_source_companies_missing_translation_fields
  ON brreg_source.companies(updated_at DESC, id)
  WHERE row_status = 'active'
    AND (
      (nullif(btrim(short_description), '') IS NOT NULL AND nullif(btrim(short_description_en), '') IS NULL)
      OR (nullif(btrim(description), '') IS NOT NULL AND nullif(btrim(description_en), '') IS NULL)
      OR (nullif(btrim(registration_status_label), '') IS NOT NULL AND nullif(btrim(registration_status_label_en), '') IS NULL)
      OR (nullif(btrim(organization_form_label), '') IS NOT NULL AND nullif(btrim(organization_form_label_en), '') IS NULL)
      OR (nullif(btrim(response_class), '') IS NOT NULL AND nullif(btrim(response_class_en), '') IS NULL)
      OR (nullif(btrim(activity_description), '') IS NOT NULL AND nullif(btrim(activity_description_en), '') IS NULL)
      OR (nullif(btrim(statutory_purpose), '') IS NOT NULL AND nullif(btrim(statutory_purpose_en), '') IS NULL)
    );

CREATE INDEX IF NOT EXISTS idx_brreg_source_addresses_missing_country_en
  ON brreg_source.addresses(company_id)
  WHERE nullif(btrim(country), '') IS NOT NULL
    AND nullif(btrim(country_en), '') IS NULL;

CREATE INDEX IF NOT EXISTS idx_brreg_source_industries_missing_source_label_en
  ON brreg_source.industries(company_id)
  WHERE nullif(btrim(source_label), '') IS NOT NULL
    AND nullif(btrim(source_label_en), '') IS NULL;

CREATE INDEX IF NOT EXISTS idx_brreg_source_websites_missing_translation_fields
  ON brreg_source.websites(company_id)
  WHERE (
    (nullif(btrim(title), '') IS NOT NULL AND nullif(btrim(title_en), '') IS NULL)
    OR (nullif(btrim(description), '') IS NOT NULL AND nullif(btrim(description_en), '') IS NULL)
  );

CREATE INDEX IF NOT EXISTS idx_brreg_source_contacts_missing_label_en
  ON brreg_source.contacts(company_id)
  WHERE nullif(btrim(label), '') IS NOT NULL
    AND nullif(btrim(label_en), '') IS NULL;

CREATE INDEX IF NOT EXISTS idx_brreg_source_capital_missing_capital_type_en
  ON brreg_source.capital(company_id)
  WHERE nullif(btrim(capital_type), '') IS NOT NULL
    AND nullif(btrim(capital_type_en), '') IS NULL;

CREATE INDEX IF NOT EXISTS idx_brreg_source_roles_missing_translation_fields
  ON brreg_source.roles(company_id)
  WHERE (
    (nullif(btrim(role_label), '') IS NOT NULL AND nullif(btrim(role_label_en), '') IS NULL)
    OR (nullif(btrim(role_group), '') IS NOT NULL AND nullif(btrim(role_group_en), '') IS NULL)
  );

CREATE OR REPLACE VIEW brreg_source.v_companies_missing_translations AS
SELECT
  missing.company_id,
  count(*)::bigint AS missing_field_count,
  count(DISTINCT lower(btrim(missing.source_text)))::bigint AS missing_term_count,
  coalesce(sum(length(missing.source_text)), 0)::bigint AS estimated_chars,
  min(missing.priority)::integer AS min_priority,
  max(company.updated_at) AS latest_source_updated_at
FROM brreg_source.v_missing_translations missing
JOIN brreg_source.companies company ON company.id = missing.company_id
WHERE company.row_status = 'active'
GROUP BY missing.company_id;
