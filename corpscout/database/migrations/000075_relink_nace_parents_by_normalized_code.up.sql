UPDATE nace_codes child
SET parent_id = parent.id,
    updated_at = CASE
      WHEN child.parent_id IS DISTINCT FROM parent.id THEN now()
      ELSE child.updated_at
    END
FROM nace_codes parent
WHERE child.classification_id = parent.classification_id
  AND child.parent_code IS NOT NULL
  AND (
    child.parent_code = parent.code
    OR regexp_replace(upper(child.parent_code), '[^0-9A-Z]', '', 'g') = parent.normalized_code
  );
