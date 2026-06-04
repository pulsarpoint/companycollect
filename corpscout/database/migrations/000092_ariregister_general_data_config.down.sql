UPDATE data_sources
SET config = jsonb_build_object(
  'api_url', 'https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip',
  'docs_url', 'https://avaandmed.ariregister.rik.ee',
  'protocol', 'Bulk CSV download (ZIP)',
  'page_size', NULL,
  'fields', jsonb_build_array('name', 'country', 'registration_number', 'status'),
  'auth_env', NULL,
  'notes', 'Daily basic open-data ZIP from the Estonian Business Register. No auth required.'
)
WHERE name = 'ariregister';
