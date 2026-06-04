UPDATE data_sources
SET config = jsonb_build_object(
  'api_url', 'https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip',
  'docs_url', 'https://avaandmed.ariregister.rik.ee/en/node/13',
  'protocol', 'Bulk JSON download (ZIP)',
  'page_size', NULL,
  'fields', jsonb_build_array(
    'registry_code',
    'name',
    'status',
    'legal_form',
    'address',
    'contacts',
    'activities',
    'annual_reports',
    'registry_cards'
  ),
  'auth_env', NULL,
  'dataset_key', 'general',
  'notes', 'Daily general-data JSON ZIP from the Estonian Business Register. No auth required for the downloadable open-data file.'
)
WHERE name = 'ariregister';
