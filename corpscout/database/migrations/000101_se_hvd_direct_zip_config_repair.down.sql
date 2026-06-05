UPDATE data_sources
SET
  config = config || jsonb_build_object(
    'metadata_url', 'https://metadata.bolagsverket.se/store/2/resource/42',
    'file_download_url', 'https://bolagsverket.se/apierochoppnadata/nedladdningsbarafiler.2517.html',
    'protocol', 'Bolagsverket/SCB HVD file download',
    'datasets', jsonb_build_array(
      jsonb_build_object(
        'dataset', 'organisationer',
        'label', 'Organisationer',
        'url', 'https://metadata.bolagsverket.se/store/2/resource/42',
        'format', 'metadata',
        'distribution_format', 'application/zip'
      )
    ),
    'notes', 'Legacy Sweden HVD metadata discovery config.'
  ),
  updated_at = now()
WHERE name = 'se';
