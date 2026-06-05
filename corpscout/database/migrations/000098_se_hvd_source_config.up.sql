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
    'notes', 'Initial ingest stores raw HVD organization records in se_workflow.raw_records. The workflow resolves the configured metadata URL to the current HVD bulk file. Edit config.datasets to use a direct zip/json URL if Bolagsverket changes its download metadata or blocks automated discovery.'
  ),
  updated_at = now()
WHERE name = 'se';
