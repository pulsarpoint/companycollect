UPDATE data_sources
SET
  config = config || jsonb_build_object(
    'metadata_url', 'https://metadata.bolagsverket.se/store/2/resource/42',
    'file_download_url', 'https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip',
    'protocol', 'Bolagsverket/SCB HVD file download',
    'datasets', jsonb_build_array(
      jsonb_build_object(
        'dataset', 'bolagsverket',
        'label', 'Bolagsverket bulk file',
        'url', 'https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip',
        'format', 'zip',
        'encoding', 'utf-8',
        'delimiter', ';',
        'target_table', 'se_workflow.bolagsverket_raw_records'
      ),
      jsonb_build_object(
        'dataset', 'scb',
        'label', 'SCB bulk file',
        'url', 'https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip',
        'format', 'zip',
        'encoding', 'iso-8859-1',
        'delimiter', '\t',
        'target_table', 'se_workflow.scb_raw_records'
      )
    ),
    'notes', 'Sweden HVD ingest uses the direct Bolagsverket and SCB ZIP files. The metadata landing page is not used for workflow downloads because it can return anti-bot HTML instead of file links.'
  ),
  updated_at = now()
WHERE name = 'se';
