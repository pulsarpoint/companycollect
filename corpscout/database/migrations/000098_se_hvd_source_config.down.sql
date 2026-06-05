UPDATE data_sources
SET
  config = (config - 'metadata_url' - 'file_download_url') || jsonb_build_object(
    'protocol', 'Bolagsverket/SCB HVD file download',
    'datasets_env', 'SE_HVD_DATASETS_JSON',
    'datasets', jsonb_build_array(
      jsonb_build_object('key', 'organisationer', 'format', 'json'),
      jsonb_build_object('key', 'arsredovisningar', 'format', 'json')
    ),
    'notes', 'Initial ingest stores raw HVD organization records in se_workflow.raw_records. Source file URLs are supplied through SE_HVD_DATASETS_JSON so portal-published HVD download URLs can be changed without a migration.'
  ),
  updated_at = now()
WHERE name = 'se';
