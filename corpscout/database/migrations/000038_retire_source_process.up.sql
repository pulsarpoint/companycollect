-- Source ingestion stops at raw input import/translation. Raw input approval is
-- handled directly from review screens, not by the legacy source_process stage.
UPDATE data_sources
SET processor_task_type = NULL,
    updated_at = now()
WHERE processor_task_type = 'source_process';

